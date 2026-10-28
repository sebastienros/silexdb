using System.Buffers;
using System.Buffers.Binary;
using Silex.Blocks;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class TransactionalOrderedReadTests
{
    private static StorageOptions Options() => new()
    {
        UseWriteAheadLog = false,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
        BlockSize = 128,
    };

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OverlayOrderingDeduplicationBoundsAndResumes(bool sst)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        byte[][] keys = [[], [0], [0, 0], [0, 1], [1], [1, 0], [1, 0, 1], [2], [255]];
        for (var i = 0; i < keys.Length; i += 2)
        {
            db.Put(keys[i], [(byte)i]);
        }
        db.Delete(keys[6]);
        if (sst) await db.FlushAndCompactAsync();
        using var transaction = db.BeginTransaction();
        for (var i = keys.Length - 1; i >= 0; i--)
        {
            if (i == 2 || i == 7) transaction.Delete(keys[i]);
            else transaction.Put(keys[i], [(byte)(20 + i)]);
        }
        // A final mutation wins, without introducing a second logical entry.
        transaction.Delete(keys[4]);
        transaction.Put(keys[4], [24]);
        foreach (var reverse in new[] { false, true })
        foreach (var lowerInclusive in new[] { false, true })
        foreach (var upperInclusive in new[] { false, true })
        foreach (var lower in Enumerable.Range(-1, keys.Length + 1))
        foreach (var upper in Enumerable.Range(-1, keys.Length + 1))
        {
            using var cursor = transaction.CreateOrderedReadCursor(new OrderedReadRange(
                lower < 0 ? (ReadOnlyMemory<byte>?)null : keys[lower], lowerInclusive,
                upper < 0 ? (ReadOnlyMemory<byte>?)null : keys[upper], upperInclusive),
                reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
            var actual = new List<int>();
            while (true)
            {
                using var batch = await cursor.ReadBatchAsync(2, 7);
                await Assert.That(batch.Count <= 2 && batch.PayloadBytes <= 7).IsTrue();
                var bytes = 0;
                for (var i = 0; i < batch.Count; i++)
                {
                    var entry = batch[i];
                    var id = entry.Value.Span[0] - 20;
                    await Assert.That(entry.Key.ToArray()).IsEquivalentTo(keys[id], CollectionOrdering.Matching);
                    bytes += entry.Key.Length + entry.Value.Length;
                    actual.Add(id);
                }
                await Assert.That(batch.PayloadBytes).IsEqualTo(bytes);
                if (batch.IsExhausted) break;
                await Assert.That(batch.Count).IsGreaterThan(0);
            }
            var expected = Enumerable.Range(0, keys.Length).Where(i => i != 2 && i != 7 &&
                (lower < 0 || i > lower || (lowerInclusive && i == lower)) &&
                (upper < 0 || i < upper || (upperInclusive && i == upper)));
            if (reverse) expected = expected.Reverse();
            await Assert.That(actual).IsEquivalentTo(expected, CollectionOrdering.Matching);
        }
    }

    [Test]
    [Arguments(false, "dispose")]
    [Arguments(true, "dispose")]
    [Arguments(false, "commit")]
    [Arguments(true, "commit")]
    [Arguments(false, "conflict")]
    [Arguments(true, "conflict")]
    [Arguments(false, "cancel")]
    [Arguments(true, "cancel")]
    public async Task ExactStartingSnapshotAndFrozenWritesSurviveCompletionAndGc(bool reverse, string completion)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put([1], [1]);
        db.Put([3], [3]);
        db.Put([5], [5]);
        using var transaction = db.BeginTransaction();
        var sequence = transaction.Sequence;
        db.Put([1], [99]);
        db.Put([4], [99]);
        transaction.Put([2], [2]);
        transaction.Put([3], [33]);
        transaction.Delete([5]);
        using var cursor = transaction.CreateOrderedReadCursor(direction:
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        await Assert.That(cursor.Sequence).IsEqualTo(sequence);
        await Assert.That(cursor.Consistency).IsEqualTo(OrderedReadConsistency.Snapshot);
        using var first = await cursor.ReadBatchAsync(1, 2);
        // Replace in-place, grow buffers, delete and insert on both sides of the continuation.
        transaction.Put([3], [88]);
        transaction.Delete([2]);
        transaction.Put([0], new byte[10000]);
        transaction.Put([6], [6]);
        var point = new byte[1];
        await Assert.That(await transaction.GetRawAsync(new byte[] { 3 }, point)).IsEqualTo(1);
        await Assert.That(point[0]).IsEqualTo((byte)88);
        using (var later = transaction.CreateOrderedReadCursor(new OrderedReadRange(new byte[] { 3 }, true, new byte[] { 3 })))
        using (var batch = await later.ReadBatchAsync(2, 10))
        {
            await Assert.That(batch[0].Value.Span[0]).IsEqualTo((byte)88);
        }
        if (completion == "commit") await transaction.CommitAsync();
        else if (completion == "conflict")
        {
            db.Put([3], [99]);
            await Assert.That(await transaction.TryCommitAsync()).IsFalse();
        }
        else if (completion == "cancel")
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.That(async () => await transaction.TryCommitAsync(cts.Token)).Throws<OperationCanceledException>();
        }
        else transaction.Dispose();
        await Assert.That(() => transaction.CreateOrderedReadCursor()).Throws<ObjectDisposedException>();
        for (byte key = 0; key <= 6; key++) db.Delete([key]);
        await db.CollectGarbageAsync(compact: true);
        var values = new List<byte> { first[0].Value.Span[0] };
        while (true)
        {
            using var batch = await cursor.ReadBatchAsync(1, 2);
            for (var i = 0; i < batch.Count; i++) values.Add(batch[i].Value.Span[0]);
            await db.GetRawAsync(new byte[] { 1 }, point);
            if (batch.IsExhausted) break;
        }
        byte[] expected = reverse ? [33, 2, 1] : [1, 2, 33];
        await Assert.That(values).IsEquivalentTo(expected, CollectionOrdering.Matching);
        cursor.Dispose();
        await Assert.That(await db.CollectGarbageAsync()).IsGreaterThan(0);
        await db.DisposeAsync();
        await Assert.That(first[0].Value.Span[0]).IsEqualTo(expected[0]);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MixedBaseAndOverlayByteThresholdsAndRetry(bool reverse)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put([1], [1]);
        db.Put([2], new byte[100]);
        db.Put([3], [3]);
        using var transaction = db.BeginTransaction();
        transaction.Put([2], new byte[9]);
        transaction.Delete([4]);
        using var cursor = transaction.CreateOrderedReadCursor(direction:
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        using var first = await cursor.ReadBatchAsync(10, 9);
        await Assert.That(first.Count).IsEqualTo(1);
        await Assert.That(first.PayloadBytes).IsEqualTo(2);
        await Assert.That(first.IsExhausted).IsFalse();
        await Assert.That(async () => await cursor.ReadBatchAsync(10, 9)).Throws<ArgumentException>();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.That(async () => await cursor.ReadBatchAsync(10, 10, cts.Token)).Throws<OperationCanceledException>();
        using var retry = await cursor.ReadBatchAsync(10, 10);
        await Assert.That(retry.Count).IsEqualTo(1);
        await Assert.That(retry.PayloadBytes).IsEqualTo(10);
        await Assert.That(retry[0].Key.Span[0]).IsEqualTo((byte)2);
        using var last = await cursor.ReadBatchAsync(10, 10);
        await Assert.That(last.Count).IsEqualTo(1);
        await Assert.That(last[0].Key.Span[0]).IsEqualTo((byte)(reverse ? 1 : 3));
        await Assert.That(last.IsExhausted).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EmptyBaseOverlayOnlyAndZeroPayloadEntries(bool reverse)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        using var transaction = db.BeginTransaction();
        transaction.Put([], []);
        transaction.Put([1], []);
        transaction.Delete([2]);
        using var cursor = transaction.CreateOrderedReadCursor(direction:
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        var keys = new List<byte[]>();
        while (true)
        {
            using var batch = await cursor.ReadBatchAsync(1, 1);
            for (var i = 0; i < batch.Count; i++) keys.Add(batch[i].Key.ToArray());
            if (batch.IsExhausted) break;
        }
        await Assert.That(keys.Count).IsEqualTo(2);
        await Assert.That(keys[reverse ? 1 : 0].Length).IsEqualTo(0);
        await Assert.That(keys[reverse ? 0 : 1]).IsEquivalentTo(new byte[] { 1 });
        using var empty = transaction.CreateOrderedReadCursor(new OrderedReadRange(new byte[] { 1 }, false, new byte[] { 1 }));
        using var emptyBatch = await empty.ReadBatchAsync(1, 1);
        await Assert.That(emptyBatch.Count).IsEqualTo(0);
        await Assert.That(emptyBatch.IsExhausted).IsTrue();
        await Assert.That(() => transaction.CreateOrderedReadCursor(direction: (OrderedReadDirection)99)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(async () => await cursor.ReadBatchAsync(1, 0)).Throws<ArgumentOutOfRangeException>();
        await db.DisposeAsync();
        await Assert.That(async () => await empty.ReadBatchAsync(1, 1)).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task RangeReadsDoNotTrackDependenciesButExplicitPointReadsStillDo()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put([1], [1]);
        using var transaction = db.BeginTransaction();
        using var cursor = transaction.CreateOrderedReadCursor();
        using var batch = await cursor.ReadBatchAsync(10, 100);
        db.Put([1], [99]);
        db.Put([2], [2]);
        transaction.Put([3], [3]);
        await Assert.That(await transaction.TryCommitAsync()).IsTrue();
        using var tracked = db.BeginTransaction();
        using var trackedCursor = tracked.CreateOrderedReadCursor();
        using var trackedBatch = await trackedCursor.ReadBatchAsync(10, 100);
        await tracked.GetForUpdateRawAsync(new byte[] { 1 }, new byte[1]);
        db.Put([1], [88]);
        tracked.Put([4], [4]);
        await Assert.That(await tracked.TryCommitAsync()).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InternalCancellationAndReadFaultRetryPreserveFrozenOverlay(bool reverse)
    {
        using var folder = TempFolder.Create();
        var options = Options();
        var encoder = new TestEncoder();
        options.BlockEncoderFactory = new TestFactory(encoder);
        await using var db = await MvccStorage.OpenAsync(folder, options);
        db.Put([2], [42]);
        using var transaction = db.BeginTransaction();
        transaction.Put([1], [1]);
        transaction.Put([3], [3]);
        using var cursor = transaction.CreateOrderedReadCursor(direction:
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        for (var i = 0; i < 200; i++) db.Put([2], [99]);
        await db.FlushAndCompactAsync();
        using var cts = new CancellationTokenSource();
        var reads = 0;
        encoder.OnRead = () => { if (++reads == 4) cts.Cancel(); };
        await Assert.That(async () => await cursor.ReadBatchAsync(10, 100, cts.Token)).Throws<OperationCanceledException>();
        await Assert.That(reads).IsEqualTo(4);
        encoder.OnRead = () => throw new IOException("Injected read fault.");
        await Assert.That(async () => await cursor.ReadBatchAsync(10, 100)).Throws<IOException>();
        encoder.OnRead = null;
        using var retry = await cursor.ReadBatchAsync(10, 100);
        await Assert.That(retry.Count).IsEqualTo(3);
        byte[] actual = Enumerable.Range(0, retry.Count).Select(i => retry[i].Value.Span[0]).ToArray();
        byte[] expected = reverse ? [3, 42, 1] : [1, 42, 3];
        await Assert.That(actual).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    [Test]
    public async Task CommitFailureFaultsOverlayOnlyAndExhaustedCursors()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        var armed = false;
        options.FaultInjector = point =>
        {
            if (armed && point == StorageFaultPoint.WriteBeforePublication) throw new IOException("Injected commit failure.");
        };
        await using var db = await MvccStorage.OpenAsync(folder, options);
        using var transaction = db.BeginTransaction();
        transaction.Put([1], [1]);
        using var cursor = transaction.CreateOrderedReadCursor();
        using var empty = transaction.CreateOrderedReadCursor(new OrderedReadRange(new byte[] { 9 }));
        using var exhausted = await empty.ReadBatchAsync(10, 100);
        await Assert.That(exhausted.IsExhausted).IsTrue();
        armed = true;
        await Assert.That(async () => await transaction.CommitAsync()).Throws<StorageOperationException>();
        await Assert.That(async () => await cursor.ReadBatchAsync(10, 100)).Throws<StorageFaultedException>();
        await Assert.That(async () => await empty.ReadBatchAsync(10, 100)).Throws<StorageFaultedException>();
        await Assert.That(() => transaction.CreateOrderedReadCursor()).Throws<ObjectDisposedException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task BoundedSstTraversalPrunesAndReseeksWithOverlay(bool reverse)
    {
        using var folder = TempFolder.Create();
        var options = Options();
        var encoder = new TestEncoder();
        options.BlockEncoderFactory = new TestFactory(encoder);
        await using var db = await MvccStorage.OpenAsync(folder, options);
        for (var i = 0; i < 1000; i++) db.Put(Key(i * 2), [42]);
        await db.FlushAndCompactAsync();
        using var transaction = db.BeginTransaction();
        transaction.Put(Key(1001), [43]);
        transaction.Delete(Key(1002));
        transaction.Put(Key(1003), [44]);
        transaction.Put(Key(9000), new byte[10000]);
        using var cursor = transaction.CreateOrderedReadCursor(new OrderedReadRange(Key(1000), true, Key(1004), false),
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        var actual = new List<int>();
        while (true)
        {
            encoder.Reads = 0;
            using var batch = await cursor.ReadBatchAsync(1, 5);
            await Assert.That(encoder.Reads <= 2).IsTrue();
            for (var i = 0; i < batch.Count; i++)
            {
                actual.Add(BinaryPrimitives.ReadInt32BigEndian(batch[i].Key.Span));
            }
            if (batch.IsExhausted) break;
        }
        int[] expected = reverse ? [1003, 1001, 1000] : [1000, 1001, 1003];
        await Assert.That(actual).IsEquivalentTo(expected, CollectionOrdering.Matching);
        using var outside = transaction.CreateOrderedReadCursor(new OrderedReadRange(Key(9000), true, Key(9001)));
        encoder.Reads = 0;
        using var overlay = await outside.ReadBatchAsync(1, 10004);
        await Assert.That(overlay.PayloadBytes).IsEqualTo(10004);
        await Assert.That(encoder.Reads).IsEqualTo(0);
    }

    [Test]
    public async Task IndependentCursorOverlapAndShutdownContract()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        var encoder = new TestEncoder();
        options.BlockEncoderFactory = new TestFactory(encoder);
        await using var db = await MvccStorage.OpenAsync(folder, options);
        db.Put([2], [2]);
        await db.FlushAndCompactAsync();
        using var transaction = db.BeginTransaction();
        transaction.Put([1], [1]);
        using var cursor = transaction.CreateOrderedReadCursor();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        encoder.OnRead = () =>
        {
            entered.Set();
            if (!release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Read was not released.");
        };
        var advance = Task.Run(async () => await cursor.ReadBatchAsync(10, 100));
        try
        {
            await Assert.That(entered.Wait(TimeSpan.FromSeconds(10))).IsTrue();
            await Assert.That(async () => await cursor.ReadBatchAsync(10, 100)).Throws<InvalidOperationException>();
            await Assert.That(() => cursor.Dispose()).Throws<InvalidOperationException>();
            transaction.Put([1], [99]);
            transaction.Dispose();
            var close = db.DisposeAsync().AsTask();
            await Assert.That(close.IsCompleted).IsFalse();
            release.Set();
            using var batch = await advance;
            await close;
            await Assert.That(batch.Count).IsEqualTo(2);
            await Assert.That(batch[0].Value.Span[0]).IsEqualTo((byte)1);
            await Assert.That(batch[1].Value.Span[0]).IsEqualTo((byte)2);
            await Assert.That(async () => await cursor.ReadBatchAsync(10, 100)).Throws<ObjectDisposedException>();
        }
        finally
        {
            release.Set();
            encoder.OnRead = null;
        }
    }

    [Test]
    public async Task GroupCommitKeepsFrozenCursorIndependent()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.UseWriteAheadLog = true;
        options.SyncWriteAheadLogToDisk = true;
        options.MvccCommitGroupSize = 2;
        await using var db = await MvccStorage.OpenAsync(folder, options);
        db.Put([1], [1]);
        using var first = db.BeginTransaction();
        using var second = db.BeginTransaction();
        first.Put([2], [2]);
        second.Put([3], [3]);
        using var cursor = first.CreateOrderedReadCursor();
        first.Put([2], [22]);
        await Task.WhenAll(first.CommitAsync().AsTask(), second.CommitAsync().AsTask());
        using var batch = await cursor.ReadBatchAsync(10, 100);
        await Assert.That(batch.Count).IsEqualTo(2);
        await Assert.That(batch[0].Value.Span[0]).IsEqualTo((byte)1);
        await Assert.That(batch[1].Value.Span[0]).IsEqualTo((byte)2);
    }

    private static byte[] Key(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        return bytes;
    }

    private sealed class TestFactory(TestEncoder encoder) : IBlockEncoderFactory
    {
        public IBlockEncoder Create() => encoder;
    }

    private sealed class TestEncoder : IBlockEncoder
    {
        private readonly IBlockEncoder _inner = new DefaultBlockEncoderFactory(128).Create();
        public Action? OnRead;
        public int Reads;
        public ushort BlockSize => _inner.BlockSize;

        public bool WritesKeyWindows => _inner.WritesKeyWindows;
        public Block Encode(ReadOnlyMemory<byte> keys, ReadOnlyMemory<byte> values, IReadOnlyList<BlockEntry> entries) =>
            _inner.Encode(keys, values, entries);
        public Block Decode(ReadOnlyMemory<byte> buffer)
        {
            Reads++;
            OnRead?.Invoke();
            return _inner.Decode(buffer);
        }
        public Block Decode(IMemoryOwner<byte> owner, int length)
        {
            Reads++;
            OnRead?.Invoke();
            return _inner.Decode(owner, length);
        }
        public RecordLocation DecodeEntry(ReadOnlyMemory<byte> data, int offset) => _inner.DecodeEntry(data, offset);
        public void DecodeRawEntry(ReadOnlyMemory<byte> data, int offset, out ReadOnlyMemory<byte> key,
            out ReadOnlyMemory<byte> value, out bool tombstone) =>
            _inner.DecodeRawEntry(data, offset, out key, out value, out tombstone);
        public ReadOnlyMemory<byte> DecodeValue(ReadOnlyMemory<byte> data, int offset, int length) =>
            _inner.DecodeValue(data, offset, length);
        public int EstimateSize(int keyLength, int valueLength, bool tombstone) =>
            _inner.EstimateSize(keyLength, valueLength, tombstone);
    }
}
