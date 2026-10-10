using System.Buffers;
using System.Buffers.Binary;
using Silex.Blocks;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class OrderedReadTests
{
    private static StorageOptions Options() => new()
    {
        UseWriteAheadLog = false,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
        BlockSize = 128,
    };

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task BoundsAndResumesCoverBinaryKeysInBothDirections(bool mvcc, bool sst)
    {
        using var folder = TempFolder.Create();
        byte[][] keys = mvcc
            ? [[], [0], [0, 0], [0, 1], [1], [1, 0], [1, 0, 1], [2], [255]]
            : [[0], [0, 0], [0, 1], [1], [1, 0], [1, 0, 1], [2], [255]];
        await using var lsm = mvcc ? null : await LsmStorage.OpenAsync(folder, Options());
        await using var versioned = mvcc ? await MvccStorage.OpenAsync(folder, Options()) : null;
        for (var i = 0; i < keys.Length; i++)
        {
            if (mvcc) versioned!.Put(keys[i], [(byte)i]);
            else lsm!.Put(keys[i], [(byte)i]);
        }
        if (sst)
        {
            if (mvcc) await versioned!.DisposeAsync();
            else await lsm!.DisposeAsync();
        }
        await using var reopenedLsm = !mvcc && sst ? await LsmStorage.OpenAsync(folder, Options()) : null;
        await using var reopenedMvcc = mvcc && sst ? await MvccStorage.OpenAsync(folder, Options()) : null;
        foreach (var reverse in new[] { false, true })
        foreach (var lowerInclusive in new[] { false, true })
        foreach (var upperInclusive in new[] { false, true })
        foreach (var lowerIndex in Enumerable.Range(-1, keys.Length + 1))
        foreach (var upperIndex in Enumerable.Range(-1, keys.Length + 1))
        {
            var range = new OrderedReadRange(
                lowerIndex < 0 ? (ReadOnlyMemory<byte>?)null : keys[lowerIndex], lowerInclusive,
                upperIndex < 0 ? (ReadOnlyMemory<byte>?)null : keys[upperIndex], upperInclusive);
            var direction = reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward;
            using var cursor = mvcc
                ? (reopenedMvcc ?? versioned)!.CreateOrderedReadCursor(range, direction)
                : (reopenedLsm ?? lsm)!.CreateOrderedReadCursor(range, direction);
            var actual = new List<int>();
            while (true)
            {
                using var batch = await cursor.ReadBatchAsync(2, 7);
                await Assert.That(batch.Count <= 2 && batch.PayloadBytes <= 7).IsTrue();
                var bytes = 0;
                for (var i = 0; i < batch.Count; i++)
                {
                    var entry = batch[i];
                    bytes += entry.Key.Length + entry.Value.Length;
                    var id = entry.Value.Span[0];
                    await Assert.That(entry.Key.ToArray()).IsEquivalentTo(keys[id], CollectionOrdering.Matching);
                    actual.Add(id);
                }
                await Assert.That(batch.PayloadBytes).IsEqualTo(bytes);
                if (batch.IsExhausted) break;
                await Assert.That(batch.Count > 0).IsTrue();
            }
            var expected = Enumerable.Range(0, keys.Length).Where(i =>
                (lowerIndex < 0 || i > lowerIndex || (lowerInclusive && i == lowerIndex)) &&
                (upperIndex < 0 || i < upperIndex || (upperInclusive && i == upperIndex)));
            if (reverse) expected = expected.Reverse();
            await Assert.That(actual).IsEquivalentTo(expected, CollectionOrdering.Matching);
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SnapshotCursorRetainsExactSequenceAcrossGcAndOriginDisposal(bool reverse)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        for (byte i = 1; i <= 5; i++) db.Put([i], [i]);
        db.Delete([2]);
        using var snapshot = db.CreateSnapshot();
        var sequence = snapshot.Sequence;
        using var cursor = snapshot.CreateOrderedReadCursor(direction:
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        snapshot.Dispose();
        using var first = await cursor.ReadBatchAsync(1, 2);
        await Assert.That(cursor.Sequence).IsEqualTo(sequence);
        await Assert.That(cursor.Consistency).IsEqualTo(OrderedReadConsistency.Snapshot);
        for (byte i = 1; i <= 5; i++)
        {
            db.Put([i], [99]);
            db.Delete([i]);
        }
        db.Put([6], [6]);
        await db.CollectGarbageAsync(compact: true);
        var ids = new List<byte> { first[0].Value.Span[0] };
        while (true)
        {
            using var batch = await cursor.ReadBatchAsync(1, 2);
            for (var i = 0; i < batch.Count; i++) ids.Add(batch[i].Value.Span[0]);
            if (batch.IsExhausted) break;
            await Assert.That(await db.GetRawAsync(new byte[] { 6 }, new byte[1])).IsEqualTo(1);
        }
        byte[] expected = reverse ? [5, 4, 3, 1] : [1, 3, 4, 5];
        await Assert.That(ids).IsEquivalentTo(expected, CollectionOrdering.Matching);
        cursor.Dispose();
        await Assert.That(await db.CollectGarbageAsync()).IsGreaterThan(0);
        // Previously returned batches survive cursor disposal, GC, and engine shutdown.
        await db.DisposeAsync();
        await Assert.That(first[0].Value.Span[0]).IsEqualTo(expected[0]);
    }

    [Test]
    public async Task LiveCursorOwnsBoundsAndBatchesAndDoesNotRevisitKeys()
    {
        using var folder = TempFolder.Create();
        await using var db = await LsmStorage.OpenAsync(folder, Options());
        db.Put([1], [1]);
        db.Put([3], [3]);
        byte[] lower = [1], upper = [4];
        using var cursor = db.CreateOrderedReadCursor(new OrderedReadRange(lower, true, upper, false));
        lower[0] = 9;
        upper[0] = 0;
        using var first = await cursor.ReadBatchAsync(1, 2);
        db.Put([1], [99]);
        db.Put([2], [2]);
        db.Delete([3]);
        db.Put([4], [4]);
        using var second = await cursor.ReadBatchAsync(10, 100);
        await Assert.That(second.Count).IsEqualTo(1);
        await Assert.That(second[0].Key.Span[0]).IsEqualTo((byte)2);
        await Assert.That(second.IsExhausted).IsTrue();
        await Assert.That(cursor.Consistency).IsEqualTo(OrderedReadConsistency.Live);
        await Assert.That(cursor.Sequence).IsNull();
        await Assert.That(await db.GetRawAsync(new byte[] { 1 }, new byte[1])).IsEqualTo(1);
        cursor.Dispose();
        await db.DisposeAsync();
        await Assert.That(first[0].Value.Span[0]).IsEqualTo((byte)1);
        first.Dispose();
        await Assert.That(() => first[0]).Throws<ObjectDisposedException>();
        await Assert.That(async () => await cursor.ReadBatchAsync(1, 1)).Throws<ObjectDisposedException>();
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task ByteLimitsAndOversizedRetriesNeverSkipEntries(bool mvcc, bool reverse)
    {
        using var folder = TempFolder.Create();
        await using var lsm = mvcc ? null : await LsmStorage.OpenAsync(folder, Options());
        await using var db = mvcc ? await MvccStorage.OpenAsync(folder, Options()) : null;
        for (byte i = 1; i <= 3; i++)
        {
            if (mvcc) db!.Put([i], new byte[i == 2 ? 9 : 1]);
            else lsm!.Put([i], new byte[i == 2 ? 9 : 1]);
        }
        var direction = reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward;
        using var cursor = mvcc ? db!.CreateOrderedReadCursor(direction: direction) :
            lsm!.CreateOrderedReadCursor(direction: direction);
        using var first = await cursor.ReadBatchAsync(10, 9);
        await Assert.That(first.Count).IsEqualTo(1);
        await Assert.That(first.PayloadBytes).IsEqualTo(2);
        await Assert.That(first.IsExhausted).IsFalse();
        await Assert.That(async () => await cursor.ReadBatchAsync(10, 9)).Throws<ArgumentException>();
        using var retry = await cursor.ReadBatchAsync(10, 10);
        await Assert.That(retry.Count).IsEqualTo(1);
        await Assert.That(retry[0].Key.Span[0]).IsEqualTo((byte)2);
        await Assert.That(retry.PayloadBytes).IsEqualTo(10);
        using var last = await cursor.ReadBatchAsync(10, 9);
        await Assert.That(last.Count).IsEqualTo(1);
        await Assert.That(last[0].Key.Span[0]).IsEqualTo((byte)(reverse ? 1 : 3));
        await Assert.That(last.IsExhausted).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SstBoundsPruneTablesAndBlocksBeforeReading(bool reverse)
    {
        using var folder = TempFolder.Create();
        var encoder = new CountingEncoder();
        var options = Options();
        options.BlockEncoderFactory = new CountingFactory(encoder);
        await using var builder = await LsmStorage.OpenAsync(folder, options);
        for (var i = 0; i < 1000; i++) builder.Put(Key(i), new byte[20]);
        await builder.DisposeAsync();
        encoder.Reads = 0;
        await using var db = await LsmStorage.OpenAsync(folder, options);
        encoder.Reads = 0;
        using var cursor = db.CreateOrderedReadCursor(new OrderedReadRange(Key(500), true, Key(502), false),
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        using var batch = await cursor.ReadBatchAsync(10, 1000);
        await Assert.That(batch.Count).IsEqualTo(2);
        await Assert.That(encoder.Reads <= 2).IsTrue();
        await Assert.That(batch.IsExhausted).IsTrue();
        encoder.Reads = 0;
        using var outside = db.CreateOrderedReadCursor(new OrderedReadRange(Key(1100), true, Key(1200)));
        using var empty = await outside.ReadBatchAsync(10, 100);
        await Assert.That(empty.Count).IsEqualTo(0);
        await Assert.That(encoder.Reads).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CancellationDuringInvisibleVersionsLeavesCursorPositionUnchanged(bool reverse)
    {
        using var folder = TempFolder.Create();
        var encoder = new CountingEncoder();
        var options = Options();
        options.BlockEncoderFactory = new CountingFactory(encoder);
        await using var db = await MvccStorage.OpenAsync(folder, options);
        db.Put([1], [42]);
        using var cursor = db.CreateOrderedReadCursor(direction:
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        for (var i = 0; i < 200; i++) db.Put([1], [99]);
        await db.FlushAndCompactAsync();
        using var cts = new CancellationTokenSource();
        var scannedBlocks = 0;
        encoder.OnRead = () =>
        {
            if (++scannedBlocks == 4) cts.Cancel();
        };
        await Assert.That(async () => await cursor.ReadBatchAsync(2, 100, cts.Token)).Throws<OperationCanceledException>();
        await Assert.That(scannedBlocks).IsEqualTo(4);
        encoder.OnRead = null;
        using var retry = await cursor.ReadBatchAsync(2, 100);
        await Assert.That(retry.Count).IsEqualTo(1);
        await Assert.That(retry[0].Value.Span[0]).IsEqualTo((byte)42);
    }

    [Test]
    public async Task ShutdownAndFailureRejectEvenExhaustedCursorAdvances()
    {
        using var folder = TempFolder.Create();
        await using var db = await LsmStorage.OpenAsync(folder, Options());
        using var cursor = db.CreateOrderedReadCursor();
        using var empty = await cursor.ReadBatchAsync(1, 1);
        await Assert.That(empty.IsExhausted).IsTrue();
        await db.DisposeAsync();
        await Assert.That(async () => await cursor.ReadBatchAsync(1, 1)).Throws<ObjectDisposedException>();
        await Assert.That(async () => await cursor.ReadBatchAsync(0, 1)).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OverlappingSourcesResolveNewestValuesAndTombstones(bool reverse)
    {
        using var folder = TempFolder.Create();
        await using var db = await LsmStorage.OpenAsync(folder, Options());
        for (byte tier = 1; tier <= 3; tier++)
        {
            for (byte key = 1; key <= 20; key++) db.Put([key], [tier]);
            await db.FlushAndCompactAsync();
        }
        db.Delete([5]);
        db.Put([10], [4]);
        using var cursor = db.CreateOrderedReadCursor(new OrderedReadRange(new byte[] { 4 }, false,
            new byte[] { 12 }, true), reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        var actual = new List<byte>();
        while (true)
        {
            using var batch = await cursor.ReadBatchAsync(2, 3);
            for (var i = 0; i < batch.Count; i++)
            {
                var entry = batch[i];
                actual.Add(entry.Key.Span[0]);
                await Assert.That(entry.Value.Span[0]).IsEqualTo((byte)(entry.Key.Span[0] == 10 ? 4 : 3));
            }
            if (batch.IsExhausted) break;
            await db.FlushAndCompactAsync();
        }
        byte[] expected = reverse ? [12, 11, 10, 9, 8, 7, 6] : [6, 7, 8, 9, 10, 11, 12];
        await Assert.That(actual).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    [Test]
    public async Task ReadFaultCanBeRetriedAndStoreFaultRejectsExhaustedReads()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        var encoder = new CountingEncoder();
        options.BlockEncoderFactory = new CountingFactory(encoder);
        var faultWrite = false;
        options.FaultInjector = point =>
        {
            if (faultWrite && point == StorageFaultPoint.WriteBeforePublication) throw new IOException("Injected write fault.");
        };
        await using var db = await LsmStorage.OpenAsync(folder, options);
        db.Put([1], [42]);
        await db.FlushAndCompactAsync();
        using var cursor = db.CreateOrderedReadCursor();
        encoder.OnRead = () => throw new IOException("Injected read fault.");
        await Assert.That(async () => await cursor.ReadBatchAsync(10, 100)).Throws<IOException>();
        encoder.OnRead = null;
        using var retry = await cursor.ReadBatchAsync(10, 100);
        await Assert.That(retry[0].Value.Span[0]).IsEqualTo((byte)42);
        await Assert.That(retry.IsExhausted).IsTrue();
        faultWrite = true;
        await Assert.That(() => db.Put([2], [2])).Throws<StorageOperationException>();
        await Assert.That(async () => await cursor.ReadBatchAsync(10, 100)).Throws<StorageFaultedException>();
    }

    [Test]
    public async Task OverlappingAdvancesAndDisposalAreRejectedAndShutdownDrainsOnlyActiveAdvance()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        var encoder = new CountingEncoder();
        options.BlockEncoderFactory = new CountingFactory(encoder);
        await using var db = await LsmStorage.OpenAsync(folder, options);
        db.Put([1], [42]);
        await db.FlushAndCompactAsync();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        encoder.OnRead = () =>
        {
            entered.Set();
            if (!release.Wait(TimeSpan.FromSeconds(10))) throw new TimeoutException("Test read was not released.");
        };
        using var cursor = db.CreateOrderedReadCursor();
        var advance = Task.Run(async () => await cursor.ReadBatchAsync(1, 2));
        try
        {
            await Assert.That(entered.Wait(TimeSpan.FromSeconds(10))).IsTrue();
            await Assert.That(async () => await cursor.ReadBatchAsync(1, 2)).Throws<InvalidOperationException>();
            await Assert.That(() => cursor.Dispose()).Throws<InvalidOperationException>();
            var close = db.CloseAsync();
            await Assert.That(close.IsCompleted).IsFalse();
            release.Set();
            using var batch = await advance;
            await close;
            await Assert.That(batch[0].Value.Span[0]).IsEqualTo((byte)42);
            await Assert.That(async () => await cursor.ReadBatchAsync(1, 2)).Throws<ObjectDisposedException>();
        }
        finally
        {
            release.Set();
            encoder.OnRead = null;
        }
    }

    private static byte[] Key(int value)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, value);
        return key;
    }

    [Test]
    public async Task NarrowMemtableReadsDoNotAllocateForTheRemainingStore()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.MemTableSizeLimit = 64 * 1024 * 1024;
        await using var db = await LsmStorage.OpenAsync(folder, options);
        for (var i = 0; i < 65536; i++) db.Put(Key(i), new byte[16]);
        var range = new OrderedReadRange(Key(32768), true, Key(32800), false);
        for (var i = 0; i < 4; i++)
        {
            using var warmup = db.CreateOrderedReadCursor(range);
            using var batch = await warmup.ReadBatchAsync(32, 640);
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        using var cursor = db.CreateOrderedReadCursor(range);
        using var result = cursor.ReadBatchAsync(32, 640).GetAwaiter().GetResult();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(result.Count).IsEqualTo(32);
        await Assert.That(result.PayloadBytes).IsEqualTo(640);
        await Assert.That(allocated <= 1024).IsTrue();
        for (var i = 0; i < result.Count; i++)
        {
            await Assert.That(result[i].Key.ToArray()).IsEquivalentTo(Key(32768 + i), CollectionOrdering.Matching);
            await Assert.That(result[i].Value.Length).IsEqualTo(16);
        }
    }

    [Test]
    public async Task NullAndEmptyBoundsAreDistinct()
    {
        using var folder = TempFolder.Create();
        await using var db = await LsmStorage.OpenAsync(folder, Options());
        db.Put([0], []);
        var unbounded = new OrderedReadRange();
        await Assert.That(unbounded.Lower.HasValue || unbounded.Upper.HasValue).IsFalse();
        var explicitNull = new OrderedReadRange(lower: null, upper: null);
        await Assert.That(explicitNull.Lower.HasValue || explicitNull.Upper.HasValue).IsFalse();
        using var all = db.CreateOrderedReadCursor(unbounded);
        using var allBatch = await all.ReadBatchAsync(1, 1);
        await Assert.That(allBatch.Count).IsEqualTo(1);
        using var empty = db.CreateOrderedReadCursor(
            new OrderedReadRange(upper: ReadOnlyMemory<byte>.Empty), OrderedReadDirection.Reverse);
        using var emptyBatch = await empty.ReadBatchAsync(1, 1);
        await Assert.That(emptyBatch.Count).IsEqualTo(0);
        await Assert.That(emptyBatch.IsExhausted).IsTrue();
    }

    private sealed class CountingFactory(CountingEncoder encoder) : IBlockEncoderFactory
    {
        public IBlockEncoder Create() => encoder;
    }

    private sealed class CountingEncoder : IBlockEncoder
    {
        private readonly IBlockEncoder _inner = new DefaultBlockEncoderFactory(128).Create();
        public int Reads;
        public Action? OnRead;
        public ushort BlockSize => _inner.BlockSize;
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
            out ReadOnlyMemory<byte> value, out bool tombstone) => _inner.DecodeRawEntry(data, offset, out key, out value, out tombstone);
        public ReadOnlyMemory<byte> DecodeValue(ReadOnlyMemory<byte> data, int offset, int length) =>
            _inner.DecodeValue(data, offset, length);
        public int EstimateSize(int keyLength, int valueLength, bool tombstone) =>
            _inner.EstimateSize(keyLength, valueLength, tombstone);
    }
}
