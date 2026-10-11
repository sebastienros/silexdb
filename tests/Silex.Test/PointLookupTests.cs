using System.Buffers.Binary;

namespace Silex.Test;

public class PointLookupTests
{
    [Test]
    public async Task ReusedCommitBatchDoesNotReplayEarlierMutationsAndHonorsUnchangedSequenceCancellation()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        var key = new byte[512];
        storage.Put(key, [10]);
        using var snapshot = storage.CreateSnapshot();
        storage.Delete(key);
        using (var transaction = storage.BeginTransaction())
        {
            transaction.Put([2], [20]);
            await Assert.That(await transaction.TryCommitAsync()).IsTrue();
        }
        await Assert.That(await storage.GetRawAsync(key, new byte[1])).IsEqualTo(-1);
        var buffer = new byte[1];
        await Assert.That(await snapshot.GetRawAsync(key, buffer)).IsEqualTo(1);
        await Assert.That(buffer[0]).IsEqualTo((byte)10);
        using var latest = storage.CreateSnapshot();
        using var writes = new MvccWriteBuffer();
        writes.TrackRead([2]);
        writes.TrackReadRange(new OrderedReadRange(null, true, null, true));
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.That(async () => await storage.TryCommitAsync(latest.Sequence, writes, canceled.Token))
            .Throws<OperationCanceledException>();
        await Assert.That(await storage.TryCommitAsync(latest.Sequence, writes, default) is null).IsTrue();
        await Assert.That(storage.PublishedSequence).IsEqualTo(latest.Sequence);
        storage.Delete([2]);
        await Assert.That((await storage.TryCommitAsync(latest.Sequence, writes, default))!.Kind)
            .IsEqualTo(MvccConflictKind.TrackedPointRead);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WarmMixedLookupsDoNotAllocateAndRetainNewestDuplicate(bool frozen)
    {
        using var folder = TempFolder.Create();
        using var inner = new LsmStorageInner(folder, Options());
        var first = Key(0);
        var sst = Key(1);
        inner.PutRaw(first, [10]);
        inner.PutRaw(Key(2), [30]);
        inner.ForceFreezeMemTable();
        await inner.ForceFlushNextImmutableMemTableAsync();
        inner.PutRaw(first, [20]);
        if (frozen) inner.ForceFreezeMemTable();
        var buffer = new byte[32];
        var sink = new CaptureSink(buffer);
        for (var i = 0; i < 32; i++) Read();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 128; i++) Read();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);

        void Read()
        {
            var result = inner.ReadFirstRawAsync(first, first.AsMemory(0, 3), sink, default, int.MaxValue)
                .GetAwaiter().GetResult();
            if (!result.Found || result.Value.Span[0] != 20)
                throw new InvalidOperationException("An older source won an exact-key tie.");
            result = inner.ReadFirstRawAsync(sst, sst.AsMemory(0, 3), sink, default, int.MaxValue)
                .GetAwaiter().GetResult();
            if (!result.Found || result.Value.Span[0] != 30)
                throw new InvalidOperationException("Mixed lookup missed its SST candidate.");
        }
    }

    private static StorageOptions Options() => new()
    {
        UseWriteAheadLog = false,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
        BlockSize = 128,
    };

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task LargeEscapedKeysReportFullLengthWithoutPartiallyWritingShortBuffers(int layout)
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.BlockSize = 4096;
        await using var storage = await MvccStorage.OpenAsync(folder, options);
        var key = new byte[512];
        key[0] = 1;
        key[^1] = 255;
        storage.Put(key, new byte[256]);
        if (layout != 0) await storage.FlushAndCompactAsync();
        using var snapshot = storage.CreateSnapshot();
        if (layout == 2) storage.Put(key, [20]);
        var buffer = new byte[] { 7, 7, 7, 7 };
        await Assert.That(await snapshot.GetRawAsync(key, buffer)).IsEqualTo(256);
        await Assert.That(buffer.All(static b => b == 7)).IsTrue();
        await Assert.That(await snapshot.TryReadRawAsync(key, 256, static (length, value) =>
        {
            if (value.Length != length) throw new InvalidOperationException("Unexpected value length.");
        })).IsTrue();
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task SnapshotsResolveBinaryKeysAndTombstonesAcrossSourcesAndGarbageCollection(int layout)
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        byte[][] keys = [[], [0], [0, 0], [0, 255], [1], [1, 0], [1, 0, 255], [255]];
        var expected = new byte?[keys.Length];
        var snapshots = new List<(MvccSnapshot Snapshot, byte?[] Values)>();
        try
        {
            for (byte epoch = 0; epoch < 5; epoch++)
            {
                for (var i = 0; i < keys.Length; i++)
                {
                    if ((epoch + i) % 3 == 0)
                    {
                        storage.Delete(keys[i]);
                        expected[i] = null;
                    }
                    else
                    {
                        storage.Put(keys[i], [epoch]);
                        expected[i] = epoch;
                    }
                }
                if (epoch % 2 == 0) snapshots.Add((storage.CreateSnapshot(), expected.ToArray()));
                if (layout == 2 || layout == 3 && epoch < 3) await storage.FlushAndCompactAsync();
            }
            if (layout == 1) await storage.FlushAndCompactAsync();
            await CheckSnapshotsAsync();
            await storage.CollectGarbageAsync();
            await CheckSnapshotsAsync();

            async Task CheckSnapshotsAsync()
            {
                var buffer = new byte[1];
                foreach (var (snapshot, values) in snapshots)
                {
                    for (var i = 0; i < keys.Length; i++)
                    {
                        var length = await snapshot.GetRawAsync(keys[i], buffer);
                        await Assert.That(length).IsEqualTo(values[i].HasValue ? 1 : -1);
                        if (values[i] is { } value) await Assert.That(buffer[0]).IsEqualTo(value);
                    }
                    await Assert.That(await snapshot.GetRawAsync([1, 0, 0], buffer)).IsEqualTo(-1);
                    await Assert.That(await snapshot.GetRawAsync([255, 0], buffer)).IsEqualTo(-1);
                }
            }
        }
        finally
        {
            foreach (var (snapshot, _) in snapshots) snapshot.Dispose();
        }
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    public async Task PhysicalDeletionShadowsDuplicatesButNotTheNextVersion(int layout)
    {
        using var folder = TempFolder.Create();
        using var storage = new LsmStorageInner(folder, Options());
        var first = Key(0);
        var second = Key(1);
        storage.PutRaw(first, [10]);
        storage.PutRaw(second, [20]);
        storage.PutRaw(Key(0, 2), [30]);
        if (layout is 2 or 3)
        {
            storage.ForceFreezeMemTable();
            await storage.ForceFlushNextImmutableMemTableAsync();
        }
        else if (layout is 1 or 4)
        {
            storage.ForceFreezeMemTable();
        }
        storage.DeleteRaw(first);
        if (layout is 3 or 4)
        {
            storage.ForceFreezeMemTable();
            if (layout == 3) await storage.ForceFlushNextImmutableMemTableAsync();
        }

        var buffer = new byte[32];
        var result = await storage.ReadFirstRawAsync(first, first.AsMemory(0, 3),
            new CaptureSink(buffer), default, int.MaxValue);
        await Assert.That(result.Found).IsTrue();
        await Assert.That(result.Key.Span.SequenceEqual(second)).IsTrue();
        await Assert.That(result.Value.Span.SequenceEqual(new byte[] { 20 })).IsTrue();
    }

    [Test]
    public async Task SortedSstLookupCrossesBlocksAndStopsAtTheUserPrefix()
    {
        using var folder = TempFolder.Create();
        using var storage = new LsmStorageInner(folder, Options());
        for (var i = 0; i < 64; i++)
        {
            storage.PutRaw(Key(i), new byte[32]);
            if (i < 48) storage.DeleteRaw(Key(i));
        }
        storage.PutRaw(Key(0, 2), [30]);
        storage.ForceFreezeMemTable();
        await storage.ForceFlushNextImmutableMemTableAsync();
        var buffer = new byte[64];
        var first = Key(0);
        var result = await storage.ReadFirstRawAsync(first, first.AsMemory(0, 3),
            new CaptureSink(buffer), default, int.MaxValue);
        await Assert.That(result.Found).IsTrue();
        await Assert.That(result.Key.Span.SequenceEqual(Key(48))).IsTrue();
        var missing = Key(64);
        result = await storage.ReadFirstRawAsync(missing, missing.AsMemory(0, 3),
            new CaptureSink(buffer), default, int.MaxValue);
        await Assert.That(result.Found).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WarmMvccPointReadsAndTrackedValidationDoNotAllocate(bool frozen)
    {
        using var folder = TempFolder.Create();
        using var inner = new LsmStorageInner(folder, Options());
        var first = Key(0);
        inner.PutRaw(first, [1]);
        if (frozen) inner.ForceFreezeMemTable();
        var buffer = new byte[32];
        var sink = new CaptureSink(buffer);
        for (var i = 0; i < 32; i++)
        {
            inner.ReadFirstRawAsync(first, first.AsMemory(0, 3), sink, default, int.MaxValue)
                .GetAwaiter().GetResult();
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 128; i++)
        {
            inner.ReadFirstRawAsync(first, first.AsMemory(0, 3), sink, default, int.MaxValue)
                .GetAwaiter().GetResult();
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);

        using var mvccFolder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(mvccFolder, Options());
        byte[] key = [1, 0, 255];
        storage.Put(key, [10]);
        using var snapshot = storage.CreateSnapshot();
        using var writes = new MvccWriteBuffer();
        writes.TrackRead(key);
        for (var i = 0; i < 32; i++)
        {
            snapshot.GetRawAsync(key, buffer).GetAwaiter().GetResult();
            storage.TryCommitAsync(snapshot.Sequence, writes, default).GetAwaiter().GetResult();
        }
        before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 128; i++)
        {
            snapshot.GetRawAsync(key, buffer).GetAwaiter().GetResult();
            storage.TryCommitAsync(snapshot.Sequence, writes, default).GetAwaiter().GetResult();
        }
        allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PointLookupHonorsCancellationAndReleasesLocksAfterReaderFailure(bool sst)
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        storage.Put([1], [10]);
        if (sst) await storage.FlushAndCompactAsync();
        using var snapshot = storage.CreateSnapshot();
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.That(async () => await snapshot.GetRawAsync([1], new byte[1], canceled.Token))
            .Throws<OperationCanceledException>();
        await Assert.That(async () => await snapshot.TryReadRawAsync([1], 0,
            static (_, _) => throw new InvalidOperationException("Reader failure.")))
            .Throws<InvalidOperationException>();
        storage.Put([1], [20]);
        var buffer = new byte[1];
        await Assert.That(await snapshot.GetRawAsync([1], buffer)).IsEqualTo(1);
        await Assert.That(buffer[0]).IsEqualTo((byte)10);
    }

    private static byte[] Key(int suffix, byte prefix = 1)
    {
        var key = new byte[11];
        key[0] = prefix;
        BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(3), suffix);
        return key;
    }

    private struct CaptureSink(Memory<byte> buffer) : IRawEntrySink
    {
        public bool Found;
        public ReadOnlyMemory<byte> Key;
        public ReadOnlyMemory<byte> Value;

        public void Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            key.CopyTo(buffer.Span);
            value.CopyTo(buffer.Span[key.Length..]);
            Key = buffer[..key.Length];
            Value = buffer.Slice(key.Length, value.Length);
            Found = true;
        }
    }
}
