using System.Buffers.Binary;

namespace Silex.Test;

public class MvccVersionstampTests
{
    private static StorageOptions Options(int groupSize) => new()
    {
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
        SyncWriteAheadLogToDisk = true,
        MvccCommitGroupSize = groupSize
    };

    private static byte[] Key(long sequence, byte suffix = 0)
    {
        var key = new byte[10];
        key[0] = 71;
        BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(1), sequence);
        key[9] = suffix;
        return key;
    }

    [Test]
    [Arguments(1)]
    [Arguments(8)]
    public async Task AtomicKeyAndValueSubstitutionSurvivesReopenAndCompaction(int groupSize)
    {
        using var folder = TempFolder.Create();
        var options = Options(groupSize);
        await using (var db = await MvccStorage.OpenAsync(folder, options))
        {
            using var snapshot = db.CreateSnapshot();
            using var tx = db.BeginTransaction();
            var template = Key(123);
            var value = new byte[10];
            value[0] = 12;
            value[9] = 34;
            tx.PutVersionstamped(template, value, keyOffset: 1, valueOffset: 1);
            tx.PutVersionstamped(Key(123, 1), [56], keyOffset: 1);
            tx.PutVersionstamped([90], value, valueOffset: 1);
            tx.Put([91], [78]);
            template[0] = 99;
            value[0] = 99; // Caller buffers are copied.
            var pending = new byte[10];
            await Assert.That(await tx.GetRawAsync(Key(123), pending)).IsEqualTo(10);
            await Assert.That(pending[0]).IsEqualTo((byte)12);
            using var cursor = tx.CreateOrderedReadCursor();
            await Assert.That(tx.CommitSequence).IsNull();
            await tx.CommitAsync();
            await Assert.That(tx.CommitSequence).IsEqualTo(1L);
            await Assert.That(await snapshot.GetRawAsync(Key(1), pending)).IsEqualTo(-1);
            await Assert.That(await db.GetRawAsync(Key(123), pending)).IsEqualTo(-1);
            await Assert.That(await db.GetRawAsync(Key(1), pending)).IsEqualTo(10);
            await Assert.That(BinaryPrimitives.ReadInt64BigEndian(pending.AsSpan(1))).IsEqualTo(1L);
            await Assert.That(pending[0]).IsEqualTo((byte)12);
            await Assert.That(pending[9]).IsEqualTo((byte)34);
            await Assert.That(await db.GetRawAsync([90], pending)).IsEqualTo(10);
            await Assert.That(BinaryPrimitives.ReadInt64BigEndian(pending.AsSpan(1))).IsEqualTo(1L);
            using var batch = await cursor.ReadBatchAsync(10, 1024);
            await Assert.That(batch.Count).IsEqualTo(4);
            await Assert.That(batch[0].Key.Span.SequenceEqual(Key(123))).IsTrue();
        }
        await using var reopened = await MvccStorage.OpenAsync(folder, options);
        await reopened.FlushAndCompactAsync();
        var bytes = new byte[10];
        await Assert.That(await reopened.GetRawAsync(Key(1), bytes)).IsEqualTo(10);
        await Assert.That(BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan(1))).IsEqualTo(1L);
        await Assert.That(await reopened.GetRawAsync(Key(1, 1), bytes)).IsEqualTo(1);
        await Assert.That(bytes[0]).IsEqualTo((byte)56);
        await Assert.That(await reopened.GetRawAsync([91], bytes)).IsEqualTo(1);
        await Assert.That(bytes[0]).IsEqualTo((byte)78);
        using var next = reopened.BeginTransaction();
        next.PutVersionstamped(Key(0), [], keyOffset: 1);
        await next.CommitAsync();
        await Assert.That(next.CommitSequence).IsEqualTo(2L);
        await Assert.That(await reopened.GetRawAsync(Key(2), Memory<byte>.Empty)).IsEqualTo(0);
    }

    [Test]
    [Arguments(1)]
    [Arguments(8)]
    public async Task SavepointsAndReplacementRestoreOrRemoveOffsets(int groupSize)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(groupSize));
        using var tx = db.BeginTransaction();
        tx.PutVersionstamped(Key(0), new byte[8], keyOffset: 1, valueOffset: 0);
        var marker = tx.CreateSavepoint();
        tx.Put(Key(0), [99]);
        tx.PutVersionstamped(Key(0, 1), [], keyOffset: 1);
        tx.RollbackToSavepoint(marker);
        tx.ReleaseSavepoint(marker);
        tx.PutVersionstamped(Key(0, 2), [], keyOffset: 1);
        tx.Delete(Key(0, 2));
        tx.PutVersionstamped(Key(0, 3), [], keyOffset: 1);
        tx.Put(Key(0, 3), [33]);
        await tx.CommitAsync();
        var value = new byte[8];
        await Assert.That(await db.GetRawAsync(Key(1), value)).IsEqualTo(8);
        await Assert.That(BinaryPrimitives.ReadInt64BigEndian(value)).IsEqualTo(1L);
        await Assert.That(await db.GetRawAsync(Key(1, 1), value)).IsEqualTo(-1);
        await Assert.That(await db.GetRawAsync(Key(1, 2), value)).IsEqualTo(-1);
        await Assert.That(await db.GetRawAsync(Key(1, 3), value)).IsEqualTo(-1);
        await Assert.That(await db.GetRawAsync(Key(0, 3), value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)33);
    }

    [Test]
    [Arguments(1)]
    [Arguments(8)]
    public async Task ValueOnlyStampsRetainNormalConflictsAndEmptyCommitsHaveNoStamp(int groupSize)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(groupSize));
        using var stale = db.BeginTransaction();
        stale.PutVersionstamped([1], new byte[8], valueOffset: 0);
        db.Put([1], [1]);
        await Assert.That(await stale.TryCommitAsync()).IsFalse();
        await Assert.That(stale.CommitSequence).IsNull();
        await Assert.That(stale.Conflict!.Kind).IsEqualTo(MvccConflictKind.WrittenKey);
        using var empty = db.BeginTransaction();
        await empty.CommitAsync();
        await Assert.That(empty.CommitSequence).IsNull();
        using var ordinary = db.BeginTransaction();
        ordinary.Put([2], []);
        await ordinary.CommitAsync();
        await Assert.That(ordinary.CommitSequence).IsEqualTo(2L);
    }

    [Test]
    [Arguments(1, 0)]
    [Arguments(8, 0)]
    [Arguments(1, 1)]
    [Arguments(8, 1)]
    [Arguments(1, 2)]
    [Arguments(8, 2)]
    public async Task PublishedDependenciesValidateResolvedKeys(int groupSize, int dependency)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(groupSize));
        using var tx = db.BeginTransaction();
        var key = Key(2);
        if (dependency == 0)
        {
            tx.PutVersionstamped(Key(0), [], keyOffset: 1);
        }
        else
        {
            if (dependency == 1) await tx.GetForUpdateRawAsync(key, Memory<byte>.Empty);
            else tx.TrackReadRange(new OrderedReadRange(key, true, key, true));
            tx.PutVersionstamped([99], new byte[8], valueOffset: 0);
        }
        db.Put(key, [1]);
        await Assert.That(await tx.TryCommitAsync()).IsFalse();
        await Assert.That(tx.CommitSequence).IsNull();
        await Assert.That(tx.Conflict!.Kind).IsEqualTo(dependency switch
        {
            0 => MvccConflictKind.WrittenKey,
            1 => MvccConflictKind.TrackedPointRead,
            _ => MvccConflictKind.TrackedRange
        });
        await Assert.That(tx.Conflict.Key.Span.SequenceEqual(key)).IsTrue();
        await Assert.That(db.PublishedSequence).IsEqualTo(1L);
    }

    [Test]
    public async Task PrefixKeyspacePreservesPrefixAndUsesFullKeyOffsets()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(1));
        using var tx = db.BeginTransaction();
        var keyspace = new PrefixKeyspace([71]);
        await Assert.That(() => keyspace.PutVersionstamped(tx, Key(0), [], keyOffset: 0))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => keyspace.PutVersionstamped(tx, new byte[10], [], keyOffset: 1))
            .Throws<ArgumentException>();
        keyspace.PutVersionstamped(tx, Key(0), [], keyOffset: 1);
        await tx.CommitAsync();
        await Assert.That(await keyspace.GetRawAsync(db, Key(1), Memory<byte>.Empty)).IsEqualTo(0);
        using var emptyPrefix = db.BeginTransaction();
        new PrefixKeyspace([]).PutVersionstamped(emptyPrefix, new byte[8], [], keyOffset: 0);
        await emptyPrefix.CommitAsync();
        await Assert.That(emptyPrefix.CommitSequence).IsEqualTo(2L);
    }

    [Test]
    [Arguments(1, 0)]
    [Arguments(8, 0)]
    [Arguments(1, 256)]
    [Arguments(8, 256)]
    public async Task ResolvedDuplicateKeysAreRejectedBeforeWriting(int groupSize, int indexedEntries)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(groupSize));
        using var tx = db.BeginTransaction();
        tx.PutVersionstamped(Key(10), [1], keyOffset: 1);
        tx.PutVersionstamped(Key(20), [2], keyOffset: 1);
        for (var i = 0; i < indexedEntries; i++) tx.Put(Key(100, (byte)i), []);
        await Assert.That(async () => await tx.CommitAsync()).Throws<ArgumentException>();
        await Assert.That(tx.CommitSequence).IsNull();
        await Assert.That(db.PublishedSequence).IsEqualTo(0L);
        await Assert.That(db.Health).IsEqualTo(StorageHealth.Healthy);
    }

    [Test]
    public async Task InvalidOffsetsAndByteLimitsLeavePendingWorkUnchanged()
    {
        using var folder = TempFolder.Create();
        var options = Options(1);
        options.MvccTransactionMaxBytes = 46;
        await using var db = await MvccStorage.OpenAsync(folder, options);
        using var tx = db.BeginTransaction();
        tx.PutVersionstamped(Key(0), new byte[8], keyOffset: 1);
        await Assert.That(() => tx.PutVersionstamped(Key(0), [], keyOffset: -2))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => tx.PutVersionstamped(Key(0), [], keyOffset: int.MaxValue))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => tx.PutVersionstamped(Key(0), [], valueOffset: 0))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => tx.PutVersionstamped(Key(0), []))
            .Throws<ArgumentException>();
        await Assert.That(() => tx.PutVersionstamped(Key(0), new byte[9], valueOffset: 1))
            .Throws<TransactionLimitException>();
        await Assert.That(tx.BufferedBytes).IsEqualTo(46L);
        await tx.CommitAsync();
        await Assert.That(await db.GetRawAsync(Key(1), new byte[8])).IsEqualTo(8);
    }

    [Test]
    [Arguments(1)]
    [Arguments(8)]
    public async Task CancellationAndPublicationFailureDoNotReturnStamp(int groupSize)
    {
        using var folder = TempFolder.Create();
        var options = Options(groupSize);
        var fail = false;
        options.FaultInjector = point =>
        {
            if (fail && point == StorageFaultPoint.MvccBeforePublication) throw new IOException("Publication failure.");
        };
        await using (var db = await MvccStorage.OpenAsync(folder, options))
        {
            using var canceled = db.BeginTransaction();
            canceled.PutVersionstamped(Key(0), [], keyOffset: 1);
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            await Assert.That(async () => await canceled.CommitAsync(cancellation.Token)).Throws<OperationCanceledException>();
            await Assert.That(canceled.CommitSequence).IsNull();
            await Assert.That(db.PublishedSequence).IsEqualTo(0L);
            using var tx = db.BeginTransaction();
            tx.PutVersionstamped(Key(0), [42], keyOffset: 1);
            tx.Put([1], [43]);
            fail = true;
            await Assert.That(async () => await tx.CommitAsync()).Throws<StorageOperationException>();
            await Assert.That(tx.CommitSequence).IsNull();
        }
        fail = false;
        await using var reopened = await MvccStorage.OpenAsync(folder, options);
        var value = new byte[1];
        await Assert.That(await reopened.GetRawAsync(Key(1), value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)42);
        await Assert.That(await reopened.GetRawAsync([1], value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)43);
    }

    [Test]
    public async Task WarmedResolutionAndIndexRebuildAllocateNothing()
    {
        using var buffer = new MvccWriteBuffer();
        for (var i = 0; i < 256; i++) buffer.AddMutation(Key(0, (byte)i), new byte[8], false, 1, 0);
        for (var i = 1; i <= 100; i++) buffer.ResolveVersionstamps(i);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 101; i <= 1100; i++) buffer.ResolveVersionstamps(i);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
        await Assert.That(buffer.FindMutation(Key(1100, 255))).IsEqualTo(255);
    }
}
