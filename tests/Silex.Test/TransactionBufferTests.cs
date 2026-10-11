using System.Buffers.Binary;
using System.IO.Hashing;
using System.Runtime.CompilerServices;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class TransactionBufferTests
{
    private static StorageOptions Options(bool group = false) => new()
    {
        UseWriteAheadLog = group,
        SyncWriteAheadLogToDisk = group,
        MvccCommitGroupSize = group ? 8 : 1,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
    };

    [Test]
    public async Task AccountingDescriptorSizesMatchTheirRepresentations()
    {
        await Assert.That(Unsafe.SizeOf<MvccMutation>()).IsEqualTo(28);
        await Assert.That(Unsafe.SizeOf<MvccKey>()).IsEqualTo(8);
        await Assert.That(Unsafe.SizeOf<MvccUndo>()).IsEqualTo(32);
        await Assert.That(Unsafe.SizeOf<MvccCheckpoint>()).IsEqualTo(24);
    }

    [Test]
    public async Task CollisionChainsEmptyKeysAndIndexRebuilds()
    {
        using var buffer = new MvccWriteBuffer(indexThreshold: 0);
        List<byte[]> keys = [[]];
        // Exercise full collision chains, not just different hashes in different buckets.
        for (var value = 0; keys.Count < 100; value++)
        {
            var key = Key(value);
            if ((XxHash3.HashToUInt64(key) & 255) == 0) keys.Add(key);
        }
        foreach (var key in keys.Take(40))
        {
            buffer.AddMutation(key, key, false);
            buffer.TrackRead(key);
        }
        var marker = buffer.CreateSavepoint();
        foreach (var key in keys.Skip(40))
        {
            buffer.AddMutation(key, key, false);
            buffer.TrackRead(key);
        }
        for (var repetition = 0; repetition < 3; repetition++)
        {
            foreach (var key in keys)
            {
                await Assert.That(buffer.FindMutation(key)).IsGreaterThanOrEqualTo(0);
                buffer.TrackRead(key);
            }
            await Assert.That(buffer.TrackedReadCount).IsEqualTo(100);
            buffer.RollbackToSavepoint(marker);
            foreach (var key in keys.Skip(40))
            {
                await Assert.That(buffer.FindMutation(key)).IsEqualTo(-1);
                await Assert.That(buffer.FindTrackedRead(key)).IsEqualTo(-1);
                buffer.AddMutation(key, key, false);
                buffer.TrackRead(key);
            }
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LimitRejectionsLeaveTransactionAndStoreHealthy(bool group)
    {
        using var folder = TempFolder.Create();
        var options = Options(group);
        options.MvccTransactionMaxBytes = 88;
        options.MvccTransactionMaxMutations = 1;
        options.MvccTransactionMaxTrackedReads = 1;
        options.MvccTransactionMaxSavepoints = 1;
        await using var db = await MvccStorage.OpenAsync(folder, options);
        options.MvccTransactionMaxMutations = 0; // Store-open policy is captured, not a mutable reference.
        using var tx = db.BeginTransaction();
        tx.Put([1], [1, 2, 3]);
        await Assert.That(tx.BufferedBytes).IsEqualTo(32L);
        await Assert.That(() => tx.Put([2], [2])).Throws<TransactionLimitException>();
        await Assert.That(() => tx.Delete([2])).Throws<TransactionLimitException>();
        var marker = tx.CreateSavepoint();
        await Assert.That(tx.BufferedBytes).IsEqualTo(56L);
        await Assert.That(() => tx.CreateSavepoint()).Throws<TransactionLimitException>();
        await Assert.That(() => tx.Put([1], [9])).Throws<TransactionLimitException>(); // 89 bytes.
        await Assert.That(() => tx.Put([1], new byte[1000])).Throws<TransactionLimitException>();
        await Assert.That(tx.BufferedBytes).IsEqualTo(56L);
        var destination = new byte[3];
        await Assert.That(await tx.GetRawAsync([1], destination)).IsEqualTo(3);
        await Assert.That(destination).IsEquivalentTo(new byte[] { 1, 2, 3 }, CollectionOrdering.Matching);
        await tx.GetForUpdateRawAsync([1], destination); // Pending key adds no tracked-read descriptor.
        await Assert.That(tx.TrackedReadCount).IsEqualTo(0);
        await tx.GetForUpdateRawAsync([3], destination);
        await tx.GetForUpdateRawAsync([3], destination);
        await Assert.That(tx.BufferedBytes).IsEqualTo(65L);
        await Assert.That(async () => await tx.GetForUpdateRawAsync([4], destination))
            .Throws<TransactionLimitException>();
        await Assert.That(tx.TrackedReadCount).IsEqualTo(1);
        tx.RollbackToSavepoint(marker);
        await Assert.That(tx.TrackedReadCount).IsEqualTo(0);
        await Assert.That(tx.BufferedBytes).IsEqualTo(56L);
        tx.Delete([1]); // Undo-only change fits exactly at 88 bytes.
        await Assert.That(tx.BufferedBytes).IsEqualTo(88L);
        tx.RollbackToSavepoint(marker);
        tx.ReleaseSavepoint(marker);
        tx.Put([1], [8]);
        await Assert.That(tx.MutationCount).IsEqualTo(1);
        await Assert.That(tx.BufferedBytes).IsEqualTo(32L); // Retained arena, not only live value bytes.
        await tx.CommitAsync();
        await Assert.That(db.Health).IsEqualTo(StorageHealth.Healthy);
        db.Put([2], [7]);
        await Assert.That(await db.GetRawAsync([1], destination)).IsEqualTo(1);
        await Assert.That(destination[0]).IsEqualTo((byte)8);
        await Assert.That(() => tx.BufferedBytes).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task ByteBudgetIncludesObsoleteBytesUndoAndCheckpointDescriptors()
    {
        using var buffer = new MvccWriteBuffer(limits: new TransactionLimits(106, 10, 10, 10));
        buffer.AddMutation([1], new byte[10], false);
        buffer.AddMutation([2], [], true);
        await Assert.That(buffer.BufferedBytes).IsEqualTo(68L);
        buffer.SetValue(0, new byte[11], false); // Non-tail growth retains the original value.
        await Assert.That(buffer.BufferedBytes).IsEqualTo(79L);
        var marker = buffer.CreateSavepoint();
        await Assert.That(buffer.BufferedBytes).IsEqualTo(103L);
        await Assert.That(() => buffer.TrackRead([])).Throws<TransactionLimitException>();
        await Assert.That(() => buffer.SetValue(0, [], true)).Throws<TransactionLimitException>();
        buffer.RollbackToSavepoint(marker);
        buffer.ReleaseSavepoint(marker);
        await Assert.That(buffer.BufferedBytes).IsEqualTo(79L);
        buffer.SetValue(0, [], true);
        buffer.SetValue(0, new byte[11], false); // Capacity reuse after deletion.
        await Assert.That(buffer.BufferedBytes).IsEqualTo(79L);
    }

    [Test]
    public async Task ZeroBudgetsPermitEmptyTransactionsAndNegativeBudgetsAreRejected()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.MvccTransactionMaxBytes = 0;
        options.MvccTransactionMaxMutations = 0;
        options.MvccTransactionMaxTrackedReads = 0;
        options.MvccTransactionMaxSavepoints = 0;
        await using (var db = await MvccStorage.OpenAsync(folder, options))
        {
            using var tx = db.BeginTransaction();
            await Assert.That(() => tx.Put([], [])).Throws<TransactionLimitException>();
            await Assert.That(() => tx.Delete([])).Throws<TransactionLimitException>();
            await Assert.That(() => tx.CreateSavepoint()).Throws<TransactionLimitException>();
            await Assert.That(async () => await tx.GetForUpdateRawAsync([], new byte[1]))
                .Throws<TransactionLimitException>();
            await Assert.That(await tx.GetRawAsync([], new byte[1])).IsEqualTo(-1);
            await tx.CommitAsync();
        }
        for (var resource = 0; resource < 4; resource++)
        {
            options = Options();
            if (resource == 0) options.MvccTransactionMaxBytes = -1;
            if (resource == 1) options.MvccTransactionMaxMutations = -1;
            if (resource == 2) options.MvccTransactionMaxTrackedReads = -1;
            if (resource == 3) options.MvccTransactionMaxSavepoints = -1;
            await Assert.That(async () => await MvccStorage.OpenAsync(folder, options))
                .Throws<ArgumentOutOfRangeException>();
        }
    }

    [Test]
    public async Task IndexedCountBudgetsRestoreCapacityAfterRollback()
    {
        using var buffer = new MvccWriteBuffer(limits: new TransactionLimits(long.MaxValue, 65, 65, 2));
        var root = buffer.CreateSavepoint();
        for (var i = 0; i < 65; i++)
        {
            buffer.AddMutation(Key(i), [], false);
            buffer.TrackRead(Key(i));
        }
        await Assert.That(() => buffer.AddMutation(Key(65), [], false)).Throws<TransactionLimitException>();
        await Assert.That(() => buffer.TrackRead(Key(65))).Throws<TransactionLimitException>();
        var bytes = buffer.BufferedBytes;
        buffer.TrackRead(Key(64));
        buffer.SetValue(buffer.FindMutation(Key(64)), [], true);
        await Assert.That(buffer.BufferedBytes).IsEqualTo(bytes + 32);
        buffer.RollbackToSavepoint(root);
        await Assert.That(buffer.BufferedBytes).IsEqualTo(24L);
        await Assert.That(buffer.MutationCount).IsEqualTo(0);
        await Assert.That(buffer.TrackedReadCount).IsEqualTo(0);
        buffer.AddMutation(Key(65), [], false);
        buffer.TrackRead(Key(65));
        await Assert.That(buffer.FindMutation(Key(65))).IsEqualTo(0);
        await Assert.That(buffer.FindTrackedRead(Key(65))).IsEqualTo(0);
        await Assert.That(buffer.FindMutation(Key(64))).IsEqualTo(-1);
    }

    [Test]
    public async Task LargeDistinctKeysAreIndexedWithoutAllocatingOnWarmedReads()
    {
        using var buffer = new MvccWriteBuffer();
        var keys = Enumerable.Range(0, 65536).Select(Key).ToArray();
        foreach (var key in keys)
        {
            if (buffer.FindMutation(key) >= 0) throw new InvalidOperationException("Unexpected duplicate.");
            buffer.AddMutation(key, key, false);
            buffer.TrackRead(key);
        }
        Probe();
        var before = GC.GetAllocatedBytesForCurrentThread();
        var found = Probe();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(found).IsEqualTo(keys.Length);
        await Assert.That(buffer.TrackedReadCount).IsEqualTo(keys.Length);
        await Assert.That(allocated).IsEqualTo(0L);

        int Probe()
        {
            var found = 0;
            foreach (var key in keys)
            {
                if (buffer.FindMutation(key) >= 0) found++;
                buffer.TrackRead(key);
            }
            return found;
        }
    }

    [Test]
    public async Task WarmedOrderedEnumerationAndInvalidationAllocateNothing()
    {
        using var buffer = new MvccWriteBuffer();
        for (var i = 255; i >= 0; i--) buffer.AddMutation(Key(i), [], false);
        for (var i = 0; i < 100; i++) Cycle();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) Cycle();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);

        void Cycle()
        {
            var marker = buffer.CreateSavepoint();
            buffer.AddMutation([255], [], true);
            var order = buffer.OrderedMutations();
            if (order.Length != 257 || buffer.GetMutationKey(order[^1]).Span[0] != 255)
                throw new InvalidOperationException("Incorrect ordered index.");
            buffer.RollbackToSavepoint(marker);
            buffer.ReleaseSavepoint(marker);
            order = buffer.OrderedMutations();
            if (order.Length != 256 || order[0] != 255)
                throw new InvalidOperationException("Incorrect restored ordering.");
        }
    }

    [Test]
    public async Task IndexedBuffersAndSavepointCyclesAllocateNothingAfterWarmup()
    {
        using var buffer = new MvccWriteBuffer();
        var keys = Enumerable.Range(0, 256).Select(Key).ToArray();
        foreach (var key in keys)
        {
            buffer.AddMutation(key, key, false);
            buffer.TrackRead(key);
        }
        for (var i = 0; i < 100; i++) Cycle();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) Cycle();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);

        void Cycle()
        {
            var marker = buffer.CreateSavepoint();
            foreach (var key in keys)
            {
                buffer.SetValue(buffer.FindMutation(key), key, false);
                buffer.TrackRead(key);
            }
            buffer.AddMutation([255], [1], false);
            buffer.TrackRead([255]);
            buffer.RollbackToSavepoint(marker);
            buffer.ReleaseSavepoint(marker);
        }
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LargeFrozenCursorSurvivesRollbackReleaseAndCommit(bool reverse)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        using var tx = db.BeginTransaction();
        for (var i = 255; i >= 0; i--) tx.Put(Key(i), Key(i));
        var marker = tx.CreateSavepoint();
        for (var i = 0; i < 256; i += 2) tx.Delete(Key(i));
        var range = new OrderedReadRange(Key(30), false, Key(230), false);
        using var frozen = tx.CreateOrderedReadCursor(range,
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        tx.RollbackToSavepoint(marker);
        tx.ReleaseSavepoint(marker);
        tx.Put(Key(256), Key(256));
        await tx.CommitAsync();
        var actual = new List<int>();
        while (true)
        {
            using var batch = await frozen.ReadBatchAsync(7, 56);
            for (var i = 0; i < batch.Count; i++)
            {
                actual.Add(BinaryPrimitives.ReadInt32BigEndian(batch[i].Key.Span));
                await Assert.That(batch[i].Key.ToArray()).IsEquivalentTo(
                    batch[i].Value.ToArray(), CollectionOrdering.Matching);
            }
            if (batch.IsExhausted) break;
        }
        var expected = Enumerable.Range(31, 199).Where(i => i % 2 == 1);
        await Assert.That(actual).IsEquivalentTo(reverse ? expected.Reverse() : expected,
            CollectionOrdering.Matching);
    }

    private static byte[] Key(int value)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, value);
        return key;
    }
}
