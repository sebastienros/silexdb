using TUnit.Assertions.Enums;

namespace Silex.Test;

public class TransactionSavepointTests
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
    [Arguments(false)]
    [Arguments(true)]
    public async Task RestoresOverwrittenValuesTombstonesAndNewKeys(bool group)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        db.Put([1], [99]);
        db.Put([5], [55]);
        using var tx = db.BeginTransaction();
        tx.Put([1], [1, 2, 3]);
        tx.Put([2], []);
        tx.Delete([3]);
        tx.Put([], [4]);
        var savepoint = tx.CreateSavepoint();
        for (var i = 0; i < 3; i++)
        {
            tx.Put([1], [7]); // Shrinking must not overwrite checkpoint bytes.
            tx.Put([1], new byte[10000]);
            tx.Delete([1]);
            tx.Put([2], [2]);
            tx.Put([3], [3]);
            tx.Delete([]);
            tx.Put([4], [4]);
            tx.Delete([5]);
            tx.RollbackToSavepoint(savepoint);
            await AssertValue(tx, [1], [1, 2, 3]);
            await AssertValue(tx, [2], []);
            await AssertValue(tx, [3], null);
            await AssertValue(tx, [], [4]);
            await AssertValue(tx, [4], null);
            await AssertValue(tx, [5], [55]);
        }
        tx.ReleaseSavepoint(savepoint);
        tx.Put([1], [8]);
        tx.Put([1], [9, 10]);
        await tx.CommitAsync();
        var bytes = new byte[3];
        await Assert.That(await db.GetRawAsync([1], bytes)).IsEqualTo(2);
        await Assert.That(bytes.Take(2)).IsEquivalentTo(new byte[] { 9, 10 }, CollectionOrdering.Matching);
        await Assert.That(await db.GetRawAsync([4], bytes)).IsEqualTo(-1);
        await Assert.That(await db.GetRawAsync([5], bytes)).IsEqualTo(1);
    }

    [Test]
    public async Task NestedMarkersReleaseOrderingAndInvalidHandles()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        using var tx = db.BeginTransaction();
        using var other = db.BeginTransaction();
        var root = tx.CreateSavepoint();
        tx.Put([1], [1]);
        var middle = tx.CreateSavepoint();
        var equalPosition = tx.CreateSavepoint();
        tx.Put([1], [2]);
        var child = tx.CreateSavepoint();
        tx.Put([2], [2]);
        await Assert.That(() => tx.RollbackToSavepoint(default)).Throws<ArgumentException>();
        await Assert.That(() => tx.ReleaseSavepoint(other.CreateSavepoint())).Throws<ArgumentException>();
        tx.ReleaseSavepoint(equalPosition);
        await Assert.That(() => tx.ReleaseSavepoint(child)).Throws<ArgumentException>();
        await Assert.That(() => tx.RollbackToSavepoint(equalPosition)).Throws<ArgumentException>();
        await AssertValue(tx, [1], [2]);
        await AssertValue(tx, [2], [2]);
        tx.RollbackToSavepoint(middle);
        await AssertValue(tx, [1], [1]);
        await AssertValue(tx, [2], null);
        var replacement = tx.CreateSavepoint();
        tx.Delete([1]);
        tx.RollbackToSavepoint(root);
        await Assert.That(() => tx.RollbackToSavepoint(replacement)).Throws<ArgumentException>();
        await Assert.That(() => tx.ReleaseSavepoint(middle)).Throws<ArgumentException>();
        await AssertValue(tx, [1], null);
        tx.ReleaseSavepoint(root);
        var next = tx.CreateSavepoint();
        await Assert.That(() => tx.RollbackToSavepoint(root)).Throws<ArgumentException>();
        tx.Put([3], [3]);
        tx.ReleaseSavepoint(next);
        await AssertValue(tx, [3], [3]);
    }

    [Test]
    [Arguments(false, "discard")]
    [Arguments(true, "discard")]
    [Arguments(false, "retain")]
    [Arguments(true, "retain")]
    [Arguments(false, "release")]
    [Arguments(true, "release")]
    [Arguments(false, "retrack")]
    [Arguments(true, "retrack")]
    public async Task TrackedReadRollbackPolicy(bool group, string behavior)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        db.Put([1], [1]);
        using var tx = db.BeginTransaction();
        var destination = new byte[1];
        if (behavior == "retain") await tx.GetForUpdateRawAsync([1], destination);
        var savepoint = tx.CreateSavepoint();
        await tx.GetForUpdateRawAsync([1], destination);
        await tx.GetForUpdateRawAsync([2], destination); // Missing reads are dependencies too.
        tx.Put([1], [9]);
        if (behavior == "release") tx.ReleaseSavepoint(savepoint);
        else tx.RollbackToSavepoint(savepoint);
        if (behavior == "retrack") await tx.GetForUpdateRawAsync([2], destination);
        db.Put([1], [2]);
        db.Put([2], [2]);
        tx.Put([3], [3]);
        await Assert.That(await tx.TryCommitAsync()).IsEqualTo(behavior == "discard");
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RollbackPreservesExactSnapshotAndFrozenCursorOverlays(bool reverse)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put([1], [1]);
        db.Put([3], [3]);
        using var tx = db.BeginTransaction();
        tx.Put([2], [2]);
        var marker = tx.CreateSavepoint();
        tx.Delete([1]);
        tx.Put([2], [22]);
        tx.Put([4], [4]);
        using var frozen = tx.CreateOrderedReadCursor(direction:
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        using var first = await frozen.ReadBatchAsync(1, 2);
        db.Put([1], [99]);
        db.Put([3], [99]);
        db.Put([5], [99]);
        tx.RollbackToSavepoint(marker);
        await AssertValue(tx, [1], [1]);
        await AssertValue(tx, [2], [2]);
        await AssertValue(tx, [3], [3]);
        await AssertValue(tx, [5], null);
        using var restored = tx.CreateOrderedReadCursor(direction:
            reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward);
        await Assert.That(restored.Sequence).IsEqualTo(tx.Sequence);
        tx.Put([2], [88]);
        await tx.CommitAsync();
        await db.CollectGarbageAsync(compact: true);
        var frozenValues = new List<byte> { first[0].Value.Span[0] };
        frozenValues.AddRange(await ReadValues(frozen));
        await Assert.That(frozenValues).IsEquivalentTo(
            reverse ? new byte[] { 4, 3, 22 } : new byte[] { 22, 3, 4 }, CollectionOrdering.Matching);
        await Assert.That(await ReadValues(restored)).IsEquivalentTo(
            reverse ? new byte[] { 3, 2, 1 } : new byte[] { 1, 2, 3 }, CollectionOrdering.Matching);
    }

    [Test]
    [Arguments("dispose")]
    [Arguments("commit")]
    [Arguments("conflict")]
    [Arguments("cancel")]
    [Arguments("failure")]
    public async Task CompletedTransactionsRejectAllSavepointOperations(string completion)
    {
        using var folder = TempFolder.Create();
        var options = Options();
        var fail = false;
        options.FaultInjector = point =>
        {
            if (fail && point == StorageFaultPoint.BatchBeforeApply) throw new IOException("Injected");
        };
        await using var db = await MvccStorage.OpenAsync(folder, options);
        using var tx = db.BeginTransaction();
        tx.Put([1], [1]);
        var marker = tx.CreateSavepoint();
        if (completion == "dispose") tx.Dispose();
        else if (completion == "conflict")
        {
            db.Put([1], [2]);
            await Assert.That(await tx.TryCommitAsync()).IsFalse();
        }
        else if (completion == "cancel")
        {
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            await Assert.That(async () => await tx.TryCommitAsync(cts.Token)).Throws<OperationCanceledException>();
        }
        else if (completion == "failure")
        {
            fail = true;
            await Assert.That(async () => await tx.TryCommitAsync()).Throws<StorageOperationException>();
        }
        else await tx.CommitAsync();
        await Assert.That(() => tx.CreateSavepoint()).Throws<ObjectDisposedException>();
        await Assert.That(() => tx.RollbackToSavepoint(marker)).Throws<ObjectDisposedException>();
        await Assert.That(() => tx.ReleaseSavepoint(marker)).Throws<ObjectDisposedException>();
        tx.Dispose();
    }

    [Test]
    [Arguments(20)]
    [Arguments(200)]
    public async Task RandomizedCheckpointsMatchCopiedWriteAndReadModel(int keyCount)
    {
        using var buffer = new MvccWriteBuffer();
        var random = new Random(9);
        var writes = new Dictionary<byte, byte[]?>();
        var reads = new HashSet<byte>();
        for (var i = 0; i < keyCount / 2; i++)
        {
            var key = (byte)i;
            buffer.AddMutation([key], [key], false);
            buffer.TrackRead([key]);
            writes.Add(key, [key]);
            reads.Add(key);
        }
        var checkpoints = new List<(MvccSavepoint Handle, Dictionary<byte, byte[]?> Writes, HashSet<byte> Reads)>();
        for (var step = 0; step < 1500; step++)
        {
            var range = new OrderedReadRange(new byte[] { (byte)(keyCount / 4) }, step % 2 == 0,
                new byte[] { (byte)(keyCount * 3 / 4) }, step % 3 == 0);
            using var frozen = OrderedWriteSnapshot.Create(buffer, range);
            var frozenModel = writes.Where(pair => !range.Before([pair.Key]) && !range.After([pair.Key]))
                .OrderBy(pair => pair.Key).ToArray();
            var key = (byte)random.Next(keyCount);
            var operation = random.Next(6);
            if (operation == 0 || checkpoints.Count == 0)
            {
                checkpoints.Add((buffer.CreateSavepoint(), new(writes), new(reads)));
            }
            else if (operation == 1 || operation == 2)
            {
                var index = random.Next(checkpoints.Count);
                var checkpoint = checkpoints[index];
                if (operation == 1)
                {
                    buffer.RollbackToSavepoint(checkpoint.Handle);
                    writes = new(checkpoint.Writes);
                    reads = new(checkpoint.Reads);
                    checkpoints.RemoveRange(index + 1, checkpoints.Count - index - 1);
                }
                else
                {
                    buffer.ReleaseSavepoint(checkpoint.Handle);
                    checkpoints.RemoveRange(index, checkpoints.Count - index);
                }
            }
            else if (operation == 3)
            {
                buffer.TrackRead([key]);
                reads.Add(key);
            }
            else
            {
                byte[]? value = operation == 4 ? new byte[random.Next(1024)] : null;
                if (value is not null) random.NextBytes(value);
                var index = buffer.FindMutation([key]);
                if (index < 0) buffer.AddMutation([key], value, value is null);
                else buffer.SetValue(index, value, value is null);
                writes[key] = value;
            }
            await Assert.That(buffer.MutationCount).IsEqualTo(writes.Count);
            foreach (var (expectedKey, expectedValue) in writes)
            {
                var index = buffer.FindMutation([expectedKey]);
                await Assert.That(index).IsGreaterThanOrEqualTo(0);
                var mutation = buffer.GetMutation(index);
                await Assert.That(mutation.IsTombstone).IsEqualTo(expectedValue is null);
                if (expectedValue is not null)
                    await Assert.That(buffer.GetValue(mutation).ToArray()).IsEquivalentTo(expectedValue, CollectionOrdering.Matching);
            }
            await Assert.That(buffer.TrackedReadCount).IsEqualTo(reads.Count);
            for (var i = 0; i < buffer.TrackedReadCount; i++)
                await Assert.That(reads.Contains(buffer.GetTrackedRead(i).Span[0])).IsTrue();
            var ordered = buffer.OrderedMutations().ToArray()
                .Select(i => buffer.GetMutationKey(i).Span[0]);
            await Assert.That(ordered).IsEquivalentTo(writes.Keys.Order(), CollectionOrdering.Matching);
            await Assert.That(frozen?.Count ?? 0).IsEqualTo(frozenModel.Length);
            for (var i = 0; i < frozenModel.Length; i++)
            {
                await Assert.That(frozen!.Key(i)[0]).IsEqualTo(frozenModel[i].Key);
                await Assert.That(frozen.IsTombstone(i)).IsEqualTo(frozenModel[i].Value is null);
                if (frozenModel[i].Value is { } value)
                    await Assert.That(frozen.Value(i).ToArray()).IsEquivalentTo(value, CollectionOrdering.Matching);
            }
        }
    }

    [Test]
    public async Task WarmedSmallSavepointCyclesAllocateNoManagedMemory()
    {
        using var buffer = new MvccWriteBuffer();
        byte[] key = [1];
        byte[] value = [1, 2, 3];
        buffer.AddMutation(key, value, false);
        for (var i = 0; i < 100; i++) Cycle();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) Cycle();
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);

        void Cycle()
        {
            var marker = buffer.CreateSavepoint();
            buffer.SetValue(0, value, false);
            buffer.RollbackToSavepoint(marker);
            buffer.ReleaseSavepoint(marker);
        }
    }

    private static async Task AssertValue(MvccTransaction tx, byte[] key, byte[]? expected)
    {
        var destination = new byte[10000];
        var length = await tx.GetRawAsync(key, destination);
        await Assert.That(length).IsEqualTo(expected?.Length ?? -1);
        if (expected is not null)
            await Assert.That(destination.Take(length)).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    private static async Task<List<byte>> ReadValues(OrderedReadCursor cursor)
    {
        var values = new List<byte>();
        while (true)
        {
            using var batch = await cursor.ReadBatchAsync(2, 4);
            for (var i = 0; i < batch.Count; i++) values.Add(batch[i].Value.Span[0]);
            if (batch.IsExhausted) return values;
        }
    }
}
