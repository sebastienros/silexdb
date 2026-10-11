namespace Silex.Test;

public class TransactionRangeDeleteTests
{
    private static StorageOptions Options(bool group = false, Action<StorageOptions>? configure = null)
    {
        var options = new StorageOptions
        {
            UseWriteAheadLog = group,
            SyncWriteAheadLogToDisk = group,
            MvccCommitGroupSize = group ? 8 : 1,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
        };
        configure?.Invoke(options);
        return options;
    }

    private static OrderedReadRange Range(byte[]? lower, bool lowerInclusive, byte[]? upper, bool upperInclusive) =>
        new(lower is null ? null : (ReadOnlyMemory<byte>?)lower, lowerInclusive,
            upper is null ? null : (ReadOnlyMemory<byte>?)upper, upperInclusive);

    private static async Task<List<byte[]>> KeysAsync(MvccStorage db)
    {
        using var snapshot = db.CreateSnapshot();
        return await KeysAsync(snapshot);
    }

    private static async Task<List<byte[]>> KeysAsync(MvccSnapshot snapshot)
    {
        var keys = new List<byte[]>();
        using var cursor = snapshot.CreateOrderedReadCursor();
        while (true)
        {
            using var batch = await cursor.ReadBatchAsync(100, 1 << 20);
            for (var i = 0; i < batch.Count; i++) keys.Add(batch[i].Key.ToArray());
            if (batch.IsExhausted) return keys;
        }
    }

    private static void Seed(MvccStorage db)
    {
        for (byte i = 0; i < 10; i++) db.Put([i], [i]);
        db.Put([5, 0], [9]);
    }

    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    public async Task DeletesBinaryBoundsFromMemtableOrSstWithoutAffectingOldSnapshots(bool group, bool sst)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        Seed(db);
        if (sst) await db.FlushAndCompactAsync();
        using var old = db.CreateSnapshot();

        using var tx = db.BeginTransaction();
        // [2, 5) deletes 2,3,4 and nothing else; [5] inclusive upper keeps [5,0].
        var result = await tx.DeleteRangeAsync(Range([2], true, [5], false), 100);
        await Assert.That(result.IsComplete).IsTrue();
        await Assert.That(result.DeletedCount).IsEqualTo(3);
        result = await tx.DeleteRangeAsync(Range([6], false, [8], true), 100);
        await Assert.That(result.DeletedCount).IsEqualTo(2);
        result = await tx.DeleteRangeAsync(Range([5], true, [5, 0], true), 100);
        await Assert.That(result.DeletedCount).IsEqualTo(2);
        await Assert.That((await KeysAsync(db)).Count).IsEqualTo(11); // nothing visible before commit
        await tx.CommitAsync();

        var keys = await KeysAsync(db);
        await Assert.That(keys.Select(k => Convert.ToHexString(k)).ToArray())
            .IsEquivalentTo(new[] { "00", "01", "06", "09" });
        await Assert.That((await KeysAsync(old)).Count).IsEqualTo(11);
        var buffer = new byte[1];
        await Assert.That(await old.GetRawAsync([3], buffer)).IsEqualTo(1);
    }

    [Test]
    public async Task BoundedBatchesReportResumeRangeAndCompleteExactly()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        for (byte i = 0; i < 10; i++) db.Put([i], [i]);
        var range = Range(null, true, null, true);
        var total = 0;
        var rounds = 0;
        while (true)
        {
            using var tx = db.BeginTransaction();
            var result = await tx.DeleteRangeAsync(range, 5 - rounds); // 5 then 4 ... partial at first
            total += result.DeletedCount;
            await tx.CommitAsync();
            rounds++;
            if (result.IsComplete) break;
            await Assert.That(result.DeletedCount).IsGreaterThan(0);
            range = result.Remaining!;
        }
        await Assert.That(total).IsEqualTo(10);
        await Assert.That((await KeysAsync(db)).Count).IsEqualTo(0);

        using var exact = db.BeginTransaction();
        db.Put([1], [1]);
        using var tx2 = db.BeginTransaction();
        var r = await tx2.DeleteRangeAsync(Range(null, true, null, true), 1);
        await Assert.That(r.IsComplete).IsTrue(); // exactly maxEntries keys and nothing remaining
        await Assert.That(r.DeletedCount).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ConcurrentInsertIntoScannedGapConflictsButRemainingSuffixDoesNot(bool group)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        db.Put([10], [1]);
        db.Put([20], [1]);
        db.Put([30], [1]);

        using var gap = db.BeginTransaction();
        await gap.DeleteRangeAsync(Range([0], true, [40], true), 2);
        db.Put([15], [1]); // inside processed span (10..20)
        await Assert.That(await gap.TryCommitAsync()).IsFalse();
        await Assert.That(gap.Conflict).IsNotNull();

        using var suffix = db.BeginTransaction();
        var result = await suffix.DeleteRangeAsync(Range([0], true, [40], true), 2);
        await Assert.That(result.IsComplete).IsFalse();
        db.Put([35], [1]); // after last processed key, in Remaining
        await Assert.That(await suffix.TryCommitAsync()).IsTrue();

        using var complete = db.BeginTransaction();
        await complete.DeleteRangeAsync(Range([0], true, [40], true), 100);
        db.Put([39], [1]);
        await Assert.That(await complete.TryCommitAsync()).IsFalse();
    }

    [Test]
    public async Task SeesOwnPendingWritesAndSkipsOwnDeletes()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put([1], [1]);
        db.Put([2], [1]);
        using var tx = db.BeginTransaction();
        tx.Put([3], [1]);
        tx.Delete([1]);
        var result = await tx.DeleteRangeAsync(new OrderedReadRange(), 10);
        await Assert.That(result.DeletedCount).IsEqualTo(2); // 2 and pending 3
        await tx.CommitAsync();
        await Assert.That((await KeysAsync(db)).Count).IsEqualTo(0);
    }

    [Test]
    public async Task LimitsAndCancellationRestoreTransactionState()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(configure: o => o.MvccTransactionMaxMutations = 3));
        for (byte i = 0; i < 6; i++) db.Put([i], [i]);

        using var tx = db.BeginTransaction();
        tx.Put([100], [1]);
        TransactionLimitException? limit = null;
        try { await tx.DeleteRangeAsync(Range([0], true, [5], true), 6); }
        catch (TransactionLimitException e) { limit = e; }
        await Assert.That(limit).IsNotNull();
        await Assert.That(tx.MutationCount).IsEqualTo(1);
        await Assert.That(tx.TrackedReadRangeCount).IsEqualTo(0);

        // A smaller bound fits and the transaction is still usable.
        var result = await tx.DeleteRangeAsync(Range([0], true, [5], true), 2);
        await Assert.That(result.DeletedCount).IsEqualTo(2);
        await Assert.That(tx.MutationCount).IsEqualTo(3);

        using var cancelled = new CancellationTokenSource();
        cancelled.Cancel();
        using var tx2 = db.BeginTransaction();
        await Assert.That(async () => await tx2.DeleteRangeAsync(new OrderedReadRange(), 1, cancelled.Token))
            .Throws<OperationCanceledException>();
        await Assert.That(tx2.MutationCount).IsEqualTo(0);
    }

    [Test]
    public async Task ReclaimsPhysicalVersionsOnlyAfterOldSnapshotsRelease()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        for (byte i = 0; i < 5; i++) db.Put([i], [i]);
        var old = db.CreateSnapshot();
        using (var tx = db.BeginTransaction())
        {
            await tx.DeleteRangeAsync(new OrderedReadRange(), 100);
            await tx.CommitAsync();
        }
        await db.CollectGarbageAsync();
        await Assert.That((await KeysAsync(old)).Count).IsEqualTo(5);
        old.Dispose();
        await db.CollectGarbageAsync();
        await Assert.That((await KeysAsync(db)).Count).IsEqualTo(0);
    }

    [Test]
    public async Task ArgumentsAndEmptyRangesAreValidated()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put([1], [1]);
        using var tx = db.BeginTransaction();
        await Assert.That(async () => await tx.DeleteRangeAsync(new OrderedReadRange(), 0)).Throws<ArgumentOutOfRangeException>();
        var result = await tx.DeleteRangeAsync(Range([2], true, [1], true), 1);
        await Assert.That(result.IsComplete).IsTrue();
        await Assert.That(result.DeletedCount).IsEqualTo(0);
        await Assert.That(tx.MutationCount).IsEqualTo(0);
        await Assert.That(tx.TrackedReadRangeCount).IsEqualTo(0);
    }
}
