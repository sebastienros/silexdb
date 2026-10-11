namespace Silex.Test;

public class TransactionRangeDependencyTests
{
    private static StorageOptions Options(bool group = false) => new()
    {
        UseWriteAheadLog = group,
        SyncWriteAheadLogToDisk = group,
        MvccCommitGroupSize = group ? 8 : 1,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
    };

    private static OrderedReadRange Range(byte[]? lower, bool lowerInclusive, byte[]? upper, bool upperInclusive) =>
        new(lower is null ? null : (ReadOnlyMemory<byte>?)lower, lowerInclusive,
            upper is null ? null : (ReadOnlyMemory<byte>?)upper, upperInclusive);

    private static async Task<bool> CommitAfterExternalWrite(bool group, Action<MvccStorage> seed, Action<MvccStorage> write,
        OrderedReadRange range, Func<MvccStorage, Task>? between = null)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        seed(db);
        using var tx = db.BeginTransaction();
        tx.TrackReadRange(range);
        tx.Put([200], [1]);
        write(db);
        if (between is not null) await between(db);
        return await tx.TryCommitAsync();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InsertUpdateDeleteAndGapsConflictButOutsideWritesDoNot(bool group)
    {
        var range = Range([10], true, [20], true);
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put([15], [1]), range)).IsFalse(); // empty range
        await Assert.That(await CommitAfterExternalWrite(group, d => d.Put([15], [1]), d => d.Put([15], [2]), range)).IsFalse();
        await Assert.That(await CommitAfterExternalWrite(group, d => d.Put([15], [1]), d => d.Delete([15]), range)).IsFalse();
        await Assert.That(await CommitAfterExternalWrite(group, d => { d.Put([11], [1]); d.Put([19], [1]); }, d => d.Put([15], [1]), range)).IsFalse(); // gap
        await Assert.That(await CommitAfterExternalWrite(group, d => d.Put([15], [1]), d => { d.Put([9], [1]); d.Put([21], [1]); d.Put([10, 0], [1]); }, range)).IsFalse(); // [10,0] > [10]
        await Assert.That(await CommitAfterExternalWrite(group, d => d.Put([15], [1]), d => { d.Put([9], [1]); d.Put([21], [1]); d.Put([9, 255], [1]); }, range)).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InclusiveExclusiveAndUnboundedBinaryBoundaries(bool group)
    {
        byte[] a = [1, 0, 2], b = [1, 0, 3];
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put(a, [1]), Range(a, true, b, false))).IsFalse();
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put(a, [1]), Range(a, false, b, true))).IsTrue();
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put(b, [1]), Range(a, true, b, false))).IsTrue();
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put(b, [1]), Range(a, false, b, true))).IsFalse();
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put([], [1]), Range(null, true, a, false))).IsFalse();
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put([2], [1]), Range(b, true, null, true))).IsFalse();
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put([255, 255], [1]), Range(b, true, null, true))).IsFalse();
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put([9], [1]), Range(a, true, a, false))).IsTrue(); // empty
        await Assert.That(await CommitAfterExternalWrite(group, _ => { }, d => d.Put(a, [1]), Range(a, true, a, true))).IsFalse();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GcOfIntermediateVersionsAndTombstonesDoesNotHideConflicts(bool group)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        db.Put([5], [1]);
        db.Put([6], [1]);
        using var tx = db.BeginTransaction();
        using var holder = db.BeginTransaction();
        tx.TrackReadRange(Range([5], true, [7], false));
        tx.Put([200], [1]);
        for (var i = 0; i < 3; i++) db.Put([5], [(byte)(i + 2)]);
        db.Delete([6]);
        db.Put([6], [9]);
        db.Delete([6]);
        await db.CollectGarbageAsync(compact: true);
        holder.Dispose();
        await db.CollectGarbageAsync(compact: true);
        await Assert.That(await tx.TryCommitAsync()).IsFalse();
        await Assert.That(tx.Conflict!.Kind).IsEqualTo(MvccConflictKind.TrackedRange);
        await Assert.That(tx.Conflict.Key.Span.SequenceEqual(new byte[] { 5 })).IsTrue();
        await Assert.That(tx.Conflict.ConflictingSequence).IsEqualTo(db.PublishedSequence - 3);
        await db.CollectGarbageAsync(compact: true);

        using var later = db.BeginTransaction();
        later.TrackReadRange(Range([5], true, [7], false));
        later.Put([200], [1]);
        await Assert.That(await later.TryCommitAsync()).IsTrue(); // tombstone for 6 predates the snapshot
    }

    [Test]
    public async Task OwnWritesAndReadOnlyTransactionsAndOrdinaryRangeReadsBehave()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        using var own = db.BeginTransaction();
        own.TrackReadRange(Range([1], true, [9], true));
        own.Put([5], [1]);
        await Assert.That(await own.TryCommitAsync()).IsTrue();

        using var readOnly = db.BeginTransaction();
        readOnly.TrackReadRange(Range([1], true, [9], true));
        db.Put([4], [1]);
        await Assert.That(await readOnly.TryCommitAsync()).IsFalse();

        using var untracked = db.BeginTransaction();
        using (var cursor = untracked.CreateOrderedReadCursor(Range([1], true, [9], true))) { }
        db.Put([6], [1]);
        untracked.Put([7], [1]);
        await Assert.That(await untracked.TryCommitAsync()).IsTrue();

        using var tx = db.BeginTransaction();
        await Assert.That(() => tx.TrackReadRange(null!)).Throws<ArgumentNullException>();
        tx.TrackReadRange(Range([2], false, [2], true));
        await Assert.That(tx.TrackedReadRangeCount).IsEqualTo(0);
    }

    [Test]
    public async Task GroupedMembersConflictWithAcceptedWritesAndRejectedMembersDoNotAffectLaterOnes()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group: true));
        using var reader = db.BeginTransaction();
        using var writer = db.BeginTransaction();
        using var other = db.BeginTransaction();
        reader.TrackReadRange(Range([10], true, [20], true));
        reader.Put([100], [1]);
        writer.Put([15], [1]);
        other.TrackReadRange(Range([30], true, [40], true));
        other.Put([101], [1]);
        var results = await Task.WhenAll(writer.TryCommitAsync().AsTask(), reader.TryCommitAsync().AsTask(),
            other.TryCommitAsync().AsTask());
        await Assert.That(results[0]).IsTrue();
        await Assert.That(results[1]).IsFalse();
        await Assert.That(results[2]).IsTrue();
        await Assert.That(await db.GetRawAsync([100], new byte[1])).IsEqualTo(-1);
    }

    [Test]
    public async Task SavepointsRollBackAndReleaseRangesAndBudgetsAreAccounted()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.MvccTransactionMaxTrackedReadRanges = 2;
        await using var db = await MvccStorage.OpenAsync(folder, options);
        using var tx = db.BeginTransaction();
        tx.TrackReadRange(Range([1], true, [2], true));
        var bytes = tx.BufferedBytes;
        await Assert.That(bytes).IsEqualTo(34L);
        var savepoint = tx.CreateSavepoint();
        tx.TrackReadRange(Range([5], true, [6], true));
        await Assert.That(tx.TrackedReadRangeCount).IsEqualTo(2);
        var failure = Assert.Throws<TransactionLimitException>(() => tx.TrackReadRange(Range([7], true, [8], true)));
        await Assert.That(failure.Resource).IsEqualTo("tracked read ranges");
        await Assert.That(tx.TrackedReadRangeCount).IsEqualTo(2);
        tx.RollbackToSavepoint(savepoint);
        await Assert.That(tx.TrackedReadRangeCount).IsEqualTo(1);
        await Assert.That(tx.BufferedBytes).IsEqualTo(bytes + 24);
        tx.ReleaseSavepoint(savepoint);
        tx.TrackReadRange(Range([1], true, [2], true)); // covered by the previous interval
        await Assert.That(tx.TrackedReadRangeCount).IsEqualTo(1);

        db.Put([5], [1]); // rolled-back interval no longer conflicts
        tx.Put([100], [1]);
        await Assert.That(await tx.TryCommitAsync()).IsTrue();

        using var kept = db.BeginTransaction();
        kept.TrackReadRange(Range([1], true, [2], true));
        var keep = kept.CreateSavepoint();
        kept.TrackReadRange(Range([8], true, [9], true));
        kept.ReleaseSavepoint(keep);
        db.Put([8], [1]);
        await Assert.That(await kept.TryCommitAsync()).IsFalse();
    }

    [Test]
    public async Task ZeroRangeBudgetIsValidAndNegativeIsRejected()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.MvccTransactionMaxTrackedReadRanges = 0;
        await using var db = await MvccStorage.OpenAsync(folder, options);
        using var tx = db.BeginTransaction();
        await Assert.That(() => tx.TrackReadRange(Range(null, true, null, true))).Throws<TransactionLimitException>();
        options.MvccTransactionMaxTrackedReadRanges = -1;
        using var other = TempFolder.Create();
        await Assert.That(async () => await MvccStorage.OpenAsync(other, options)).Throws<ArgumentOutOfRangeException>();
    }
}
