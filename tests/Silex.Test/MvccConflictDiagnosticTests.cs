namespace Silex.Test;

public class MvccConflictDiagnosticTests
{
    private static StorageOptions Options(bool group) => new()
    {
        UseWriteAheadLog = group,
        SyncWriteAheadLogToDisk = group,
        MvccCommitGroupSize = group ? 8 : 1,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
    };

    [Test]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 2)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    public async Task ReportsOwnedKeyCategorySequencesAndRange(bool group, int kind)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        byte[] key = [1, 0, 255];
        db.Put(key, [1]);
        using var tx = db.BeginTransaction();
        await Assert.That(tx.Conflict is null).IsTrue();
        if (kind == 0)
        {
            await tx.GetForUpdateRawAsync(key, new byte[1]);
            tx.Put(key, [2]); // Written dependency takes precedence.
            tx.TrackReadRange(new OrderedReadRange());
        }
        else if (kind == 1)
        {
            await tx.GetForUpdateRawAsync(key, new byte[1]);
            tx.TrackReadRange(new OrderedReadRange());
        }
        else
        {
            tx.TrackReadRange(new OrderedReadRange(key, false, new byte[] { 2 }, true));
            key = [1, 0, 255, 0]; // In the gap above the exclusive lower bound.
        }
        db.Delete(key);
        var expectedSequence = db.PublishedSequence;
        await Assert.That(await tx.TryCommitAsync()).IsFalse();
        var conflict = tx.Conflict!;
        await Assert.That(conflict.Kind).IsEqualTo((MvccConflictKind)kind);
        await Assert.That(conflict.Key.Span.SequenceEqual(key)).IsTrue();
        await Assert.That(conflict.SnapshotSequence).IsEqualTo(tx.Sequence);
        await Assert.That(conflict.ConflictingSequence).IsEqualTo(expectedSequence);
        await Assert.That(conflict.Range is not null).IsEqualTo(kind == 2);
        if (conflict.Range is { } range)
        {
            await Assert.That(range.Lower!.Value.Span.SequenceEqual(new byte[] { 1, 0, 255 })).IsTrue();
            await Assert.That(range.LowerInclusive).IsFalse();
            await Assert.That(range.Upper!.Value.Span.SequenceEqual(new byte[] { 2 })).IsTrue();
            await Assert.That(range.UpperInclusive).IsTrue();
        }
        tx.Dispose();
        Array.Fill(key, (byte)9);
        using var later = db.BeginTransaction();
        later.Put([8, 8, 8], [1]);
        await later.CommitAsync();
        await Assert.That(conflict.Key.Span[0]).IsEqualTo((byte)1);
        await Assert.That(ReferenceEquals(conflict, tx.Conflict)).IsTrue();
        await Assert.That(async () => await tx.TryCommitAsync()).Throws<ObjectDisposedException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ExceptionCarriesSameDiagnosticAndSupportsEmptyKeys(bool group)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        using var tx = db.BeginTransaction();
        await tx.GetForUpdateRawAsync([], Memory<byte>.Empty);
        db.Put([], [1]);
        MvccConflictException? failure = null;
        try { await tx.CommitAsync(); }
        catch (MvccConflictException exception) { failure = exception; }
        await Assert.That(failure is not null).IsTrue();
        await Assert.That(ReferenceEquals(failure!.Conflict, tx.Conflict)).IsTrue();
        await Assert.That(tx.Conflict!.Kind).IsEqualTo(MvccConflictKind.TrackedPointRead);
        await Assert.That(tx.Conflict.Key.Length).IsEqualTo(0);
        await Assert.That(new MvccConflictException().Conflict is null).IsTrue();
        await Assert.That(() => new MvccConflictException(null!)).Throws<ArgumentNullException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UnboundedRangeAndEmptyKeyStayDistinct(bool group)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        using var tx = db.BeginTransaction();
        tx.TrackReadRange(new OrderedReadRange(upper: ReadOnlyMemory<byte>.Empty));
        db.Put([], []);
        await Assert.That(await tx.TryCommitAsync()).IsFalse();
        var conflict = tx.Conflict!;
        await Assert.That(conflict.Kind).IsEqualTo(MvccConflictKind.TrackedRange);
        await Assert.That(conflict.Key.Length).IsEqualTo(0);
        await Assert.That(conflict.Range!.Lower.HasValue).IsFalse();
        await Assert.That(conflict.Range.Upper.HasValue).IsTrue();
        await Assert.That(conflict.Range.Upper!.Value.Length).IsEqualTo(0);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SavepointRollbackRemovesDependenciesWhileReleaseKeepsThem(bool group)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        using var tx = db.BeginTransaction();
        await tx.GetForUpdateRawAsync([1], Memory<byte>.Empty);
        var savepoint = tx.CreateSavepoint();
        tx.Put([2], [1]);
        await tx.GetForUpdateRawAsync([3], Memory<byte>.Empty);
        tx.TrackReadRange(new OrderedReadRange(new byte[] { 4 }, upper: new byte[] { 5 }));
        tx.RollbackToSavepoint(savepoint);
        tx.ReleaseSavepoint(savepoint);
        var kept = tx.CreateSavepoint();
        tx.TrackReadRange(new OrderedReadRange(new byte[] { 6 }, upper: new byte[] { 7 }));
        tx.ReleaseSavepoint(kept);
        for (byte key = 2; key <= 6; key++) db.Put([key], [1]);
        await Assert.That(await tx.TryCommitAsync()).IsFalse();
        await Assert.That(tx.Conflict!.Kind).IsEqualTo(MvccConflictKind.TrackedRange);
        await Assert.That(tx.Conflict.Key.Span.SequenceEqual(new byte[] { 6 })).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SuccessCancellationAndStorageFailureHaveNoConflict(bool group)
    {
        using var folder = TempFolder.Create();
        var options = Options(group);
        var failWrites = false;
        options.FaultInjector = point =>
        {
            if (failWrites && point == StorageFaultPoint.BatchBeforeApply) throw new IOException("Injected failure.");
        };
        await using var db = await MvccStorage.OpenAsync(folder, options);
        using var successful = db.BeginTransaction();
        await successful.GetRawAsync([1], Memory<byte>.Empty);
        db.Put([1], [1]);
        await Assert.That(await successful.TryCommitAsync()).IsTrue();
        await Assert.That(successful.Conflict is null).IsTrue();

        using var canceled = db.BeginTransaction();
        canceled.Put([1], [2]);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await canceled.TryCommitAsync(cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(canceled.Conflict is null).IsTrue();
        await Assert.That(() => canceled.Put([2], [1])).Throws<ObjectDisposedException>();

        using var failed = db.BeginTransaction();
        failed.Put([2], [1]);
        failWrites = true;
        await Assert.That(async () => await failed.TryCommitAsync()).Throws<StorageOperationException>();
        await Assert.That(failed.Conflict is null).IsTrue();
        failWrites = false;
    }
}
