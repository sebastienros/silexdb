namespace Silex.Test;

public class ConditionalMutationTests
{
    private static StorageOptions Options(int groupSize = 1) => new()
    {
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
        SyncWriteAheadLogToDisk = groupSize > 1,
        MvccCommitGroupSize = groupSize
    };

    [Test]
    [Arguments(1)]
    [Arguments(8)]
    public async Task ConcurrentConditionalInsertsHaveOneWinner(int groupSize)
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options(groupSize));
        using var first = storage.BeginTransaction();
        using var second = storage.BeginTransaction();
        await Assert.That(await StageInsertAsync(first, [1], [10])).IsTrue();
        await Assert.That(await StageInsertAsync(second, [1], [20])).IsTrue();
        first.Put([2], [10]);
        second.Put([3], [20]);
        using var before = storage.CreateSnapshot();

        var commits = await Task.WhenAll(first.TryCommitAsync().AsTask(), second.TryCommitAsync().AsTask());

        await Assert.That(commits.Count(static committed => committed)).IsEqualTo(1);
        var loser = commits[0] ? second : first;
        await Assert.That(loser.Conflict!.Kind).IsEqualTo(MvccConflictKind.WrittenKey);
        await AssertValueAsync(storage, [1], commits[0] ? new byte[] { 10 } : [20]);
        await AssertValueAsync(storage, commits[0] ? new byte[] { 2 } : [3], commits[0] ? new byte[] { 10 } : [20]);
        await Assert.That(await storage.GetRawAsync(commits[0] ? new byte[] { 3 } : [2], new byte[1])).IsEqualTo(-1);
        await Assert.That(await before.GetRawAsync([1], new byte[1])).IsEqualTo(-1);
        await Assert.That(async () => await loser.TryCommitAsync()).Throws<ObjectDisposedException>();
    }

    [Test]
    [Arguments(1)]
    [Arguments(8)]
    public async Task ConcurrentCompareAndSetRejectsStaleReplacement(int groupSize)
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options(groupSize));
        storage.Put([1], [10]);
        await storage.FlushAndCompactAsync();
        using var first = storage.BeginTransaction();
        using var second = storage.BeginTransaction();
        await Assert.That(await StageCompareAndSetAsync(first, [1], [10], [11], new byte[1])).IsTrue();
        await Assert.That(await StageCompareAndSetAsync(second, [1], [10], [12], new byte[1])).IsTrue();
        first.Put([2], [11]);
        second.Put([3], [12]);

        var commits = await Task.WhenAll(first.TryCommitAsync().AsTask(), second.TryCommitAsync().AsTask());

        await Assert.That(commits.Count(static committed => committed)).IsEqualTo(1);
        await AssertValueAsync(storage, [1], commits[0] ? new byte[] { 11 } : [12]);
        await Assert.That(await storage.GetRawAsync(commits[0] ? new byte[] { 3 } : [2], new byte[1])).IsEqualTo(-1);
    }

    [Test]
    [Arguments(1)]
    [Arguments(8)]
    public async Task UnmodifiedGuardStillProtectsEntireBatch(int groupSize)
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options(groupSize));
        storage.Put([1], [10]);
        storage.Put([3], [30]);
        using var transaction = storage.BeginTransaction();
        var scratch = new byte[1];
        await Assert.That(await transaction.GetForUpdateRawAsync([1], scratch)).IsEqualTo(1);
        await Assert.That(scratch[0]).IsEqualTo((byte)10);
        transaction.Put([2], [20]);
        transaction.Delete([3]);
        storage.Put([1], [11]);

        await Assert.That(await transaction.TryCommitAsync()).IsFalse();
        await Assert.That(transaction.Conflict!.Kind).IsEqualTo(MvccConflictKind.TrackedPointRead);
        await Assert.That(transaction.Conflict.Key.Span.SequenceEqual(new byte[] { 1 })).IsTrue();
        await Assert.That(await storage.GetRawAsync([2], scratch)).IsEqualTo(-1);
        await AssertValueAsync(storage, [3], [30]);
    }

    [Test]
    public async Task AbsentUnmodifiedGuardRejectsDependentBatchAfterInsert()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        using var transaction = storage.BeginTransaction();
        await Assert.That(await transaction.GetForUpdateRawAsync([1], Memory<byte>.Empty)).IsEqualTo(-1);
        transaction.Put([2], [20]);
        storage.Put([1], []);

        await Assert.That(await transaction.TryCommitAsync()).IsFalse();
        await Assert.That(transaction.Conflict!.Kind).IsEqualTo(MvccConflictKind.TrackedPointRead);
        await Assert.That(await storage.GetRawAsync([1], Memory<byte>.Empty)).IsEqualTo(0);
        await Assert.That(await storage.GetRawAsync([2], new byte[1])).IsEqualTo(-1);
    }

    [Test]
    public async Task ComparisonDistinguishesMissingEmptyWrongLengthAndWrongBytes()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        storage.Put([1], []);
        storage.Put([2], [10, 20]);
        using var transaction = storage.BeginTransaction();
        var scratch = new byte[] { 10 };

        await Assert.That(await StageCompareAndSetAsync(transaction, [3], [], [30], scratch)).IsFalse();
        await Assert.That(await StageCompareAndSetAsync(transaction, [2], [10], [30], scratch)).IsFalse();
        await Assert.That(await StageCompareAndSetAsync(transaction, [2], [10, 21], [30], new byte[2])).IsFalse();
        await Assert.That(transaction.MutationCount).IsEqualTo(0);
        await Assert.That(await StageCompareAndSetAsync(transaction, [1], [], [30], Memory<byte>.Empty)).IsTrue();
        await transaction.CommitAsync();
        await AssertValueAsync(storage, [1], [30]);
        await AssertValueAsync(storage, [2], [10, 20]);
    }

    [Test]
    public async Task ConditionsReadPendingWritesAndSavepointRollbackRestoresThem()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        storage.Put([1], [10]);
        using var transaction = storage.BeginTransaction();
        var savepoint = transaction.CreateSavepoint();
        transaction.Delete([1]);
        await Assert.That(await StageInsertAsync(transaction, [1], [])).IsTrue();
        await Assert.That(await StageInsertAsync(transaction, [1], [20])).IsFalse();
        await Assert.That(await StageCompareAndSetAsync(transaction, [1], [], [20], new byte[1])).IsTrue();
        transaction.RollbackToSavepoint(savepoint);
        await Assert.That(transaction.MutationCount).IsEqualTo(0);
        await Assert.That(transaction.TrackedReadCount).IsEqualTo(0);
        await Assert.That(await StageCompareAndSetAsync(transaction, [1], [10], [30], new byte[1])).IsTrue();
        transaction.ReleaseSavepoint(savepoint);
        await transaction.CommitAsync();
        await AssertValueAsync(storage, [1], [30]);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InterveningChangesConflictEvenWhenConditionBecomesTrueAgain(bool insert)
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        if (!insert) storage.Put([1], [10]);
        using var transaction = storage.BeginTransaction();
        var staged = insert
            ? await StageInsertAsync(transaction, [1], [30])
            : await StageCompareAndSetAsync(transaction, [1], [10], [30], new byte[1]);
        await Assert.That(staged).IsTrue();
        storage.Put([1], [20]);
        if (insert) storage.Delete([1]);
        else storage.Put([1], [10]);
        await storage.CollectGarbageAsync(compact: true);

        await Assert.That(await transaction.TryCommitAsync()).IsFalse();
        await Assert.That(transaction.Conflict!.Kind).IsEqualTo(MvccConflictKind.WrittenKey);
        await Assert.That(await storage.GetRawAsync([1], new byte[1])).IsEqualTo(insert ? -1 : 1);
        if (!insert) await AssertValueAsync(storage, [1], [10]);
    }

    [Test]
    public async Task AcceptedGuardedBatchPreservesSnapshotAndSurvivesReopen()
    {
        using var folder = TempFolder.Create();
        {
            await using var storage = await MvccStorage.OpenAsync(folder, Options());
            storage.Put([1], [10]);
            storage.Put([3], [30]);
            using var before = storage.CreateSnapshot();
            using var transaction = storage.BeginTransaction();
            await Assert.That(await StageCompareAndSetAsync(transaction, [1], [10], [11], new byte[1])).IsTrue();
            transaction.Put([2], [20]);
            transaction.Delete([3]);
            await transaction.CommitAsync();
            await AssertValueAsync(storage, [1], [11]);
            var old = new byte[1];
            await Assert.That(await before.GetRawAsync([1], old)).IsEqualTo(1);
            await Assert.That(old[0]).IsEqualTo((byte)10);
            await Assert.That(await before.GetRawAsync([2], old)).IsEqualTo(-1);
        }
        await using var reopened = await MvccStorage.OpenAsync(folder, Options());
        await AssertValueAsync(reopened, [1], [11]);
        await AssertValueAsync(reopened, [2], [20]);
        await Assert.That(await reopened.GetRawAsync([3], new byte[1])).IsEqualTo(-1);
    }

    [Test]
    public async Task CanceledConditionalCommitDoesNotApplyAndConsumesTransaction()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        using var transaction = storage.BeginTransaction();
        await Assert.That(await StageInsertAsync(transaction, [1], [10])).IsTrue();
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await transaction.TryCommitAsync(cancellation.Token)).Throws<OperationCanceledException>();
        await Assert.That(transaction.Conflict is null).IsTrue();
        await Assert.That(await storage.GetRawAsync([1], new byte[1])).IsEqualTo(-1);
        await Assert.That(async () => await transaction.GetRawAsync([1], new byte[1])).Throws<ObjectDisposedException>();
    }

    private static async ValueTask<bool> StageInsertAsync(MvccTransaction transaction, byte[] key, byte[] value)
    {
        if (await transaction.GetForUpdateRawAsync(key, Memory<byte>.Empty) != -1) return false;
        transaction.Put(key, value);
        return true;
    }

    private static async ValueTask<bool> StageCompareAndSetAsync(
        MvccTransaction transaction, byte[] key, byte[] expected, byte[] replacement, Memory<byte> scratch)
    {
        if (scratch.Length < expected.Length) throw new ArgumentException("Scratch must fit the expected value.", nameof(scratch));
        var length = await transaction.GetForUpdateRawAsync(key, scratch);
        if (length != expected.Length || !scratch.Span[..length].SequenceEqual(expected)) return false;
        transaction.Put(key, replacement);
        return true;
    }

    private static async Task AssertValueAsync(MvccStorage storage, byte[] key, byte[] expected)
    {
        var actual = new byte[expected.Length];
        await Assert.That(await storage.GetRawAsync(key, actual)).IsEqualTo(expected.Length);
        await Assert.That(actual.AsSpan().SequenceEqual(expected)).IsTrue();
    }
}
