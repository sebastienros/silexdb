using System.Buffers.Binary;
using Silex.Wal;

namespace Silex.Test;

public class MvccGroupCommitTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private static StorageOptions Options => new()
    {
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
        SyncWriteAheadLogToDisk = true,
        MvccCommitGroupSize = 8
    };

    [Test]
    [Arguments(0)]
    [Arguments(256)]
    public async Task OrderedValidationIncludesAcceptedWritesAndTrackedReads(int indexedEntries)
    {
        await using var harness = await Harness.OpenAsync();
        var storage = harness.Storage;
        storage.Put([1], [10]);
        using var snapshot = storage.CreateSnapshot();
        using var first = storage.BeginTransaction();
        using var conflicting = storage.BeginTransaction();
        using var tracked = storage.BeginTransaction();
        using var independent = storage.BeginTransaction();
        first.Put([1], [11]);
        first.Put([2], []);
        conflicting.Delete([1]);
        await tracked.GetForUpdateRawAsync([1], new byte[1]);
        tracked.Put([3], [33]);
        independent.Delete([4]);
        independent.Put([5], [55]);
        for (var i = 0; i < indexedEntries; i++)
        {
            var key = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(key, i);
            first.Put(key, []);
            conflicting.Delete(key);
            await tracked.GetForUpdateRawAsync(key, Memory<byte>.Empty);
        }
        await harness.BlockAsync();
        var points = new List<StorageFaultPoint>();
        harness.Inject = points.Add;
        var commits = new[] { first.TryCommitAsync().AsTask(), conflicting.TryCommitAsync().AsTask(),
            tracked.TryCommitAsync().AsTask(), independent.TryCommitAsync().AsTask() };
        harness.Release.Set();
        await harness.BlockingTask;
        var results = await Task.WhenAll(commits).WaitAsync(Timeout);
        await Assert.That(results.SequenceEqual([true, false, false, true])).IsTrue();
        await Assert.That(points.Count(p => p == StorageFaultPoint.WalBeforeWrite)).IsEqualTo(2);
        await Assert.That(points.Count(p => p == StorageFaultPoint.WalBeforeSync)).IsEqualTo(1);
        await Assert.That(points.IndexOf(StorageFaultPoint.BatchBeforeApply))
            .IsGreaterThan(points.IndexOf(StorageFaultPoint.WalAfterSync));
        await Assert.That(storage.PublishedSequence).IsEqualTo(4L);
        await AssertValueAsync(storage, 1, 11);
        await Assert.That(await storage.GetRawAsync([2], Memory<byte>.Empty)).IsEqualTo(0);
        await Assert.That(await storage.GetRawAsync([3], new byte[1])).IsEqualTo(-1);
        var old = new byte[1];
        await snapshot.GetRawAsync([1], old);
        await Assert.That(old[0]).IsEqualTo((byte)10);
        await storage.SynchronizeAsync();
        await storage.FlushAndCompactAsync();
        await AssertValueAsync(storage, 5, 55);
    }

    [Test]
    public async Task CancellationBeforeAcceptanceExcludesFrameButAfterAcceptanceWaitsForSyncAndBarrier()
    {
        await using var harness = await Harness.OpenAsync();
        using var canceled = new CancellationTokenSource();
        using var acceptedCancellation = new CancellationTokenSource();
        using var first = harness.Storage.BeginTransaction();
        using var second = harness.Storage.BeginTransaction();
        first.Put([1], [11]);
        second.Put([2], [22]);
        await harness.BlockAsync();
        using var syncRelease = new ManualResetEventSlim();
        var syncEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.Inject = point =>
        {
            if (point == StorageFaultPoint.WalBeforeSync)
            {
                syncEntered.TrySetResult();
                if (!syncRelease.Wait(Timeout))
                {
                    throw new TimeoutException("Sync was not released.");
                }
            }
        };
        var firstCommit = first.TryCommitAsync(canceled.Token).AsTask();
        var secondCommit = second.TryCommitAsync(acceptedCancellation.Token).AsTask();
        canceled.Cancel();
        harness.Release.Set();
        try
        {
            await syncEntered.Task.WaitAsync(Timeout);
            acceptedCancellation.Cancel();
            await Assert.That(secondCommit.IsCompleted).IsFalse();
            var barrier = harness.Storage.SynchronizeAsync();
            await Assert.That(barrier.IsCompleted).IsFalse();
            syncRelease.Set();
            await barrier.WaitAsync(Timeout);
            await Assert.That(await secondCommit).IsTrue();
        }
        finally
        {
            syncRelease.Set();
        }
        await Assert.That(async () => await firstCommit).Throws<OperationCanceledException>();
        await Assert.That(harness.Storage.PublishedSequence).IsEqualTo(2L);
        await Assert.That(await harness.Storage.GetRawAsync([1], new byte[1])).IsEqualTo(-1);
        await AssertValueAsync(harness.Storage, 2, 22);
    }

    [Test]
    [Arguments((int)StorageFaultPoint.WalBeforeWrite, 1, 0)]
    [Arguments((int)StorageFaultPoint.WalBeforeWrite, 2, 1)]
    [Arguments((int)StorageFaultPoint.WalDuringWrite, 2, 1)]
    [Arguments((int)StorageFaultPoint.WalAfterWrite, 2, 2)]
    [Arguments((int)StorageFaultPoint.WalBeforeSync, 1, 2)]
    [Arguments((int)StorageFaultPoint.WalAfterSync, 1, 2)]
    [Arguments((int)StorageFaultPoint.BatchBeforeApply, 1, 2)]
    [Arguments((int)StorageFaultPoint.BatchAfterApplyEntry, 2, 2)]
    [Arguments((int)StorageFaultPoint.BatchBeforeApply, 2, 2)]
    [Arguments((int)StorageFaultPoint.WriteBeforePublication, 1, 2)]
    [Arguments((int)StorageFaultPoint.MvccBeforePublication, 1, 2)]
    [Arguments((int)StorageFaultPoint.MvccBeforePublication, 2, 2)]
    [Arguments((int)StorageFaultPoint.MemTableBeforeCreate, 1, 2)]
    public async Task FailuresReportIndividualOutcomesAndRecoverAtomicFrames(int point, int occurrence, int recovered)
    {
        await using var harness = await Harness.OpenAsync(memTableLimit:
            point == (int)StorageFaultPoint.MemTableBeforeCreate ? 1 : long.MaxValue);
        using var first = harness.Storage.BeginTransaction();
        using var second = harness.Storage.BeginTransaction();
        using var third = harness.Storage.BeginTransaction();
        first.Put([1], [11]);
        first.Put([2], [22]);
        second.Put([3], [33]);
        second.Put([4], [44]);
        third.Put([5], [55]);
        await harness.BlockAsync();
        var cause = new IOException("Injected group I/O failure.");
        var hits = 0;
        harness.Inject = p =>
        {
            if ((int)p == point && ++hits == occurrence)
            {
                throw cause;
            }
        };
        var commits = new[] { CaptureAsync(first), CaptureAsync(second), CaptureAsync(third) };
        harness.Release.Set();
        var failures = await Task.WhenAll(commits).WaitAsync(Timeout);
        var beforeFirst = point == (int)StorageFaultPoint.WalBeforeWrite && occurrence == 1;
        var allApplied = point == (int)StorageFaultPoint.MvccBeforePublication ||
            point == (int)StorageFaultPoint.MemTableBeforeCreate;
        var firstApplied = allApplied ||
            point == (int)StorageFaultPoint.WriteBeforePublication ||
            (point == (int)StorageFaultPoint.BatchBeforeApply && occurrence == 2);
        var secondApplied = allApplied;
        await Assert.That(failures[0].Outcome).IsEqualTo(beforeFirst ? StorageCommitOutcome.NotCommitted
            : firstApplied ? StorageCommitOutcome.Committed : StorageCommitOutcome.Indeterminate);
        var secondNotSubmitted = point == (int)StorageFaultPoint.WalBeforeWrite;
        await Assert.That(failures[1].Outcome).IsEqualTo(secondNotSubmitted ? StorageCommitOutcome.NotCommitted
            : secondApplied ? StorageCommitOutcome.Committed : StorageCommitOutcome.Indeterminate);
        var thirdSubmitted = point is not ((int)StorageFaultPoint.WalBeforeWrite)
            and not ((int)StorageFaultPoint.WalDuringWrite) and not ((int)StorageFaultPoint.WalAfterWrite);
        await Assert.That(failures[2].Outcome).IsEqualTo(!thirdSubmitted ? StorageCommitOutcome.NotCommitted
            : secondApplied ? StorageCommitOutcome.Committed : StorageCommitOutcome.Indeterminate);
        await Assert.That(failures.All(f => ReferenceEquals(f.InnerException, cause))).IsTrue();
        await Assert.That(harness.Storage.Health).IsEqualTo(beforeFirst ? StorageHealth.Healthy : StorageHealth.Faulted);
        if (!beforeFirst)
        {
            await Assert.That(() => harness.Storage.BeginTransaction()).Throws<StorageFaultedException>();
            await Assert.That(async () => await harness.Storage.SynchronizeAsync()).Throws<StorageFaultedException>();
        }
        harness.Inject = null;
        if (beforeFirst)
        {
            using var retry = harness.Storage.BeginTransaction();
            retry.Put([9], [99]);
            await retry.CommitAsync();
        }
        await harness.Storage.DisposeAsync();
        await using var reopened = await MvccStorage.OpenAsync(harness.Folder, Options);
        for (byte key = 1; key <= 4; key++)
        {
            var present = key <= recovered * 2;
            await Assert.That(await reopened.GetRawAsync([key], new byte[1])).IsEqualTo(present ? 1 : -1);
        }
        await Assert.That(await reopened.GetRawAsync([5], new byte[1])).IsEqualTo(thirdSubmitted ? 1 : -1);
    }

    [Test]
    public async Task TornFinalTransactionDoesNotDiscardEarlierCompleteTransaction()
    {
        await using var harness = await Harness.OpenAsync();
        using var recoveredFolder = TempFolder.Create();
        using var first = harness.Storage.BeginTransaction();
        using var second = harness.Storage.BeginTransaction();
        first.Put([1], [11]);
        first.Put([2], [22]);
        second.Put([3], [33]);
        second.Put([4], [44]);
        await harness.BlockAsync();
        var commits = new[] { first.CommitAsync().AsTask(), second.CommitAsync().AsTask() };
        harness.Release.Set();
        await Task.WhenAll(commits).WaitAsync(Timeout);
        foreach (var path in Directory.GetFiles(harness.Folder))
        {
            if (!path.EndsWith(".wal", StringComparison.Ordinal) && Path.GetFileName(path) != "manifest")
            {
                continue;
            }
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            var bytes = new byte[checked((int)stream.Length)];
            stream.ReadExactly(bytes);
            if (path.EndsWith(".wal", StringComparison.Ordinal))
            {
                var offset = WriteAheadLog.FileHeaderSize;
                var last = offset;
                while (offset < bytes.Length)
                {
                    last = offset;
                    offset += WriteAheadLog.FrameHeaderSize +
                        checked((int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(offset))) + WriteAheadLog.FrameFooterSize;
                }
                bytes = bytes[..(last + WriteAheadLog.FrameHeaderSize + 1)];
            }
            File.WriteAllBytes(Path.Combine(recoveredFolder, Path.GetFileName(path)), bytes);
        }
        await using var reopened = await MvccStorage.OpenAsync(recoveredFolder, Options);
        await AssertValueAsync(reopened, 1, 11);
        await AssertValueAsync(reopened, 2, 22);
        await Assert.That(await reopened.GetRawAsync([3], new byte[1])).IsEqualTo(-1);
        await Assert.That(await reopened.GetRawAsync([4], new byte[1])).IsEqualTo(-1);
        await Assert.That(reopened.PublishedSequence).IsEqualTo(2L);
    }

    [Test]
    [Arguments((int)StorageFaultPoint.WalBeforeSync)]
    [Arguments((int)StorageFaultPoint.MvccBeforePublication)]
    public async Task UnsafeFailureRejectsFollowingQueuedGroup(int point)
    {
        await using var harness = await Harness.OpenAsync(groupSize: 2);
        using var first = harness.Storage.BeginTransaction();
        using var second = harness.Storage.BeginTransaction();
        using var pending = harness.Storage.BeginTransaction();
        first.Put([1], [11]);
        second.Put([2], [22]);
        pending.Put([3], [33]);
        await harness.BlockAsync();
        harness.Inject = p =>
        {
            if ((int)p == point)
            {
                throw new IOException("Injected unsafe group failure.");
            }
        };
        var failures = new[] { CaptureAsync(first), CaptureAsync(second) };
        var rejected = pending.CommitAsync().AsTask();
        harness.Release.Set();
        await Task.WhenAll(failures).WaitAsync(Timeout);
        await Assert.That(async () => await rejected).Throws<StorageFaultedException>();
        await Assert.That(harness.Storage.Health).IsEqualTo(StorageHealth.Faulted);
    }

    [Test]
    public async Task AdmissionAndGroupSizeAreBoundedAndWaitingAdmissionIsCancelable()
    {
        await using var harness = await Harness.OpenAsync(groupSize: 2);
        using var cancellation = new CancellationTokenSource();
        var transactions = Enumerable.Range(1, 5).Select(i =>
        {
            var transaction = harness.Storage.BeginTransaction();
            transaction.Put([(byte)i], [(byte)i]);
            return transaction;
        }).ToArray();
        try
        {
            await harness.BlockAsync();
            var points = new List<StorageFaultPoint>();
            harness.Inject = points.Add;
            var commits = transactions.Take(4).Select(t => t.CommitAsync().AsTask()).ToArray();
            var waiting = transactions[4].CommitAsync(cancellation.Token).AsTask();
            cancellation.Cancel();
            await Assert.That(async () => await waiting).Throws<OperationCanceledException>();
            harness.Release.Set();
            await Task.WhenAll(commits).WaitAsync(Timeout);
            await Assert.That(points.Count(p => p == StorageFaultPoint.WalBeforeWrite)).IsEqualTo(4);
            await Assert.That(points.Count(p => p == StorageFaultPoint.WalBeforeSync)).IsEqualTo(2);
            await Assert.That(harness.Storage.PublishedSequence).IsEqualTo(5L);
            await Assert.That(await harness.Storage.GetRawAsync([5], new byte[1])).IsEqualTo(-1);
        }
        finally
        {
            foreach (var transaction in transactions)
            {
                transaction.Dispose();
            }
        }
    }

    [Test]
    [Arguments(1)]
    [Arguments(8)]
    public async Task ReopenedCommitsValidateMissingKeysAndConflictsAgainstOrdinaryWrites(int groupSize)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.MvccCommitGroupSize = groupSize;
        await using (var storage = await MvccStorage.OpenAsync(folder, options))
        {
            storage.Put([1], [11]);
        }
        await using var reopened = await MvccStorage.OpenAsync(folder, options);
        await Assert.That(await reopened.GetRawAsync([2], new byte[1])).IsEqualTo(-1);
        using var stale = reopened.BeginTransaction();
        stale.Put([1], [22]);
        reopened.Delete([1]);
        await Assert.That(await stale.TryCommitAsync()).IsFalse();
        using var fresh = reopened.BeginTransaction();
        fresh.Put([2], [22]);
        await fresh.CommitAsync();
        await AssertValueAsync(reopened, 2, 22);
        using var empty = reopened.BeginTransaction();
        await Assert.That(await empty.TryCommitAsync()).IsTrue();
    }

    [Test]
    public async Task ShutdownDrainsAcceptedGroupAndRejectsPendingTransactions()
    {
        await using var harness = await Harness.OpenAsync();
        using var first = harness.Storage.BeginTransaction();
        using var second = harness.Storage.BeginTransaction();
        first.Put([1], [11]);
        second.Put([2], [22]);
        using var syncRelease = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        harness.Inject = point =>
        {
            if (point == StorageFaultPoint.WalBeforeSync)
            {
                entered.TrySetResult();
                if (!syncRelease.Wait(Timeout))
                {
                    throw new TimeoutException("Sync was not released.");
                }
            }
        };
        var firstCommit = first.CommitAsync().AsTask();
        Task? disposal = null;
        try
        {
            await entered.Task.WaitAsync(Timeout);
            var pending = second.CommitAsync().AsTask();
            disposal = harness.Storage.DisposeAsync().AsTask();
            await Assert.That(disposal.IsCompleted).IsFalse();
            syncRelease.Set();
            await firstCommit.WaitAsync(Timeout);
            await Assert.That(async () => await pending).Throws<ObjectDisposedException>();
            await disposal.WaitAsync(Timeout);
        }
        finally
        {
            syncRelease.Set();
            if (disposal is not null)
            {
                await disposal;
            }
        }
        await using var reopened = await MvccStorage.OpenAsync(harness.Folder, Options);
        await AssertValueAsync(reopened, 1, 11);
        await Assert.That(await reopened.GetRawAsync([2], new byte[1])).IsEqualTo(-1);
    }

    [Test]
    [Arguments(0, true, true)]
    [Arguments(65, true, true)]
    [Arguments(8, false, true)]
    [Arguments(8, true, false)]
    public async Task InvalidGroupingConfigurationIsRejected(int size, bool wal, bool sync)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.MvccCommitGroupSize = size;
        options.UseWriteAheadLog = wal;
        options.SyncWriteAheadLogToDisk = sync;
        await Assert.That(async () => await MvccStorage.OpenAsync(folder, options)).Throws<ArgumentException>();
    }

    private static async Task<StorageOperationException> CaptureAsync(MvccTransaction transaction)
    {
        try
        {
            await transaction.CommitAsync();
        }
        catch (StorageOperationException exception)
        {
            return exception;
        }
        throw new InvalidOperationException("Expected a failed group commit.");
    }

    private static async Task AssertValueAsync(MvccStorage storage, byte key, byte expected)
    {
        var value = new byte[1];
        await Assert.That(await storage.GetRawAsync([key], value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo(expected);
    }

    private sealed class Harness : IAsyncDisposable
    {
        public TempFolder Folder { get; } = TempFolder.Create();
        public MvccStorage Storage { get; private set; } = null!;
        public ManualResetEventSlim Release { get; } = new();
        public Task BlockingTask { get; private set; } = Task.CompletedTask;
        public Action<StorageFaultPoint>? Inject;
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _block;

        public static async Task<Harness> OpenAsync(int groupSize = 8, long memTableLimit = long.MaxValue)
        {
            var harness = new Harness();
            var options = Options;
            options.MvccCommitGroupSize = groupSize;
            options.MemTableSizeLimit = memTableLimit;
            options.FaultInjector = point =>
            {
                if (point == StorageFaultPoint.MvccBeforePublication && Interlocked.Exchange(ref harness._block, 0) != 0)
                {
                    harness._entered.TrySetResult();
                    if (!harness.Release.Wait(Timeout))
                    {
                        throw new TimeoutException("Commit gate was not released.");
                    }
                }
                else
                {
                    harness.Inject?.Invoke(point);
                }
            };
            harness.Storage = await MvccStorage.OpenAsync(harness.Folder, options);
            return harness;
        }

        public async Task BlockAsync()
        {
            Volatile.Write(ref _block, 1);
            BlockingTask = Task.Run(() => Storage.Put([250], [1]));
            await _entered.Task.WaitAsync(Timeout);
        }

        public async ValueTask DisposeAsync()
        {
            Release.Set();
            await BlockingTask;
            await Storage.DisposeAsync();
            Release.Dispose();
            Folder.Dispose();
        }
    }
}
