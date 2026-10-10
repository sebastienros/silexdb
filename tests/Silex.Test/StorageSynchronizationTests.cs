using Silex.Wal;

namespace Silex.Test;

public class StorageSynchronizationTests
{
    private static StorageOptions Options => new() { FlushPeriod = TimeSpan.Zero, CompactionStrategy = CompactionStrategy.None };
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RawWalWriteHotPathRemainsAllocationFree(bool sync)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.SyncWriteAheadLogToDisk = sync;
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        for (var i = 0; i < 100; i++)
        {
            storage.Put([1], [11]);
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 100; i++)
        {
            storage.Put([1], [22]);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WriteAcknowledgmentFollowsConfiguredWalBoundary(bool sync)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.SyncWriteAheadLogToDisk = sync;
        var points = new List<StorageFaultPoint>();
        options.FaultInjector = points.Add;
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        points.Clear();
        storage.Put([1], [11]);
        var expected = new List<StorageFaultPoint>
        {
            StorageFaultPoint.WalBeforeWrite, StorageFaultPoint.WalDuringWrite, StorageFaultPoint.WalAfterWrite
        };
        if (sync)
        {
            expected.Add(StorageFaultPoint.WalBeforeSync);
            expected.Add(StorageFaultPoint.WalAfterSync);
        }
        expected.AddRange([StorageFaultPoint.BatchBeforeApply, StorageFaultPoint.BatchAfterApplyEntry,
            StorageFaultPoint.WriteBeforePublication]);
        await Assert.That(points.SequenceEqual(expected)).IsTrue();
        // Recovery can already see the complete frame, independently of disposal or memtable flushing.
        using var recovered = new Silex.MemTables.MemTable(1);
        WriteAheadLog.Replay(Directory.GetFiles(folder, "*.wal").Single(), recovered);
        await Assert.That(recovered.Count).IsEqualTo(1);
        await AssertValueAsync(storage, 1, 11);
    }

    [Test]
    [Arguments("current")]
    [Arguments("rotated")]
    [Arguments("recovered")]
    [Arguments("flushed")]
    [Arguments("tiered")]
    [Arguments("leveled")]
    public async Task BarrierCoversLiveRecoveryFilesWithoutChangingContents(string mode)
    {
        using var folder = TempFolder.Create();
        if (mode == "recovered")
        {
            await CrashRecoveryTestProcess.WriteBatchAndExitWithoutDisposalAsync(folder, 2);
        }
        var options = mode is "tiered" or "leveled"
            ? StorageFailureTests.MaintenanceOptions(mode == "leveled") : Options;
        if (mode == "rotated")
        {
            options.MemTableSizeLimit = 1;
        }
        var points = new List<StorageFaultPoint>();
        options.FaultInjector = points.Add;
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        if (mode != "recovered")
        {
            storage.Put([1], [11]);
            if (mode is "tiered" or "leveled")
            {
                await storage.FlushAndCompactAsync();
            }
            storage.Put([2], [22]);
        }
        if (mode is "flushed" or "tiered" or "leveled")
        {
            await storage.FlushAndCompactAsync();
        }
        var manifest = Manifest.TryRead(folder)!;
        if (mode == "leveled")
        {
            await Assert.That(manifest.Levels.SelectMany(level => level).Any()).IsTrue();
        }
        var before = ReadRecoveryFiles(folder);
        var walCount = before.Keys.Count(path => path.EndsWith(".wal", StringComparison.Ordinal));
        var sstCount = manifest.AllSstIds().Count();
        points.Clear();
        await storage.SynchronizeAsync();
        var expected = new List<StorageFaultPoint>();
        for (var i = 0; i < walCount; i++)
        {
            expected.AddRange([StorageFaultPoint.SynchronizationBeforeWal, StorageFaultPoint.SynchronizationAfterWal]);
        }
        for (var i = 0; i < sstCount; i++)
        {
            expected.AddRange([StorageFaultPoint.SynchronizationBeforeSst, StorageFaultPoint.SynchronizationAfterSst]);
        }
        expected.AddRange([StorageFaultPoint.SynchronizationBeforeManifest, StorageFaultPoint.SynchronizationAfterManifest]);
        await Assert.That(points.SequenceEqual(expected)).IsTrue();
        await AssertFilesUnchangedAsync(before, folder);
        await storage.SynchronizeAsync();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
    }

    [Test]
    [Arguments((int)StorageFaultPoint.SynchronizationBeforeWal)]
    [Arguments((int)StorageFaultPoint.SynchronizationAfterWal)]
    [Arguments((int)StorageFaultPoint.SynchronizationBeforeSst)]
    [Arguments((int)StorageFaultPoint.SynchronizationAfterSst)]
    [Arguments((int)StorageFaultPoint.SynchronizationBeforeManifest)]
    [Arguments((int)StorageFaultPoint.SynchronizationAfterManifest)]
    public async Task BarrierFailureFaultsWithoutChangingCommittedOutcomesOrRecoveryEvidence(int point)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        var armed = false;
        var cause = new IOException("Injected synchronization failure.");
        options.FaultInjector = p =>
        {
            if (armed && (int)p == point)
            {
                throw cause;
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        storage.Put([1], [11]);
        await storage.FlushAndCompactAsync();
        storage.Put([2], [22]);
        var before = ReadRecoveryFiles(folder);
        armed = true;
        StorageSynchronizationException? failure = null;
        try
        {
            await storage.SynchronizeAsync();
        }
        catch (StorageSynchronizationException exception)
        {
            failure = exception;
        }
        await Assert.That(failure is not null).IsTrue();
        await Assert.That(ReferenceEquals(failure!.InnerException, cause)).IsTrue();
        await Assert.That(ReferenceEquals(storage.Fault, failure)).IsTrue();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Faulted);
        await Assert.That(async () => await storage.SynchronizeAsync()).Throws<StorageFaultedException>();
        await Assert.That(() => storage.Put([3], [33])).Throws<StorageFaultedException>();
        await storage.DisposeAsync();
        await AssertFilesUnchangedAsync(before, folder);
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await AssertValueAsync(reopened, 1, 11);
        await AssertValueAsync(reopened, 2, 22);
        await reopened.SynchronizeAsync();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DisabledWalAndReadOnlyStoresRejectBarrierWithoutFaulting(bool salvage)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.UseWriteAheadLog = salvage;
        var storage = salvage ? await LsmStorage.OpenSalvageAsync(folder, options)
            : await LsmStorage.OpenAsync(folder, options);
        await using (storage)
        {
            if (salvage)
            {
                await Assert.That(async () => await storage.SynchronizeAsync()).Throws<InvalidOperationException>();
            }
            else
            {
                storage.Put([1], [11]);
                await Assert.That(async () => await storage.SynchronizeAsync()).Throws<NotSupportedException>();
                await AssertValueAsync(storage, 1, 11);
            }
            await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
        }
    }

    [Test]
    public async Task CancellationBeforeIoAndWhileWaitingForMaintenanceLeavesHealthyInstance()
    {
        using var folder = TempFolder.Create();
        var options = Options;
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        options.FaultInjector = point =>
        {
            if (point == StorageFaultPoint.FlushBeforeBuild)
            {
                entered.TrySetResult();
                if (!release.Wait(Timeout))
                {
                    throw new TimeoutException("Maintenance was not released.");
                }
            }
        };
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        storage.Put([1], [11]);
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        await Assert.That(async () => await storage.SynchronizeAsync(canceled.Token)).Throws<OperationCanceledException>();
        var maintenance = Task.Run(() => storage.FlushAndCompactAsync());
        try
        {
            await entered.Task.WaitAsync(Timeout);
            using var waiting = new CancellationTokenSource();
            var barrier = storage.SynchronizeAsync(waiting.Token);
            await Assert.That(barrier.IsCompleted).IsFalse();
            waiting.Cancel();
            await Assert.That(async () => await barrier.WaitAsync(Timeout)).Throws<OperationCanceledException>();
            await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
        }
        finally
        {
            release.Set();
        }
        await maintenance.WaitAsync(Timeout);
        await storage.SynchronizeAsync();
    }

    [Test]
    [Arguments("write")]
    [Arguments("barrier")]
    [Arguments("maintenance")]
    [Arguments("shutdown")]
    public async Task BarrierSerializesWithMutationsMaintenanceAndShutdown(string mode)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        using var release = new ManualResetEventSlim();
        using var cancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var block = true;
        options.FaultInjector = point =>
        {
            if (point == StorageFaultPoint.SynchronizationBeforeWal && block)
            {
                block = false;
                entered.TrySetResult();
                if (!release.Wait(Timeout))
                {
                    throw new TimeoutException("Barrier was not released.");
                }
                cancellation.Cancel();
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        storage.Put([1], [11]);
        var barrier = Task.Run(() => storage.SynchronizeAsync(cancellation.Token));
        Task? competing = null;
        try
        {
            await entered.Task.WaitAsync(Timeout);
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            competing = Task.Run(async () =>
            {
                if (mode == "shutdown")
                {
                    var close = storage.CloseAsync();
                    started.SetResult();
                    await close;
                    return;
                }
                started.SetResult();
                if (mode == "write")
                {
                    storage.Put([2], [22]);
                }
                else if (mode == "barrier")
                {
                    await storage.SynchronizeAsync();
                }
                else if (mode == "maintenance")
                {
                    await storage.FlushAndCompactAsync();
                }
            });
            await started.Task.WaitAsync(Timeout);
            await Assert.That(competing.IsCompleted).IsFalse();
            if (mode == "shutdown")
            {
                await Assert.That(() => storage.SynchronizeAsync()).Throws<ObjectDisposedException>();
                await Assert.That(async () => await LsmStorage.OpenAsync(folder, Options)).Throws<IOException>();
            }
        }
        finally
        {
            release.Set();
        }
        // Cancellation after file I/O begins must not turn a successful boundary into cancellation.
        await barrier.WaitAsync(Timeout);
        await competing!.WaitAsync(Timeout);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await AssertValueAsync(reopened, 1, 11);
        if (mode == "write")
        {
            await AssertValueAsync(reopened, 2, 22);
        }
    }

    [Test]
    public async Task MvccBarrierIncludesPublishedCommitButNotStagedTransactionsAndPropagatesFailure()
    {
        using var folder = TempFolder.Create();
        var options = Options;
        var fail = false;
        options.FaultInjector = point =>
        {
            if (fail && point == StorageFaultPoint.SynchronizationAfterManifest)
            {
                throw new IOException("Injected MVCC barrier failure.");
            }
        };
        var storage = await MvccStorage.OpenAsync(folder, options);
        using (var committed = storage.BeginTransaction())
        {
            committed.Put([1], [11]);
            await committed.CommitAsync();
        }
        using (var staged = storage.BeginTransaction())
        {
            staged.Put([2], [22]);
            var published = storage.PublishedSequence;
            await storage.SynchronizeAsync();
            await Assert.That(storage.PublishedSequence).IsEqualTo(published);
            await Assert.That(await storage.GetRawAsync(new byte[] { 2 }, new byte[1])).IsEqualTo(-1);
        }
        fail = true;
        await Assert.That(async () => await storage.SynchronizeAsync()).Throws<StorageSynchronizationException>();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Faulted);
        await storage.DisposeAsync();
        await using var reopened = await MvccStorage.OpenAsync(folder, Options);
        var value = new byte[1];
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);
        await Assert.That(await reopened.GetRawAsync(new byte[] { 2 }, value)).IsEqualTo(-1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task BarrierWaitsForAnAlreadyStartedWriteAndMvccPublication(bool mvcc)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var armed = false;
        var synchronized = false;
        options.FaultInjector = point =>
        {
            if (armed && point == (mvcc ? StorageFaultPoint.MvccBeforePublication : StorageFaultPoint.WalAfterWrite))
            {
                entered.TrySetResult();
                if (!release.Wait(Timeout))
                {
                    throw new TimeoutException("Writer was not released.");
                }
            }
            if (point == StorageFaultPoint.SynchronizationBeforeWal)
            {
                synchronized = true;
            }
        };
        var plain = mvcc ? null : await LsmStorage.OpenAsync(folder, options);
        var versioned = mvcc ? await MvccStorage.OpenAsync(folder, options) : null;
        armed = true;
        var write = Task.Run(() =>
        {
            if (versioned is not null)
            {
                versioned.Put([1], [11]);
            }
            else
            {
                plain!.Put([1], [11]);
            }
        });
        Task? barrier = null;
        try
        {
            await entered.Task.WaitAsync(Timeout);
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            barrier = Task.Run(async () =>
            {
                started.SetResult();
                if (versioned is not null)
                {
                    await versioned.SynchronizeAsync();
                }
                else
                {
                    await plain!.SynchronizeAsync();
                }
            });
            await started.Task.WaitAsync(Timeout);
            await Assert.That(barrier.IsCompleted).IsFalse();
            await Assert.That(synchronized).IsFalse();
        }
        finally
        {
            release.Set();
        }
        await write.WaitAsync(Timeout);
        await barrier!.WaitAsync(Timeout);
        await Assert.That(synchronized).IsTrue();
        if (versioned is not null)
        {
            await Assert.That(versioned.PublishedSequence).IsEqualTo(1L);
            await versioned.DisposeAsync();
        }
        else
        {
            await AssertValueAsync(plain!, 1, 11);
            await plain!.DisposeAsync();
        }
    }

    [Test]
    [Arguments((int)StorageFaultPoint.SynchronizationBeforeWal)]
    [Arguments((int)StorageFaultPoint.SynchronizationAfterWal)]
    [Arguments((int)StorageFaultPoint.SynchronizationBeforeManifest)]
    [Arguments((int)StorageFaultPoint.SynchronizationAfterManifest)]
    public async Task ProcessCrashDuringBarrierRecoversPreviouslyAcknowledgedBatch(int point)
    {
        using var folder = TempFolder.Create();
        await CrashRecoveryTestProcess.CrashAtAsync(folder, point, "synchronize");
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await StorageFailureTests.AssertValuesAsync(reopened);
        await reopened.SynchronizeAsync();
    }

    private static async Task AssertValueAsync(LsmStorage storage, byte key, byte expected)
    {
        var value = new byte[1];
        await Assert.That(await storage.GetRawAsync(new byte[] { key }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo(expected);
    }

    private static Dictionary<string, byte[]> ReadRecoveryFiles(string folder)
    {
        var files = new Dictionary<string, byte[]>();
        foreach (var path in Directory.GetFiles(folder))
        {
            if (path.EndsWith(".wal", StringComparison.Ordinal) || path.EndsWith(".sst", StringComparison.Ordinal)
                || path == Manifest.GetPath(folder))
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                var bytes = new byte[checked((int)stream.Length)];
                stream.ReadExactly(bytes);
                files.Add(path, bytes);
            }
        }
        return files;
    }

    private static async Task AssertFilesUnchangedAsync(Dictionary<string, byte[]> before, string folder)
    {
        var after = ReadRecoveryFiles(folder);
        await Assert.That(after.Count).IsEqualTo(before.Count);
        foreach (var (path, bytes) in before)
        {
            await Assert.That(after[path].SequenceEqual(bytes)).IsTrue();
        }
    }
}
