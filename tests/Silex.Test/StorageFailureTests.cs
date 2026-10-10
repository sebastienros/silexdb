namespace Silex.Test;

public class StorageFailureTests
{
    private static StorageOptions Options => new()
    {
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None
    };

    [Test]
    [Arguments((int)StorageFaultPoint.WalBeforeWrite, (int)StorageCommitOutcome.NotCommitted, 0)]
    [Arguments((int)StorageFaultPoint.WalDuringWrite, (int)StorageCommitOutcome.Indeterminate, 0)]
    [Arguments((int)StorageFaultPoint.WalAfterWrite, (int)StorageCommitOutcome.Indeterminate, 2)]
    [Arguments((int)StorageFaultPoint.WalBeforeSync, (int)StorageCommitOutcome.Indeterminate, 2)]
    [Arguments((int)StorageFaultPoint.WalAfterSync, (int)StorageCommitOutcome.Indeterminate, 2)]
    [Arguments((int)StorageFaultPoint.BatchBeforeApply, (int)StorageCommitOutcome.Indeterminate, 2)]
    [Arguments((int)StorageFaultPoint.BatchAfterApplyEntry, (int)StorageCommitOutcome.Indeterminate, 2)]
    [Arguments((int)StorageFaultPoint.WriteBeforePublication, (int)StorageCommitOutcome.Committed, 2)]
    public async Task WriteFailureReportsOutcomeAndPreservesWal(int point, int outcome, int recoveredCount)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.SyncWriteAheadLogToDisk = true;
        var cause = new IOException("Injected disk full / I/O failure.");
        options.FaultInjector = p =>
        {
            if ((int)p == point)
            {
                throw cause;
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        var batch = Batch();
        var failure = CaptureWriteFailure(() => storage.Write(batch));
        await Assert.That(failure.Outcome).IsEqualTo((StorageCommitOutcome)outcome);
        await Assert.That(ReferenceEquals(failure.InnerException, cause)).IsTrue();
        if (failure.Outcome == StorageCommitOutcome.NotCommitted)
        {
            await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
            await Assert.That(await CountAsync(storage)).IsEqualTo(0L);
        }
        else
        {
            await AssertFaultedAsync(storage);
        }
        var walFiles = Directory.GetFiles(folder, "*.wal");
        var walBytes = walFiles.Select(File.ReadAllBytes).ToArray();
        await storage.DisposeAsync();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Disposed);
        if (failure.Outcome != StorageCommitOutcome.NotCommitted)
        {
            for (var i = 0; i < walFiles.Length; i++)
            {
                await Assert.That(File.ReadAllBytes(walFiles[i]).SequenceEqual(walBytes[i])).IsTrue();
            }
        }
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await Assert.That(await CountAsync(reopened)).IsEqualTo((long)recoveredCount);
        if (recoveredCount == 2)
        {
            await AssertValuesAsync(reopened);
        }
    }

    [Test]
    [Arguments("put")]
    [Arguments("delete")]
    [Arguments("batch")]
    public async Task SingleMutationAndWalDisabledApplicationFailuresFaultInstance(string mode)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.UseWriteAheadLog = false;
        var armed = false;
        options.FaultInjector = point =>
        {
            if (armed && point == StorageFaultPoint.BatchAfterApplyEntry)
            {
                throw new OutOfMemoryException("Injected arena allocation failure.");
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        storage.Put([1], [11]);
        await storage.FlushAndCompactAsync();
        armed = true;
        var failure = CaptureWriteFailure(() =>
        {
            if (mode == "delete")
            {
                storage.Delete([1]);
            }
            else if (mode == "put")
            {
                storage.Put([2], [22]);
            }
            else
            {
                storage.Write(Batch());
            }
        });
        await Assert.That(failure.Outcome).IsEqualTo(StorageCommitOutcome.Indeterminate);
        await AssertFaultedAsync(storage);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, options: new StorageOptions
        {
            UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero
        });
        await Assert.That(await CountAsync(reopened)).IsEqualTo(1L);
        var value = new byte[1];
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);
    }

    [Test]
    [Arguments(false, (int)StorageFaultPoint.WalBeforeWrite)]
    [Arguments(false, (int)StorageFaultPoint.WalDuringWrite)]
    [Arguments(false, (int)StorageFaultPoint.WalAfterWrite)]
    [Arguments(true, (int)StorageFaultPoint.WalBeforeWrite)]
    [Arguments(true, (int)StorageFaultPoint.WalDuringWrite)]
    [Arguments(true, (int)StorageFaultPoint.WalAfterWrite)]
    public async Task SingleWalMutationFailureRetainsPriorCommittedPrefix(bool delete, int point)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        var armed = false;
        options.FaultInjector = p =>
        {
            if (armed && (int)p == point)
            {
                throw new IOException("Injected single-mutation WAL failure.");
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        storage.Put([1], [11]);
        armed = true;
        var failure = CaptureWriteFailure(() =>
        {
            if (delete)
            {
                storage.Delete([1]);
            }
            else
            {
                storage.Put([1], [22]);
            }
        });
        var beforeWrite = point == (int)StorageFaultPoint.WalBeforeWrite;
        await Assert.That(failure.Outcome).IsEqualTo(beforeWrite
            ? StorageCommitOutcome.NotCommitted : StorageCommitOutcome.Indeterminate);
        await Assert.That(storage.Health).IsEqualTo(beforeWrite ? StorageHealth.Healthy : StorageHealth.Faulted);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        var newFrame = point == (int)StorageFaultPoint.WalAfterWrite;
        var value = new byte[1];
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(delete && newFrame ? -1 : 1);
        if (!delete || !newFrame)
        {
            await Assert.That(value[0]).IsEqualTo(newFrame ? (byte)22 : (byte)11);
        }
    }

    [Test]
    public async Task NotCommittedFailureAllowsSafeReuseAndReadCallbackFailureDoesNotFault()
    {
        using var folder = TempFolder.Create();
        var options = Options;
        var fail = true;
        options.FaultInjector = p =>
        {
            if (fail && p == StorageFaultPoint.WalBeforeWrite)
            {
                throw new OutOfMemoryException("Injected frame preparation failure.");
            }
        };
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        var failure = CaptureWriteFailure(() => storage.Write(Batch()));
        await Assert.That(failure.Outcome).IsEqualTo(StorageCommitOutcome.NotCommitted);
        await Assert.That(storage.Fault is null).IsTrue();
        fail = false;
        storage.Write(Batch());
        await Assert.That(async () => await storage.TryReadRawAsync(new byte[] { 1 }, 0,
            static (_, _) => throw new InvalidOperationException("Caller callback failure.")))
            .Throws<InvalidOperationException>();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
        await AssertValuesAsync(storage);
    }

    [Test]
    public async Task CommittedWriteCanFailWhileCreatingNextWal()
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.MemTableSizeLimit = 1;
        var armed = false;
        options.FaultInjector = p =>
        {
            if (armed && p == StorageFaultPoint.MemTableBeforeCreate)
            {
                throw new IOException("Injected failure creating the next WAL.");
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        armed = true;
        var failure = CaptureWriteFailure(() => storage.Write(Batch()));
        await Assert.That(failure.Outcome).IsEqualTo(StorageCommitOutcome.Committed);
        await AssertFaultedAsync(storage);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await AssertValuesAsync(reopened);
    }

    [Test]
    [Arguments((int)StorageFaultPoint.FlushBeforeBuild)]
    [Arguments((int)StorageFaultPoint.FlushBeforeInstall)]
    [Arguments((int)StorageFaultPoint.FlushAfterInstall)]
    [Arguments((int)StorageFaultPoint.ManifestBeforePublish)]
    [Arguments((int)StorageFaultPoint.ManifestAfterPublish)]
    public async Task FlushFailureRequiresReopenAndRecoversAcknowledgedWrites(int point)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.FaultInjector = p =>
        {
            if ((int)p == point)
            {
                throw new IOException("Injected flush failure.");
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        storage.Write(Batch());
        await Assert.That(async () => await storage.FlushAndCompactAsync()).Throws<IOException>();
        await AssertFaultedAsync(storage);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await AssertValuesAsync(reopened);
    }

    [Test]
    [Arguments(false, (int)StorageFaultPoint.CompactionBeforeBuild)]
    [Arguments(false, (int)StorageFaultPoint.CompactionBeforeInstall)]
    [Arguments(false, (int)StorageFaultPoint.CompactionAfterInstall)]
    [Arguments(false, (int)StorageFaultPoint.ManifestBeforePublish)]
    [Arguments(false, (int)StorageFaultPoint.ManifestAfterPublish)]
    [Arguments(true, (int)StorageFaultPoint.CompactionBeforeBuild)]
    [Arguments(true, (int)StorageFaultPoint.CompactionBeforeInstall)]
    [Arguments(true, (int)StorageFaultPoint.CompactionAfterInstall)]
    [Arguments(true, (int)StorageFaultPoint.ManifestBeforePublish)]
    [Arguments(true, (int)StorageFaultPoint.ManifestAfterPublish)]
    public async Task CompactionFailureRequiresReopenWithoutLosingInputs(bool leveled, int point)
    {
        using var folder = TempFolder.Create();
        await SeedTwoTablesAsync(folder);
        var options = MaintenanceOptions(leveled);
        options.FaultInjector = p =>
        {
            if ((int)p == point)
            {
                throw new IOException("Injected compaction failure.");
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        await Assert.That(async () => await storage.FlushAndCompactAsync()).Throws<IOException>();
        await AssertFaultedAsync(storage);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await AssertValuesAsync(reopened);
    }

    [Test]
    public async Task BackgroundFailureIsObservableAndDisposalDoesNotFlush()
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.MemTableSizeLimit = 1;
        options.MemTableMaxCount = 1;
        options.FlushPeriod = TimeSpan.FromMilliseconds(1);
        options.FaultInjector = p =>
        {
            if (p == StorageFaultPoint.FlushBeforeBuild)
            {
                throw new IOException("Injected background failure.");
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        storage.Write(Batch());
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (storage.Health == StorageHealth.Healthy)
        {
            await Task.Delay(1, timeout.Token);
        }
        await AssertFaultedAsync(storage);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await AssertValuesAsync(reopened);
    }

    [Test]
    public async Task MaintenanceCancellationBeforeStartIsHealthyButAfterStartRequiresReopen()
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.FaultInjector = p =>
        {
            if (p == StorageFaultPoint.FlushAfterInstall)
            {
                throw new OperationCanceledException();
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        storage.Write(Batch());
        await Assert.That(async () => await storage.FlushAndCompactAsync(new CancellationToken(true)))
            .Throws<OperationCanceledException>();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
        await Assert.That(async () => await storage.FlushAndCompactAsync()).Throws<OperationCanceledException>();
        await AssertFaultedAsync(storage);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await AssertValuesAsync(reopened);
    }

    [Test]
    public async Task MvccCancellationAndPublicationHaveExplicitOutcomes()
    {
        using var folder = TempFolder.Create();
        using var cancellation = new CancellationTokenSource();
        var options = Options;
        var mode = 0;
        options.FaultInjector = p =>
        {
            if (mode == 1 && p == StorageFaultPoint.WalBeforeWrite)
            {
                cancellation.Cancel();
            }
            if (mode == 2 && p == StorageFaultPoint.MvccBeforePublication)
            {
                throw new IOException("Injected publication failure.");
            }
        };
        var storage = await MvccStorage.OpenAsync(folder, options);
        using (var transaction = storage.BeginTransaction())
        {
            transaction.Put([1], [11]);
            await Assert.That(async () => await transaction.CommitAsync(new CancellationToken(true)))
                .Throws<OperationCanceledException>();
        }
        await Assert.That(storage.PublishedSequence).IsEqualTo(0L);
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
        mode = 1;
        using (var transaction = storage.BeginTransaction())
        {
            transaction.Put([1], [11]);
            await transaction.CommitAsync(cancellation.Token);
        }
        await Assert.That(cancellation.IsCancellationRequested).IsTrue();
        await Assert.That(storage.PublishedSequence).IsEqualTo(1L);
        mode = 2;
        var failure = CaptureWriteFailure(() => storage.Put([2], [22]));
        await Assert.That(failure.Outcome).IsEqualTo(StorageCommitOutcome.Committed);
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Faulted);
        await Assert.That(storage.PublishedSequence).IsEqualTo(1L);
        await Assert.That(() => storage.CreateSnapshot()).Throws<StorageFaultedException>();
        await storage.DisposeAsync();
        await using var reopened = await MvccStorage.OpenAsync(folder, Options);
        await Assert.That(reopened.PublishedSequence).IsEqualTo(2L);
        var value = new byte[1];
        await Assert.That(await reopened.GetRawAsync(new byte[] { 2 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)22);
    }

    [Test]
    [Arguments((int)StorageFaultPoint.RecoveryBeforeReplay)]
    [Arguments((int)StorageFaultPoint.RecoveryAfterApplyEntry)]
    public async Task RecoveryFailureReturnsNoInstanceAndPreservesSourceWal(int point)
    {
        using var folder = TempFolder.Create();
        await CrashRecoveryTestProcess.WriteBatchAndExitWithoutDisposalAsync(folder, 2);
        var wal = Directory.GetFiles(folder, "*.wal").Single();
        var original = File.ReadAllBytes(wal);
        var options = Options;
        options.FaultInjector = p =>
        {
            if ((int)p == point)
            {
                throw new OutOfMemoryException("Injected recovery allocation failure.");
            }
        };
        await Assert.That(async () => await LsmStorage.OpenAsync(folder, options)).Throws<OutOfMemoryException>();
        await Assert.That(File.ReadAllBytes(wal).SequenceEqual(original)).IsTrue();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await Assert.That(await CountAsync(reopened)).IsEqualTo(2L);
    }

    [Test]
    public async Task RealSstCreationFailureFaultsAndPreservesWal()
    {
        using var folder = TempFolder.Create();
        var storage = await LsmStorage.OpenAsync(folder, Options);
        storage.Write(Batch());
        var wal = Directory.GetFiles(folder, "*.wal").Single();
        var blockedPath = Path.ChangeExtension(wal, ".sst.tmp");
        Directory.CreateDirectory(blockedPath);
        await Assert.That(async () => await storage.FlushAndCompactAsync()).Throws<Exception>();
        await AssertFaultedAsync(storage);
        await storage.DisposeAsync();
        Directory.Delete(blockedPath);
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await AssertValuesAsync(reopened);
    }

    [Test]
    public async Task ReaderWaitingOnFailedBatchCannotObservePartialMemory()
    {
        using var folder = TempFolder.Create();
        using var applying = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var options = Options;
        options.FaultInjector = p =>
        {
            if (p == StorageFaultPoint.BatchAfterApplyEntry)
            {
                applying.Set();
                if (!release.Wait(TimeSpan.FromSeconds(10)))
                {
                    throw new TimeoutException("Reader did not start.");
                }
                throw new OutOfMemoryException("Injected partial batch application.");
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        var write = Task.Run(() => CaptureWriteFailure(() => storage.Write(Batch())));
        var readerStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task? read = null;
        try
        {
            await Assert.That(applying.Wait(TimeSpan.FromSeconds(10))).IsTrue();
            read = Task.Run(async () =>
            {
                readerStarted.SetResult();
                await storage.GetRawAsync(new byte[] { 1 }, new byte[1]);
            });
            await readerStarted.Task.WaitAsync(TimeSpan.FromSeconds(10));
        }
        finally
        {
            release.Set();
        }
        await Assert.That((await write).Outcome).IsEqualTo(StorageCommitOutcome.Indeterminate);
        await Assert.That(async () => await read!).Throws<StorageFaultedException>();
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await AssertValuesAsync(reopened);
    }

    [Test]
    public async Task TypedReadsAndExistingIteratorRejectFaultedStore()
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.FaultInjector = p =>
        {
            if (p == StorageFaultPoint.BatchAfterApplyEntry)
            {
                throw new OutOfMemoryException("Injected typed mutation failure.");
            }
        };
        var storage = await LsmStorage.OpenAsync<int, int>(folder, options);
        var iterator = storage.CreateIterator();
        var failure = CaptureWriteFailure(() => storage.Put(1, 11));
        await Assert.That(failure.Outcome).IsEqualTo(StorageCommitOutcome.Indeterminate);
        await Assert.That(async () => await storage.GetAsync(1)).Throws<StorageFaultedException>();
        await Assert.That(() => storage.CreateIterator()).Throws<StorageFaultedException>();
        await Assert.That(async () =>
        {
            await foreach (var entry in iterator.EnumerateAsync())
            {
                GC.KeepAlive(entry);
            }
        }).Throws<StorageFaultedException>();
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync<int, int>(folder, Options);
        await Assert.That(await reopened.GetAsync(1)).IsEqualTo(11);
    }

    [Test]
    public async Task MvccShutdownFailureReleasesFaultedStoreAndPreservesRecovery()
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.FaultInjector = p =>
        {
            if (p == StorageFaultPoint.FlushAfterInstall)
            {
                throw new IOException("Injected shutdown failure.");
            }
        };
        var storage = await MvccStorage.OpenAsync(folder, options);
        storage.Put([1], [11]);
        await Assert.That(async () => await storage.DisposeAsync()).Throws<IOException>();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Disposed);
        await using var reopened = await MvccStorage.OpenAsync(folder, Options);
        await Assert.That(reopened.PublishedSequence).IsEqualTo(1L);
        var value = new byte[1];
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);
    }

    internal static StorageOptions MaintenanceOptions(bool leveled) => new()
    {
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = leveled ? CompactionStrategy.Leveled : CompactionStrategy.Tiered,
        Level0CompactionThreshold = 2,
        MinMergeWidth = 2,
        MaxCompactionTiers = 2,
        MaxSizeAmplificationPercent = 0
    };

    internal static async Task SeedTwoTablesAsync(string folder)
    {
        await using var storage = await LsmStorage.OpenAsync(folder, Options);
        storage.Put([1], [11]);
        await storage.FlushAndCompactAsync();
        storage.Put([2], [22]);
        await storage.FlushAndCompactAsync();
    }

    private static LsmWriteBatch Batch()
    {
        var batch = new LsmWriteBatch();
        batch.Put([1], [11]);
        batch.Put([2], [22]);
        return batch;
    }

    private static StorageOperationException CaptureWriteFailure(Action write)
    {
        try
        {
            write();
        }
        catch (StorageOperationException exception)
        {
            return exception;
        }
        throw new InvalidOperationException("Expected an injected mutation failure.");
    }

    internal static async Task AssertValuesAsync(LsmStorage storage)
    {
        var value = new byte[1];
        await Assert.That(await CountAsync(storage)).IsEqualTo(2L);
        await Assert.That(await storage.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);
        await Assert.That(await storage.GetRawAsync(new byte[] { 2 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)22);
    }

    private static ValueTask<long> CountAsync(LsmStorage storage) => storage.ScanRawAsync(0, static (_, _, _) => true);

    private static async Task AssertFaultedAsync(LsmStorage storage)
    {
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Faulted);
        await Assert.That(storage.Fault is not null).IsTrue();
        await Assert.That(() => storage.Put([9], [9])).Throws<StorageFaultedException>();
        await Assert.That(() => storage.Write(new LsmWriteBatch())).Throws<StorageFaultedException>();
        await Assert.That(async () => await storage.GetRawAsync(new byte[] { 1 }, new byte[1])).Throws<StorageFaultedException>();
        await Assert.That(async () => await CountAsync(storage)).Throws<StorageFaultedException>();
        await Assert.That(async () => await storage.SeekRawAsync(new byte[] { 1 }, 0, static (_, _, _) => true))
            .Throws<StorageFaultedException>();
        await Assert.That(async () => await storage.FlushAndCompactAsync()).Throws<StorageFaultedException>();
    }
}
