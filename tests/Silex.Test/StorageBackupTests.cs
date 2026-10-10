using System.Text.Json;

namespace Silex.Test;

public class StorageBackupTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);
    private static StorageOptions Options => new() { FlushPeriod = TimeSpan.Zero, CompactionStrategy = CompactionStrategy.None };

    [Test]
    [Arguments("current")]
    [Arguments("rotated")]
    [Arguments("recovered")]
    [Arguments("flushed")]
    [Arguments("tiered")]
    [Arguments("leveled")]
    [Arguments("no-wal")]
    [Arguments("no-wal-tiered")]
    [Arguments("no-wal-leveled")]
    [Arguments("empty")]
    public async Task CheckpointRestoresPhysicalStateWithoutChangingSource(string mode)
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        var options = mode.EndsWith("tiered") || mode.EndsWith("leveled")
            ? StorageFailureTests.MaintenanceOptions(mode.EndsWith("leveled")) : Options;
        options.UseWriteAheadLog = !mode.StartsWith("no-wal");
        if (mode == "rotated" || mode.StartsWith("no-wal"))
        {
            options.MemTableSizeLimit = 10;
        }
        if (mode == "recovered")
        {
            await CrashRecoveryTestProcess.WriteBatchAndExitWithoutDisposalAsync(source, 2);
        }
        await using var storage = await LsmStorage.OpenAsync(source, options);
        if (mode != "empty")
        {
            storage.Put([1], [10]);
            if (mode.EndsWith("tiered") || mode.EndsWith("leveled") || mode == "flushed")
            {
                await storage.FlushAndCompactAsync();
            }
            var batch = new LsmWriteBatch();
            batch.Put([1], [11]);
            batch.Put([2], []);
            batch.Put([3], [33]);
            batch.Delete([3]);
            storage.Write(batch);
        }
        var before = RecoveryFiles(source);
        await storage.CreateBackupAsync(backup);
        await AssertUnchangedAsync(before, source);
        storage.Put([1], [99]);
        await storage.FlushAndCompactAsync();
        await LsmStorage.RestoreBackupAsync(backup, destination, Options);
        await Assert.That(File.Exists(Path.Combine(destination, StorageBackup.FileName))).IsFalse();
        await Assert.That((await LsmStorage.VerifyIntegrityAsync(destination, Options)).IsRecoveryComplete).IsTrue();
        await using var restored = await LsmStorage.OpenAsync(destination, Options);
        await AssertValueAsync(restored, 1, mode == "empty" ? -1 : 11);
        await Assert.That(await restored.GetRawAsync(new byte[] { 2 }, new byte[1])).IsEqualTo(mode == "empty" ? -1 : 0);
        await AssertValueAsync(restored, 3, -1);
        restored.Put([4], [44]);
        await AssertValueAsync(restored, 4, 44);
        // Restoring does not consume or modify the backup.
        StorageBackup.Read(backup, false, Options).VerifyFiles(backup, default);
    }

    [Test]
    [Arguments(1, true)]
    [Arguments(8, true)]
    [Arguments(1, false)]
    public async Task MvccBackupPreservesPublishedAtomicCommitsButExcludesStagingAndLaterWrites(int groupSize, bool wal)
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        var options = Options;
        options.MvccCommitGroupSize = groupSize;
        options.UseWriteAheadLog = wal;
        options.SyncWriteAheadLogToDisk = groupSize > 1;
        await using var storage = await MvccStorage.OpenAsync(source, options);
        var commits = Enumerable.Range(1, 8).Select(async i =>
        {
            using var transaction = storage.BeginTransaction();
            transaction.Put(new byte[] { (byte)i }, new byte[] { (byte)(i + 10) });
            transaction.Put(new byte[] { (byte)(i + 20) }, []);
            await transaction.CommitAsync();
        }).ToArray();
        await Task.WhenAll(commits);
        using var staged = storage.BeginTransaction();
        staged.Put([99], [99]);
        var sequence = storage.PublishedSequence;
        await storage.CreateBackupAsync(backup);
        storage.Put([1], [100]);
        await storage.CollectGarbageAsync(compact: true);
        await MvccStorage.RestoreBackupAsync(backup, destination, options);
        await using var restored = await MvccStorage.OpenAsync(destination, options);
        await Assert.That(restored.PublishedSequence).IsEqualTo(sequence);
        var value = new byte[1];
        for (var i = 1; i <= 8; i++)
        {
            await Assert.That(await restored.GetRawAsync(new byte[] { (byte)i }, value)).IsEqualTo(1);
            await Assert.That(value[0]).IsEqualTo((byte)(i + 10));
            await Assert.That(await restored.GetRawAsync(new byte[] { (byte)(i + 20) }, value)).IsEqualTo(0);
        }
        await Assert.That(await restored.GetRawAsync(new byte[] { 99 }, value)).IsEqualTo(-1);
        restored.Put([99], [9]);
        await Assert.That(restored.PublishedSequence).IsGreaterThan(sequence);
    }

    [Test]
    [Arguments("manifest")]
    [Arguments("sst")]
    [Arguments("wal")]
    [Arguments("missing-wal")]
    [Arguments("missing-manifest")]
    [Arguments("marker")]
    [Arguments("extra")]
    [Arguments("traversal")]
    [Arguments("wrong-mode")]
    [Arguments("disabled-wal")]
    [Arguments("pending")]
    public async Task RestoreRejectsUnhealthyOrIncompleteBackupBeforeCopying(string mode)
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(source, Options);
        storage.Put([1], [11]);
        await storage.FlushAndCompactAsync();
        storage.Put([2], [22]);
        await storage.CreateBackupAsync(backup);
        var marker = Path.Combine(backup, StorageBackup.FileName);
        var options = Options;
        if (mode is "missing-wal" or "missing-manifest")
        {
            File.Delete(mode == "missing-wal" ? Directory.GetFiles(backup, "*.wal").Single() : Manifest.GetPath(backup));
        }
        else if (mode is "manifest" or "sst" or "wal")
        {
            var path = mode == "manifest" ? Manifest.GetPath(backup) : Directory.GetFiles(backup, $"*.{mode}").Single();
            File.WriteAllBytes(path, [0, 1, 2]);
        }
        else if (mode == "marker")
        {
            File.Delete(marker);
        }
        else if (mode == "extra")
        {
            File.WriteAllText(Path.Combine(backup, "unexpected.sst"), "not a recovery file");
        }
        else if (mode == "pending")
        {
            File.WriteAllText(Path.Combine(backup, StorageBackup.PendingFileName), "incomplete");
        }
        else if (mode == "disabled-wal")
        {
            options.UseWriteAheadLog = false;
        }
        else
        {
            var inventory = StorageBackup.Read(backup, false, Options);
            if (mode == "traversal")
            {
                inventory.Files[0].Name = "../evidence";
            }
            else
            {
                inventory.Mvcc = true;
            }
            File.WriteAllText(marker, JsonSerializer.Serialize(inventory));
        }
        var before = RecoveryFiles(backup);
        Exception? failure = null;
        try
        {
            await LsmStorage.RestoreBackupAsync(backup, destination, options);
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        await Assert.That(failure is IOException or InvalidDataException or JsonException).IsTrue();
        await AssertEmptyAsync(destination);
        await AssertUnchangedAsync(before, backup);
    }

    [Test]
    [Arguments("wal")]
    [Arguments("sst")]
    [Arguments("manifest")]
    [Arguments("mvcc")]
    public async Task RestoreChecksRecoveryStructureEvenWhenInventoryChecksumsMatch(string mode)
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(source, Options);
        storage.Put([1], [11]);
        await storage.FlushAndCompactAsync();
        storage.Put([2], [22]);
        await storage.CreateBackupAsync(backup);
        var inventory = StorageBackup.Read(backup, false, Options);
        if (mode == "mvcc")
        {
            inventory.Mvcc = true;
        }
        else
        {
            var file = inventory.Files.Single(file => mode == "manifest" ? file.Name == "manifest" : file.Name.EndsWith($".{mode}"));
            var path = Path.Combine(backup, file.Name);
            File.WriteAllBytes(path, [0, 1, 2]);
            var replacement = StorageBackup.DescribeFile(path, default);
            file.Length = replacement.Length;
            file.Sha256 = replacement.Sha256;
        }
        File.WriteAllText(Path.Combine(backup, StorageBackup.FileName), JsonSerializer.Serialize(inventory));
        Exception? failure = null;
        try
        {
            if (mode == "mvcc")
            {
                await MvccStorage.RestoreBackupAsync(backup, destination, Options);
            }
            else
            {
                await LsmStorage.RestoreBackupAsync(backup, destination, Options);
            }
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        await Assert.That(failure is StorageRecoveryException or InvalidDataException).IsTrue();
        await AssertEmptyAsync(destination);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FailedOrCanceledCopyCleansOnlyDestinationAndKeepsSourceUsable(bool restore)
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        using var cancellation = new CancellationTokenSource();
        var options = Options;
        var armed = false;
        var cancel = false;
        options.FaultInjector = point =>
        {
            if (armed && point == StorageFaultPoint.BackupAfterCopy)
            {
                if (cancel)
                {
                    cancellation.Cancel();
                }
                else
                {
                    throw new IOException("Injected copy failure.");
                }
            }
        };
        await using var storage = await LsmStorage.OpenAsync(source, options);
        storage.Put([1], [11]);
        await storage.CreateBackupAsync(backup);
        var before = RecoveryFiles(restore ? backup : source);
        armed = true;
        await Assert.That(async () =>
        {
            if (restore)
            {
                await LsmStorage.RestoreBackupAsync(backup, destination, options);
            }
            else
            {
                await storage.CreateBackupAsync(destination);
            }
        }).Throws<IOException>();
        await AssertEmptyAsync(destination);
        cancel = true;
        await Assert.That(async () =>
        {
            if (restore)
            {
                await LsmStorage.RestoreBackupAsync(backup, destination, options, cancellation.Token);
            }
            else
            {
                await storage.CreateBackupAsync(destination, cancellation.Token);
            }
        }).Throws<OperationCanceledException>();
        await AssertEmptyAsync(destination);
        await AssertUnchangedAsync(before, restore ? backup : source);
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
        storage.Put([2], [22]);
        armed = false;
        if (restore)
        {
            await LsmStorage.RestoreBackupAsync(backup, destination, options);
        }
        else
        {
            await storage.CreateBackupAsync(destination);
        }
    }

    [Test]
    [Arguments("write")]
    [Arguments("maintenance")]
    [Arguments("synchronize")]
    [Arguments("shutdown")]
    public async Task CapturePinsFilesAndLifetimeAndExcludesRacingWrites(string mode)
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = Options;
        options.FaultInjector = point =>
        {
            if (point == StorageFaultPoint.BackupBeforeCopy)
            {
                entered.TrySetResult();
                if (!release.Wait(Timeout))
                {
                    throw new TimeoutException("Backup was not released.");
                }
            }
        };
        var storage = await LsmStorage.OpenAsync(source, options);
        storage.Put([1], [11]);
        var capture = storage.CreateBackupAsync(backup);
        Task? competing = null;
        try
        {
            await entered.Task.WaitAsync(Timeout);
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            competing = Task.Run(async () =>
            {
                started.SetResult();
                if (mode == "write")
                {
                    storage.Put([2], [22]);
                }
                else if (mode == "maintenance")
                {
                    await storage.FlushAndCompactAsync();
                }
                else if (mode == "synchronize")
                {
                    await storage.SynchronizeAsync();
                }
                else
                {
                    await storage.CloseAsync();
                }
            });
            await started.Task.WaitAsync(Timeout);
            await Assert.That(competing.IsCompleted).IsFalse();
            await Assert.That(async () => await LsmStorage.OpenAsync(source, Options)).Throws<IOException>();
            await Assert.That(async () => await LsmStorage.OpenAsync(backup, Options)).Throws<IOException>();
        }
        finally
        {
            release.Set();
        }
        await capture.WaitAsync(Timeout);
        await competing!.WaitAsync(Timeout);
        await storage.DisposeAsync();
        await LsmStorage.RestoreBackupAsync(backup, destination, Options);
        await using var restored = await LsmStorage.OpenAsync(destination, Options);
        await AssertValueAsync(restored, 1, 11);
        await AssertValueAsync(restored, 2, -1);
    }

    [Test]
    public async Task DestinationsMustBeEmptyDisjointAndUnownedAndSourcesCannotBeSalvage()
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        await using (var storage = await LsmStorage.OpenAsync(source, Options))
        {
            storage.Put([1], [11]);
            await Assert.That(async () => await storage.CreateBackupAsync(source)).Throws<ArgumentException>();
            await Assert.That(async () => await storage.CreateBackupAsync(Path.Combine(source, "child"))).Throws<ArgumentException>();
            await Assert.That(async () => await storage.CreateBackupAsync(Path.GetDirectoryName(source.Path)!)).Throws<ArgumentException>();
            await using (var owner = await LsmStorage.OpenAsync(destination, Options))
            {
                await Assert.That(async () => await storage.CreateBackupAsync(destination)).Throws<IOException>();
            }
            await Assert.That(async () => await storage.CreateBackupAsync(destination)).Throws<IOException>();
            await storage.CreateBackupAsync(backup);
            await Assert.That(async () => await LsmStorage.RestoreBackupAsync(backup, source, Options)).Throws<IOException>();
            await Assert.That(async () => await LsmStorage.RestoreBackupAsync(backup, backup, Options)).Throws<ArgumentException>();
            await using var backupOwner = await LsmStorage.OpenSalvageAsync(backup, Options);
            await Assert.That(async () => await LsmStorage.RestoreBackupAsync(backup, destination, Options)).Throws<IOException>();
        }
        await using var salvage = await LsmStorage.OpenSalvageAsync(source, Options);
        await Assert.That(async () => await salvage.CreateBackupAsync(backup)).Throws<InvalidOperationException>();
    }

    [Test]
    [Arguments((int)StorageFaultPoint.BackupBeforeCopy)]
    [Arguments((int)StorageFaultPoint.BackupAfterCopy)]
    [Arguments((int)StorageFaultPoint.BackupBeforePublish)]
    public async Task CrashedBackupIsNotAcceptedAndSourceStillRecovers(int point)
    {
        using var source = TempFolder.Create();
        using var destination = TempFolder.Create();
        await CrashRecoveryTestProcess.CrashAtAsync(source, point, "backup");
        var backup = source.Path + "-backup";
        try
        {
            await Assert.That(File.Exists(Path.Combine(backup, StorageBackup.PendingFileName))).IsTrue();
            await Assert.That(async () => await LsmStorage.OpenAsync(backup, Options)).Throws<InvalidDataException>();
            await Assert.That(async () => await LsmStorage.RestoreBackupAsync(backup, destination, Options)).Throws<InvalidDataException>();
            await using var reopened = await LsmStorage.OpenAsync(source, Options);
            await StorageFailureTests.AssertValuesAsync(reopened);
        }
        finally
        {
            Directory.Delete(backup, recursive: true);
        }
    }

    [Test]
    [Arguments((int)StorageFaultPoint.BackupBeforeCopy)]
    [Arguments((int)StorageFaultPoint.BackupAfterCopy)]
    public async Task CrashedRestoreIsGuardedAndDoesNotConsumeBackup(int point)
    {
        using var source = TempFolder.Create();
        using var destination = TempFolder.Create();
        await CrashRecoveryTestProcess.CrashAtAsync(source, point, "restore");
        var backup = source.Path + "-backup";
        var partial = source.Path + "-restored";
        try
        {
            await Assert.That(async () => await LsmStorage.OpenAsync(partial, Options)).Throws<InvalidDataException>();
            var report = await LsmStorage.VerifyIntegrityAsync(partial, Options);
            await Assert.That(report.IsRecoveryComplete).IsFalse();
            await Assert.That(report.Issues.Single().Kind).IsEqualTo(StorageIntegrityIssueKind.IncompletePhysicalCopy);
            await Assert.That(async () => await LsmStorage.OpenSalvageAsync(partial, Options)).Throws<StorageRecoveryException>();
            await LsmStorage.RestoreBackupAsync(backup, destination, Options);
            await using var restored = await LsmStorage.OpenAsync(destination, Options);
            await StorageFailureTests.AssertValuesAsync(restored);
            await Assert.That(async () => await LsmStorage.OpenAsync(backup, Options)).Throws<InvalidOperationException>();
        }
        finally
        {
            Directory.Delete(partial, recursive: true);
            Directory.Delete(backup, recursive: true);
        }
    }

    [Test]
    public async Task RecoveredUncommittedWalTailRemainsRecoverableAndIsNotCommittedCorruption()
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        await CrashRecoveryTestProcess.WriteBatchAndExitWithoutDisposalAsync(source, 2);
        using (var stream = new FileStream(Directory.GetFiles(source, "*.wal").Single(), FileMode.Append))
        {
            stream.Write([0, 1, 2]);
        }
        await using var storage = await LsmStorage.OpenAsync(source, Options);
        await storage.CreateBackupAsync(backup);
        var report = await LsmStorage.VerifyIntegrityAsync(backup, Options);
        await Assert.That(report.IsRecoveryComplete).IsTrue();
        await Assert.That(report.Issues.Single().Kind).IsEqualTo(StorageIntegrityIssueKind.IncompleteWalTail);
        await LsmStorage.RestoreBackupAsync(backup, destination, Options);
        await using var restored = await LsmStorage.OpenAsync(destination, Options);
        var value = new byte[4];
        await Assert.That(await restored.GetRawAsync(new byte[] { 0, 0, 0, 0 }, value)).IsEqualTo(4);
        await Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(value)).IsEqualTo(1);
        await Assert.That(await restored.GetRawAsync(new byte[] { 1, 0, 0, 0 }, value)).IsEqualTo(4);
        await Assert.That(System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(value)).IsEqualTo(2);
    }

    [Test]
    public async Task CancellationWhileWaitingForMaintenanceCreatesNoUsablePartialBackup()
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var release = new ManualResetEventSlim();
        using var cancellation = new CancellationTokenSource();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = Options;
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
        await using var storage = await LsmStorage.OpenAsync(source, options);
        storage.Put([1], [11]);
        var maintenance = Task.Run(() => storage.FlushAndCompactAsync());
        try
        {
            await entered.Task.WaitAsync(Timeout);
            var capture = storage.CreateBackupAsync(backup, cancellation.Token);
            await Assert.That(capture.IsCompleted).IsFalse();
            cancellation.Cancel();
            await Assert.That(async () => await capture.WaitAsync(Timeout)).Throws<OperationCanceledException>();
            await AssertEmptyAsync(backup);
        }
        finally
        {
            release.Set();
        }
        await maintenance.WaitAsync(Timeout);
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
        await storage.CreateBackupAsync(backup);
    }

    [Test]
    [Arguments(1)]
    [Arguments(8)]
    public async Task MvccBackupWaitsForCommitPublicationAndThenCapturesWholeCommit(int groupSize)
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = Options;
        options.MvccCommitGroupSize = groupSize;
        options.SyncWriteAheadLogToDisk = true;
        options.FaultInjector = point =>
        {
            if (point == StorageFaultPoint.MvccBeforePublication)
            {
                entered.TrySetResult();
                if (!release.Wait(Timeout))
                {
                    throw new TimeoutException("Publication was not released.");
                }
            }
        };
        await using var storage = await MvccStorage.OpenAsync(source, options);
        var commit = Task.Run(async () =>
        {
            using var transaction = storage.BeginTransaction();
            transaction.Put([1], [11]);
            transaction.Put([2], [22]);
            await transaction.CommitAsync();
        });
        Task? capture = null;
        try
        {
            await entered.Task.WaitAsync(Timeout);
            capture = storage.CreateBackupAsync(backup);
            await Assert.That(capture.IsCompleted).IsFalse();
            await Assert.That(Directory.GetFiles(backup).Length).IsEqualTo(0);
        }
        finally
        {
            release.Set();
        }
        await commit.WaitAsync(Timeout);
        await capture!.WaitAsync(Timeout);
        await MvccStorage.RestoreBackupAsync(backup, destination, Options);
        await using var restored = await MvccStorage.OpenAsync(destination, Options);
        await Assert.That(restored.PublishedSequence).IsEqualTo(1L);
        var value = new byte[1];
        await Assert.That(await restored.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);
        await Assert.That(await restored.GetRawAsync(new byte[] { 2 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)22);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CheckpointValidationOrPublicationFailureLeavesSourceEvidenceAndDestinationRetryable(bool corrupt)
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        var options = Options;
        options.FaultInjector = point =>
        {
            if (!corrupt && point == StorageFaultPoint.BackupBeforePublish)
            {
                throw new IOException("Injected backup publication failure.");
            }
        };
        await using var storage = await LsmStorage.OpenAsync(source, options);
        storage.Put([1], [11]);
        await storage.FlushAndCompactAsync();
        if (corrupt)
        {
            File.Delete(Directory.GetFiles(source, "*.sst").Single());
        }
        var before = RecoveryFiles(source);
        await Assert.That(async () => await storage.CreateBackupAsync(backup)).Throws<IOException>();
        await AssertUnchangedAsync(before, source);
        await AssertEmptyAsync(backup);
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
    }

    [Test]
    public async Task DirectoryAliasesCannotBypassOverlapAndRecoveryFileLinksAreRejected()
    {
        if (OperatingSystem.IsWindows())
        {
            return; // Creating symlinks on Windows requires privileges not supplied by all test hosts.
        }
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        using var destination = TempFolder.Create();
        using var aliases = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(source, Options);
        storage.Put([1], [11]);
        var alias = Path.Combine(aliases, "source");
        Directory.CreateSymbolicLink(alias, source);
        await Assert.That(async () => await storage.CreateBackupAsync(Path.Combine(alias, "child"))).Throws<ArgumentException>();
        await storage.CreateBackupAsync(backup);
        var wal = Directory.GetFiles(backup, "*.wal").Single();
        var evidence = Path.Combine(aliases, "evidence");
        File.Move(wal, evidence);
        File.CreateSymbolicLink(wal, evidence);
        await Assert.That(async () => await LsmStorage.RestoreBackupAsync(backup, destination, Options)).Throws<IOException>();
        await AssertEmptyAsync(destination);
        await Assert.That(File.Exists(evidence)).IsTrue();
    }

    [Test]
    public async Task CleanupFailureIsExplicitAndRetainsIncompleteCopyGuard()
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        var options = Options;
        options.FaultInjector = point =>
        {
            if (point == StorageFaultPoint.BackupAfterCopy)
            {
                // Emulate a filesystem obstruction that prevents deleting a call-owned output.
                var path = Directory.GetFiles(backup, "*.wal").Single();
                File.Delete(path);
                Directory.CreateDirectory(path);
                throw new IOException("Injected failure with an obstructed cleanup path.");
            }
        };
        await using var storage = await LsmStorage.OpenAsync(source, options);
        storage.Put([1], [11]);
        await Assert.That(async () => await storage.CreateBackupAsync(backup)).Throws<AggregateException>();
        await Assert.That(File.Exists(Path.Combine(backup, StorageBackup.PendingFileName))).IsTrue();
        await Assert.That(async () => await LsmStorage.OpenAsync(backup, Options)).Throws<InvalidDataException>();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Healthy);
        await AssertValueAsync(storage, 1, 11);
    }

    [Test]
    public async Task FaultedSourceRejectsBackupBeforeCreatingDestinationFiles()
    {
        using var source = TempFolder.Create();
        using var backup = TempFolder.Create();
        var options = Options;
        var armed = false;
        options.FaultInjector = point =>
        {
            if (armed && point == StorageFaultPoint.WalDuringWrite)
            {
                throw new IOException("Injected uncertain mutation.");
            }
        };
        await using var storage = await LsmStorage.OpenAsync(source, options);
        armed = true;
        await Assert.That(() => storage.Put([1], [11])).Throws<StorageOperationException>();
        await Assert.That(async () => await storage.CreateBackupAsync(backup)).Throws<StorageFaultedException>();
        await Assert.That(Directory.GetFiles(backup).Length).IsEqualTo(0);
    }

    private static async Task AssertEmptyAsync(string path) =>
        await Assert.That(Directory.GetFileSystemEntries(path).All(entry => Path.GetFileName(entry) == DirectoryOwnership.FileName)).IsTrue();

    private static async Task AssertValueAsync(LsmStorage storage, byte key, int expected)
    {
        var value = new byte[1];
        await Assert.That(await storage.GetRawAsync(new byte[] { key }, value)).IsEqualTo(expected < 0 ? -1 : 1);
        if (expected >= 0)
        {
            await Assert.That(value[0]).IsEqualTo((byte)expected);
        }
    }

    private static Dictionary<string, byte[]> RecoveryFiles(string folder)
    {
        var files = new Dictionary<string, byte[]>();
        foreach (var path in Directory.GetFiles(folder))
        {
            if (Path.GetFileName(path) is DirectoryOwnership.FileName or StorageBackup.FileName)
            {
                continue;
            }
            using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            var bytes = new byte[checked((int)input.Length)];
            input.ReadExactly(bytes);
            files.Add(path, bytes);
        }
        return files;
    }

    private static async Task AssertUnchangedAsync(Dictionary<string, byte[]> before, string folder)
    {
        var after = RecoveryFiles(folder);
        await Assert.That(after.Count).IsEqualTo(before.Count);
        foreach (var (path, bytes) in before)
        {
            await Assert.That(after[path].SequenceEqual(bytes)).IsTrue();
        }
    }
}
