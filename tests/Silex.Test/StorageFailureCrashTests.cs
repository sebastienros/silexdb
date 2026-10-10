namespace Silex.Test;

public class StorageFailureCrashTests
{
    [Test]
    [Arguments((int)StorageFaultPoint.WalBeforeWrite, 0)]
    [Arguments((int)StorageFaultPoint.WalDuringWrite, 0)]
    [Arguments((int)StorageFaultPoint.WalAfterWrite, 2)]
    [Arguments((int)StorageFaultPoint.WalBeforeSync, 2)]
    [Arguments((int)StorageFaultPoint.WalAfterSync, 2)]
    [Arguments((int)StorageFaultPoint.BatchBeforeApply, 2)]
    [Arguments((int)StorageFaultPoint.BatchAfterApplyEntry, 2)]
    [Arguments((int)StorageFaultPoint.WriteBeforePublication, 2)]
    public async Task CrashDuringWriteRecoversWholeFrameOrNone(int point, int count)
    {
        using var folder = TempFolder.Create();
        await CrashRecoveryTestProcess.CrashAtAsync(folder, point);
        await using var reopened = await LsmStorage.OpenAsync(folder, new StorageOptions { FlushPeriod = TimeSpan.Zero });
        await Assert.That(await reopened.ScanRawAsync(0, static (_, _, _) => true)).IsEqualTo((long)count);
        if (count == 2)
        {
            await StorageFailureTests.AssertValuesAsync(reopened);
        }
    }

    [Test]
    [Arguments("flush", (int)StorageFaultPoint.FlushBeforeBuild)]
    [Arguments("flush", (int)StorageFaultPoint.FlushBeforeInstall)]
    [Arguments("flush", (int)StorageFaultPoint.FlushAfterInstall)]
    [Arguments("flush", (int)StorageFaultPoint.SynchronizationBeforeSst)]
    [Arguments("flush", (int)StorageFaultPoint.SynchronizationAfterSst)]
    [Arguments("flush", (int)StorageFaultPoint.ManifestBeforeSync)]
    [Arguments("flush", (int)StorageFaultPoint.ManifestAfterSync)]
    [Arguments("flush", (int)StorageFaultPoint.ManifestBeforePublish)]
    [Arguments("flush", (int)StorageFaultPoint.ManifestAfterPublish)]
    [Arguments("tiered", (int)StorageFaultPoint.CompactionBeforeBuild)]
    [Arguments("tiered", (int)StorageFaultPoint.CompactionBeforeInstall)]
    [Arguments("tiered", (int)StorageFaultPoint.CompactionAfterInstall)]
    [Arguments("tiered", (int)StorageFaultPoint.SynchronizationBeforeSst)]
    [Arguments("tiered", (int)StorageFaultPoint.SynchronizationAfterSst)]
    [Arguments("tiered", (int)StorageFaultPoint.ManifestBeforeSync)]
    [Arguments("tiered", (int)StorageFaultPoint.ManifestAfterSync)]
    [Arguments("tiered", (int)StorageFaultPoint.ManifestBeforePublish)]
    [Arguments("tiered", (int)StorageFaultPoint.ManifestAfterPublish)]
    [Arguments("leveled", (int)StorageFaultPoint.CompactionBeforeBuild)]
    [Arguments("leveled", (int)StorageFaultPoint.CompactionBeforeInstall)]
    [Arguments("leveled", (int)StorageFaultPoint.CompactionAfterInstall)]
    [Arguments("leveled", (int)StorageFaultPoint.SynchronizationBeforeSst)]
    [Arguments("leveled", (int)StorageFaultPoint.SynchronizationAfterSst)]
    [Arguments("leveled", (int)StorageFaultPoint.ManifestBeforeSync)]
    [Arguments("leveled", (int)StorageFaultPoint.ManifestAfterSync)]
    [Arguments("leveled", (int)StorageFaultPoint.ManifestBeforePublish)]
    [Arguments("leveled", (int)StorageFaultPoint.ManifestAfterPublish)]
    public async Task CrashDuringMaintenancePreservesAcknowledgedData(string mode, int point)
    {
        using var folder = TempFolder.Create();
        if (mode != "flush")
        {
            await StorageFailureTests.SeedTwoTablesAsync(folder);
        }
        await CrashRecoveryTestProcess.CrashAtAsync(folder, point, mode);
        await using var reopened = await LsmStorage.OpenAsync(folder, new StorageOptions { FlushPeriod = TimeSpan.Zero });
        await StorageFailureTests.AssertValuesAsync(reopened);
    }

    [Test]
    public async Task CrashBeforeMvccPublicationRecoversCommittedSequence()
    {
        using var folder = TempFolder.Create();
        await CrashRecoveryTestProcess.CrashAtAsync(folder, (int)StorageFaultPoint.MvccBeforePublication, "mvcc");
        await using var reopened = await MvccStorage.OpenAsync(folder, new StorageOptions { FlushPeriod = TimeSpan.Zero });
        await Assert.That(reopened.PublishedSequence).IsEqualTo(1L);
        var value = new byte[1];
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);
    }

    [Test]
    [Arguments((int)StorageFaultPoint.RecoveryBeforeReplay)]
    [Arguments((int)StorageFaultPoint.RecoveryAfterApplyEntry)]
    public async Task CrashDuringRecoveryPreservesOriginalWal(int point)
    {
        using var folder = TempFolder.Create();
        await CrashRecoveryTestProcess.WriteBatchAndExitWithoutDisposalAsync(folder, 2);
        await CrashRecoveryTestProcess.CrashAtAsync(folder, point, "recovery");
        await using var reopened = await LsmStorage.OpenAsync(folder, new StorageOptions { FlushPeriod = TimeSpan.Zero });
        await Assert.That(await reopened.ScanRawAsync(0, static (_, _, _) => true)).IsEqualTo(2L);
    }
}
