using System.Buffers.Binary;
using Silex.Wal;

namespace Silex.Test;

public class StorageIntegrityTests
{
    private static StorageOptions Options => new() { FlushPeriod = TimeSpan.Zero, CompactionStrategy = CompactionStrategy.None };

    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task NormalOpenRejectsMissingCommittedSstWithoutChangingFiles(bool leveled, bool survivingWal)
    {
        using var folder = TempFolder.Create();
        await CreateSstAsync(folder);
        var manifest = Manifest.TryRead(folder)!;
        if (leveled)
        {
            manifest.Levels.Add(manifest.L0);
            manifest.L0 = [];
            manifest.Write(folder);
        }

        var id = manifest.AllSstIds().Single();
        File.Delete(Path.Combine(folder, $"{id}.sst"));
        // A matching surviving WAL does not prove that all committed SST contents can be recovered.
        if (survivingWal)
        {
            using var wal = new WriteAheadLog(Path.Combine(folder, $"{id}.wal"), false);
            wal.AppendRaw([1], [11]);
        }

        await File.WriteAllBytesAsync(Path.Combine(folder, "999999.sst.tmp"), [1]);
        var before = Snapshot(folder);
        await Assert.That(async () => await LsmStorage.OpenAsync(folder, Options)).Throws<StorageRecoveryException>();
        var report = await LsmStorage.VerifyIntegrityAsync(folder, Options);
        await Assert.That(report.IsRecoveryComplete).IsFalse();
        await Assert.That(report.Issues.Any(issue => issue.Kind == StorageIntegrityIssueKind.MissingSst
            && issue.Path == Path.Combine(folder, $"{id}.sst") && issue.Message.Contains("backup"))).IsTrue();
        await AssertUnchangedAsync(folder, before);
    }

    [Test]
    public async Task SalvageIsReadOnlyAndPreservesEvidenceOnClose()
    {
        using var folder = TempFolder.Create();
        using var destinationFolder = TempFolder.Create();
        await CreateSstAsync(folder);
        var missingSst = Directory.GetFiles(folder, "*.sst").Single();
        await CreateSstAsync(folder, key: 2);
        File.Delete(missingSst);
        await File.WriteAllBytesAsync(Path.Combine(folder, "999999.sst"), [0]);
        await File.WriteAllBytesAsync(Path.Combine(folder, "999999.sst.tmp"), [0]);
        using (var wal = new WriteAheadLog(Path.Combine(folder, "999998.wal"), false))
        {
            wal.AppendRaw([3], [33]);
        }

        var before = Snapshot(folder);
        await using (var salvage = await LsmStorage.OpenSalvageAsync(folder, Options))
        {
            await Assert.That(salvage.IsReadOnly).IsTrue();
            await Assert.That(salvage.RecoveryReport!.IsRecoveryComplete).IsFalse();
            var value = new byte[8];
            await Assert.That(await salvage.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(-1);
            await Assert.That(await salvage.GetRawAsync(new byte[] { 2 }, value)).IsEqualTo(1);
            await Assert.That(value[0]).IsEqualTo((byte)11);
            await Assert.That(await salvage.GetRawAsync(new byte[] { 3 }, value)).IsEqualTo(1);
            await Assert.That(value[0]).IsEqualTo((byte)33);
            await Assert.That(() => salvage.Put([4], [44])).Throws<InvalidOperationException>();
            await Assert.That(() => salvage.Delete([2])).Throws<InvalidOperationException>();
            await Assert.That(() => salvage.Write(new LsmWriteBatch())).Throws<InvalidOperationException>();
            await Assert.That(async () => await salvage.FlushAndCompactAsync()).Throws<InvalidOperationException>();
            await using (var destination = await LsmStorage.OpenAsync(destinationFolder, Options))
            {
                var extracted = await salvage.ScanRawAsync(destination, static (db, key, value) =>
                {
                    db.Put(key, value);
                    return true;
                });
                await Assert.That(extracted).IsEqualTo(2L);
                await Assert.That(await destination.GetRawAsync(new byte[] { 2 }, value)).IsEqualTo(1);
                await Assert.That(await destination.GetRawAsync(new byte[] { 3 }, value)).IsEqualTo(1);
            }

            await salvage.CloseAsync();
        }

        await AssertUnchangedAsync(folder, before);
        await Assert.That(async () => await LsmStorage.OpenAsync(folder, Options)).Throws<StorageRecoveryException>();
    }

    [Test]
    public async Task TornWalTailDoesNotMeanIncompleteCommittedRecovery()
    {
        using var folder = TempFolder.Create();
        await CreateSstAsync(folder);
        var walPath = Path.Combine(folder, "999998.wal");
        using (var wal = new WriteAheadLog(walPath, false))
        {
            wal.AppendRaw([2], [22]);
            wal.AppendRaw([3], [33]);
        }

        using (var stream = File.OpenWrite(walPath))
        {
            stream.SetLength(stream.Length - 1);
        }

        var before = Snapshot(folder);
        var report = await LsmStorage.VerifyIntegrityAsync(folder, Options);
        await Assert.That(report.IsRecoveryComplete).IsTrue();
        await Assert.That(report.Issues.Single().Kind).IsEqualTo(StorageIntegrityIssueKind.IncompleteWalTail);
        await AssertUnchangedAsync(folder, before);
        await using var recovered = await LsmStorage.OpenAsync(folder, Options);
        var value = new byte[8];
        await Assert.That(await recovered.GetRawAsync(new byte[] { 2 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)22);
        await Assert.That(await recovered.GetRawAsync(new byte[] { 3 }, value)).IsEqualTo(-1);
        await Assert.That(recovered.IsReadOnly).IsFalse();
    }

    [Test]
    public async Task CorruptCommittedWalRejectsNormalOpenAndSalvagesOnlyValidPrefix()
    {
        using var folder = TempFolder.Create();
        await CreateSstAsync(folder);
        var path = Path.Combine(folder, "999998.wal");
        using (var wal = new WriteAheadLog(path, false))
        {
            wal.AppendRaw([2], [22]);
            wal.AppendRaw([3], [33]);
        }

        var bytes = File.ReadAllBytes(path);
        var secondFrame = WriteAheadLog.FileHeaderSize + WriteAheadLog.FrameHeaderSize
            + (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(WriteAheadLog.FileHeaderSize))
            + WriteAheadLog.FrameFooterSize;
        bytes[secondFrame + WriteAheadLog.FrameHeaderSize + sizeof(uint) + 1] ^= 1;
        File.WriteAllBytes(path, bytes);
        var before = Snapshot(folder);

        var report = await LsmStorage.VerifyIntegrityAsync(folder, Options);
        await Assert.That(report.IsRecoveryComplete).IsFalse();
        await Assert.That(report.Issues.Single().Kind).IsEqualTo(StorageIntegrityIssueKind.CorruptWal);
        await Assert.That(async () => await LsmStorage.OpenAsync(folder, Options)).Throws<InvalidDataException>();
        await AssertUnchangedAsync(folder, before);
        await using (var salvage = await LsmStorage.OpenSalvageAsync(folder, Options))
        {
            var value = new byte[8];
            await Assert.That(await salvage.GetRawAsync(new byte[] { 2 }, value)).IsEqualTo(1);
            await Assert.That(value[0]).IsEqualTo((byte)22);
            await Assert.That(await salvage.GetRawAsync(new byte[] { 3 }, value)).IsEqualTo(-1);
        }

        await AssertUnchangedAsync(folder, before);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IntegrityReadsDataBlocksAndSalvageSkipsCorruptSst(bool corruptMetadata)
    {
        using var folder = TempFolder.Create();
        await CreateSstAsync(folder);
        var filename = Directory.GetFiles(folder, "*.sst").Single();
        var bytes = File.ReadAllBytes(filename);
        if (corruptMetadata)
        {
            bytes = bytes[..2];
        }
        else
        {
            bytes[0] ^= 1;
        }
        File.WriteAllBytes(filename, bytes);
        var before = Snapshot(folder);
        var report = await LsmStorage.VerifyIntegrityAsync(folder, Options);
        await Assert.That(report.IsRecoveryComplete).IsFalse();
        await Assert.That(report.Issues.Single().Kind).IsEqualTo(StorageIntegrityIssueKind.CorruptSst);
        await using (var salvage = await LsmStorage.OpenSalvageAsync(folder, Options))
        {
            await Assert.That(await salvage.GetRawAsync(new byte[] { 1 }, new byte[8])).IsEqualTo(-1);
            await Assert.That(salvage.RecoveryReport!.IsRecoveryComplete).IsFalse();
        }

        await AssertUnchangedAsync(folder, before);
    }

    [Test]
    [Arguments("{")]
    [Arguments("null")]
    [Arguments("{\"Version\":2}")]
    [Arguments("{\"L0\":null}")]
    [Arguments("{\"L0\":[1,1]}")]
    [Arguments("{\"Levels\":[null]}")]
    public async Task CorruptManifestIsReportedAndCannotBeSalvagedByGuessing(string json)
    {
        using var folder = TempFolder.Create();
        await File.WriteAllTextAsync(Path.Combine(folder, "manifest"), json);
        var before = Snapshot(folder);
        var report = await LsmStorage.VerifyIntegrityAsync(folder, Options);
        await Assert.That(report.IsRecoveryComplete).IsFalse();
        await Assert.That(report.Issues.Single().Kind).IsEqualTo(StorageIntegrityIssueKind.CorruptManifest);
        await Assert.That(async () => await LsmStorage.OpenSalvageAsync(folder, Options)).Throws<StorageRecoveryException>();
        await Assert.That(async () => await LsmStorage.OpenAsync(folder, Options)).Throws<Exception>();
        await AssertUnchangedAsync(folder, before);
    }

    [Test]
    [Arguments(SstCompression.None)]
    [Arguments(SstCompression.Lz4)]
    [Arguments(SstCompression.Zstandard)]
    public async Task IntegrityAcceptsValidCompressedMultiBlockTables(SstCompression compression)
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.Compression = compression;
        options.BlockSize = 256;
        await using (var db = await LsmStorage.OpenAsync(folder, options))
        {
            for (var i = 0; i < 100; i++)
            {
                db.Put(i, new string('x', 64));
            }

            db.Delete(0);
            db.Put("empty", "");
        }

        var before = Snapshot(folder);
        var report = await LsmStorage.VerifyIntegrityAsync(folder, options);
        await Assert.That(report.IsRecoveryComplete).IsTrue();
        await Assert.That(report.Issues.Count).IsEqualTo(0);
        await AssertUnchangedAsync(folder, before);
    }

    [Test]
    public async Task IntegritySkipsStaleWalAndOrphanSstButChecksLegacyStores()
    {
        using var folder = TempFolder.Create();
        await CreateSstAsync(folder);
        var sst = Directory.GetFiles(folder, "*.sst").Single();
        File.WriteAllBytes(Path.ChangeExtension(sst, ".wal"), [255]);
        File.WriteAllBytes(Path.Combine(folder, "999999.sst"), [255]);
        await Assert.That((await LsmStorage.VerifyIntegrityAsync(folder, Options)).IsRecoveryComplete).IsTrue();
        File.Delete(Path.Combine(folder, "999999.sst"));
        File.Delete(Path.Combine(folder, "manifest"));
        await Assert.That((await LsmStorage.VerifyIntegrityAsync(folder, Options)).IsRecoveryComplete).IsTrue();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, new byte[8])).IsEqualTo(1);
    }

    private static async Task CreateSstAsync(string path, byte key = 1)
    {
        await using var db = await LsmStorage.OpenAsync(path, Options);
        db.Put([key], [11]);
    }

    private static Dictionary<string, byte[]> Snapshot(string path) =>
        Directory.GetFiles(path).ToDictionary(filename => Path.GetFileName(filename), File.ReadAllBytes);

    private static async Task AssertUnchangedAsync(string path, Dictionary<string, byte[]> before)
    {
        var after = Snapshot(path);
        await Assert.That(after.Count).IsEqualTo(before.Count);
        foreach (var (name, bytes) in before)
        {
            await Assert.That(after.TryGetValue(name, out var actual) && bytes.AsSpan().SequenceEqual(actual)).IsTrue();
        }
    }
}
