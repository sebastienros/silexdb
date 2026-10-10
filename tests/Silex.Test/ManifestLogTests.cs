namespace Silex.Test;

public class ManifestLogTests
{
    private static StorageOptions Options => new()
    {
        UseWriteAheadLog = false,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
    };

    private static async Task<string> FlushThreeAsync(string folder)
    {
        var inner = new LsmStorageInner(folder, Options);

        for (var i = 1; i <= 3; i++)
        {
            inner.Put(i, i * 10);
            inner.ForceFreezeMemTable();
            await inner.ForceFlushNextImmutableMemTableAsync();
        }

        inner.Dispose();
        return Directory.GetFiles(folder, "manifest.*.log").Single();
    }

    private static async Task AssertReopensAsync(string folder, int expected)
    {
        var storage = await LsmStorage.OpenAsync<int, int>(folder, Options);

        try
        {
            await Assert.That(storage._inner._state.LevelZeroTables.Count).IsEqualTo(expected);
        }
        finally
        {
            await storage.DisposeAsync();
        }
    }

    [Test]
    public async Task FlushesAppendToLogAndReopenReplaysThem()
    {
        using var folder = TempFolder.Create();
        var log = await FlushThreeAsync(folder);

        // The first flush writes the snapshot; the next two are 13-byte appends.
        await Assert.That(new FileInfo(log).Length).IsEqualTo(2 * 13);
        await Assert.That(Manifest.TryRead(folder)!.L0.Count).IsEqualTo(3);
        await AssertReopensAsync(folder, 3);
    }

    [Test]
    public async Task TornFinalLogRecordIsIgnored()
    {
        using var folder = TempFolder.Create();
        var log = await FlushThreeAsync(folder);

        await using (var stream = new FileStream(log, FileMode.Append))
        {
            stream.Write(new byte[7]);
        }

        await Assert.That(Manifest.TryRead(folder)!.L0.Count).IsEqualTo(3);

        // A complete final record with a bad checksum is also a torn write.
        var bytes = File.ReadAllBytes(log)[..(2 * 13)];
        bytes[^1] ^= 0xFF;
        File.WriteAllBytes(log, bytes);
        await Assert.That(Manifest.TryRead(folder)!.L0.Count).IsEqualTo(2);
    }

    [Test]
    public async Task CorruptNonFinalLogRecordIsRejected()
    {
        using var folder = TempFolder.Create();
        var log = await FlushThreeAsync(folder);

        var bytes = File.ReadAllBytes(log);
        bytes[2] ^= 0xFF;
        File.WriteAllBytes(log, bytes);

        await Assert.That(() => Manifest.TryRead(folder)).Throws<InvalidDataException>();
    }

    [Test]
    public async Task SnapshotWriteRetiresPreviousLog()
    {
        using var folder = TempFolder.Create();
        await FlushThreeAsync(folder);

        Manifest.TryRead(folder)!.Write(folder);

        await Assert.That(Directory.GetFiles(folder, "manifest.*.log")).IsEmpty();
        await Assert.That(Manifest.TryRead(folder)!.L0.Count).IsEqualTo(3);
    }
}
