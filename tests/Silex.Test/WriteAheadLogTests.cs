using System.Buffers.Binary;
using Silex.MemTables;
using Silex.Wal;

namespace Silex.Test;

public class WriteAheadLogTests
{
    [Test]
    public async Task ReplayRejectsHeaderlessLogs()
    {
        using var folder = TempFolder.Create();
        var path = Path.Combine(folder, "headerless.wal");
        await File.WriteAllBytesAsync(path, [1, 7, 1, 9, 1, 8, 0]);

        using var memTable = new MemTable(1);
        await Assert.That(() => WriteAheadLog.Replay(path, memTable)).Throws<InvalidDataException>();
    }

    [Test]
    public async Task ReplayRejectsCorruptedVersionedHeader()
    {
        using var folder = TempFolder.Create();
        var path = Path.Combine(folder, "corrupt-header.wal");

        using (var wal = new WriteAheadLog(path, syncToDisk: false))
        {
            wal.AppendRaw([1], [2]);
        }

        var bytes = await File.ReadAllBytesAsync(path);
        bytes[0] ^= 1;
        await File.WriteAllBytesAsync(path, bytes);

        using var memTable = new MemTable(1);
        await Assert.That(() => WriteAheadLog.Replay(path, memTable)).Throws<InvalidDataException>();
    }

    [Test]
    public async Task ReplayRejectsCorruptedPayloadIncludingTheFinalFrame()
    {
        using var folder = TempFolder.Create();
        var path = Path.Combine(folder, "corrupt.wal");

        using (var wal = new WriteAheadLog(path, syncToDisk: false))
        {
            wal.AppendRaw([1], [2]);
            wal.AppendRaw([3], [4]);
        }

        var bytes = await File.ReadAllBytesAsync(path);
        var firstPayloadLength = (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(WriteAheadLog.FileHeaderSize));
        var secondFrameOffset =
            WriteAheadLog.FileHeaderSize
            + WriteAheadLog.FrameHeaderSize
            + firstPayloadLength
            + WriteAheadLog.FrameFooterSize;
        bytes[secondFrameOffset + WriteAheadLog.FrameHeaderSize + sizeof(uint) + 1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes);

        using var memTable = new MemTable(1);
        await Assert.That(() => WriteAheadLog.Replay(path, memTable)).Throws<InvalidDataException>();
    }

    [Test]
    public async Task ReplayRejectsCorruptedFrameLength()
    {
        using var folder = TempFolder.Create();
        var path = Path.Combine(folder, "corrupt-length.wal");

        using (var wal = new WriteAheadLog(path, syncToDisk: false))
        {
            wal.AppendRaw([1], [2]);
        }

        var bytes = await File.ReadAllBytesAsync(path);
        bytes[WriteAheadLog.FileHeaderSize] ^= 1;
        await File.WriteAllBytesAsync(path, bytes);

        using var memTable = new MemTable(1);
        await Assert.That(() => WriteAheadLog.Replay(path, memTable)).Throws<InvalidDataException>();
    }

    [Test]
    public async Task ReplayRejectsCorruptedCommittedFooterLength()
    {
        using var folder = TempFolder.Create();
        var path = Path.Combine(folder, "corrupt-footer.wal");

        using (var wal = new WriteAheadLog(path, syncToDisk: false))
        {
            wal.AppendRaw([1], [2]);
        }

        var bytes = await File.ReadAllBytesAsync(path);
        bytes[^WriteAheadLog.FrameFooterSize] ^= 1;
        await File.WriteAllBytesAsync(path, bytes);

        using var memTable = new MemTable(1);
        await Assert.That(() => WriteAheadLog.Replay(path, memTable)).Throws<InvalidDataException>();
    }

    [Test]
    public async Task ReplayToleratesEveryPossibleTearInTheFinalFrame()
    {
        using var folder = TempFolder.Create();
        var completePath = Path.Combine(folder, "complete.wal");

        using (var wal = new WriteAheadLog(completePath, syncToDisk: false))
        {
            wal.AppendRaw([1], [2]);
            wal.AppendRaw([3], [4]);
        }

        var complete = await File.ReadAllBytesAsync(completePath);
        var firstPayloadLength = (int)BinaryPrimitives.ReadUInt32LittleEndian(complete.AsSpan(WriteAheadLog.FileHeaderSize));
        var finalFrameOffset =
            WriteAheadLog.FileHeaderSize
            + WriteAheadLog.FrameHeaderSize
            + firstPayloadLength
            + WriteAheadLog.FrameFooterSize;

        for (var length = finalFrameOffset; length < complete.Length; length++)
        {
            var tornPath = Path.Combine(folder, $"torn-{length}.wal");
            await File.WriteAllBytesAsync(tornPath, complete.AsMemory(0, length).ToArray());

            using var memTable = new MemTable(length);
            WriteAheadLog.Replay(tornPath, memTable);
            await AssertValue(memTable, [1], [2]);
            await AssertMissing(memTable, [3]);
        }
    }

    [Test]
    public async Task ReplayToleratesAFullLengthUncommittedFinalFrame()
    {
        using var folder = TempFolder.Create();
        var path = Path.Combine(folder, "uncommitted-final-frame.wal");

        using (var wal = new WriteAheadLog(path, syncToDisk: false))
        {
            wal.AppendRaw([1], [2]);
            wal.AppendRaw([3], [4]);
        }

        var bytes = await File.ReadAllBytesAsync(path);
        var firstPayloadLength = (int)BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(WriteAheadLog.FileHeaderSize));
        var finalFrameOffset =
            WriteAheadLog.FileHeaderSize
            + WriteAheadLog.FrameHeaderSize
            + firstPayloadLength
            + WriteAheadLog.FrameFooterSize;
        bytes.AsSpan(finalFrameOffset + WriteAheadLog.FrameHeaderSize).Clear();
        await File.WriteAllBytesAsync(path, bytes);

        using var memTable = new MemTable(1);
        WriteAheadLog.Replay(path, memTable);
        await AssertValue(memTable, [1], [2]);
        await AssertMissing(memTable, [3]);
    }

    [Test]
    public async Task BatchUsesOneWriteAndReplaysAsOneRecoveryUnit()
    {
        using var folder = TempFolder.Create();
        var path = Path.Combine(folder, "batch.wal");
        var batch = new LsmWriteBatch();
        batch.Put([1], [2]);
        batch.Put([3], []);
        batch.Delete([4]);

        using (var wal = new WriteAheadLog(path, syncToDisk: true))
        {
            wal.AppendBatch(batch);
            await Assert.That(wal.RecordWriteCount).IsEqualTo(1);
            await Assert.That(wal.DiskFlushCount).IsEqualTo(1);
        }

        using var memTable = new MemTable(1);
        WriteAheadLog.Replay(path, memTable);
        await AssertValue(memTable, [1], [2]);
        await AssertValue(memTable, [3], []);
        await AssertTombstone(memTable, [4]);

        var complete = await File.ReadAllBytesAsync(path);
        for (var length = WriteAheadLog.FileHeaderSize; length < complete.Length; length++)
        {
            var tornPath = Path.Combine(folder, $"batch-torn-{length}.wal");
            await File.WriteAllBytesAsync(tornPath, complete.AsMemory(0, length).ToArray());
            using var tornMemTable = new MemTable(length);
            WriteAheadLog.Replay(tornPath, tornMemTable);
            await AssertMissing(tornMemTable, [1]);
            await AssertMissing(tornMemTable, [3]);
            await AssertMissing(tornMemTable, [4]);
        }
    }

    [Test]
    public async Task BatchReducesOneThousandMutationWritesToOne()
    {
        using var folder = TempFolder.Create();
        var batch = new LsmWriteBatch();
        for (var i = 0; i < 1_000; i++)
        {
            batch.Put(BitConverter.GetBytes(i), BitConverter.GetBytes(i + 1));
        }

        using var individual = new WriteAheadLog(Path.Combine(folder, "individual.wal"), syncToDisk: false);
        for (var i = 0; i < batch.Count; i++)
        {
            batch.GetEntry(i, out var key, out var value, out var isTombstone);
            if (isTombstone)
            {
                throw new InvalidOperationException("The benchmark batch contains only puts.");
            }

            individual.AppendRaw(key, value);
        }

        using var grouped = new WriteAheadLog(Path.Combine(folder, "grouped.wal"), syncToDisk: false);
        grouped.AppendBatch(batch);

        await Assert.That(individual.RecordWriteCount).IsEqualTo(1_000);
        await Assert.That(grouped.RecordWriteCount).IsEqualTo(1);
    }

    [Test]
    public async Task StorageBatchPersistsAllMutations()
    {
        using var folder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };
        var storage = await LsmStorage.OpenAsync(folder, options);
        var batch = new LsmWriteBatch();
        batch.Put([1], [2]);
        batch.Put([3], []);
        batch.Delete([4]);

        storage.Write(batch);

        await Assert.That(await storage.GetRawAsync([1], new byte[1])).IsEqualTo(1);
        await Assert.That(await storage.GetRawAsync([3], Memory<byte>.Empty)).IsEqualTo(0);
        await storage.CloseAsync();
    }

    [Test]
    public async Task StorageBatchRecoversAfterProcessCrash()
    {
        using var folder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        await CrashRecoveryTestProcess.WriteBatchAndExitWithoutDisposalAsync(folder, entryCount: 10);

        var reopened = await LsmStorage.OpenAsync(folder, options);
        var value = new byte[sizeof(int)];
        for (var i = 0; i < 10; i++)
        {
            await Assert.That(await reopened.GetRawAsync(BitConverter.GetBytes(i), value)).IsEqualTo(sizeof(int));
            await Assert.That(BinaryPrimitives.ReadInt32LittleEndian(value)).IsEqualTo(i + 1);
        }

        await reopened.CloseAsync();
    }

    [Test]
    public async Task ConcurrentWritesRecoverFromTheirWal()
    {
        using var folder = TempFolder.Create();
        using var crashed = TempFolder.Create();
        const int Threads = 8;
        const int EntriesPerThread = 3_000;
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero, MemTableSizeLimit = 64 * 1024 };

        var storage = await LsmStorage.OpenAsync(folder, options);
        WriteConcurrently(storage, Threads, EntriesPerThread);

        // Every acknowledged write has reached the OS, so the files as they stand are what a process crash leaves.
        foreach (var file in Directory.GetFiles(folder))
        {
            if (Path.GetFileName(file) != DirectoryOwnership.FileName)
            {
                using var source = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                using var target = File.Create(Path.Combine(crashed, Path.GetFileName(file)));
                source.CopyTo(target);
            }
        }
        await storage.CloseAsync();

        var recovered = await LsmStorage.OpenAsync(crashed, options);
        await AssertConcurrentWrites(recovered, Threads, EntriesPerThread);
        await recovered.CloseAsync();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ConcurrentWritesSurviveMemTableFreezesAndReopen(bool syncWal)
    {
        using var folder = TempFolder.Create();
        const int Threads = 8;
        var entriesPerThread = syncWal ? 300 : 2_000;
        var options = new StorageOptions
        {
            FlushPeriod = TimeSpan.Zero,
            MemTableSizeLimit = 16 * 1024,
            SyncWriteAheadLogToDisk = syncWal,
        };

        var storage = await LsmStorage.OpenAsync(folder, options);
        WriteConcurrently(storage, Threads, entriesPerThread);
        await AssertConcurrentWrites(storage, Threads, entriesPerThread);
        await storage.CloseAsync();

        var reopened = await LsmStorage.OpenAsync(folder, options);
        await AssertConcurrentWrites(reopened, Threads, entriesPerThread);
        await reopened.CloseAsync();
    }

    /// <summary>
    /// Each thread puts its own keys with values of varying length, overwrites every third one and deletes every
    /// fourth, so the threads' writes share write groups. <see cref="ExpectedConcurrentValue"/> gives the final state.
    /// </summary>
    private static void WriteConcurrently(LsmStorage storage, int threads, int entriesPerThread)
    {
        using var start = new Barrier(threads);
        var errors = new Exception?[threads];
        var workers = Enumerable.Range(0, threads).Select(thread => new Thread(() =>
        {
            try
            {
                var key = new byte[ConcurrentKeyLength(thread)];
                start.SignalAndWait();
                for (var i = 0; i < entriesPerThread; i++)
                {
                    FormatConcurrentKey(thread, i, key);
                    storage.Put(key, ConcurrentValue(thread, i, generation: 0));
                    if (i % 3 == 0)
                    {
                        storage.Put(key, ConcurrentValue(thread, i, generation: 1));
                    }
                    if (i % 4 == 3)
                    {
                        storage.Delete(key);
                    }
                }
            }
            catch (Exception exception)
            {
                errors[thread] = exception;
            }
        })).ToArray();
        foreach (var worker in workers)
        {
            worker.Start();
        }
        foreach (var worker in workers)
        {
            worker.Join();
        }
        if (errors.FirstOrDefault(error => error is not null) is { } failure)
        {
            throw failure;
        }
    }

    private static int ConcurrentKeyLength(int thread) => 5 + thread % 3 * 40;

    private static void FormatConcurrentKey(int thread, int i, byte[] key)
    {
        key[0] = (byte)thread;
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(1), i);
    }

    private static byte[]? ExpectedConcurrentValue(int thread, int i) =>
        i % 4 == 3 ? null : ConcurrentValue(thread, i, i % 3 == 0 ? 1 : 0);

    private static byte[] ConcurrentValue(int thread, int i, int generation)
    {
        var value = new byte[i % 400];
        value.AsSpan().Fill((byte)(thread * 7 + i + generation));
        return value;
    }

    private static async Task AssertConcurrentWrites(LsmStorage storage, int threads, int entriesPerThread)
    {
        var buffer = new byte[400];
        var mismatches = 0;
        for (var thread = 0; thread < threads; thread++)
        {
            var key = new byte[ConcurrentKeyLength(thread)];
            for (var i = 0; i < entriesPerThread; i++)
            {
                FormatConcurrentKey(thread, i, key);
                var expected = ExpectedConcurrentValue(thread, i);
                var length = await storage.GetRawAsync(key, buffer);
                if (expected is null ? length != -1 : length != expected.Length || !buffer.AsSpan(0, length).SequenceEqual(expected))
                {
                    mismatches++;
                }
            }
        }

        await Assert.That(mismatches).IsEqualTo(0);
    }

    private static async Task AssertValue(MemTable memTable, byte[] keyBytes, byte[] expected)
    {
        using var key = OwnedByteSlice.CopyFrom(keyBytes);
        await Assert.That(memTable.TryGet(key.Slice, out var value)).IsTrue();
        await Assert.That(value!.IsTombstone).IsFalse();
        await Assert.That(value.Span.ToArray()).IsEquivalentTo(expected);
    }

    private static async Task AssertTombstone(MemTable memTable, byte[] keyBytes)
    {
        using var key = OwnedByteSlice.CopyFrom(keyBytes);
        await Assert.That(memTable.TryGet(key.Slice, out var value)).IsTrue();
        await Assert.That(value!.IsTombstone).IsTrue();
    }

    private static async Task AssertMissing(MemTable memTable, byte[] keyBytes)
    {
        using var key = OwnedByteSlice.CopyFrom(keyBytes);
        await Assert.That(memTable.TryGet(key.Slice, out _)).IsFalse();
    }
}
