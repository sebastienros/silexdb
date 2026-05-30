using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using Silex.BloomFilters;
using Silex.Serialization;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class StorageTests
{
    // These in-memory unit tests construct LsmStorageInner directly against the shared system temp
    // folder, so the write-ahead log is disabled to avoid littering it (and the per-append flush).
    private readonly StorageOptions _defaultStorageOptions = new() { UseWriteAheadLog = false };
    private readonly TextWriter? _output = null;

    [Test]
    public async Task CanPutArray()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6];

        storage.Put(key, value);
        var result = await storage.GetAsync(key);

        await Assert.That(result).IsEqualTo(value);
        await Assert.That(storage._state.CurrentMemTable.Size).IsEqualTo(10);
    }

    [Test]
    public async Task GetReturnsZeroCopyBorrowFromMemTable()
    {
        // Zero-copy is a core principle: a value served from a memtable is the same instance that was
        // put (a read-only borrow), not a defensive copy. This locks in that contract.
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6];

        storage.Put(key, value);

        var result = await storage.GetAsync(key);
        await Assert.That(result).IsSameReferenceAs(value);

        var scanned = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().Single();
        await Assert.That(scanned.Key).IsSameReferenceAs(key);
        await Assert.That(scanned.Value).IsSameReferenceAs(value);
    }

    [Test]
    public async Task PutValueIsCopied()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        byte[] key1 = [1];
        byte[] key2 = [2];
        byte[] value = [4, 5, 6];

        storage.Put(key1, value);
        storage.Put(key2, value);

        var result1 = await storage.GetAsync(key1);
        var result2 = await storage.GetAsync(key2);

        await Assert.That(result1).IsEqualTo(value);
        await Assert.That(result2).IsEqualTo(value);
        await Assert.That(storage._state.CurrentMemTable.Size).IsEqualTo(16);
    }

    [Test]
    public async Task DeleteShouldStoreTombStone()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];

        storage.Delete(key);
        var result = await storage.GetAsync(key);

        await Assert.That(result?.Length == 0).IsTrue();
        await Assert.That(storage._state.CurrentMemTable.Size).IsEqualTo(7);
    }

    [Test]
    [Arguments(1, 1, 0)]
    [Arguments(7, 10, 1)]
    [Arguments(100, 100, 100)]
    [Arguments(10, 500, 83)]
    public async Task ShouldFreezeMemTablesWhenSizeIsOverLimit(int valueSize, int entries, int expectedImmutableTables)
    {
        var memTableSizeLimit = 100;

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit, UseWriteAheadLog = false };
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<int, byte[]>(tempFolder, storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = i; // 4 bytes
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);

            storage.Put(key, value);
        }

        await Assert.That(storage._state.ImmutableMemTables.Count()).IsEqualTo(expectedImmutableTables);
    }

    [Test]
    public async Task GetFromImmutableMemTables()
    {
        var memTableSizeLimit = 100;
        int valueSize = 10;
        int entries = 100;

        var dictionary = new Dictionary<int, byte[]>();

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit, UseWriteAheadLog = false };
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<int, byte[]>(tempFolder, storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = i;
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);

            dictionary[i] = value;
            storage.Put(key, value);
        }

        for (var i = 1; i <= entries; i++)
        {
            var expectedValue = dictionary[i];
            var actualValue = await storage.GetAsync(i);

            await Assert.That(actualValue).IsEqualTo(expectedValue);
        }
    }

    [Test]
    public async Task DeletedEntriesShouldAppearAfterPuts()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = FillImmutableMemTables(tempFolder);

        int key = 10;

        var result = await storage.GetAsync(key);
        await Assert.That(result?.Length).IsEqualTo(10);

        storage.Delete(key);
        result = await storage.GetAsync(key);

        await Assert.That(result?.Length).IsEqualTo(0);
    }

    [Test]
    public async Task ScanListsAllMemTables()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<char, byte[]>(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        // table1: b->del, c->4, d->5
        // table2: a->1, b->2, c->3
        // table3: e->4

        storage.Put('e', [4]);
        storage.ForceFreezeMemTable();

        storage.Put('a', [1]);
        storage.Put('b', [2]);
        storage.Put('c', [3]);
        storage.ForceFreezeMemTable();

        storage.Delete('b');
        storage.Put('c', [4]);
        storage.Put('d', [5]);

        var iterator = storage.CreateIterator();
        var list = iterator.EnumerateAsync().ToBlockingEnumerable().ToList();

        // a->1, c->4, d->5, e->4 and b->del should be discarded

        await Assert.That(list.Count).IsEqualTo(4);

        await Assert.That((int)list[0].Key).IsEqualTo((int)'a');
        await Assert.That((int)list[1].Key).IsEqualTo((int)'c');
        await Assert.That((int)list[2].Key).IsEqualTo((int)'d');
        await Assert.That((int)list[3].Key).IsEqualTo((int)'e');
    }

    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(5)]
    [Arguments(10)]
    public async Task ShouldHandleConcurrentClients(int levelOfConcurrency)
    {
        var maxKeysValue = 50; // Limit the number of unique ids to generate collisions
        var iterations = 50;
        var storageOptions = new StorageOptions { MemTableSizeLimit = 100, UseWriteAheadLog = false };
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<long, byte[]>(tempFolder, storageOptions);
        var iterator = storage.CreateIterator();

        var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token;

        await Parallel.ForAsync(0, levelOfConcurrency, timeout, (i, cancellationToken) =>
        {
            return Work(storage);
        });

        var allEntries = iterator.EnumerateAsync().ToBlockingEnumerable().ToList();

        _output?.WriteLine($"Entries: {allEntries.Count}");
        _output?.WriteLine($"Immutable MemTables: {storage._state.ImmutableMemTables.Count()}");

        _output?.WriteLine($"Current MemTable: {storage._state.CurrentMemTable.Size}");

        foreach (var table in storage._state.ImmutableMemTables)
        {
            _output?.WriteLine($"ImmutableMemTable: {table.Size}");
        }

        _output?.WriteLine($"Entries:");
        foreach (var entry in allEntries)
        {
            var key = entry.Key.ToString(CultureInfo.InvariantCulture);
            var entryValue = await storage.GetAsync(entry.Key);
            var value = entryValue.Length == 0 ? "del" : BinaryPrimitives.ReadUInt64LittleEndian(entryValue.AsSpan()).ToString(CultureInfo.InvariantCulture);

            _output?.WriteLine($"{key} -> {value}");
        }

        await Assert.That(allEntries.Count <= maxKeysValue).IsTrue();

        async ValueTask Work(LsmStorageInner<long, byte[]> storage)
        {
            for (var i = 0; i < iterations; i++)
            {
                var id = Random.Shared.NextInt64(maxKeysValue);
                var value = Random.Shared.NextInt64();
                storage.Put(id, BitConverter.GetBytes(value));
            }

            for (var i = 0; i < iterations; i++)
            {
                var operation = Random.Shared.NextInt64(10);
                var id = Random.Shared.NextInt64(maxKeysValue);

                switch (operation)
                {
                    case 0: // Put
                        var value = Random.Shared.NextInt64();
                        storage.Put(id, BitConverter.GetBytes(value));
                        break;

                    case 1: // Get
                        var actualValue = await storage.GetAsync(id);
                        break;

                    case 2: // Delete
                        storage.Delete(id);
                        break;

                    case 3: // Scan
                        _ = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
                        break;
                }
            }
        }
    }

    [Test]
    [Arguments(null)]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(5)]
    public async Task ScanWithBoundsShouldFilterResults(int? lowerBound)
    {
        var count = 10;

        using var tempFolder = TempFolder.Create();
        using var storage = FillImmutableMemTables(tempFolder, entries: count, memTableSizeLimit: 1.KiB());
        int lowerBytes = lowerBound == null ? -1 : lowerBound.Value;

        var expectedKeys = Enumerable.Range(1, count);

        var entries = storage.CreateIterator().EnumerateAsync(lowerBytes).ToBlockingEnumerable().ToList();

        if (lowerBound.HasValue)
        {
            expectedKeys = expectedKeys.Where(x => x >= lowerBound);

            foreach (var e in entries)
            {
                await Assert.That(lowerBytes <= e.Key).IsTrue();
            }
        }

        var actualKeys = storage.CreateIterator().EnumerateAsync(lowerBytes).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(actualKeys).IsEquivalentTo(expectedKeys, CollectionOrdering.Matching);
    }

    [Test]
    public async Task OpenAsyncShouldCreateFolder()
    {
        using var tempFolder = TempFolder.Create();

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, _defaultStorageOptions);

        await Assert.That(Directory.Exists(tempFolder)).IsTrue();

        await storage.CloseAsync();
    }

    [Test]
    public async Task ForceFlushShouldNotFailWithNoImmutableMemTable()
    {
        using var tempFolder = TempFolder.Create();

        var storage = await LsmStorage.OpenAsync<char, byte[]>(tempFolder, new StorageOptions());

        storage.Put('e', [4]);

        // Don't freeze current MemTable

        // Nothing to flush
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        await Assert.That(Directory.Exists(tempFolder)).IsTrue();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).IsEmpty();

        await storage.CloseAsync();
    }

    [Test]
    public async Task ForceFlushShouldFlushMemTable()
    {
        using var tempFolder = TempFolder.Create();

        var storage = await LsmStorage.OpenAsync<char, byte[]>(tempFolder, new());

        storage.Put('e', [4]);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        await Assert.That(Directory.Exists(tempFolder)).IsTrue();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        await storage.CloseAsync();
    }

    [Test]
    public async Task GetShouldReadFromFlushedSsTable()
    {
        using var tempFolder = TempFolder.Create();

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, new());

        const int count = 200;

        // Values are offset by 1 so a missing key (default 0) is distinguishable from a stored value.
        for (var i = 0; i < count; i++)
        {
            storage.Put(i, i + 1);
        }

        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        // Everything has been flushed: a single SST, no immutable mem tables, and an empty current mem table.
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();
        await Assert.That(storage._inner._state.ImmutableMemTables).IsEmpty();
        await Assert.That(storage._inner._state.CurrentMemTable.Size).IsEqualTo(0L);

        // Reads must go through the bloom filter and block decoding of the SST.
        for (var i = 0; i < count; i++)
        {
            await Assert.That(await storage.GetAsync(i)).IsEqualTo(i + 1);
        }

        // A key that was never inserted returns the default value.
        await Assert.That(await storage.GetAsync(10000)).IsEqualTo(0);

        await storage.CloseAsync();
    }

    [Test]
    public async Task CompacterShouldCreateSst()
    {
        // When the number of mem tables is higher than MemTableMaxCount it should
        // flush the oldest mem table to disk

        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { MemTableMaxCount = 2 };
        var storage = await LsmStorage.OpenAsync<char, int>(tempFolder, options);

        storage.Put('a', 1);
        storage._inner.ForceFreezeMemTable();

        await Task.Delay(100);
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).IsEmpty();
        await Assert.That(storage._inner._state.ImmutableMemTables).HasSingleItem();
        await Assert.That(storage._inner._state.ImmutableMemTables.Peek().TryGet('a', out _)).IsTrue();
        await Assert.That(storage._inner._state.ImmutableMemTables.Peek().TryGet('b', out _)).IsFalse();

        storage.Put('b', 2);
        storage._inner.ForceFreezeMemTable();

        await Task.Delay(100);
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();
        await Assert.That(storage._inner._state.ImmutableMemTables).HasSingleItem();
        await Assert.That(storage._inner._state.ImmutableMemTables.Peek().TryGet('a', out _)).IsFalse();
        await Assert.That(storage._inner._state.ImmutableMemTables.Peek().TryGet('b', out _)).IsTrue();

        await storage.CloseAsync();
    }

    [Test]
    public async Task CloseAsyncShouldFlushToDisk()
    {
        using var tempFolder = TempFolder.Create();
        var storage = await LsmStorage.OpenAsync<char, int>(tempFolder, new StorageOptions());

        storage.Put('a', 1);
        await storage.CloseAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        await storage.CloseAsync();
    }

    [Test]
    public async Task CloseAsyncCanBeInvokedMultipleTimes()
    {
        using var tempFolder = TempFolder.Create();
        var storage = await LsmStorage.OpenAsync<char, int>(tempFolder, new StorageOptions());

        storage.Put('a', 1);
        await storage.CloseAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        await storage.CloseAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        storage = await LsmStorage.OpenAsync<char, int>(tempFolder, new StorageOptions());
        storage.Put('a', 2);
        await storage.CloseAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst").Count()).IsEqualTo(2);

        await storage.CloseAsync();
    }

    [Test]
    public async Task FinalizerShouldNotFlushOrCrashWhenStorageIsNotClosed()
    {
        // A storage that is never closed must not flush to disk during finalization (no WAL exists,
        // durability is only guaranteed by CloseAsync). Even when its directory has been removed, the
        // finalizer must be a no-op and never crash the process.
        using var tempFolder = TempFolder.Create();

        await CreateUnclosedStorageAsync(tempFolder);
        tempFolder.Dispose();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Reaching this point means finalization neither flushed to the deleted folder nor crashed.
        await Assert.That(Directory.Exists(tempFolder)).IsFalse();

        // The storage was never closed, so nothing was ever persisted.
        static async Task CreateUnclosedStorageAsync(string folder)
        {
            var storage = await LsmStorage.OpenAsync<char, int>(folder, new StorageOptions { UseWriteAheadLog = false });
            storage.Put('a', 1);
            // Intentionally not closed/disposed: it becomes eligible for finalization on return.
        }
    }

    [Test]
    public async Task GetShouldReturnMostRecentImmutableMemTableValue()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<int, int>(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        storage.Put(1, 100);
        storage.ForceFreezeMemTable();

        storage.Put(1, 200);
        storage.ForceFreezeMemTable();

        // Both immutable mem tables hold key 1; the most recently frozen value must win.
        await Assert.That(storage._state.ImmutableMemTables.Count()).IsEqualTo(2);
        await Assert.That(await storage.GetAsync(1)).IsEqualTo(200);
    }

    [Test]
    public async Task ScanShouldReturnMostRecentImmutableMemTableValue()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<int, int>(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        storage.Put(1, 100);
        storage.ForceFreezeMemTable();

        storage.Put(1, 200);
        storage.ForceFreezeMemTable();

        var list = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

        await Assert.That(list).HasSingleItem();
        await Assert.That(list[0].Value).IsEqualTo(200);
    }

    [Test]
    public async Task GetShouldFindByteArrayKeyByContent()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner<byte[], int>(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        storage.Put([1, 2, 3], 42);

        // A different array instance with the same content must resolve to the stored value.
        await Assert.That(await storage.GetAsync([1, 2, 3])).IsEqualTo(42);
    }

    [Test]
    public async Task ReopenShouldPreserveLevelZeroRecency()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        storage.Put(1, 100);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        storage.Put(1, 200);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst").Count()).IsEqualTo(2);
        await storage.CloseAsync();

        // After reopening, the SSTs must be ordered by creation so the newest value still wins.
        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        await Assert.That(await reopened.GetAsync(1)).IsEqualTo(200);

        await reopened.CloseAsync();
    }

    [Test]
    public async Task GetShouldReadBytesValueOfArbitraryLengthFromSsTable()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        var storage = await LsmStorage.OpenAsync<int, Bytes>(tempFolder, options);

        // A Bytes value whose length is not 4 bytes used to trip an incorrect decode assertion.
        Bytes value = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        storage.Put(1, value);

        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        var result = await storage.GetAsync(1);
        await Assert.That(result).IsEqualTo(value);

        await storage.CloseAsync();
    }

    [Test]
    public async Task WriteAheadLogRecoversUnflushedEntriesAfterCrash()
    {
        using var tempFolder = TempFolder.Create();
        // Disable background flushing so the data stays only in the memtable + WAL (never an SST).
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        await SimulateCrashAfterPutsAsync(tempFolder, options);

        // Finalize the abandoned instance so its WAL handle is released (the file and its data remain).
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Nothing was ever flushed, yet the WAL is on disk holding the unflushed writes.
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).IsEmpty();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.wal")).IsNotEmpty();

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        for (var i = 0; i < 10; i++)
        {
            await Assert.That(await reopened.GetAsync(i)).IsEqualTo(i + 1);
        }

        await reopened.CloseAsync();
        static async Task SimulateCrashAfterPutsAsync(string folder, StorageOptions options)
        {
            var storage = await LsmStorage.OpenAsync<int, int>(folder, options);

            for (var i = 0; i < 10; i++)
            {
                storage.Put(i, i + 1);
            }

            // Abandon without closing to simulate a crash: the WAL is left on disk.
        }
    }

    [Test]
    public async Task WriteAheadLogRecoveryRequiresTheWalFile()
    {
        // Sanity check that the recovery above is genuinely driven by the WAL: with the WAL removed
        // (and nothing flushed), the data is gone after a crash.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        await SimulateCrashAfterPutsAsync(tempFolder, options);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        foreach (var wal in Directory.EnumerateFiles(tempFolder, "*.wal"))
        {
            File.Delete(wal);
        }

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        await Assert.That(await reopened.GetAsync(0)).IsEqualTo(0);

        await reopened.CloseAsync();
        static async Task SimulateCrashAfterPutsAsync(string folder, StorageOptions options)
        {
            var storage = await LsmStorage.OpenAsync<int, int>(folder, options);
            storage.Put(0, 1);
        }
    }

    [Test]
    public async Task WriteAheadLogRecoveryToleratesTornTrailingRecord()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        await SimulateCrashAfterPutsAsync(tempFolder, options);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Truncate the final byte to simulate a crash in the middle of the last append.
        var walFile = Directory.EnumerateFiles(tempFolder, "*.wal").Single();
        var bytes = await File.ReadAllBytesAsync(walFile);
        await File.WriteAllBytesAsync(walFile, bytes[..^1]);

        // Recovery must not throw and the earlier, intact records must still be recovered.
        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        await Assert.That(await reopened.GetAsync(0)).IsEqualTo(1);

        await reopened.CloseAsync();
        static async Task SimulateCrashAfterPutsAsync(string folder, StorageOptions options)
        {
            var storage = await LsmStorage.OpenAsync<int, int>(folder, options);

            for (var i = 0; i < 10; i++)
            {
                storage.Put(i, i + 1);
            }
        }
    }

    [Test]
    public async Task RecoveryDeletesStaleWalWhenSstAlreadyExists()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        storage.Put(1, 42);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        var sstFile = Directory.EnumerateFiles(tempFolder, "*.sst").Single();
        var sstId = Path.GetFileNameWithoutExtension(sstFile);
        await storage.CloseAsync();

        // Simulate a crash that wrote and durably persisted the SST but didn't get to delete the WAL.
        // The bytes are intentionally garbage: if replayed they could fail, so recovery must skip them.
        var staleWal = Path.Combine(tempFolder, sstId + ".wal");
        await File.WriteAllBytesAsync(staleWal, new byte[] { 0xFF, 0xFF, 0xFF });

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        await Assert.That(await reopened.GetAsync(1)).IsEqualTo(42);
        // The stale WAL was deleted (and never replayed) because its SST was already loaded. The
        // reopened store has its own fresh current-memtable WAL, so only assert the stale one is gone.
        await Assert.That(File.Exists(staleWal)).IsFalse();

        await reopened.CloseAsync();
    }

    [Test]
    public async Task TieredCompactionMergesAllTiersUnderSpaceAmplification()
    {
        // Three equally sized single-entry tiers hit the space-amplification trigger (sum of the newer
        // tiers reaches MaxSizeAmplificationPercent of the oldest), so all three merge into one SST.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 3 };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(3);

            var compacted = await storage.TryTieredCompactionAsync();

            await Assert.That(compacted).IsTrue();
            await Assert.That(storage._state.LevelZeroTables).HasSingleItem();
            await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();
            await Assert.That(await storage.GetAsync(1)).IsEqualTo(10);
            await Assert.That(await storage.GetAsync(2)).IsEqualTo(20);
            await Assert.That(await storage.GetAsync(3)).IsEqualTo(30);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionDropsTombstonesOnFullCompaction()
    {
        // A full compaction (the oldest tier participates) may drop tombstones. The delete of key 1 in a
        // newer tier shadows its value in the oldest tier and is then discarded entirely.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 3 };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 100));
            await FlushTierAsync(storage, () => storage.Delete(1));
            await FlushTierAsync(storage, () => storage.Put(2, 200));

            var compacted = await storage.TryTieredCompactionAsync();

            await Assert.That(compacted).IsTrue();
            await Assert.That(storage._state.LevelZeroTables).HasSingleItem();
            await Assert.That(await storage.GetAsync(1)).IsEqualTo(0);
            await Assert.That(await storage.GetAsync(2)).IsEqualTo(200);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionPartialMergeKeepsTombstones()
    {
        // A large oldest tier plus several tiny newest tiers avoids the space-amplification trigger and
        // instead fires the size-ratio trigger, merging only the newest tiers. Because the oldest tier is
        // excluded, a tombstone for a key it still holds must be preserved (the key stays logically gone).
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 4 };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1000; k < 1500; k++)
                {
                    storage.Put(k, k);
                }
            });
            await FlushTierAsync(storage, () => storage.Put(1, 1));
            await FlushTierAsync(storage, () => storage.Delete(1000));
            await FlushTierAsync(storage, () => storage.Put(3, 3));
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(4);

            var compacted = await storage.TryTieredCompactionAsync();

            await Assert.That(compacted).IsTrue();
            // The big oldest tier is untouched; the three newest tiers collapse into one.
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(2);
            // The tombstone was kept, so the key the oldest tier still holds remains logically deleted.
            await Assert.That(await storage.GetAsync(1000)).IsEqualTo(0);
            // Untouched keys from the oldest tier are still readable.
            await Assert.That(await storage.GetAsync(1001)).IsEqualTo(1001);
            await Assert.That(await storage.GetAsync(1)).IsEqualTo(1);
            await Assert.That(await storage.GetAsync(3)).IsEqualTo(3);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionBelowThresholdIsNoOp()
    {
        // With fewer tiers than MaxCompactionTiers there is nothing to do: the call is a no-op.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 8 };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            var compacted = await storage.TryTieredCompactionAsync();

            await Assert.That(compacted).IsFalse();
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(2);
            await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst").Count()).IsEqualTo(2);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionDisabledByStrategyIsNoOp()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 2, CompactionStrategy = CompactionStrategy.None };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));

            await Assert.That(await storage.TryTieredCompactionAsync()).IsFalse();
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(3);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionRecencySurvivesReopen()
    {
        // After a full compaction the merged output gets the highest id, so reopen-by-id still resolves
        // the newest value for a key written across several tiers.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero, MaxCompactionTiers = 3 };

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        for (var v = 1; v <= 3; v++)
        {
            storage.Put(1, v * 100);
            storage._inner.ForceFreezeMemTable();
            await storage._inner.ForceFlushNextImmutableMemTableAsync();
        }

        await Assert.That(await storage._inner.TryTieredCompactionAsync()).IsTrue();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();
        await storage.CloseAsync();

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        await Assert.That(await reopened.GetAsync(1)).IsEqualTo(300);

        await reopened.CloseAsync();
    }

    [Test]
    public async Task GetContinuesToOlderSsTableOnBloomFalsePositive()
    {
        // A newer SST whose key range straddles the target key but does not contain it must not mask an
        // older SST that does. Forcing the bloom filter to always report "maybe present" simulates the
        // false positive that would otherwise make the lookup stop prematurely.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            BloomFilterFactory = new AlwaysPositiveBloomFilterFactory(),
        };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            // Older table holds key 5.
            await FlushTierAsync(storage, () => storage.Put(5, 50));
            // Newer table spans [1, 10] (covering 5) but does not contain 5.
            await FlushTierAsync(storage, () =>
            {
                storage.Put(1, 10);
                storage.Put(10, 100);
            });

            await Assert.That(await storage.GetAsync(5)).IsEqualTo(50);
            // Keys that really are in the newer table still resolve from it.
            await Assert.That(await storage.GetAsync(1)).IsEqualTo(10);
            await Assert.That(await storage.GetAsync(10)).IsEqualTo(100);
        }
        finally
        {
            storage.Dispose();
        }
    }

    private sealed class AlwaysPositiveBloomFilterFactory : IBloomFilterFactory
    {
        private static readonly DefaultBloomFilterFactory _inner = new();

        public IBloomFilter CreateBloomFilter(int n, double p) => new AlwaysPositiveBloomFilter(_inner.CreateBloomFilter(n, p));

        public IBloomFilter CreateBloomFilter(ReadOnlySpan<byte> bytes, int k) => new AlwaysPositiveBloomFilter(_inner.CreateBloomFilter(bytes, k));

        public IBloomFilter CreateBloomFilterFromOwnedBytes(byte[] bytes, int k) => new AlwaysPositiveBloomFilter(_inner.CreateBloomFilterFromOwnedBytes(bytes, k));
    }

    private sealed class AlwaysPositiveBloomFilter(IBloomFilter inner) : IBloomFilter
    {
        public int K => inner.K;

        public void Add(ReadOnlySpan<byte> value) => inner.Add(value);

        public bool Probe(ReadOnlySpan<byte> item) => true;

        public ReadOnlySpan<byte> GetBytes() => inner.GetBytes();
    }

    [Test]
    public async Task ConcurrentFlushAndScanNeverMissesCommittedData()
    {
        // Regression guard for the flush/scan race: while an immutable MemTable is mid-flush (dequeued from
        // the queue but its SST not yet published), a concurrent scan must still observe its data. Flush makes
        // that transition atomic under the level0 write lock, so every previously committed key stays visible.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            const int keyCount = 80;
            var committedCount = 0;
            using var done = new CancellationTokenSource();

            var scanner = Task.Run(async () =>
            {
                while (!done.IsCancellationRequested)
                {
                    // Keys are committed in ascending order, so observing N commits means keys 1..N must
                    // all be present in a scan regardless of any in-flight flush.
                    var expected = Volatile.Read(ref committedCount);
                    var keys = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable()
                        .Select(e => e.Key).ToHashSet();

                    for (var k = 1; k <= expected; k++)
                    {
                        await Assert.That(keys).Contains(k);
                    }
                }
            });

            for (var k = 1; k <= keyCount; k++)
            {
                storage.Put(k, k);
                storage.ForceFreezeMemTable();
                // Once frozen the key lives in the immutable queue and is committed: it must remain visible to
                // scans throughout the subsequent flush, including the brief mid-flush transition window.
                Volatile.Write(ref committedCount, k);
                await storage.ForceFlushNextImmutableMemTableAsync();
            }

            done.Cancel();
            await scanner;
        }
        finally
        {
            storage.Dispose();
        }
    }

    private static async Task FlushTierAsync(LsmStorageInner<int, int> storage, Action write)
    {
        write();
        storage.ForceFreezeMemTable();
        await storage.ForceFlushNextImmutableMemTableAsync();
    }

    [Test]
    public async Task FullScanIncludesFlushedSsTableData()
    {
        // Data flushed to L0 SSTs (with nothing left in the MemTables) must still appear in a full scan.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(new[] { 1, 2, 3 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(new[] { 10, 20, 30 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task ScanReturnsNewestValueAcrossMemTableAndSsTable()
    {
        // A key written to an SST and later overwritten in the current MemTable must scan as the newer value.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            // Overwrite key 1 in the live MemTable; this value is newer than the flushed one.
            storage.Put(1, 999);

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(new[] { 1, 2 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(new[] { 999, 20 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task ScanHidesKeysDeletedAfterFlush()
    {
        // A key present in an SST but deleted afterwards (tombstone in the MemTable) must be absent from scans.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            storage.Delete(1);

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(new[] { 2 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(new[] { 20 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task RangeScanIncludesSsTableData()
    {
        // A bounded scan (keys >= from) must include matching entries that live only in SSTs.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));
            storage.Put(4, 40);

            var entries = storage.CreateIterator().EnumerateAsync(2).ToBlockingEnumerable().ToList();

            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(new[] { 2, 3, 4 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(new[] { 20, 30, 40 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task ScanAfterReopenIncludesSsTableData()
    {
        // After reopening, all live data is in SSTs (MemTables are empty), so the scan exercises the SST path.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };

        {
            var storage = new LsmStorageInner<int, int>(tempFolder, options);
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            storage.Dispose();

            var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                var entries = reopened._inner.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

                await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(new[] { 1, 2 }, CollectionOrdering.Matching);
                await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(new[] { 10, 20 }, CollectionOrdering.Matching);
            }
            finally
            {
                reopened.Dispose();
            }
        }
    }


    [Test]
    public async Task GetReturnsDefaultForSentinelTombstoneStoredInSsTable()
    {
        // Sentinel-based encoders (here int) persist a deletion as a fixed non-empty value. Reading that
        // key back from an SST must resolve to the default (deleted), not the raw sentinel.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 100));
            await FlushTierAsync(storage, () => storage.Delete(1));

            // The delete lives in the newest SST as a sentinel value; it must shadow the older value.
            await Assert.That(await storage.GetAsync(1)).IsEqualTo(0);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionFlushesL0IntoL1()
    {
        // Once Level0CompactionThreshold L0 SSTs accumulate, leveled compaction merges them all into a
        // single L1 sorted run and empties L0.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 4,
        };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));
            await FlushTierAsync(storage, () => storage.Put(4, 40));
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(4);

            var compacted = await storage.TryLeveledCompactionAsync();

            await Assert.That(compacted).IsTrue();
            await Assert.That(storage._state.LevelZeroTables).IsEmpty();
            await Assert.That(storage._state.LeveledSsTables).HasSingleItem();
            await Assert.That(storage._state.LeveledSsTables[0]).HasSingleItem();
            await Assert.That(await storage.GetAsync(1)).IsEqualTo(10);
            await Assert.That(await storage.GetAsync(2)).IsEqualTo(20);
            await Assert.That(await storage.GetAsync(3)).IsEqualTo(30);
            await Assert.That(await storage.GetAsync(4)).IsEqualTo(40);

            // The merged run is also visible to a full scan in key order.
            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(new[] { 1, 2, 3, 4 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(new[] { 10, 20, 30, 40 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionPushesOverSizedLevelDown()
    {
        // With a tiny base target every level is over budget, so after L0 flushes into L1 a second
        // compaction pushes L1 down into L2.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
            BaseLevelTargetBytes = 1,
        };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();
            await Assert.That(storage._state.LeveledSsTables[0]).HasSingleItem(); // L1 populated

            // Second action: L1 is over its (tiny) target, so it is pushed down into L2.
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();
            await Assert.That(storage._state.LeveledSsTables.Count).IsEqualTo(2);
            await Assert.That(storage._state.LeveledSsTables[0]).IsEmpty(); // L1 now empty
            await Assert.That(storage._state.LeveledSsTables[1]).HasSingleItem(); // L2 populated

            await Assert.That(await storage.GetAsync(1)).IsEqualTo(10);
            await Assert.That(await storage.GetAsync(2)).IsEqualTo(20);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionDropsTombstonesAtBottomLevel()
    {
        // When the destination is the last non-empty level, a delete may be discarded entirely because no
        // older value survives below it.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
        };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () =>
            {
                storage.Put(1, 100);
                storage.Put(2, 200);
            });
            await FlushTierAsync(storage, () => storage.Delete(1));

            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            await Assert.That(await storage.GetAsync(1)).IsEqualTo(0);
            await Assert.That(await storage.GetAsync(2)).IsEqualTo(200);

            // The tombstone for key 1 was dropped, so only key 2 survives in L1.
            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(new[] { 2 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionKeepsTombstoneWhenLowerLevelHoldsKey()
    {
        // A tombstone must be preserved when a deeper level still holds the key it shadows, otherwise the
        // older value would be resurrected.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
            BaseLevelTargetBytes = 1,
        };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            // Seed L2 with key 1 = 100.
            await FlushTierAsync(storage, () => storage.Put(1, 100));
            await FlushTierAsync(storage, () => storage.Put(2, 200));
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue(); // L0 -> L1
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue(); // L1 -> L2

            // Now delete key 1 in a fresh L0 batch and compact into L1 (which sits above the L2 that holds 1).
            await FlushTierAsync(storage, () => storage.Delete(1));
            await FlushTierAsync(storage, () => storage.Put(3, 300));
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue(); // L0 -> L1, must keep the tombstone

            // The delete is preserved: key 1 reads as deleted even though L2 still physically holds 100.
            await Assert.That(await storage.GetAsync(1)).IsEqualTo(0);
            await Assert.That(await storage.GetAsync(2)).IsEqualTo(200);
            await Assert.That(await storage.GetAsync(3)).IsEqualTo(300);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledReopenRestoresLevelsViaManifest()
    {
        // The manifest persists the L0/level structure so a reopen restores the exact levels (and recency)
        // even though leveled SST ids no longer encode it.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
            BaseLevelTargetBytes = 1,
        };

        {
            var building = new LsmStorageInner<int, int>(tempFolder, options);
            await FlushTierAsync(building, () => building.Put(1, 10));
            await FlushTierAsync(building, () => building.Put(2, 20));
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue(); // L0 -> L1
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue(); // L1 -> L2
            await FlushTierAsync(building, () => building.Put(3, 30));
            await FlushTierAsync(building, () => building.Put(4, 40));
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue(); // L0 -> L1
            building.Dispose();

            await Assert.That(File.Exists(Path.Combine(tempFolder, "manifest"))).IsTrue();

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                // L1 holds {3,4}; L2 holds {1,2}.
                await Assert.That(storage._inner._state.LeveledSsTables.Count).IsEqualTo(2);
                await Assert.That(storage._inner._state.LeveledSsTables[0]).HasSingleItem();
                await Assert.That(storage._inner._state.LeveledSsTables[1]).HasSingleItem();
                await Assert.That(storage._inner._state.LevelZeroTables).IsEmpty();

                var entries = storage._inner.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
                await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(new[] { 1, 2, 3, 4 }, CollectionOrdering.Matching);
                await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(new[] { 10, 20, 30, 40 }, CollectionOrdering.Matching);
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task LeveledReopenDeletesOrphanSstNotInManifest()
    {
        // An SST left behind by a flush/compaction that crashed before the manifest commit is not
        // referenced by the manifest and must be deleted on reopen.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
        };

        {
            var building = new LsmStorageInner<int, int>(tempFolder, options);
            await FlushTierAsync(building, () => building.Put(1, 10));
            await FlushTierAsync(building, () => building.Put(2, 20));
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue();
            building.Dispose();

            // Simulate an orphan output (id far in the future, not referenced by the manifest).
            var orphan = Path.Combine(tempFolder, "999999.sst");
            File.WriteAllText(orphan, "not a real sst");

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                await Assert.That(File.Exists(orphan)).IsFalse();
                await Assert.That(await storage.GetAsync(1)).IsEqualTo(10);
                await Assert.That(await storage.GetAsync(2)).IsEqualTo(20);
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task LeveledCompactionSplitsOutputIntoMultipleSsTables()
    {
        // With a small per-SST target, a level holds several size-bounded, non-overlapping runs instead of
        // one giant SST.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 1,
            BlockSize = 128,
            TargetSstSizeBytes = 128,
        };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1; k <= 400; k++)
                {
                    storage.Put(k, k * 10);
                }
            });

            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            // The single L1 run was split into several SSTs.
            var l1 = storage._state.LeveledSsTables[0];
            await Assert.That(l1.Count > 1).IsTrue();

            // They are sorted and non-overlapping.
            for (var i = 1; i < l1.Count; i++)
            {
                await Assert.That(l1[i - 1].LastKey < l1[i].FirstKey).IsTrue();
            }

            // Every key is still readable and a full scan returns them all in order.
            await Assert.That(await storage.GetAsync(250)).IsEqualTo(2500);
            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(Enumerable.Range(1, 400), CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(Enumerable.Range(1, 400).Select(k => k * 10), CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionRewritesOnlyOverlappingTargetSsTables()
    {
        // Partial selection: an L0 batch that overlaps only the low key range rewrites just the overlapping
        // L1 SSTs; the non-overlapping L1 SSTs keep their identity (same file) and data.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 1,
            BlockSize = 128,
            TargetSstSizeBytes = 128,
        };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            // Seed L1 with keys 1..400 split across several SSTs (default base target is large, so no
            // size-triggered cascade fires).
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1; k <= 400; k++)
                {
                    storage.Put(k, k);
                }
            });
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            // Record the L1 SSTs that do not overlap the low range [1,5]; these must survive untouched.
            var untouchedBefore = storage._state.LeveledSsTables[0]
                .Where(t => t.FirstKey > 5)
                .Select(t => t.Filename)
                .ToHashSet();
            await Assert.That(untouchedBefore).IsNotEmpty();

            // A small batch overwriting only keys 1..5 overlaps just the first L1 SST.
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1; k <= 5; k++)
                {
                    storage.Put(k, k * 1000);
                }
            });
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            // The non-overlapping L1 SSTs were carried over by reference (same files), not rewritten.
            var l1After = storage._state.LeveledSsTables[0].Select(t => t.Filename).ToHashSet();
            await Assert.That(l1After.IsSupersetOf(untouchedBefore)).IsTrue();

            // Updated keys reflect the new values; everything else is unchanged.
            await Assert.That(await storage.GetAsync(1)).IsEqualTo(1000);
            await Assert.That(await storage.GetAsync(5)).IsEqualTo(5000);
            await Assert.That(await storage.GetAsync(6)).IsEqualTo(6);
            await Assert.That(await storage.GetAsync(400)).IsEqualTo(400);

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(Enumerable.Range(1, 400), CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledReopenRestoresSplitLevelsViaManifest()
    {
        // A reopen restores a level made of multiple split SSTs from the manifest, with all data intact.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 1,
            BlockSize = 128,
            TargetSstSizeBytes = 128,
        };

        {
            var building = new LsmStorageInner<int, int>(tempFolder, options);
            await FlushTierAsync(building, () =>
            {
                for (var k = 1; k <= 400; k++)
                {
                    building.Put(k, k * 10);
                }
            });
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue();
            var splitCount = building._state.LeveledSsTables[0].Count;
            await Assert.That(splitCount > 1).IsTrue();
            building.Dispose();

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                await Assert.That(storage._inner._state.LeveledSsTables[0].Count).IsEqualTo(splitCount);

                var entries = storage._inner.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
                await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(Enumerable.Range(1, 400), CollectionOrdering.Matching);
                await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(Enumerable.Range(1, 400).Select(k => k * 10), CollectionOrdering.Matching);
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task ParallelSubcompactionProducesCompleteSortedData()
    {
        // With parallelism enabled and an input large enough to split across several key-range partitions,
        // the parallel leveled subcompaction must produce sorted, non-overlapping SSTs holding every key.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 1,
            BlockSize = 128,
            TargetSstSizeBytes = 128,
            MaxCompactionParallelism = 8,
        };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1; k <= 800; k++)
                {
                    storage.Put(k, k * 7);
                }
            });

            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            var l1 = storage._state.LeveledSsTables[0];
            await Assert.That(l1.Count > 1).IsTrue();
            for (var i = 1; i < l1.Count; i++)
            {
                await Assert.That(l1[i - 1].LastKey < l1[i].FirstKey).IsTrue();
            }

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(Enumerable.Range(1, 800), CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(Enumerable.Range(1, 800).Select(k => k * 7), CollectionOrdering.Matching);
            await Assert.That(await storage.GetAsync(700)).IsEqualTo(700 * 7);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task SequentialAndParallelCompactionProduceSameData()
    {
        // DOP=1 and DOP>1 must yield identical logical contents (same keys/values on a full scan).
        static async Task<List<KeyValuePair<int, int>>> RunAsync(int dop)
        {
            using var tempFolder = TempFolder.Create();
            var options = new StorageOptions
            {
                UseWriteAheadLog = false,
                CompactionStrategy = CompactionStrategy.Leveled,
                Level0CompactionThreshold = 1,
                BlockSize = 128,
                TargetSstSizeBytes = 128,
                MaxCompactionParallelism = dop,
            };
            var storage = new LsmStorageInner<int, int>(tempFolder, options);
            try
            {
                await FlushTierAsync(storage, () =>
                {
                    for (var k = 1; k <= 600; k++)
                    {
                        storage.Put(k, k * 3);
                    }
                });
                await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();
                return storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
            }
            finally
            {
                storage.Dispose();
            }
        }

        var sequential = await RunAsync(1);
        var parallel = await RunAsync(8);

        await Assert.That(parallel.Select(e => e.Key)).IsEquivalentTo(sequential.Select(e => e.Key), CollectionOrdering.Matching);
        await Assert.That(parallel.Select(e => e.Value)).IsEquivalentTo(sequential.Select(e => e.Value), CollectionOrdering.Matching);
        await Assert.That(parallel.Select(e => e.Key)).IsEquivalentTo(Enumerable.Range(1, 600), CollectionOrdering.Matching);
    }

    [Test]
    public async Task ParallelL0ProbeReturnsNewestValueAndHandlesTombstones()
    {
        // With read parallelism on and enough accumulated L0 tables to cross the probe threshold, point reads
        // must still honor recency (newest table wins), tombstones, and absent keys.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.None,
            MaxReadParallelism = 8,
        };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            // 12 L0 tables (> the parallel probe threshold of 8). Key 1 is overwritten in successive tables;
            // key 2 is later deleted; key 3 only ever exists in one table; key 99 never exists.
            for (var t = 0; t < 12; t++)
            {
                var snapshot = t;
                await FlushTierAsync(storage, () =>
                {
                    storage.Put(1, snapshot * 100);
                    if (snapshot == 0)
                    {
                        storage.Put(3, 333);
                    }
                    if (snapshot == 5)
                    {
                        storage.Put(2, 222);
                    }
                    if (snapshot == 9)
                    {
                        storage.Delete(2);
                    }
                });
            }

            await Assert.That(storage._state.LevelZeroTables.Count >= 8).IsTrue();
            await Assert.That(await storage.GetAsync(1)).IsEqualTo(1100); // newest write wins
            await Assert.That(await storage.GetAsync(3)).IsEqualTo(333);
            await Assert.That(await storage.GetAsync(2)).IsEqualTo(0); // tombstoned -> default
            await Assert.That(await storage.GetAsync(99)).IsEqualTo(0); // never written -> default
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task ParallelReopenRestoresData()
    {
        // OpenAsync loads SSTs in parallel when read parallelism is enabled; a reopen must restore all data.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
            MaxReadParallelism = 8,
        };

        {
            var building = new LsmStorageInner<int, int>(tempFolder, options);
            for (var t = 0; t < 10; t++)
            {
                var snapshot = t;
                await FlushTierAsync(building, () =>
                {
                    for (var k = snapshot * 10 + 1; k <= snapshot * 10 + 10; k++)
                    {
                        building.Put(k, k * 2);
                    }
                });
            }
            building.Dispose();

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
            try
            {
                for (var k = 1; k <= 100; k++)
                {
                    await Assert.That(await storage._inner.GetAsync(k)).IsEqualTo(k * 2);
                }
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task TryGetRawAsyncWritesValueFromMemTable()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6, 7];
        storage.Put(key, value);

        var destination = new ArrayBufferWriter<byte>();
        var found = await storage.TryGetRawAsync(key, destination);

        await Assert.That(found).IsTrue();
        await Assert.That(destination.WrittenSpan.ToArray()).IsEquivalentTo(value, CollectionOrdering.Matching);
    }

    [Test]
    public async Task TryGetRawAsyncReturnsFalseForMissingKey()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        var destination = new ArrayBufferWriter<byte>();
        var found = await storage.TryGetRawAsync([9, 9], destination);

        await Assert.That(found).IsFalse();
        await Assert.That(destination.WrittenCount).IsEqualTo(0);
    }

    [Test]
    public async Task TryGetRawAsyncReturnsFalseForDeletedKeyInMemTable()
    {
        // Unlike GetAsync (which surfaces an empty array for a memtable tombstone), the raw API reports a
        // deleted key as not found.
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        storage.Put(key, [4, 5, 6]);
        storage.Delete(key);

        var destination = new ArrayBufferWriter<byte>();
        var found = await storage.TryGetRawAsync(key, destination);

        await Assert.That(found).IsFalse();
        await Assert.That(destination.WrittenCount).IsEqualTo(0);
    }

    [Test]
    public async Task GetRawAsyncCopiesIntoDestinationAndReturnsLength()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6, 7, 8];
        storage.Put(key, value);

        var destination = new byte[16];
        var length = await storage.GetRawAsync(key, destination);

        await Assert.That(length).IsEqualTo(value.Length);
        await Assert.That(destination.AsSpan(0, length).ToArray()).IsEquivalentTo(value, CollectionOrdering.Matching);
    }

    [Test]
    public async Task GetRawAsyncReturnsMinusOneForMissingKey()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        var length = await storage.GetRawAsync([7], new byte[8]);

        await Assert.That(length).IsEqualTo(-1);
    }

    [Test]
    public async Task GetRawAsyncReportsLengthWithoutWritingWhenDestinationTooSmall()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        byte[] key = [1];
        byte[] value = [4, 5, 6, 7, 8];
        storage.Put(key, value);

        var destination = new byte[2];
        var length = await storage.GetRawAsync(key, destination);

        // The full length is reported so the caller can resize and retry; nothing was written.
        await Assert.That(length).IsEqualTo(value.Length);
        await Assert.That(destination).IsEquivalentTo(new byte[2], CollectionOrdering.Matching);
    }

    [Test]
    public async Task TryReadRawAsyncInspectsValueWithoutCopy()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6, 7];
        storage.Put(key, value);

        var holder = new byte[1][];
        var found = await storage.TryReadRawAsync(key, holder, static (state, span) =>
        {
            // The raw byte[] path borrows the stored array's own memory, so the span content matches.
            state[0] = span.ToArray();
        });

        await Assert.That(found).IsTrue();
        await Assert.That(holder[0]).IsEquivalentTo(value, CollectionOrdering.Matching);
    }

    [Test]
    public async Task TryReadRawAsyncReturnsFalseForMissingKey()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner<byte[], byte[]>(tempFolder, _defaultStorageOptions);

        var invoked = false;
        var found = await storage.TryReadRawAsync([5], invoked, static (_, _) => { });

        await Assert.That(found).IsFalse();
    }

    [Test]
    public async Task RawReadsResolveAgainstFlushedSsTable()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = await LsmStorage.OpenAsync<byte[], byte[]>(tempFolder, new() { UseWriteAheadLog = false });

        byte[] liveKey = [1, 2, 3];
        byte[] liveValue = [10, 20, 30, 40];
        byte[] deletedKey = [4, 5, 6];

        storage.Put(liveKey, liveValue);
        storage.Put(deletedKey, [1]);
        storage.Delete(deletedKey);

        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        await Assert.That(storage._inner._state.CurrentMemTable.Size).IsEqualTo(0L);

        var destination = new ArrayBufferWriter<byte>();
        var found = await storage.TryGetRawAsync(liveKey, destination);
        await Assert.That(found).IsTrue();
        await Assert.That(destination.WrittenSpan.ToArray()).IsEquivalentTo(liveValue, CollectionOrdering.Matching);

        // A tombstone flushed to the SST must read back as not found, not as an empty value.
        var deletedDestination = new ArrayBufferWriter<byte>();
        var deletedFound = await storage.TryGetRawAsync(deletedKey, deletedDestination);
        await Assert.That(deletedFound).IsFalse();
        await Assert.That(deletedDestination.WrittenCount).IsEqualTo(0);

        await storage.CloseAsync();
    }

    [Test]
    public async Task RawReadsWorkWithSentinelTombstoneEncoder()
    {
        // int uses a sentinel tombstone (not an empty value); the raw path must decode to recognise it.
        using var tempFolder = TempFolder.Create();
        using var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, new() { UseWriteAheadLog = false });

        storage.Put(1, 42);
        storage.Put(2, 7);
        storage.Delete(2);

        var destination = new byte[8];
        var length = await storage.GetRawAsync(1, destination);
        await Assert.That(length).IsEqualTo(sizeof(int));
        // The stored bytes are the encoder's order-preserving form, so decode through the encoder.
        await Assert.That(new Int32Encoder().Decode(destination.AsSpan(0, length))).IsEqualTo(42);

        var deletedLength = await storage.GetRawAsync(2, destination);
        await Assert.That(deletedLength).IsEqualTo(-1);

        var missingLength = await storage.GetRawAsync(999, destination);
        await Assert.That(missingLength).IsEqualTo(-1);

        await storage.CloseAsync();
    }

    private static LsmStorageInner<int, byte[]> FillImmutableMemTables(string path, int entries = 100, int valueSize = 10, long memTableSizeLimit = 100)
    {
        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit, UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, byte[]>(path, storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = i;
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);

            storage.Put(key, value);
        }

        return storage;
    }
}
