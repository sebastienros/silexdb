using System.Buffers.Binary;
using System.Globalization;
using Silex.BloomFilters;
using Xunit.Abstractions;

namespace Silex.Test;

public class StorageTests
{
    // These in-memory unit tests construct LsmStorageInner directly against the shared system temp
    // folder, so the write-ahead log is disabled to avoid littering it (and the per-append flush).
    private readonly StorageOptions _defaultStorageOptions = new() { UseWriteAheadLog = false };
    private readonly ITestOutputHelper? _output;

    public StorageTests(ITestOutputHelper _)
    {
        _output = null;
    }

    [Fact]
    public async Task CanPutArray()
    {
        using var storage = new LsmStorageInner<byte[], byte[]>(Path.GetTempPath(), _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6];

        storage.Put(key, value);
        var result = await storage.GetAsync(key);

        Assert.Equal(value, result);
        Assert.Equal(10, storage._state.CurrentMemTable.Size);
    }

    [Fact]
    public async Task GetReturnsZeroCopyBorrowFromMemTable()
    {
        // Zero-copy is a core principle: a value served from a memtable is the same instance that was
        // put (a read-only borrow), not a defensive copy. This locks in that contract.
        using var storage = new LsmStorageInner<byte[], byte[]>(Path.GetTempPath(), _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6];

        storage.Put(key, value);

        var result = await storage.GetAsync(key);
        Assert.Same(value, result);

        var scanned = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().Single();
        Assert.Same(key, scanned.Key);
        Assert.Same(value, scanned.Value);
    }

    [Fact]
    public async Task PutValueIsCopied()
    {
        using var storage = new LsmStorageInner<byte[], byte[]>(Path.GetTempPath(), _defaultStorageOptions);

        byte[] key1 = [1];
        byte[] key2 = [2];
        byte[] value = [4, 5, 6];

        storage.Put(key1, value);
        storage.Put(key2, value);

        var result1 = await storage.GetAsync(key1);
        var result2 = await storage.GetAsync(key2);

        Assert.Equal(value, result1);
        Assert.Equal(value, result2);
        Assert.Equal(16, storage._state.CurrentMemTable.Size);
    }

    [Fact]
    public async Task DeleteShouldStoreTombStone()
    {
        using var storage = new LsmStorageInner<byte[], byte[]>(Path.GetTempPath(), _defaultStorageOptions);

        byte[] key = [1, 2, 3];

        storage.Delete(key);
        var result = await storage.GetAsync(key);

        Assert.True(result?.Length == 0);
        Assert.Equal(7, storage._state.CurrentMemTable.Size);
    }

    [Theory]
    [InlineData(1, 1, 0)]
    [InlineData(7, 10, 1)]
    [InlineData(100, 100, 100)]
    [InlineData(10, 500, 83)]
    public void ShouldFreezeMemTablesWhenSizeIsOverLimit(int valueSize, int entries, int expectedImmutableTables)
    {
        var memTableSizeLimit = 100;

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit, UseWriteAheadLog = false };
        using var storage = new LsmStorageInner<int, byte[]>(Path.GetTempPath(), storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = i; // 4 bytes
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);

            storage.Put(key, value);
        }

        Assert.Equal(expectedImmutableTables, storage._state.ImmutableMemTables.Count());
    }

    [Fact]
    public async Task GetFromImmutableMemTables()
    {
        var memTableSizeLimit = 100;
        int valueSize = 10;
        int entries = 100;

        var dictionary = new Dictionary<int, byte[]>();

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit, UseWriteAheadLog = false };
        using var storage = new LsmStorageInner<int, byte[]>(Path.GetTempPath(), storageOptions);

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

            Assert.Equal(expectedValue, actualValue);
        }            
    }

    [Fact]
    public async Task DeletedEntriesShouldAppearAfterPuts ()
    {
        using var storage = FillImmutableMemTables();

        int key = 10;

        var result = await storage.GetAsync(key);
        Assert.Equal(10, result?.Length);

         storage.Delete(key);
        result = await storage.GetAsync(key);

        Assert.Equal(0, result?.Length);
    }

    [Fact]
    public void ScanListsAllMemTables()
    {
        using var storage = new LsmStorageInner<char, byte[]>(Path.GetTempPath(), new StorageOptions { UseWriteAheadLog = false });

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

        Assert.Equal(4, list.Count);

        Assert.Equal((Bytes)'a', list[0].Key);
        Assert.Equal((Bytes)'c', list[1].Key);
        Assert.Equal((Bytes)'d', list[2].Key);
        Assert.Equal((Bytes)'e', list[3].Key);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    [InlineData(10)]
    public async Task ShouldHandleConcurrentClients(int levelOfConcurrency)
    {
        var maxKeysValue = 50; // Limit the number of unique ids to generate collisions
        var iterations = 50;
        var storageOptions = new StorageOptions { MemTableSizeLimit = 100, UseWriteAheadLog = false };
        using var storage = new LsmStorageInner<long, byte[]>(Path.GetTempPath(), storageOptions);
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

        Assert.True(allEntries.Count <= maxKeysValue);

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

    [Theory]
    [InlineData(null)]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(5)]
    public void ScanWithBoundsShouldFilterResults(int? lowerBound)
    {
        var count = 10;

        using var storage = FillImmutableMemTables(entries: count, memTableSizeLimit: 1.KiB());
        int lowerBytes = lowerBound == null ? -1 : lowerBound.Value;

        var expectedKeys = Enumerable.Range(1, count);

        var entries = storage.CreateIterator().EnumerateAsync(lowerBytes).ToBlockingEnumerable().ToList();

        if (lowerBound.HasValue)
        {
            expectedKeys = expectedKeys.Where(x => x >= lowerBound);

            Assert.All(entries, e => Assert.True(lowerBytes <= e.Key));
        }

        var actualKeys = storage.CreateIterator().EnumerateAsync(lowerBytes).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        Assert.Equal(expectedKeys, actualKeys);
    }

    [Fact]
    public async Task OpenAsyncShouldCreateFolder()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, _defaultStorageOptions);

        Assert.True(Directory.Exists(tempFolder));

        await storage.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task ForceFlushShouldNotFailWithNoImmutableMemTable()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        var storage = await LsmStorage.OpenAsync<char, byte[]>(tempFolder, new StorageOptions());

        storage.Put('e', [4]);

        // Don't freeze current MemTable

        // Nothing to flush
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        Assert.True(Directory.Exists(tempFolder));
        Assert.Empty(Directory.EnumerateFiles(tempFolder, "*.sst"));

        await storage.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task ForceFlushShouldFlushMemTable()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        var storage = await LsmStorage.OpenAsync<char, byte[]>(tempFolder, new());

        storage.Put('e', [4]);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        Assert.True(Directory.Exists(tempFolder));
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));

        await storage.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task GetShouldReadFromFlushedSsTable()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

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
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));
        Assert.Empty(storage._inner._state.ImmutableMemTables);
        Assert.Equal(0L, storage._inner._state.CurrentMemTable.Size);

        // Reads must go through the bloom filter and block decoding of the SST.
        for (var i = 0; i < count; i++)
        {
            Assert.Equal(i + 1, await storage.GetAsync(i));
        }

        // A key that was never inserted returns the default value.
        Assert.Equal(0, await storage.GetAsync(10000));

        await storage.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task CompacterShouldCreateSst()
    {
        // When the number of mem tables is higher than MemTableMaxCount it should
        // flush the oldest mem table to disk

        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var options = new StorageOptions { MemTableMaxCount = 2 };
        var storage = await LsmStorage.OpenAsync<char, int>(tempFolder, options);

        storage.Put('a', 1);
        storage._inner.ForceFreezeMemTable();

        await Task.Delay(100);
        Assert.Empty(Directory.EnumerateFiles(tempFolder, "*.sst"));
        Assert.Single(storage._inner._state.ImmutableMemTables);
        Assert.True(storage._inner._state.ImmutableMemTables.Peek().TryGet('a', out _));
        Assert.False(storage._inner._state.ImmutableMemTables.Peek().TryGet('b', out _));

        storage.Put('b', 2);
        storage._inner.ForceFreezeMemTable();

        await Task.Delay(100);
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));
        Assert.Single(storage._inner._state.ImmutableMemTables);
        Assert.False(storage._inner._state.ImmutableMemTables.Peek().TryGet('a', out _));
        Assert.True(storage._inner._state.ImmutableMemTables.Peek().TryGet('b', out _));

        await storage.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task CloseAsyncShouldFlushToDisk()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var storage = await LsmStorage.OpenAsync<char, int>(tempFolder, new StorageOptions());

        storage.Put('a', 1);
        await storage.CloseAsync();
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));

        await storage.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task CloseAsyncCanBeInvokedMultipleTimes()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var storage = await LsmStorage.OpenAsync<char, int>(tempFolder, new StorageOptions());

        storage.Put('a', 1);
        await storage.CloseAsync();
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));

        await storage.CloseAsync();
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));

        storage = await LsmStorage.OpenAsync<char, int>(tempFolder, new StorageOptions());
        storage.Put('a', 2);
        await storage.CloseAsync();
        Assert.Equal(2, Directory.EnumerateFiles(tempFolder, "*.sst").Count());

        await storage.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task FinalizerShouldNotFlushOrCrashWhenStorageIsNotClosed()
    {
        // A storage that is never closed must not flush to disk during finalization (no WAL exists,
        // durability is only guaranteed by CloseAsync). Even when its directory has been removed, the
        // finalizer must be a no-op and never crash the process.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        await CreateUnclosedStorageAsync(tempFolder);

        Directory.Delete(tempFolder, true);

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Reaching this point means finalization neither flushed to the deleted folder nor crashed.
        Assert.False(Directory.Exists(tempFolder));

        // The storage was never closed, so nothing was ever persisted.
        static async Task CreateUnclosedStorageAsync(string folder)
        {
            var storage = await LsmStorage.OpenAsync<char, int>(folder, new StorageOptions { UseWriteAheadLog = false });
            storage.Put('a', 1);
            // Intentionally not closed/disposed: it becomes eligible for finalization on return.
        }
    }

    [Fact]
    public async Task GetShouldReturnMostRecentImmutableMemTableValue()
    {
        using var storage = new LsmStorageInner<int, int>(Path.GetTempPath(), new StorageOptions { UseWriteAheadLog = false });

        storage.Put(1, 100);
        storage.ForceFreezeMemTable();

        storage.Put(1, 200);
        storage.ForceFreezeMemTable();

        // Both immutable mem tables hold key 1; the most recently frozen value must win.
        Assert.Equal(2, storage._state.ImmutableMemTables.Count());
        Assert.Equal(200, await storage.GetAsync(1));
    }

    [Fact]
    public void ScanShouldReturnMostRecentImmutableMemTableValue()
    {
        using var storage = new LsmStorageInner<int, int>(Path.GetTempPath(), new StorageOptions { UseWriteAheadLog = false });

        storage.Put(1, 100);
        storage.ForceFreezeMemTable();

        storage.Put(1, 200);
        storage.ForceFreezeMemTable();

        var list = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

        Assert.Single(list);
        Assert.Equal(200, list[0].Value);
    }

    [Fact]
    public async Task GetShouldFindByteArrayKeyByContent()
    {
        using var storage = new LsmStorageInner<byte[], int>(Path.GetTempPath(), new StorageOptions { UseWriteAheadLog = false });

        storage.Put([1, 2, 3], 42);

        // A different array instance with the same content must resolve to the stored value.
        Assert.Equal(42, await storage.GetAsync([1, 2, 3]));
    }

    [Fact]
    public async Task ReopenShouldPreserveLevelZeroRecency()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        storage.Put(1, 100);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        storage.Put(1, 200);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        Assert.Equal(2, Directory.EnumerateFiles(tempFolder, "*.sst").Count());
        await storage.CloseAsync();

        // After reopening, the SSTs must be ordered by creation so the newest value still wins.
        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        Assert.Equal(200, await reopened.GetAsync(1));

        await reopened.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task GetShouldReadBytesValueOfArbitraryLengthFromSsTable()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        var storage = await LsmStorage.OpenAsync<int, Bytes>(tempFolder, options);

        // A Bytes value whose length is not 4 bytes used to trip an incorrect decode assertion.
        Bytes value = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        storage.Put(1, value);

        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));

        var result = await storage.GetAsync(1);
        Assert.Equal(value, result);

        await storage.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task WriteAheadLogRecoversUnflushedEntriesAfterCrash()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        // Disable background flushing so the data stays only in the memtable + WAL (never an SST).
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        await SimulateCrashAfterPutsAsync(tempFolder, options);

        // Finalize the abandoned instance so its WAL handle is released (the file and its data remain).
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Nothing was ever flushed, yet the WAL is on disk holding the unflushed writes.
        Assert.Empty(Directory.EnumerateFiles(tempFolder, "*.sst"));
        Assert.NotEmpty(Directory.EnumerateFiles(tempFolder, "*.wal"));

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        for (var i = 0; i < 10; i++)
        {
            Assert.Equal(i + 1, await reopened.GetAsync(i));
        }

        await reopened.CloseAsync();
        Directory.Delete(tempFolder, true);

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

    [Fact]
    public async Task WriteAheadLogRecoveryRequiresTheWalFile()
    {
        // Sanity check that the recovery above is genuinely driven by the WAL: with the WAL removed
        // (and nothing flushed), the data is gone after a crash.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
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
        Assert.Equal(0, await reopened.GetAsync(0));

        await reopened.CloseAsync();
        Directory.Delete(tempFolder, true);

        static async Task SimulateCrashAfterPutsAsync(string folder, StorageOptions options)
        {
            var storage = await LsmStorage.OpenAsync<int, int>(folder, options);
            storage.Put(0, 1);
        }
    }

    [Fact]
    public async Task WriteAheadLogRecoveryToleratesTornTrailingRecord()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
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
        Assert.Equal(1, await reopened.GetAsync(0));

        await reopened.CloseAsync();
        Directory.Delete(tempFolder, true);

        static async Task SimulateCrashAfterPutsAsync(string folder, StorageOptions options)
        {
            var storage = await LsmStorage.OpenAsync<int, int>(folder, options);

            for (var i = 0; i < 10; i++)
            {
                storage.Put(i, i + 1);
            }
        }
    }

    [Fact]
    public async Task RecoveryDeletesStaleWalWhenSstAlreadyExists()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
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

        Assert.Equal(42, await reopened.GetAsync(1));
        // The stale WAL was deleted (and never replayed) because its SST was already loaded. The
        // reopened store has its own fresh current-memtable WAL, so only assert the stale one is gone.
        Assert.False(File.Exists(staleWal));

        await reopened.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task TieredCompactionMergesAllTiersUnderSpaceAmplification()
    {
        // Three equally sized single-entry tiers hit the space-amplification trigger (sum of the newer
        // tiers reaches MaxSizeAmplificationPercent of the oldest), so all three merge into one SST.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 3 };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));
            Assert.Equal(3, storage._state.LevelZeroTables.Count);

            var compacted = await storage.TryTieredCompactionAsync();

            Assert.True(compacted);
            Assert.Single(storage._state.LevelZeroTables);
            Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));
            Assert.Equal(10, await storage.GetAsync(1));
            Assert.Equal(20, await storage.GetAsync(2));
            Assert.Equal(30, await storage.GetAsync(3));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task TieredCompactionDropsTombstonesOnFullCompaction()
    {
        // A full compaction (the oldest tier participates) may drop tombstones. The delete of key 1 in a
        // newer tier shadows its value in the oldest tier and is then discarded entirely.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 3 };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 100));
            await FlushTierAsync(storage, () => storage.Delete(1));
            await FlushTierAsync(storage, () => storage.Put(2, 200));

            var compacted = await storage.TryTieredCompactionAsync();

            Assert.True(compacted);
            Assert.Single(storage._state.LevelZeroTables);
            Assert.Equal(0, await storage.GetAsync(1));
            Assert.Equal(200, await storage.GetAsync(2));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task TieredCompactionPartialMergeKeepsTombstones()
    {
        // A large oldest tier plus several tiny newest tiers avoids the space-amplification trigger and
        // instead fires the size-ratio trigger, merging only the newest tiers. Because the oldest tier is
        // excluded, a tombstone for a key it still holds must be preserved (the key stays logically gone).
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
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
            Assert.Equal(4, storage._state.LevelZeroTables.Count);

            var compacted = await storage.TryTieredCompactionAsync();

            Assert.True(compacted);
            // The big oldest tier is untouched; the three newest tiers collapse into one.
            Assert.Equal(2, storage._state.LevelZeroTables.Count);
            // The tombstone was kept, so the key the oldest tier still holds remains logically deleted.
            Assert.Equal(0, await storage.GetAsync(1000));
            // Untouched keys from the oldest tier are still readable.
            Assert.Equal(1001, await storage.GetAsync(1001));
            Assert.Equal(1, await storage.GetAsync(1));
            Assert.Equal(3, await storage.GetAsync(3));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task TieredCompactionBelowThresholdIsNoOp()
    {
        // With fewer tiers than MaxCompactionTiers there is nothing to do: the call is a no-op.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 8 };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            var compacted = await storage.TryTieredCompactionAsync();

            Assert.False(compacted);
            Assert.Equal(2, storage._state.LevelZeroTables.Count);
            Assert.Equal(2, Directory.EnumerateFiles(tempFolder, "*.sst").Count());
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task TieredCompactionDisabledByStrategyIsNoOp()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 2, CompactionStrategy = CompactionStrategy.None };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));

            Assert.False(await storage.TryTieredCompactionAsync());
            Assert.Equal(3, storage._state.LevelZeroTables.Count);
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task TieredCompactionRecencySurvivesReopen()
    {
        // After a full compaction the merged output gets the highest id, so reopen-by-id still resolves
        // the newest value for a key written across several tiers.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero, MaxCompactionTiers = 3 };

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        for (var v = 1; v <= 3; v++)
        {
            storage.Put(1, v * 100);
            storage._inner.ForceFreezeMemTable();
            await storage._inner.ForceFlushNextImmutableMemTableAsync();
        }

        Assert.True(await storage._inner.TryTieredCompactionAsync());
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));
        await storage.CloseAsync();

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        Assert.Equal(300, await reopened.GetAsync(1));

        await reopened.CloseAsync();
        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task GetContinuesToOlderSsTableOnBloomFalsePositive()
    {
        // A newer SST whose key range straddles the target key but does not contain it must not mask an
        // older SST that does. Forcing the bloom filter to always report "maybe present" simulates the
        // false positive that would otherwise make the lookup stop prematurely.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
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

            Assert.Equal(50, await storage.GetAsync(5));
            // Keys that really are in the newer table still resolve from it.
            Assert.Equal(10, await storage.GetAsync(1));
            Assert.Equal(100, await storage.GetAsync(10));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    private sealed class AlwaysPositiveBloomFilterFactory : IBloomFilterFactory
    {
        private static readonly DefaultBloomFilterFactory _inner = new();

        public IBloomFilter CreateBloomFilter(int n, double p) => new AlwaysPositiveBloomFilter(_inner.CreateBloomFilter(n, p));

        public IBloomFilter CreateBloomFilter(ReadOnlySpan<byte> bytes, int k) => new AlwaysPositiveBloomFilter(_inner.CreateBloomFilter(bytes, k));
    }

    private sealed class AlwaysPositiveBloomFilter(IBloomFilter inner) : IBloomFilter
    {
        public int K => inner.K;

        public void Add(ReadOnlySpan<byte> value) => inner.Add(value);

        public bool Probe(ReadOnlySpan<byte> item) => true;

        public Span<byte> GetBytes() => inner.GetBytes();
    }

    [Fact]
    public async Task ConcurrentFlushAndScanNeverMissesCommittedData()
    {
        // Regression guard for the flush/scan race: while an immutable MemTable is mid-flush (dequeued from
        // the queue but its SST not yet published), a concurrent scan must still observe its data. Flush makes
        // that transition atomic under the level0 write lock, so every previously committed key stays visible.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            const int keyCount = 80;
            var committedCount = 0;
            using var done = new CancellationTokenSource();

            var scanner = Task.Run(() =>
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
                        Assert.Contains(k, keys);
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
            Directory.Delete(tempFolder, true);
        }
    }

    private static async Task FlushTierAsync(LsmStorageInner<int, int> storage, Action write)
    {
        write();
        storage.ForceFreezeMemTable();
        await storage.ForceFlushNextImmutableMemTableAsync();
    }

    [Fact]
    public async Task FullScanIncludesFlushedSsTableData()
    {
        // Data flushed to L0 SSTs (with nothing left in the MemTables) must still appear in a full scan.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

            Assert.Equal(new[] { 1, 2, 3 }, entries.Select(e => e.Key));
            Assert.Equal(new[] { 10, 20, 30 }, entries.Select(e => e.Value));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task ScanReturnsNewestValueAcrossMemTableAndSsTable()
    {
        // A key written to an SST and later overwritten in the current MemTable must scan as the newer value.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            // Overwrite key 1 in the live MemTable; this value is newer than the flushed one.
            storage.Put(1, 999);

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

            Assert.Equal(new[] { 1, 2 }, entries.Select(e => e.Key));
            Assert.Equal(new[] { 999, 20 }, entries.Select(e => e.Value));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task ScanHidesKeysDeletedAfterFlush()
    {
        // A key present in an SST but deleted afterwards (tombstone in the MemTable) must be absent from scans.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            storage.Delete(1);

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

            Assert.Equal(new[] { 2 }, entries.Select(e => e.Key));
            Assert.Equal(new[] { 20 }, entries.Select(e => e.Value));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task RangeScanIncludesSsTableData()
    {
        // A bounded scan (keys >= from) must include matching entries that live only in SSTs.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));
            storage.Put(4, 40);

            var entries = storage.CreateIterator().EnumerateAsync(2).ToBlockingEnumerable().ToList();

            Assert.Equal(new[] { 2, 3, 4 }, entries.Select(e => e.Key));
            Assert.Equal(new[] { 20, 30, 40 }, entries.Select(e => e.Value));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task ScanAfterReopenIncludesSsTableData()
    {
        // After reopening, all live data is in SSTs (MemTables are empty), so the scan exercises the SST path.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };

        try
        {
            var storage = new LsmStorageInner<int, int>(tempFolder, options);
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            storage.Dispose();

            var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                var entries = reopened._inner.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();

                Assert.Equal(new[] { 1, 2 }, entries.Select(e => e.Key));
                Assert.Equal(new[] { 10, 20 }, entries.Select(e => e.Value));
            }
            finally
            {
                reopened.Dispose();
            }
        }
        finally
        {
            Directory.Delete(tempFolder, true);
        }
    }


    [Fact]
    public async Task GetReturnsDefaultForSentinelTombstoneStoredInSsTable()
    {
        // Sentinel-based encoders (here int) persist a deletion as a fixed non-empty value. Reading that
        // key back from an SST must resolve to the default (deleted), not the raw sentinel.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, int>(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 100));
            await FlushTierAsync(storage, () => storage.Delete(1));

            // The delete lives in the newest SST as a sentinel value; it must shadow the older value.
            Assert.Equal(0, await storage.GetAsync(1));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task LeveledCompactionFlushesL0IntoL1()
    {
        // Once Level0CompactionThreshold L0 SSTs accumulate, leveled compaction merges them all into a
        // single L1 sorted run and empties L0.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
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
            Assert.Equal(4, storage._state.LevelZeroTables.Count);

            var compacted = await storage.TryLeveledCompactionAsync();

            Assert.True(compacted);
            Assert.Empty(storage._state.LevelZeroTables);
            Assert.Single(storage._state.LeveledSsTables);
            Assert.Single(storage._state.LeveledSsTables[0]);
            Assert.Equal(10, await storage.GetAsync(1));
            Assert.Equal(20, await storage.GetAsync(2));
            Assert.Equal(30, await storage.GetAsync(3));
            Assert.Equal(40, await storage.GetAsync(4));

            // The merged run is also visible to a full scan in key order.
            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
            Assert.Equal(new[] { 1, 2, 3, 4 }, entries.Select(e => e.Key));
            Assert.Equal(new[] { 10, 20, 30, 40 }, entries.Select(e => e.Value));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task LeveledCompactionPushesOverSizedLevelDown()
    {
        // With a tiny base target every level is over budget, so after L0 flushes into L1 a second
        // compaction pushes L1 down into L2.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
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

            Assert.True(await storage.TryLeveledCompactionAsync());
            Assert.Single(storage._state.LeveledSsTables[0]); // L1 populated

            // Second action: L1 is over its (tiny) target, so it is pushed down into L2.
            Assert.True(await storage.TryLeveledCompactionAsync());
            Assert.Equal(2, storage._state.LeveledSsTables.Count);
            Assert.Empty(storage._state.LeveledSsTables[0]); // L1 now empty
            Assert.Single(storage._state.LeveledSsTables[1]); // L2 populated

            Assert.Equal(10, await storage.GetAsync(1));
            Assert.Equal(20, await storage.GetAsync(2));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task LeveledCompactionDropsTombstonesAtBottomLevel()
    {
        // When the destination is the last non-empty level, a delete may be discarded entirely because no
        // older value survives below it.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
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

            Assert.True(await storage.TryLeveledCompactionAsync());

            Assert.Equal(0, await storage.GetAsync(1));
            Assert.Equal(200, await storage.GetAsync(2));

            // The tombstone for key 1 was dropped, so only key 2 survives in L1.
            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
            Assert.Equal(new[] { 2 }, entries.Select(e => e.Key));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task LeveledCompactionKeepsTombstoneWhenLowerLevelHoldsKey()
    {
        // A tombstone must be preserved when a deeper level still holds the key it shadows, otherwise the
        // older value would be resurrected.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
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
            Assert.True(await storage.TryLeveledCompactionAsync()); // L0 -> L1
            Assert.True(await storage.TryLeveledCompactionAsync()); // L1 -> L2

            // Now delete key 1 in a fresh L0 batch and compact into L1 (which sits above the L2 that holds 1).
            await FlushTierAsync(storage, () => storage.Delete(1));
            await FlushTierAsync(storage, () => storage.Put(3, 300));
            Assert.True(await storage.TryLeveledCompactionAsync()); // L0 -> L1, must keep the tombstone

            // The delete is preserved: key 1 reads as deleted even though L2 still physically holds 100.
            Assert.Equal(0, await storage.GetAsync(1));
            Assert.Equal(200, await storage.GetAsync(2));
            Assert.Equal(300, await storage.GetAsync(3));
        }
        finally
        {
            storage.Dispose();
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task LeveledReopenRestoresLevelsViaManifest()
    {
        // The manifest persists the L0/level structure so a reopen restores the exact levels (and recency)
        // even though leveled SST ids no longer encode it.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
            BaseLevelTargetBytes = 1,
        };

        try
        {
            var building = new LsmStorageInner<int, int>(tempFolder, options);
            await FlushTierAsync(building, () => building.Put(1, 10));
            await FlushTierAsync(building, () => building.Put(2, 20));
            Assert.True(await building.TryLeveledCompactionAsync()); // L0 -> L1
            Assert.True(await building.TryLeveledCompactionAsync()); // L1 -> L2
            await FlushTierAsync(building, () => building.Put(3, 30));
            await FlushTierAsync(building, () => building.Put(4, 40));
            Assert.True(await building.TryLeveledCompactionAsync()); // L0 -> L1
            building.Dispose();

            Assert.True(File.Exists(Path.Combine(tempFolder, "manifest")));

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                // L1 holds {3,4}; L2 holds {1,2}.
                Assert.Equal(2, storage._inner._state.LeveledSsTables.Count);
                Assert.Single(storage._inner._state.LeveledSsTables[0]);
                Assert.Single(storage._inner._state.LeveledSsTables[1]);
                Assert.Empty(storage._inner._state.LevelZeroTables);

                var entries = storage._inner.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToList();
                Assert.Equal(new[] { 1, 2, 3, 4 }, entries.Select(e => e.Key));
                Assert.Equal(new[] { 10, 20, 30, 40 }, entries.Select(e => e.Value));
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
        finally
        {
            Directory.Delete(tempFolder, true);
        }
    }

    [Fact]
    public async Task LeveledReopenDeletesOrphanSstNotInManifest()
    {
        // An SST left behind by a flush/compaction that crashed before the manifest commit is not
        // referenced by the manifest and must be deleted on reopen.
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        Directory.CreateDirectory(tempFolder);
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
        };

        try
        {
            var building = new LsmStorageInner<int, int>(tempFolder, options);
            await FlushTierAsync(building, () => building.Put(1, 10));
            await FlushTierAsync(building, () => building.Put(2, 20));
            Assert.True(await building.TryLeveledCompactionAsync());
            building.Dispose();

            // Simulate an orphan output (id far in the future, not referenced by the manifest).
            var orphan = Path.Combine(tempFolder, "999999.sst");
            File.WriteAllText(orphan, "not a real sst");

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                Assert.False(File.Exists(orphan));
                Assert.Equal(10, await storage.GetAsync(1));
                Assert.Equal(20, await storage.GetAsync(2));
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
        finally
        {
            Directory.Delete(tempFolder, true);
        }
    }

    private static LsmStorageInner<int, byte[]> FillImmutableMemTables(int entries = 100, int valueSize = 10, long memTableSizeLimit = 100)
    {
        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit, UseWriteAheadLog = false };
        var storage = new LsmStorageInner<int, byte[]>(Path.GetTempPath(), storageOptions);

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
