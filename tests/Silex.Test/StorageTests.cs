using System.Buffers.Binary;
using System.Globalization;
using Xunit.Abstractions;

namespace Silex.Test;

public class StorageTests
{
    private readonly StorageOptions _defaultStorageOptions = new();
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

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit };
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

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit };
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
        using var storage = new LsmStorageInner<char, byte[]>(Path.GetTempPath(), new StorageOptions());

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
        var storageOptions = new StorageOptions { MemTableSizeLimit = 100 };
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
            var storage = await LsmStorage.OpenAsync<char, int>(folder, new StorageOptions());
            storage.Put('a', 1);
            // Intentionally not closed/disposed: it becomes eligible for finalization on return.
        }
    }

    private static LsmStorageInner<int, byte[]> FillImmutableMemTables(int entries = 100, int valueSize = 10, long memTableSizeLimit = 100)
    {
        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit };
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
