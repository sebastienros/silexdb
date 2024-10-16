namespace Silex.Test;

using System.Buffers.Binary;
using System.Globalization;
using Xunit.Abstractions;

public class StorageTests
{
    private readonly StorageOptions _defaultStorageOptions = new();
    private readonly ITestOutputHelper? _output;

    public StorageTests(ITestOutputHelper _)
    {
        _output = null;
    }

    [Fact]
    public void CanPutArray()
    {
        var storage = new LsmStorageInner(Path.GetTempPath(), _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6];

        storage.Put(key, value);
        _ = storage.TryGet(key, out var result);

        Assert.Equal(value, result);
        Assert.Equal(6, storage._state.CurrentMemTable.Size);
    }

    [Fact]
    public void PutValueIsCopied()
    {
        var storage = new LsmStorageInner(Path.GetTempPath(), _defaultStorageOptions);

        byte[] key1 = [1];
        byte[] key2 = [2];
        byte[] value = [4, 5, 6];

        storage.Put(key1, value);
        storage.Put(key2, value);

        _ = storage.TryGet(key1, out var result1);
        _ = storage.TryGet(key2, out var result2);

        Assert.Equal(value, result1);
        Assert.Equal(value, result2);
        Assert.Equal(8, storage._state.CurrentMemTable.Size);
    }

    [Fact]
    public void DeleteShouldStoreTombStone()
    {
        var storage = new LsmStorageInner(Path.GetTempPath(), _defaultStorageOptions);

        byte[] key = [1, 2, 3];

        storage.Delete(key);
        var deleted = storage.TryGet(key, out var result);

        Assert.True(deleted);
        Assert.Equal(0, result.Length);
        Assert.Equal(3, storage._state.CurrentMemTable.Size);
    }

    [Theory]
    [InlineData(1, 1, 0)]
    [InlineData(7, 10, 1)]
    [InlineData(100, 100, 100)]
    [InlineData(10, 500, 62)]
    public void ShouldFreezeMemTablesWhenSizeIsOverLimit(int valueSize, int entries, int expectedImmutableTables)
    {
        var memTableSizeLimit = 100;

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit };
        var storage = new LsmStorageInner(Path.GetTempPath(), storageOptions);

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
    public void GetFromImmutableMemTables()
    {
        var memTableSizeLimit = 100;
        int valueSize = 10;
        int entries = 100;

        var dictionary = new Dictionary<int, byte[]>();

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit };
        var storage = new LsmStorageInner(Path.GetTempPath(), storageOptions);

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
            var result = storage.TryGet(i, out var actualValue);

            Assert.True(result);
            Assert.Equal(expectedValue, actualValue);
        }            
    }

    [Fact]
    public void DeletedEntriesShouldAppearAfterPuts ()
    {
        var storage = FillImmutableMemTables();

        Bytes key = 10;

        var current = storage.TryGet(key, out var result);
        Assert.True(current);
        Assert.Equal(10, result.Length);

        storage.Delete(key);
        var deleted = storage.TryGet(key, out result);

        Assert.True(deleted);
        Assert.Equal(0, result.Length);
    }

    [Fact]
    public void ScanListsAllMemTables()
    {
        var storage = new LsmStorageInner(Path.GetTempPath(), _defaultStorageOptions);

        // table1: b->del, c->4, d->5
        // table2: a->1, b->2, c->3
        // table3: e->4

        storage.Put('e', new byte[] { 4 });
        storage.ForceFreezeMemTable();

        storage.Put('a', new byte[] { 1 });
        storage.Put('b', new byte[] { 2 });
        storage.Put('c', new byte[] { 3 });
        storage.ForceFreezeMemTable();

        storage.Delete('b');
        storage.Put('c', new byte[] { 4 });
        storage.Put('d', new byte[] { 5 });

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
        var storage = new LsmStorageInner(Path.GetTempPath(), storageOptions);
        var iterator = storage.CreateIterator();

        var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token;

        await Parallel.ForAsync(0, levelOfConcurrency, timeout, (i, cancellationToken) =>
        {
            Work(storage);
            return ValueTask.CompletedTask;
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
            var key = entry.Key.Span.Length == 0 ? "0" : BinaryPrimitives.ReadUInt32LittleEndian(entry.Key.Span).ToString(CultureInfo.InvariantCulture);
            storage.TryGet(entry.Key, out var entryValue);
            var value = entryValue.Length == 0 ? "del" : BinaryPrimitives.ReadUInt64LittleEndian(entryValue.Span).ToString(CultureInfo.InvariantCulture);

            _output?.WriteLine($"{key} -> {value}");
        }

        Assert.True(allEntries.Count <= maxKeysValue);

        void Work(LsmStorageInner storage)
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
                        storage.TryGet(id, out var actualValue);
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

        var storage = FillImmutableMemTables(entries: count, memTableSizeLimit: 1.KiB());
        Bytes lowerBytes = lowerBound == null ? Bytes.Empty : lowerBound.Value;

        var expectedKeys = Enumerable.Range(1, count);

        var entries = storage.CreateIterator().EnumerateAsync(lowerBytes).ToBlockingEnumerable().ToList();

        if (lowerBound.HasValue)
        {
            expectedKeys = expectedKeys.Where(x => x >= lowerBound);

            Assert.All(entries, e => Assert.True(lowerBytes <= e.Key));
        }

        var actualKeys = storage.CreateIterator().EnumerateAsync(lowerBytes).ToBlockingEnumerable().Select(x => (int)BinaryPrimitives.ReadUInt32LittleEndian(x.Key.Span)).ToArray();

        Assert.Equal(expectedKeys, actualKeys);
    }

    [Fact]
    public async Task OpenAsyncShouldCreateFolder()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        await LsmStorage.OpenAsync(tempFolder, _defaultStorageOptions);

        Assert.True(Directory.Exists(tempFolder));

        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task ForceFlushShouldNotFailWithNoImmutableMemTable()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        var storage = await LsmStorage.OpenAsync(tempFolder, _defaultStorageOptions);

        storage.Put('e', new byte[] { 4 });

        // Don't freeze current MemTable

        // Nothing to flush
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        Assert.True(Directory.Exists(tempFolder));
        Assert.Empty(Directory.EnumerateFiles(tempFolder, "*.sst"));

        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task ForceFlushShouldFlushMemTable()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

        var storage = await LsmStorage.OpenAsync(tempFolder, _defaultStorageOptions);

        storage.Put('e', new byte[] { 4 });
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        Assert.True(Directory.Exists(tempFolder));
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));

        Directory.Delete(tempFolder, true);
    }

    [Fact]
    public async Task CompacterShouldCreateSst()
    {
        // When the number of mem tables is higher than MemTableMaxCount it should
        // flush the oldest mem table to disk

        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var options = new StorageOptions { MemTableMaxCount = 2 };
        var storage = await LsmStorage.OpenAsync(tempFolder, options);

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

        Directory.Delete(tempFolder, true);
    }


    [Fact]
    public async Task CloseAsyncShouldFlushToDisk()
    {
        var tempFolder = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var storage = await LsmStorage.OpenAsync(tempFolder, _defaultStorageOptions);

        storage.Put('a', 1);
        await storage.CloseAsync();
        Assert.Single(Directory.EnumerateFiles(tempFolder, "*.sst"));

        Directory.Delete(tempFolder, true);
    }

    private static LsmStorageInner FillImmutableMemTables(int entries = 100, int valueSize = 10, long memTableSizeLimit = 100)
    {
        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit };
        var storage = new LsmStorageInner(Path.GetTempPath(), storageOptions);

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
