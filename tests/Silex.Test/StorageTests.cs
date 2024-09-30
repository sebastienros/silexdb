namespace Silex.Test;

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
        var storage = new LsmStorageInner(_defaultStorageOptions);

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
        var storage = new LsmStorageInner(_defaultStorageOptions);

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
        var storage = new LsmStorageInner(_defaultStorageOptions);

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
        var storage = new LsmStorageInner(storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = BitConverter.GetBytes(i); // 4 bytes
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
        var storage = new LsmStorageInner(storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = BitConverter.GetBytes(i);
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);

            dictionary[i] = value;
            storage.Put(key, value);
        }

        for (var i = 1; i <= entries; i++)
        {
            var expectedValue = dictionary[i];
            var result = storage.TryGet(BitConverter.GetBytes(i), out var actualValue);

            Assert.True(result);
            Assert.Equal(expectedValue, actualValue);
        }            
    }

    [Fact]
    public void DeletedEntriesShouldAppearAfterPuts ()
    {
        var storage = FillImmutableMemTables();

        byte[] key = BitConverter.GetBytes(10);

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
        var storage = new LsmStorageInner(_defaultStorageOptions);

        // table1: b->del, c->4, d->5
        // table2: a->1, b->2, c->3
        // table3: e->4

        storage.Put(BitConverter.GetBytes('e'), new byte[] { 4 });
        storage.ForceFreezeMemTable();

        storage.Put(BitConverter.GetBytes('a'), new byte[] { 1 });
        storage.Put(BitConverter.GetBytes('b'), new byte[] { 2 });
        storage.Put(BitConverter.GetBytes('c'), new byte[] { 3 });
        storage.ForceFreezeMemTable();

        storage.Delete(BitConverter.GetBytes('b'));
        storage.Put(BitConverter.GetBytes('c'), new byte[] { 4 });
        storage.Put(BitConverter.GetBytes('d'), new byte[] { 5 });

        var list = storage.Scan().ToList();

        // a->1, c->4, d->5, e->4 and b->del should be discarded

        Assert.Equal(4, list.Count);

        Assert.Equal('a', BitConverter.ToChar(list[0].Key.Span));
        Assert.Equal(1, list[0].Value.Span[0]);

        Assert.Equal('c', BitConverter.ToChar(list[1].Key.Span));
        Assert.Equal(4, list[1].Value.Span[0]);

        Assert.Equal('d', BitConverter.ToChar(list[2].Key.Span));
        Assert.Equal(5, list[2].Value.Span[0]);

        Assert.Equal('e', BitConverter.ToChar(list[3].Key.Span));
        Assert.Equal(4, list[3].Value.Span[0]);
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
        var storage = new LsmStorageInner(storageOptions);

        var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token;

        await Parallel.ForAsync(0, levelOfConcurrency, timeout, (i, cancellationToken) =>
        {
            Work(storage);
            return ValueTask.CompletedTask;
        });

        var allEntries = storage.Scan().ToList();

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
            var key = entry.Key.Span.Length == 0 ? "0" : BitConverter.ToInt32(entry.Key.Span).ToString(CultureInfo.InvariantCulture);
            var value = entry.Value.Span.Length == 0 ? "del" : BitConverter.ToInt64(entry.Value.Span).ToString(CultureInfo.InvariantCulture);

            _output?.WriteLine($"{key} -> {value}");
        }

        Assert.True(allEntries.Count <= maxKeysValue);

        void Work(LsmStorageInner storage)
        {
            for (var i = 0; i < iterations; i++)
            {
                var id = Random.Shared.NextInt64(maxKeysValue);
                var value = Random.Shared.NextInt64();
                storage.Put(BitConverter.GetBytes(id), BitConverter.GetBytes(value));
            }

            for (var i = 0; i < iterations; i++)
            {
                var operation = Random.Shared.NextInt64(10);
                var id = Random.Shared.NextInt64(maxKeysValue);

                switch (operation)
                {
                    case 0: // Put
                        var value = Random.Shared.NextInt64();
                        storage.Put(BitConverter.GetBytes(id), BitConverter.GetBytes(value));
                        break;

                    case 1: // Get
                        storage.TryGet(BitConverter.GetBytes(id), out var actualValue);
                        break;

                    case 2: // Delete
                        storage.Delete(BitConverter.GetBytes(id));
                        break;

                    case 3: // Scan
                        _ = storage.Scan().ToList();
                        break;
                }
            }
        }
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(0, 0)]
    [InlineData(1, 1)]
    [InlineData(1, 5)]
    [InlineData(5, 10)]
    [InlineData(null, 5)]
    [InlineData(5, null)]
    public void ScanWithBoundsShouldFilterResults(int? lowerBound, int? upperBound)
    {
        var count = 10;

        var storage = FillImmutableMemTables(entries: count, memTableSizeLimit: 1.KiB());
        ReadOnlyMemory<byte> lowerBytes = lowerBound == null ? ReadOnlyMemory<byte>.Empty : BitConverter.GetBytes(lowerBound.Value);
        ReadOnlyMemory<byte> upperBytes = upperBound == null ? ReadOnlyMemory<byte>.Empty : BitConverter.GetBytes(upperBound.Value);

        var expectedKeys = Enumerable.Range(1, count);

        var entries = storage.Scan(lowerBytes, upperBytes);

        if (lowerBound.HasValue)
        {
            expectedKeys = expectedKeys.Where(x => x >= lowerBound);

            Assert.All(entries, e => Assert.True(ByteArrayComparer.Instance.Compare(lowerBytes, e.Key) <= 0));
        }

        if (upperBound.HasValue)
        {
            expectedKeys = expectedKeys.Where(x => x <= upperBound);

            Assert.All(entries, e => Assert.True(ByteArrayComparer.Instance.Compare(upperBytes, e.Key) >= 0));
        }

        var actualKeys = storage.Scan(lowerBytes, upperBytes).Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Equal(expectedKeys, actualKeys);
    }

    private static LsmStorageInner FillImmutableMemTables(int entries = 100, int valueSize = 10, long memTableSizeLimit = 100)
    {
        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit };
        var storage = new LsmStorageInner(storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = BitConverter.GetBytes(i);
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);

            storage.Put(key, value);
        }

        return storage;
    }
}
