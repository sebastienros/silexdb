namespace Silex.Test;

public class SilexTests
{
    private readonly StorageOptions _defaultStorageOptions = new();

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

    private static LsmStorageInner FillImmutableMemTables(int entries = 100, int valueSize = 10, int memTableSizeLimit = 100)
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