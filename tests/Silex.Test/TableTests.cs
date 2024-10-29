namespace Silex.Test;

using Microsoft.Extensions.Caching.Memory;
using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Serialization;
using Silex.Tables;
using System.Buffers.Binary;
using System.Numerics;

public class TableTests
{
    [Fact]
    public async Task ShouldCreateTable()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new SsTableBuilder<ushort, string>(new DefaultSsTableEncoder<ushort, string>(), new DefaultBlockEncoder<ushort, string>(), new DefaultBloomFilterFactory(), 100);

        ushort key = 7;
        string value = "hello";

        builder.Add(key, value);

        var table = await builder.BuildAsync(tempFilename);

        Assert.Single(table.BlockMetadata);
        using var block = await table.ReadBlockAsync(0);
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block!.Memory);

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldLoadExistingTable()
    {
        var tempFilename = Path.GetRandomFileName();
        var blockBuilder = new BlockBuilder<ushort, string>(new DefaultBlockEncoder<ushort, string>());

        using var builder = new SsTableBuilder<ushort, string>(new DefaultSsTableEncoder<ushort, string>(), new DefaultBlockEncoder<ushort, string>(), new DefaultBloomFilterFactory(), 100);

        ushort key = 7;
        string value = "hello";

        builder.Add(key, value);

        await builder.BuildAsync(tempFilename);

        var table = await SsTable<ushort, string>.LoadSsTableAsync(tempFilename, new DefaultSsTableEncoder<ushort, string>(), blockBuilder, new DefaultBloomFilterFactory());

        Assert.Single(table.BlockMetadata);
        using var block = await table.ReadBlockAsync(0);
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block!.Memory);

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(10)]
    [InlineData(39)]
    [InlineData(100)]
    [InlineData(1000)]
    public async Task ShouldIterateAllEntries(int count)
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new SsTableBuilder<uint, byte[]>(new DefaultSsTableEncoder<uint, byte[]>(), new DefaultBlockEncoder<uint, byte[]>(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        //var value = new byte[100.B()];
        //Random.Shared.NextBytes(value);
        var value = Enumerable.Range(0, 100).Select(x => (byte)7).ToArray();
        for (uint i = 0; i < count; i++)
        {
            builder.Add(i, value);
        }

        var table = await builder.BuildAsync(tempFilename);

        Assert.True(table.BlockMetadata.Count > 0);

        var iterator = new SsTableIterator<uint, byte[]>(table);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => x.Key).ToArray();

        Assert.Equivalent(Enumerable.Range(0, count), result);

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldIterateFromKey()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new SsTableBuilder<uint, byte[]>(new DefaultSsTableEncoder<uint, byte[]>(), new DefaultBlockEncoder<uint, byte[]>(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            builder.Add(i, value);
        }

        var table = await builder.BuildAsync(tempFilename);

        // Check we have one table with multiple blocks
        Assert.True(table.BlockMetadata.Count > 0);

        var iterator = new SsTableIterator<uint, byte[]>(table);

        var result = iterator.EnumerateAsync(13).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        Assert.Equivalent(Enumerable.Range(13, 100 - 13), result);

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldIterateFromUnknownKey()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new SsTableBuilder<uint, byte[]>(new DefaultSsTableEncoder<uint, byte[]>(), new DefaultBlockEncoder<uint, byte[]>(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 50; i++)
        {
            builder.Add(i, value);
        }

        var table = await builder.BuildAsync(tempFilename);

        // Check we have one table with multiple blocks
        Assert.True(table.BlockMetadata.Count > 0);

        var iterator = new SsTableIterator<uint, byte[]>(table);

        var result = iterator.EnumerateAsync(101).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        Assert.Empty(result);

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldCacheBlocks()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new SsTableBuilder<ushort, string>(new DefaultSsTableEncoder<ushort, string>(), new DefaultBlockEncoder<ushort, string>(), new DefaultBloomFilterFactory(), 100);
        var memoryCache = new MemoryCache(new MemoryCacheOptions());
        ushort key = 7;
        string value = "hello";

        builder.Add(key, value);

        var table = await builder.BuildAsync(tempFilename);

        Assert.Single(table.BlockMetadata);
        using var block1 = await table.ReadBlockCachedAsync(0, memoryCache, new());
        using var block2 = await table.ReadBlockCachedAsync(0, memoryCache, new());
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block1!.Memory);
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block2!.Memory);
        Assert.Same(block1, block2);

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldCacheBlocksConcurrently()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new SsTableBuilder<ushort, string>(new DefaultSsTableEncoder<ushort, string>(), new DefaultBlockEncoder<ushort, string>(), new DefaultBloomFilterFactory(), 100);
        var memoryCache = new MemoryCache(new MemoryCacheOptions());
        ushort key = 7;
        string value = "hello";

        builder.Add(key, value);

        var table = await builder.BuildAsync(tempFilename);

        Assert.Single(table.BlockMetadata);
        
        var blocks = new List<Task<Block<ushort, string>?>>();

        for (var i = 0; i < 100; i++)
        {
            blocks.Add(table.ReadBlockCachedAsync(0, memoryCache, new()));
        }

        await Task.WhenAll(blocks);

        var result1 = await blocks[0];

        foreach (var block in blocks)
        {
            var result2 = await block;
            Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, result2!.Memory);
            Assert.Same(result1, result2);
        }

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldLoadBloomFilter()
    {
        var entries = Enumerable.Range(0, 100).Select(x => new KeyValuePair<int, int>(x, x)).ToList();
        var (table, tempFilename) = await CreateAndLoadSsTableAsync(entries);

        var serializer = BinaryEncoderFactory<int>.BinarySerializer;

        var bloomFilter = table.BloomFilter;

        Span<byte> bytes = stackalloc byte[sizeof(int)];

        var falseNegative = 0;

        // Actual entries should always say "maybe" with a tolerable rate
        var iterations = 1000;

        for (var i = entries.Count; i < 1000 + entries.Count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes, i);
            if (bloomFilter.Probe(bytes))
            {
                falseNegative++;
            }
        }

        // Assume 10% or better
        Assert.True(falseNegative < iterations * 0.1);

        // Wrong entries should always true false
        foreach (var e in entries)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes, e.Key);
            Assert.False(bloomFilter.Probe(bytes));            
        }

        table.Dispose();
        File.Delete(tempFilename);
    }

    private async Task<(SsTable<TKey, TValue>, string)> CreateAndLoadSsTableAsync<TKey, TValue>(IReadOnlyList<KeyValuePair<TKey, TValue>> entries)
    {
        var tempFilename = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var blockEncoder = new DefaultBlockEncoder<TKey, TValue>();
        var blockBuilder = new BlockBuilder<TKey, TValue>(blockEncoder);
        var ssTableEncoder = new DefaultSsTableEncoder<TKey, TValue>();
        using var builder = new SsTableBuilder<TKey, TValue>(ssTableEncoder, blockEncoder, new DefaultBloomFilterFactory(), entries.Count);

        foreach (var entry in entries)
        {
            builder.Add(entry.Key, entry.Value);
        }

        await builder.BuildAsync(tempFilename);

        var table = await SsTable<TKey, TValue>.LoadSsTableAsync(tempFilename, ssTableEncoder, blockBuilder, new DefaultBloomFilterFactory());

        return (table, tempFilename);
    }
}

