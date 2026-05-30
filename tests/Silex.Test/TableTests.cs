using Microsoft.Extensions.Caching.Memory;
using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Tables;
using System.Buffers.Binary;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class TableTests
{
    [Test]
    public async Task ShouldCreateTable()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<ushort, string>(tempFilename, new DefaultSsTableEncoder<ushort, string>(), new DefaultBlockEncoder<ushort, string>(), new DefaultBloomFilterFactory(), 100);

        ushort key = 7;
        string value = "hello";

        await builder.AddAsync(key, value);

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();
        using var block = await table.ReadBlockAsync(0);
        await Assert.That(block!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Test]
    public async Task ShouldLoadExistingTable()
    {
        var tempFilename = Path.GetRandomFileName();
        var blockBuilder = new BlockBuilder<ushort, string>(new DefaultBlockEncoder<ushort, string>());

        using var builder = new BufferedSsTableBuilder<ushort, string>(tempFilename, new DefaultSsTableEncoder<ushort, string>(), new DefaultBlockEncoder<ushort, string>(), new DefaultBloomFilterFactory(), 100);

        ushort key = 7;
        string value = "hello";

        await builder.AddAsync(key, value);

        var table =  await builder.BuildAsync();
        table.Dispose();

        table = await SsTable<ushort, string>.LoadSsTableAsync(tempFilename, new DefaultSsTableEncoder<ushort, string>(), blockBuilder, new DefaultBloomFilterFactory());

        await Assert.That(table.BlockMetadata).HasSingleItem();
        using var block = await table.ReadBlockAsync(0);
        await Assert.That(block!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Test]
    [Arguments(1)]
    [Arguments(10)]
    [Arguments(39)]
    [Arguments(100)]
    [Arguments(1000)]
    public async Task ShouldIterateAllEntries(int count)
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<uint, byte[]>(tempFilename, new DefaultSsTableEncoder<uint, byte[]>(), new DefaultBlockEncoder<uint, byte[]>(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);
        
        for (uint i = 0; i < count; i++)
        {
            await builder.AddAsync(i, value);
        }

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata.Count > 0).IsTrue();

        var iterator = new SsTableIterator<uint, byte[]>(table);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(0, count).Select(i => (uint)i));

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Test]
    public async Task ShouldIterateFromKey()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<uint, byte[]>(tempFilename, new DefaultSsTableEncoder<uint, byte[]>(), new DefaultBlockEncoder<uint, byte[]>(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            await builder.AddAsync(i, value);
        }

        var table = await builder.BuildAsync();

        // Check we have one table with multiple blocks
        await Assert.That(table.BlockMetadata.Count > 0).IsTrue();

        var iterator = new SsTableIterator<uint, byte[]>(table);

        var result = iterator.EnumerateAsync(13).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(13, 100 - 13).Select(i => (uint)i));

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Test]
    public async Task ShouldIterateFromUnknownKey()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<uint, byte[]>(tempFilename, new DefaultSsTableEncoder<uint, byte[]>(), new DefaultBlockEncoder<uint, byte[]>(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 50; i++)
        {
            await builder.AddAsync(i, value);
        }

        var table = await builder.BuildAsync();

        // Check we have one table with multiple blocks
        await Assert.That(table.BlockMetadata.Count > 0).IsTrue();

        var iterator = new SsTableIterator<uint, byte[]>(table);

        var result = iterator.EnumerateAsync(101).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEmpty();

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Test]
    public async Task ShouldIterateFromKeyBeforeFirst()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<uint, byte[]>(tempFilename, new DefaultSsTableEncoder<uint, byte[]>(), new DefaultBlockEncoder<uint, byte[]>(), new DefaultBloomFilterFactory(), 100);

        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        // Keys start at 10, so a 'from' of 5 precedes every block's first key.
        for (uint i = 10; i < 110; i++)
        {
            await builder.AddAsync(i, value);
        }

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata.Count > 1).IsTrue();

        var iterator = new SsTableIterator<uint, byte[]>(table);

        var result = iterator.EnumerateAsync(5).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(10, 100).Select(i => (uint)i));

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Test]
    public async Task ShouldCacheBlocks()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<ushort, string>(tempFilename, new DefaultSsTableEncoder<ushort, string>(), new DefaultBlockEncoder<ushort, string>(), new DefaultBloomFilterFactory(), 100);
        var memoryCache = new MemoryCache(new MemoryCacheOptions());
        ushort key = 7;
        string value = "hello";

        await builder.AddAsync(key, value);

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();
        using var block1 = await table.ReadBlockCachedAsync(0, memoryCache, new());
        using var block2 = await table.ReadBlockCachedAsync(0, memoryCache, new());
        await Assert.That(block1!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
        await Assert.That(block2!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
        await Assert.That(block2).IsSameReferenceAs(block1);

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Test]
    public async Task ShouldCacheBlocksConcurrently()
    {
        var tempFilename = Path.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<ushort, string>(tempFilename, new DefaultSsTableEncoder<ushort, string>(), new DefaultBlockEncoder<ushort, string>(), new DefaultBloomFilterFactory(), 100);
        var memoryCache = new MemoryCache(new MemoryCacheOptions());
        ushort key = 7;
        string value = "hello";

        await builder.AddAsync(key, value);

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();
        
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
            await Assert.That(result2!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
            await Assert.That(result2).IsSameReferenceAs(result1);
        }

        table.Dispose();
        File.Delete(tempFilename);
    }

    [Test]
    public async Task ShouldLoadBloomFilter()
    {
        var entries = Enumerable.Range(0, 100).Select(x => new KeyValuePair<int, int>(x, x)).ToList();
        var table = await CreateAndLoadSsTableAsync(entries);

        var bloomFilter = table.BloomFilter;

        var bytes = new byte[sizeof(int)];

        // Actual entries must always probe true (a bloom filter never produces false negatives).
        foreach (var e in entries)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes, e.Key);
            await Assert.That(bloomFilter.Probe(bytes)).IsTrue();
        }

        // Non-inserted keys should mostly probe false, within a tolerable false positive rate.
        var falsePositives = 0;
        var iterations = 1000;

        for (var i = entries.Count; i < iterations + entries.Count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes, i);
            if (bloomFilter.Probe(bytes))
            {
                falsePositives++;
            }
        }

        // Assume 10% or better
        await Assert.That(falsePositives < iterations * 0.1).IsTrue();

        table.Dispose();
        File.Delete(table.Filename);
    }

    private async Task<SsTable<TKey, TValue>> CreateAndLoadSsTableAsync<TKey, TValue>(IReadOnlyList<KeyValuePair<TKey, TValue>> entries)
    {
        var tempFilename = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());
        var blockEncoder = new DefaultBlockEncoder<TKey, TValue>();
        var blockBuilder = new BlockBuilder<TKey, TValue>(blockEncoder);
        var ssTableEncoder = new DefaultSsTableEncoder<TKey, TValue>();
        using var builder = new BufferedSsTableBuilder<TKey, TValue>(tempFilename, ssTableEncoder, blockEncoder, new DefaultBloomFilterFactory(), entries.Count);

        foreach (var entry in entries)
        {
            await builder.AddAsync(entry.Key, entry.Value);
        }

        var table = await builder.BuildAsync();
        table.Dispose();

        return await SsTable<TKey, TValue>.LoadSsTableAsync(tempFilename, ssTableEncoder, blockBuilder, new DefaultBloomFilterFactory());
    }
}

