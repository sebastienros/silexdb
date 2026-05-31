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
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<byte[]>(tempFilename, new DefaultSsTableEncoder<byte[]>(), new DefaultBlockEncoder<byte[]>(), new DefaultBloomFilterFactory(), 100);

        var key = new byte[] { 7, 0 };
        var value = "hello"u8.ToArray();

        await builder.AddAsync(key, new ValueBuffer(value));

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();
        using var block = await table.ReadBlockAsync(0);
        await Assert.That(block!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);

        table.Dispose();
    }

    [Test]
    public async Task ShouldLoadExistingTable()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();
        var blockBuilder = new BlockBuilder<byte[]>(new DefaultBlockEncoder<byte[]>());

        using var builder = new BufferedSsTableBuilder<byte[]>(tempFilename, new DefaultSsTableEncoder<byte[]>(), new DefaultBlockEncoder<byte[]>(), new DefaultBloomFilterFactory(), 100);

        var key = new byte[] { 7, 0 };
        var value = "hello"u8.ToArray();

        await builder.AddAsync(key, new ValueBuffer(value));

        var table = await builder.BuildAsync();
        table.Dispose();

        table = await SsTable<byte[]>.LoadSsTableAsync(tempFilename, new DefaultSsTableEncoder<byte[]>(), blockBuilder, new DefaultBloomFilterFactory());

        await Assert.That(table.BlockMetadata).HasSingleItem();
        using var block = await table.ReadBlockAsync(0);
        await Assert.That(block!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);

        table.Dispose();
    }

    [Test]
    [Arguments(1)]
    [Arguments(10)]
    [Arguments(39)]
    [Arguments(100)]
    [Arguments(1000)]
    public async Task ShouldIterateAllEntries(int count)
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<uint>(tempFilename, new DefaultSsTableEncoder<uint>(), new DefaultBlockEncoder<uint>(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < count; i++)
        {
            await builder.AddAsync(i, new ValueBuffer(value));
        }

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata.Count > 0).IsTrue();

        var iterator = new SsTableIterator<uint>(table);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(0, count).Select(i => (uint)i));

        table.Dispose();
    }

    [Test]
    public async Task ShouldIterateFromKey()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<uint>(tempFilename, new DefaultSsTableEncoder<uint>(), new DefaultBlockEncoder<uint>(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            await builder.AddAsync(i, new ValueBuffer(value));
        }

        var table = await builder.BuildAsync();

        // Check we have one table with multiple blocks
        await Assert.That(table.BlockMetadata.Count > 0).IsTrue();

        var iterator = new SsTableIterator<uint>(table);

        var result = iterator.EnumerateAsync(13).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(13, 100 - 13).Select(i => (uint)i));

        table.Dispose();
    }

    [Test]
    public async Task ShouldIterateFromUnknownKey()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<uint>(tempFilename, new DefaultSsTableEncoder<uint>(), new DefaultBlockEncoder<uint>(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 50; i++)
        {
            await builder.AddAsync(i, new ValueBuffer(value));
        }

        var table = await builder.BuildAsync();

        // Check we have one table with multiple blocks
        await Assert.That(table.BlockMetadata.Count > 0).IsTrue();

        var iterator = new SsTableIterator<uint>(table);

        var result = iterator.EnumerateAsync(101).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEmpty();

        table.Dispose();
    }

    [Test]
    public async Task ShouldIterateFromKeyBeforeFirst()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<uint>(tempFilename, new DefaultSsTableEncoder<uint>(), new DefaultBlockEncoder<uint>(), new DefaultBloomFilterFactory(), 100);

        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        // Keys start at 10, so a 'from' of 5 precedes every block's first key.
        for (uint i = 10; i < 110; i++)
        {
            await builder.AddAsync(i, new ValueBuffer(value));
        }

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata.Count > 1).IsTrue();

        var iterator = new SsTableIterator<uint>(table);

        var result = iterator.EnumerateAsync(5).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(10, 100).Select(i => (uint)i));

        table.Dispose();
    }

    [Test]
    public async Task ShouldCacheBlocks()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<byte[]>(tempFilename, new DefaultSsTableEncoder<byte[]>(), new DefaultBlockEncoder<byte[]>(), new DefaultBloomFilterFactory(), 100);
        using var blockCache = new BlockCache<byte[]>(1.MiB());
        var key = new byte[] { 7, 0 };
        var value = "hello"u8.ToArray();

        await builder.AddAsync(key, new ValueBuffer(value));

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();
        using var block1 = await table.ReadBlockCachedAsync(0, blockCache);
        using var block2 = await table.ReadBlockCachedAsync(0, blockCache);
        await Assert.That(block1.Block!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
        await Assert.That(block2.Block!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
        await Assert.That(block2.Block).IsSameReferenceAs(block1.Block);

        table.Dispose();
    }

    [Test]
    public async Task ShouldCacheBlocksConcurrently()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<byte[]>(tempFilename, new DefaultSsTableEncoder<byte[]>(), new DefaultBlockEncoder<byte[]>(), new DefaultBloomFilterFactory(), 100);
        using var blockCache = new BlockCache<byte[]>(1.MiB());
        var key = new byte[] { 7, 0 };
        var value = "hello"u8.ToArray();

        await builder.AddAsync(key, new ValueBuffer(value));

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();

        var blocks = new List<Task<BlockLease<byte[]>>>();

        for (var i = 0; i < 100; i++)
        {
            blocks.Add(table.ReadBlockCachedAsync(0, blockCache).AsTask());
        }

        var leases = await Task.WhenAll(blocks);

        try
        {
            var result1 = leases[0].Block;

            foreach (var lease in leases)
            {
                var result2 = lease.Block;
                await Assert.That(result2!.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
                await Assert.That(result2).IsSameReferenceAs(result1);
            }
        }
        finally
        {
            foreach (var lease in leases)
            {
                lease.Dispose();
            }
        }

        table.Dispose();
    }

    [Test]
    public async Task ShouldEvictBlocksWhenCacheIsFull()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<uint>(tempFilename, new DefaultSsTableEncoder<uint>(), new DefaultBlockEncoder<uint>(), new DefaultBloomFilterFactory(), 100);
        using var blockCache = new BlockCache<uint>(1);

        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            await builder.AddAsync(i, new ValueBuffer(value));
        }

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata.Count > 1).IsTrue();

        using var block1 = await table.ReadBlockCachedAsync(0, blockCache);
        using var block2 = await table.ReadBlockCachedAsync(1, blockCache);
        using var block3 = await table.ReadBlockCachedAsync(0, blockCache);

        await Assert.That(block2.Block).IsNotNull();
        await Assert.That(block3.Block).IsNotNull();
        await Assert.That(ReferenceEquals(block3.Block, block1.Block)).IsFalse();

        table.Dispose();
    }

    [Test]
    public async Task ShouldNotCacheBlocksWhenCacheSizeIsZero()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder<byte[]>(tempFilename, new DefaultSsTableEncoder<byte[]>(), new DefaultBlockEncoder<byte[]>(), new DefaultBloomFilterFactory(), 100);
        using var blockCache = new BlockCache<byte[]>(0);

        await builder.AddAsync(new byte[] { 7, 0 }, new ValueBuffer("hello"u8.ToArray()));
        var table = await builder.BuildAsync();

        using var block1 = await table.ReadBlockCachedAsync(0, blockCache);
        using var block2 = await table.ReadBlockCachedAsync(0, blockCache);

        await Assert.That(block1.Block).IsNotNull();
        await Assert.That(block2.Block).IsNotNull();
        await Assert.That(ReferenceEquals(block2.Block, block1.Block)).IsFalse();

        table.Dispose();
    }

    [Test]
    public async Task ShouldLoadBloomFilter()
    {
        var entries = Enumerable.Range(0, 100).Select(x => new KeyValuePair<uint, byte[]>((uint)x, BitConverter.GetBytes(x))).ToList();
        using var tempFolder = TempFolder.Create();
        var table = await CreateAndLoadSsTableAsync(entries, tempFolder);

        var bloomFilter = table.BloomFilter;

        var bytes = new byte[sizeof(uint)];

        // Actual entries must always probe true (a bloom filter never produces false negatives).
        foreach (var e in entries)
        {
            BinaryPrimitives.WriteUInt32BigEndian(bytes, e.Key);
            await Assert.That(bloomFilter.Probe(bytes)).IsTrue();
        }

        // Non-inserted keys should mostly probe false, within a tolerable false positive rate.
        var falsePositives = 0;
        var iterations = 1000;

        for (var i = entries.Count; i < iterations + entries.Count; i++)
        {
            BinaryPrimitives.WriteUInt32BigEndian(bytes, (uint)i);
            if (bloomFilter.Probe(bytes))
            {
                falsePositives++;
            }
        }

        // Assume 10% or better
        await Assert.That(falsePositives < iterations * 0.1).IsTrue();

        table.Dispose();
    }

    private async Task<SsTable<TKey>> CreateAndLoadSsTableAsync<TKey>(IReadOnlyList<KeyValuePair<TKey, byte[]>> entries, TempFolder tempFolder)
        where TKey : notnull
    {
        var tempFilename = tempFolder.GetRandomFileName();
        var blockEncoder = new DefaultBlockEncoder<TKey>();
        var blockBuilder = new BlockBuilder<TKey>(blockEncoder);
        var ssTableEncoder = new DefaultSsTableEncoder<TKey>();
        using var builder = new BufferedSsTableBuilder<TKey>(tempFilename, ssTableEncoder, blockEncoder, new DefaultBloomFilterFactory(), entries.Count);

        foreach (var entry in entries)
        {
            await builder.AddAsync(entry.Key, new ValueBuffer(entry.Value));
        }

        var table = await builder.BuildAsync();
        table.Dispose();

        return await SsTable<TKey>.LoadSsTableAsync(tempFilename, ssTableEncoder, blockBuilder, new DefaultBloomFilterFactory());
    }
}
