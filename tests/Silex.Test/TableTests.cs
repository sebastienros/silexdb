using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Tables;
using System.Buffers.Binary;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class TableTests
{
    private static uint DecodeUInt32(ByteSlice value) => new Silex.Serialization.UInt32Encoder().Decode(value.Span);

    [Test]
    public async Task ShouldCreateTable()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);

        ushort key = 7;
        string value = "hello";

        await builder.AddAsync(key, value);

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();
        using var block = await table.ReadBlockAsync(0);
        await Assert.That(block!.Memory).IsEquivalentTo(new byte[] { 2, 0, 7, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);

        table.Dispose();
    }

    [Test]
    public async Task ShouldLoadExistingTable()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);

        ushort key = 7;
        string value = "hello";

        await builder.AddAsync(key, value);

        var table = await builder.BuildAsync();
        table.Dispose();

        table = await SsTable.LoadSsTableAsync(tempFilename, new DefaultSsTableEncoder(), blockBuilder, new DefaultBloomFilterFactory());

        await Assert.That(table.BlockMetadata).HasSingleItem();
        await Assert.That(table.BloomFilter.AlgorithmVersion).IsEqualTo(BloomFilter.CurrentAlgorithmVersion);
        using var block = await table.ReadBlockAsync(0);
        await Assert.That(block!.Memory).IsEquivalentTo(new byte[] { 2, 0, 7, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);

        table.Dispose();
    }

    [Test]
    [Arguments(SstCompression.Lz4)]
    [Arguments(SstCompression.Zstandard)]
    public async Task ShouldCompressAndLoadBlocks(SstCompression compression)
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();
        var blockEncoder = new DefaultBlockEncoder(512);
        var value = Enumerable.Repeat((byte)0x5A, 400).ToArray();

        using (var builder = new BufferedSsTableBuilder(
            tempFilename,
            new DefaultSsTableEncoder(),
            blockEncoder,
            new DefaultBloomFilterFactory(),
            16,
            compression))
        {
            for (uint i = 0; i < 16; i++)
            {
                await builder.AddAsync(i, value);
            }

            using var table = await builder.BuildAsync();
            await Assert.That(table.BlockMetadata.All(x => x.Compression == compression)).IsTrue();
            await Assert.That(table.BlockMetadata.All(x => x.UncompressedLength > 0)).IsTrue();
        }

        using var blockBuilder = new BlockBuilder(new DefaultBlockEncoder(512));
        using var loaded = await SsTable.LoadSsTableAsync(
            tempFilename,
            new DefaultSsTableEncoder(),
            blockBuilder,
            new DefaultBloomFilterFactory());

        await Assert.That(loaded.BlockMetadata.All(x => x.Compression == compression)).IsTrue();
        var entries = new SsTableIterator(loaded).EnumerateAsync().ToBlockingEnumerable().ToArray();
        await Assert.That(entries.Length).IsEqualTo(16);
        await Assert.That(entries.All(x => x.Value.Span.SequenceEqual(value))).IsTrue();
    }

    [Test]
    [Arguments(SstCompression.Lz4)]
    [Arguments(SstCompression.Zstandard)]
    public async Task ShouldStoreBlocksRawWhenSavingsAreInsufficient(SstCompression compression)
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();
        var value = new byte[400];
        new Random(42).NextBytes(value);

        using var builder = new BufferedSsTableBuilder(
            tempFilename,
            new DefaultSsTableEncoder(),
            new DefaultBlockEncoder(512),
            new DefaultBloomFilterFactory(),
            1,
            compression,
            minimumCompressionSavingsPercent: 99);

        await builder.AddAsync(1u, value);
        using var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();
        await Assert.That(table.BlockMetadata[0].Compression).IsEqualTo(SstCompression.None);
        using var block = await table.ReadBlockAsync(0);
        var found = block!.TryGetValue(ByteSliceTestExtensions.Slice(1u), out var stored);
        var valueMatches = stored.SequenceEqual(value);
        await Assert.That(found).IsTrue();
        await Assert.That(valueMatches).IsTrue();
    }

    [Test]
    public async Task ShouldLoadLegacyUncompressedTable()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using (var builder = new BufferedSsTableBuilder(
            tempFilename,
            new DefaultSsTableEncoder(),
            new DefaultBlockEncoder(),
            new DefaultBloomFilterFactory(),
            1,
            formatVersion: SsTableFormat.LegacyVersion))
        {
            await builder.AddAsync(7, "legacy");
            using var table = await builder.BuildAsync();
        }

        using var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());
        using var loaded = await SsTable.LoadSsTableAsync(
            tempFilename,
            new DefaultSsTableEncoder(),
            blockBuilder,
            new DefaultBloomFilterFactory());

        await Assert.That(loaded.BlockMetadata[0].UncompressedLength).IsEqualTo(0);
        using var block = await loaded.ReadBlockAsync(0);
        await Assert.That(block!.Memory.Length > 0).IsTrue();
    }

    [Test]
    [Arguments(SstCompression.Lz4)]
    [Arguments(SstCompression.Zstandard)]
    public async Task ShouldRejectCorruptedCompressedBlock(SstCompression compression)
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using (var builder = new BufferedSsTableBuilder(
            tempFilename,
            new DefaultSsTableEncoder(),
            new DefaultBlockEncoder(),
            new DefaultBloomFilterFactory(),
            1,
            compression))
        {
            await builder.AddAsync(1u, Enumerable.Repeat((byte)0x41, 1000).ToArray());
            using var table = await builder.BuildAsync();
            await Assert.That(table.BlockMetadata[0].Compression).IsEqualTo(compression);
        }

        var bytes = await File.ReadAllBytesAsync(tempFilename);
        bytes[0] ^= 0xFF;
        await File.WriteAllBytesAsync(tempFilename, bytes);

        using var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());
        using var loaded = await SsTable.LoadSsTableAsync(
            tempFilename,
            new DefaultSsTableEncoder(),
            blockBuilder,
            new DefaultBloomFilterFactory());

        await Assert.That(async () =>
        {
            using var block = await loaded.ReadBlockAsync(0);
        }).Throws<InvalidDataException>();
    }

    [Test]
    public async Task ShouldRejectInvalidBloomMetadata()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using (var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100))
        {
            await builder.AddAsync(7, "hello");
            var table = await builder.BuildAsync();
            table.Dispose();
        }

        var bytes = await File.ReadAllBytesAsync(tempFilename);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(bytes.Length - 16), uint.MaxValue);
        await File.WriteAllBytesAsync(tempFilename, bytes);

        using var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());
        await Assert.That(async () =>
            await SsTable.LoadSsTableAsync(tempFilename, new DefaultSsTableEncoder(), blockBuilder, new DefaultBloomFilterFactory()))
            .Throws<InvalidDataException>();
    }

    [Test]
    public async Task ShouldLoadDisabledBloomFilter()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();
        var bloomFilterFactory = new DisabledBloomFilterFactory();

        using (var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), bloomFilterFactory, 100))
        {
            await builder.AddAsync(7, "hello");
            var table = await builder.BuildAsync();
            table.Dispose();
        }

        using var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());
        var loaded = await SsTable.LoadSsTableAsync(tempFilename, new DefaultSsTableEncoder(), blockBuilder, bloomFilterFactory);

        await Assert.That(loaded.BloomFilter.K).IsEqualTo(0);
        await Assert.That(loaded.BloomFilter.Probe("anything"u8)).IsTrue();
        loaded.Dispose();
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

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < count; i++)
        {
            await builder.AddAsync(i, value);
        }

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata.Count > 0).IsTrue();

        var iterator = new SsTableIterator(table);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => DecodeUInt32(x.Key)).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(0, count).Select(i => (uint)i));

        table.Dispose();
    }

    [Test]
    public async Task ShouldIterateFromKey()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);

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

        var iterator = new SsTableIterator(table);

        var result = iterator.EnumerateAsync(ByteSliceTestExtensions.Slice((uint)13)).ToBlockingEnumerable().Select(x => DecodeUInt32(x.Key)).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(13, 100 - 13).Select(i => (uint)i));

        table.Dispose();
    }

    [Test]
    public async Task ShouldIterateFromUnknownKey()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);

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

        var iterator = new SsTableIterator(table);

        var result = iterator.EnumerateAsync(ByteSliceTestExtensions.Slice((uint)101)).ToBlockingEnumerable().Select(x => DecodeUInt32(x.Key)).ToArray();

        await Assert.That(result).IsEmpty();

        table.Dispose();
    }

    [Test]
    public async Task ShouldIterateFromKeyBeforeFirst()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);

        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        // Keys start at 10, so a 'from' of 5 precedes every block's first key.
        for (uint i = 10; i < 110; i++)
        {
            await builder.AddAsync(i, value);
        }

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata.Count > 1).IsTrue();

        var iterator = new SsTableIterator(table);

        var result = iterator.EnumerateAsync(ByteSliceTestExtensions.Slice((uint)5)).ToBlockingEnumerable().Select(x => DecodeUInt32(x.Key)).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(10, 100).Select(i => (uint)i));

        table.Dispose();
    }

    [Test]
    public async Task ShouldIterateAllEntriesBackwards()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();
        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);
        var value = new byte[100.B()];

        for (uint i = 0; i < 100; i++)
        {
            await builder.AddAsync(i, value);
        }

        using var table = await builder.BuildAsync();
        await Assert.That(table.BlockMetadata.Count > 1).IsTrue();

        var result = new SsTableIterator(table).EnumerateBackwardsAsync().ToBlockingEnumerable().Select(x => DecodeUInt32(x.Key)).ToArray();

        await Assert.That(result).IsEquivalentTo(Enumerable.Range(0, 100).Reverse().Select(i => (uint)i), CollectionOrdering.Matching);
    }

    [Test]
    [Arguments((uint)53, 54)]
    [Arguments((uint)101, 100)]
    [Arguments((uint)5, 6)]
    public async Task ShouldIterateBackwardsFromKey(uint from, int expectedCount)
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();
        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);
        var value = new byte[100.B()];

        for (uint i = 0; i < 100; i++)
        {
            await builder.AddAsync(i, value);
        }

        using var table = await builder.BuildAsync();
        var result = new SsTableIterator(table).EnumerateBackwardsAsync(from).ToBlockingEnumerable().Select(x => DecodeUInt32(x.Key)).ToArray();
        var expected = Enumerable.Range(0, expectedCount).Reverse().Select(i => (uint)i);

        await Assert.That(result).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    [Test]
    public async Task ShouldCacheBlocks()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);
        using var blockCache = new BlockCache(1.MiB());
        ushort key = 7;
        string value = "hello";

        await builder.AddAsync(key, value);

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();
        using var block1 = await table.ReadBlockCachedAsync(0, blockCache);
        using var block2 = await table.ReadBlockCachedAsync(0, blockCache);
        await Assert.That(block1.Block!.Memory).IsEquivalentTo(new byte[] { 2, 0, 7, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
        await Assert.That(block2.Block!.Memory).IsEquivalentTo(new byte[] { 2, 0, 7, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
        await Assert.That(block2.Block).IsSameReferenceAs(block1.Block);

        table.Dispose();
    }

    [Test]
    public async Task ShouldCacheBlocksConcurrently()
    {
        using var tempFolder = TempFolder.Create();
        var tempFilename = tempFolder.GetRandomFileName();

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);
        using var blockCache = new BlockCache(1.MiB());
        ushort key = 7;
        string value = "hello";

        await builder.AddAsync(key, value);

        var table = await builder.BuildAsync();

        await Assert.That(table.BlockMetadata).HasSingleItem();

        var blocks = new List<Task<BlockLease>>();

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
                await Assert.That(result2!.Memory).IsEquivalentTo(new byte[] { 2, 0, 7, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
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

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);
        using var blockCache = new BlockCache(1);

        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            await builder.AddAsync(i, value);
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

        using var builder = new BufferedSsTableBuilder(tempFilename, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), new DefaultBloomFilterFactory(), 100);
        using var blockCache = new BlockCache(0);

        await builder.AddAsync(7, "hello");
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
        var entries = Enumerable.Range(0, 100).Select(x => new KeyValuePair<int, int>(x, x)).ToList();
        using var tempFolder = TempFolder.Create();
        var table = await CreateAndLoadSsTableAsync(entries, tempFolder);

        var bloomFilter = table.BloomFilter;

        var bytes = new byte[sizeof(int)];

        // Actual entries must always probe true (a bloom filter never produces false negatives).
        foreach (var e in entries)
        {
            BinaryPrimitives.WriteUInt32BigEndian(bytes, (uint)e.Key ^ 0x8000_0000u);
            await Assert.That(bloomFilter.Probe(bytes)).IsTrue();
        }

        // Non-inserted keys should mostly probe false, within a tolerable false positive rate.
        var falsePositives = 0;
        var iterations = 1000;

        for (var i = entries.Count; i < iterations + entries.Count; i++)
        {
            BinaryPrimitives.WriteUInt32BigEndian(bytes, (uint)i ^ 0x8000_0000u);
            if (bloomFilter.Probe(bytes))
            {
                falsePositives++;
            }
        }

        // Assume 10% or better
        await Assert.That(falsePositives < iterations * 0.1).IsTrue();

        table.Dispose();
    }

    private async Task<SsTable> CreateAndLoadSsTableAsync(IReadOnlyList<KeyValuePair<int, int>> entries, TempFolder tempFolder)
    {
        var tempFilename = tempFolder.GetRandomFileName();
        var blockEncoder = new DefaultBlockEncoder();
        var blockBuilder = new BlockBuilder(blockEncoder);
        var ssTableEncoder = new DefaultSsTableEncoder();
        using var builder = new BufferedSsTableBuilder(tempFilename, ssTableEncoder, blockEncoder, new DefaultBloomFilterFactory(), entries.Count);

        foreach (var entry in entries)
        {
            await builder.AddAsync(entry.Key, entry.Value);
        }

        var table = await builder.BuildAsync();
        table.Dispose();

        return await SsTable.LoadSsTableAsync(tempFilename, ssTableEncoder, blockBuilder, new DefaultBloomFilterFactory());
    }

    private sealed class DisabledBloomFilterFactory : IBloomFilterFactory
    {
        public IBloomFilter CreateBloomFilter(int n, double p) => DisabledBloomFilter.Instance;

        public IBloomFilter CreateBloomFilter(ReadOnlySpan<byte> bytes, int k)
            => k == 0 ? DisabledBloomFilter.Instance : new BloomFilter(bytes.ToArray(), k);

        public IBloomFilter CreateBloomFilterFromOwnedBytes(byte[] bytes, int k, int algorithmVersion)
            => k == 0 ? DisabledBloomFilter.Instance : new BloomFilter(bytes, k, algorithmVersion);
    }

    private sealed class DisabledBloomFilter : IBloomFilter
    {
        public static DisabledBloomFilter Instance { get; } = new();

        public int K => 0;

        public void Add(ReadOnlySpan<byte> value)
        {
        }

        public bool Probe(ReadOnlySpan<byte> item) => true;

        public ReadOnlySpan<byte> GetBytes() => [];
    }
}
