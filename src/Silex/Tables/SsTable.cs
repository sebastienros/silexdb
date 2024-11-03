using Microsoft.Extensions.Caching.Memory;
using Silex.Blocks;
using Silex.BloomFilters;
using System.Buffers;
using System.Buffers.Binary;

namespace Silex.Tables;

public class SsTable<TKey, TValue> : IDisposable
{
    private static readonly WorkDispatcher<BlockCacheKey, Block<TKey, TValue>> _dispatcher = new();

    private readonly long _id;
    private readonly string _filename;
    private readonly TKey? _firstKey;
    private readonly TKey? _lastKey;
    private readonly BlockBuilder<TKey, TValue> _blockBuilder;
    private readonly FileStream _stream;
    private bool _disposed;

    public SsTable(long id, FileStream stream, string filename, IReadOnlyList<BlockMetadata<TKey>> blockMetadata, long metadataBlockOffset, BlockBuilder<TKey, TValue> blockBuilder, IBloomFilter bloomFilter)
    {
        _id = id;
        _filename = filename;
        _stream = stream;
        BlockMetadata = blockMetadata;
        MetaBlockOffset = metadataBlockOffset;
        _blockBuilder = blockBuilder;
        BloomFilter = bloomFilter;
        if (blockMetadata.Count > 0)
        {
            _firstKey = BlockMetadata[0].FirstKey;
            _lastKey = BlockMetadata[BlockMetadata.Count - 1].LastKey;
        }
    }

    public IReadOnlyList<BlockMetadata<TKey>> BlockMetadata { get; } = [];

    public long MetaBlockOffset { get; }

    public IBloomFilter BloomFilter { get; }

    public string Filename => _filename;

    public TKey FirstKey => _firstKey!;

    public TKey LastKey => _lastKey!;

    public async Task<Block<TKey, TValue>?> ReadBlockAsync(int index, CancellationToken cancellationToken = default)
    {
        var offset = BlockMetadata[index].Offset;
        
        // If there is a single block it ends at the metadata block
        var offsetEnd = BlockMetadata.Count > index + 1
            ? BlockMetadata[index + 1].Offset
            : MetaBlockOffset;

        var length = (int)(offsetEnd - offset);

        var buffer = ArrayPool<byte>.Shared.Rent(length)!;

        _stream.Seek(offset, SeekOrigin.Begin);
        await _stream.ReadExactlyAsync(buffer, 0, length, cancellationToken);
        
        var block = _blockBuilder.Decode(new ReadOnlyMemory<byte>(buffer, 0, length));

        ArrayPool<byte>.Shared.Return(buffer);

        return block;
    }

    public Task<Block<TKey, TValue>?> ReadBlockCachedAsync(int index, IMemoryCache blockCache, MemoryCacheEntryOptions cacheEntryOptions, CancellationToken cancellationToken = default)
    {
        var key = new BlockCacheKey(_id, index);

        // Try without the dispatcher first since this is a cheap lookup
        if (blockCache.TryGetValue(key, out var block))
        {
            return Task.FromResult(block as Block<TKey, TValue>);
        }

        // Use a dispatcher to prevent cache stampede
        return _dispatcher.ScheduleAsync(key, (key) =>
        {
            return blockCache.GetOrCreateAsync(key, async entry =>
            {
                var block = await ReadBlockAsync(index, cancellationToken);

                if (block != null)
                {
                    entry.SetSize(block.Memory.Length);
                    entry.SetOptions(cacheEntryOptions);
                }

                return block;
            });
        });
    }

    public static async Task<SsTable<TKey, TValue>> LoadSsTableAsync(string filename, ISsTableEncoder<TKey, TValue> tableEncoder, BlockBuilder<TKey, TValue> blockBuilder, IBloomFilterFactory bloomFilterFactory, CancellationToken cancellationToken = default)
    {
        byte[] uintBuffer = ArrayPool<byte>.Shared.Rent(sizeof(uint));

        var stream = File.OpenRead(filename);

        // Read the bloom filter k in the previous 8 - 4 digits
        stream.Seek(stream.Length - 8, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(uintBuffer, 0, 4, cancellationToken);
        var bloomFilterK = (int)BinaryPrimitives.ReadUInt32LittleEndian(uintBuffer);

        // Read the bloom filter offset in the last four bytes
        await stream.ReadExactlyAsync(uintBuffer, 0, 4, cancellationToken);
        var bloomFilterOffset = BinaryPrimitives.ReadUInt32LittleEndian(uintBuffer);

        // Read the bloom filter content
        var bloomContentLength = 4;
        var bloomKLength = 4;
        var bloomFilterLength = stream.Length - bloomContentLength - bloomKLength - bloomFilterOffset;

        var buffer = ArrayPool<byte>.Shared.Rent((int)bloomFilterLength);
        stream.Seek(bloomFilterOffset, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(buffer, 0, (int)bloomFilterLength, cancellationToken);
        var bloomFilter = bloomFilterFactory.CreateBloomFilter(buffer.AsSpan().Slice(0, (int)bloomFilterLength), bloomFilterK);
        ArrayPool<byte>.Shared.Return(buffer);

        // Read the metadata block offset in the last four bytes before the bloom filter
        stream.Seek(bloomFilterOffset - 4, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(uintBuffer, 0, 4, cancellationToken);
        var metaBlockOffset = BinaryPrimitives.ReadUInt32LittleEndian(uintBuffer);

        // Read the metadata block content
        var metadataLength = bloomFilterOffset - 4 - metaBlockOffset;

        buffer = ArrayPool<byte>.Shared.Rent((int)metadataLength);
        stream.Seek(metaBlockOffset, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(buffer, 0, (int)metadataLength, cancellationToken);
        var blockMetadata = tableEncoder.DecodeMetadata(buffer, 0);
        ArrayPool<byte>.Shared.Return(buffer);

        ArrayPool<byte>.Shared.Return(uintBuffer);

        var table = new SsTable<TKey, TValue>(IdGenerator.GetNextId(), stream, filename, blockMetadata, metaBlockOffset, blockBuilder, bloomFilter);

        return table;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        DisposeInternal();
    }

    private void DisposeInternal()
    {
        if (_disposed)
        {
            return;
        }

        _stream.Dispose();
        _disposed = true;
    }

    ~SsTable()
    {
        DisposeInternal();
    }

    private record struct BlockCacheKey(long Id, int Index);
}
