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

    /// <summary>
    /// The numeric id of this table. For tables loaded from disk this matches the on-disk filename id;
    /// for freshly built tables it is a generator id and may differ from the filename.
    /// </summary>
    public long Id => _id;

    /// <summary>
    /// The size of the backing SST file in bytes. Used by tiered compaction to measure the size of a
    /// sorted run (tier).
    /// </summary>
    public long Size => _stream.Length;

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

        // Use positioned reads (RandomAccess) rather than Seek + Read so that several readers can read
        // different blocks of the same SST concurrently without racing on the shared FileStream
        // position. The file is immutable once built, so reads never conflict with writes.
        var handle = _stream.SafeFileHandle;
        var read = 0;
        while (read < length)
        {
            var n = await RandomAccess.ReadAsync(handle, buffer.AsMemory(read, length - read), offset + read, cancellationToken);
            if (n == 0)
            {
                break;
            }

            read += n;
        }

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
                    // Apply the shared options first, then the size, otherwise SetOptions would
                    // overwrite the size with the (unset) value from the shared options and the
                    // cache would reject the entry when a SizeLimit is configured.
                    entry.SetOptions(cacheEntryOptions);
                    entry.SetSize(block.Memory.Length);
                }

                return block;
            });
        });
    }

    public static async Task<SsTable<TKey, TValue>> LoadSsTableAsync(string filename, ISsTableEncoder<TKey, TValue> tableEncoder, BlockBuilder<TKey, TValue> blockBuilder, IBloomFilterFactory bloomFilterFactory, long? id = null, CancellationToken cancellationToken = default)
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

        var bloomFilterBytes = new byte[(int)bloomFilterLength];
        stream.Seek(bloomFilterOffset, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(bloomFilterBytes, 0, (int)bloomFilterLength, cancellationToken);
        var bloomFilter = bloomFilterFactory.CreateBloomFilterFromOwnedBytes(bloomFilterBytes, bloomFilterK);

        // Read the metadata block offset in the last four bytes before the bloom filter
        stream.Seek(bloomFilterOffset - 4, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(uintBuffer, 0, 4, cancellationToken);
        var metaBlockOffset = BinaryPrimitives.ReadUInt32LittleEndian(uintBuffer);

        // Read the metadata block content
        var metadataLength = bloomFilterOffset - 4 - metaBlockOffset;

        var buffer = ArrayPool<byte>.Shared.Rent((int)metadataLength);
        stream.Seek(metaBlockOffset, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(buffer, 0, (int)metadataLength, cancellationToken);
        var blockMetadata = tableEncoder.DecodeMetadata(buffer, 0);
        ArrayPool<byte>.Shared.Return(buffer);

        ArrayPool<byte>.Shared.Return(uintBuffer);

        var table = new SsTable<TKey, TValue>(id ?? IdGenerator.GetNextId(), stream, filename, blockMetadata, metaBlockOffset, blockBuilder, bloomFilter);

        return table;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);
        DisposeInternal();

        _disposed = true;
    }

    private void DisposeInternal()
    {
        _stream.Dispose();
        _blockBuilder.Dispose();
    }

    ~SsTable()
    {
        DisposeInternal();
    }

    private record struct BlockCacheKey(long Id, int Index);
}
