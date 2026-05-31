using Silex.Blocks;
using Silex.BloomFilters;
using System.Buffers;
using System.Buffers.Binary;

namespace Silex.Tables;

public class SsTable<TKey> : IDisposable
{
    private readonly long _id;
    private readonly string _filename;
    private readonly TKey? _firstKey;
    private readonly TKey? _lastKey;
    private readonly BlockBuilder<TKey> _blockBuilder;
    private readonly FileStream _stream;
    private bool _disposed;

    public SsTable(long id, FileStream stream, string filename, IReadOnlyList<BlockMetadata<TKey>> blockMetadata, long metadataBlockOffset, BlockBuilder<TKey> blockBuilder, IBloomFilter bloomFilter)
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

    public async Task<Block<TKey>?> ReadBlockAsync(int index, CancellationToken cancellationToken = default)
    {
        var (offset, length) = GetBlockExtent(index);

        // Read straight into the buffer that will back the decoded block, so the block bytes are never
        // copied a second time. The owner is handed to the block on success and disposed otherwise.
        var owner = MemoryPool<byte>.Shared.Rent(length);

        try
        {
            // Use positioned reads (RandomAccess) rather than Seek + Read so that several readers can read
            // different blocks of the same SST concurrently without racing on the shared FileStream
            // position. The file is immutable once built, so reads never conflict with writes.
            var handle = _stream.SafeFileHandle;
            var read = 0;
            while (read < length)
            {
                var n = await RandomAccess.ReadAsync(handle, owner.Memory.Slice(read, length - read), offset + read, cancellationToken);
                if (n == 0)
                {
                    break;
                }

                read += n;
            }

            var block = _blockBuilder.Decode(owner, length);
            owner = null;
            return block;
        }
        finally
        {
            owner?.Dispose();
        }
    }

    /// <summary>
    /// Synchronous block read used by the cache-miss fast path. On Unix there is no kernel async I/O for
    /// regular files, so <see cref="RandomAccess.ReadAsync"/> dispatches every read to the thread pool; a
    /// synchronous positioned read instead runs the <c>pread</c> inline on the calling thread, removing the
    /// per-miss thread-pool round-trip. The file is immutable once built, so positioned reads never race.
    /// </summary>
    public Block<TKey>? ReadBlock(int index)
    {
        var (offset, length) = GetBlockExtent(index);

        var owner = MemoryPool<byte>.Shared.Rent(length);

        try
        {
            var handle = _stream.SafeFileHandle;
            var read = 0;
            while (read < length)
            {
                var n = RandomAccess.Read(handle, owner.Memory.Span.Slice(read, length - read), offset + read);
                if (n == 0)
                {
                    break;
                }

                read += n;
            }

            var block = _blockBuilder.Decode(owner, length);
            owner = null;
            return block;
        }
        finally
        {
            owner?.Dispose();
        }
    }

    private (long Offset, int Length) GetBlockExtent(int index)
    {
        var offset = BlockMetadata[index].Offset;

        // If there is a single block it ends at the metadata block
        var offsetEnd = BlockMetadata.Count > index + 1
            ? BlockMetadata[index + 1].Offset
            : MetaBlockOffset;

        return (offset, (int)(offsetEnd - offset));
    }

    internal ValueTask<BlockLease<TKey>> ReadBlockCachedAsync(int index, BlockCache<TKey> blockCache, CancellationToken cancellationToken = default)
    {
        return blockCache.GetOrLoadAsync(
            new BlockCacheKey(_id, index),
            new BlockLoader(this, index),
            cancellationToken);
    }

    /// <summary>
    /// Struct loader passed to <see cref="BlockCache{TKey}.GetOrLoadAsync"/> so the cache can populate a
    /// miss without allocating a closure on every read (including cache hits, which never invoke it). The miss
    /// reads the block synchronously on the calling thread to avoid a per-miss thread-pool dispatch.
    /// </summary>
    private readonly struct BlockLoader : IBlockLoader<TKey>
    {
        private readonly SsTable<TKey> _table;
        private readonly int _index;

        public BlockLoader(SsTable<TKey> table, int index)
        {
            _table = table;
            _index = index;
        }

        public Task<Block<TKey>?> LoadAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.FromResult(_table.ReadBlock(_index));
        }
    }

    public static async Task<SsTable<TKey>> LoadSsTableAsync(string filename, ISsTableEncoder<TKey> tableEncoder, BlockBuilder<TKey> blockBuilder, IBloomFilterFactory bloomFilterFactory, long? id = null, CancellationToken cancellationToken = default)
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

        var table = new SsTable<TKey>(id ?? IdGenerator.GetNextId(), stream, filename, blockMetadata, metaBlockOffset, blockBuilder, bloomFilter);

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
}
