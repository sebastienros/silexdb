using Silex.Blocks;
using Silex.BloomFilters;
using System.Buffers;
using System.Buffers.Binary;
using Microsoft.Win32.SafeHandles;

namespace Silex.Tables;

internal sealed class SsTable : IDisposable
{
    private readonly long _id;
    private readonly string _filename;
    private readonly ByteSlice? _firstKey;
    private readonly ByteSlice? _lastKey;
    private readonly BlockBuilder _blockBuilder;
    private readonly FileStream _stream;
    private readonly SafeFileHandle _handle;
    private bool _disposed;

    public SsTable(long id, FileStream stream, string filename, IReadOnlyList<BlockMetadata> blockMetadata, long metadataBlockOffset, BlockBuilder blockBuilder, IBloomFilter bloomFilter)
    {
        _id = id;
        _filename = filename;
        _stream = stream;
        _handle = stream.SafeFileHandle;
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

    public IReadOnlyList<BlockMetadata> BlockMetadata { get; } = [];

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

    public ByteSlice FirstKey => _firstKey!;

    public ByteSlice LastKey => _lastKey!;

    public async Task<Block?> ReadBlockAsync(int index, CancellationToken cancellationToken = default)
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
            var read = 0;
            while (read < length)
            {
                var n = await RandomAccess.ReadAsync(_handle, owner.Memory.Slice(read, length - read), offset + read, cancellationToken);
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
    public Block? ReadBlock(int index)
    {
        var (offset, length) = GetBlockExtent(index);

        var owner = MemoryPool<byte>.Shared.Rent(length);

        try
        {
            var read = 0;
            while (read < length)
            {
                var n = RandomAccess.Read(_handle, owner.Memory.Span.Slice(read, length - read), offset + read);
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

    internal ValueTask<BlockLease> ReadBlockCachedAsync(int index, BlockCache blockCache, CancellationToken cancellationToken = default)
    {
        return blockCache.GetOrLoadAsync(
            new BlockCacheKey(_id, index),
            new BlockLoader(this, index),
            cancellationToken);
    }

    /// <summary>
    /// Struct loader passed to <see cref="BlockCache{ByteSlice, ByteSlice}.GetOrLoadAsync"/> so the cache can populate a
    /// miss without allocating a closure on every read (including cache hits, which never invoke it). The miss
    /// reads the block synchronously on the calling thread to avoid a per-miss thread-pool dispatch.
    /// </summary>
    private readonly struct BlockLoader : IBlockLoader
    {
        private readonly SsTable _table;
        private readonly int _index;

        public BlockLoader(SsTable table, int index)
        {
            _table = table;
            _index = index;
        }

        public Block? Load(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return _table.ReadBlock(_index);
        }
    }

    public static async Task<SsTable> LoadSsTableAsync(string filename, ISsTableEncoder tableEncoder, BlockBuilder blockBuilder, IBloomFilterFactory bloomFilterFactory, long? id = null, CancellationToken cancellationToken = default)
    {
        byte[] uintBuffer = ArrayPool<byte>.Shared.Rent(sizeof(uint));
        var stream = File.OpenRead(filename);
        var success = false;

        try
        {
            if (stream.Length < BloomFilterPersistence.LegacyFooterLength)
            {
                throw new InvalidDataException("The SST is too short to contain a Bloom filter footer.");
            }

            stream.Seek(-BloomFilterPersistence.LegacyFooterLength, SeekOrigin.End);
            await stream.ReadExactlyAsync(uintBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
            var serializedK = BinaryPrimitives.ReadUInt32LittleEndian(uintBuffer);
            await stream.ReadExactlyAsync(uintBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
            var bloomFilterOffset = BinaryPrimitives.ReadUInt32LittleEndian(uintBuffer);

            var footerLength = BloomFilterPersistence.LegacyFooterLength;
            var algorithmVersion = 0;

            if (serializedK == BloomFilterPersistence.VersionedSentinel
                && bloomFilterOffset != stream.Length - BloomFilterPersistence.LegacyFooterLength
                && stream.Length >= BloomFilterPersistence.VersionedFooterLength)
            {
                stream.Seek(-3 * sizeof(uint), SeekOrigin.End);
                await stream.ReadExactlyAsync(uintBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
                var marker = BinaryPrimitives.ReadUInt32LittleEndian(uintBuffer);

                if (BloomFilterPersistence.TryDecodeMarker(marker, out algorithmVersion))
                {
                    stream.Seek(-BloomFilterPersistence.VersionedFooterLength, SeekOrigin.End);
                    await stream.ReadExactlyAsync(uintBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
                    serializedK = BinaryPrimitives.ReadUInt32LittleEndian(uintBuffer);
                    footerLength = BloomFilterPersistence.VersionedFooterLength;
                }
            }

            if (serializedK > int.MaxValue)
            {
                throw new InvalidDataException("The SST contains an invalid Bloom filter hash count.");
            }

            var contentEnd = stream.Length - footerLength;
            if (bloomFilterOffset < sizeof(uint) || bloomFilterOffset > contentEnd)
            {
                throw new InvalidDataException("The SST contains an invalid Bloom filter offset.");
            }

            var bloomFilterLength = contentEnd - bloomFilterOffset;
            if (bloomFilterLength is < 0 or > int.MaxValue
                || (bloomFilterLength == 0 && (serializedK != 0 || algorithmVersion != 0)))
            {
                throw new InvalidDataException("The SST contains an invalid Bloom filter length.");
            }

            var bloomFilterBytes = new byte[(int)bloomFilterLength];
            stream.Seek(bloomFilterOffset, SeekOrigin.Begin);
            await stream.ReadExactlyAsync(bloomFilterBytes, cancellationToken);

            IBloomFilter bloomFilter;
            try
            {
                bloomFilter = bloomFilterFactory.CreateBloomFilterFromOwnedBytes(bloomFilterBytes, (int)serializedK, algorithmVersion);
            }
            catch (ArgumentException exception)
            {
                throw new InvalidDataException("The SST contains invalid Bloom filter metadata.", exception);
            }

            stream.Seek(bloomFilterOffset - sizeof(uint), SeekOrigin.Begin);
            await stream.ReadExactlyAsync(uintBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
            var metaBlockOffset = BinaryPrimitives.ReadUInt32LittleEndian(uintBuffer);
            var metadataLength = (long)bloomFilterOffset - sizeof(uint) - metaBlockOffset;

            if (metadataLength is <= 0 or > int.MaxValue)
            {
                throw new InvalidDataException("The SST contains an invalid metadata block.");
            }

            var buffer = ArrayPool<byte>.Shared.Rent((int)metadataLength);
            IReadOnlyList<BlockMetadata> blockMetadata;
            try
            {
                stream.Seek(metaBlockOffset, SeekOrigin.Begin);
                await stream.ReadExactlyAsync(buffer.AsMemory(0, (int)metadataLength), cancellationToken);
                blockMetadata = tableEncoder.DecodeMetadata(buffer.AsMemory(0, (int)metadataLength), 0);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            success = true;
            return new SsTable(id ?? IdGenerator.GetNextId(), stream, filename, blockMetadata, metaBlockOffset, blockBuilder, bloomFilter);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(uintBuffer);
            if (!success)
            {
                stream.Dispose();
            }
        }
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
        foreach (var metadata in BlockMetadata)
        {
            metadata.Dispose();
        }
    }

    ~SsTable()
    {
        DisposeInternal();
    }
}
