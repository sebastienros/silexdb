using Silex.Blocks;
using Silex.BloomFilters;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
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
        var metadata = BlockMetadata[index];
        var (offset, length) = GetBlockExtent(index);

        if (metadata.Compression != SstCompression.None)
        {
            var compressed = ArrayPool<byte>.Shared.Rent(length);
            IMemoryOwner<byte>? uncompressedOwner = null;

            try
            {
                await ReadExactlyAsync(compressed.AsMemory(0, length), offset, cancellationToken);
                VerifyChecksum(metadata, compressed.AsSpan(0, length));
                uncompressedOwner = MemoryPool<byte>.Shared.Rent(metadata.UncompressedLength);
                BlockDecompressor.Decompress(
                    metadata.Compression,
                    compressed.AsSpan(0, length),
                    uncompressedOwner.Memory.Span[..metadata.UncompressedLength]);

                var block = _blockBuilder.Decode(uncompressedOwner, metadata.UncompressedLength);
                uncompressedOwner = null;
                return block;
            }
            finally
            {
                uncompressedOwner?.Dispose();
                ArrayPool<byte>.Shared.Return(compressed);
            }
        }

        // Read straight into the buffer that will back the decoded block, so the block bytes are never
        // copied a second time. The owner is handed to the block on success and disposed otherwise.
        var owner = MemoryPool<byte>.Shared.Rent(length);

        try
        {
            await ReadExactlyAsync(owner.Memory[..length], offset, cancellationToken);
            VerifyChecksum(metadata, owner.Memory.Span[..length]);

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
        var metadata = BlockMetadata[index];
        var (offset, length) = GetBlockExtent(index);

        if (metadata.Compression != SstCompression.None)
        {
            var compressed = ArrayPool<byte>.Shared.Rent(length);
            IMemoryOwner<byte>? uncompressedOwner = null;

            try
            {
                ReadExactly(compressed.AsSpan(0, length), offset);
                VerifyChecksum(metadata, compressed.AsSpan(0, length));
                uncompressedOwner = MemoryPool<byte>.Shared.Rent(metadata.UncompressedLength);
                BlockDecompressor.Decompress(
                    metadata.Compression,
                    compressed.AsSpan(0, length),
                    uncompressedOwner.Memory.Span[..metadata.UncompressedLength]);

                var block = _blockBuilder.Decode(uncompressedOwner, metadata.UncompressedLength);
                uncompressedOwner = null;
                return block;
            }
            finally
            {
                uncompressedOwner?.Dispose();
                ArrayPool<byte>.Shared.Return(compressed);
            }
        }

        var owner = MemoryPool<byte>.Shared.Rent(length);

        try
        {
            ReadExactly(owner.Memory.Span[..length], offset);
            VerifyChecksum(metadata, owner.Memory.Span[..length]);

            var block = _blockBuilder.Decode(owner, length);
            owner = null;
            return block;
        }
        finally
        {
            owner?.Dispose();
        }
    }

    private async ValueTask ReadExactlyAsync(Memory<byte> destination, long offset, CancellationToken cancellationToken)
    {
        var read = 0;
        while (read < destination.Length)
        {
            var count = await RandomAccess.ReadAsync(_handle, destination[read..], offset + read, cancellationToken);
            if (count == 0)
            {
                throw new EndOfStreamException("The SST ended while reading a data block.");
            }

            read += count;
        }
    }

    private void ReadExactly(Span<byte> destination, long offset)
    {
        var read = 0;
        while (read < destination.Length)
        {
            var count = RandomAccess.Read(_handle, destination[read..], offset + read);
            if (count == 0)
            {
                throw new EndOfStreamException("The SST ended while reading a data block.");
            }

            read += count;
        }
    }

    private static void VerifyChecksum(BlockMetadata metadata, ReadOnlySpan<byte> storedBlock)
    {
        if (metadata.UncompressedLength != 0
            && XxHash32.HashToUInt32(storedBlock) != metadata.Checksum)
        {
            throw new InvalidDataException("The SST data block checksum does not match its contents.");
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
        byte[] footerBuffer = ArrayPool<byte>.Shared.Rent(SsTableFormat.FooterLength);
        var stream = File.OpenRead(filename);
        var success = false;

        try
        {
            var contentLength = stream.Length;
            var formatVersion = SsTableFormat.LegacyVersion;

            if (contentLength >= SsTableFormat.FooterLength)
            {
                stream.Seek(-SsTableFormat.FooterLength, SeekOrigin.End);
                await stream.ReadExactlyAsync(footerBuffer.AsMemory(0, SsTableFormat.FooterLength), cancellationToken);
                formatVersion = SsTableFormat.TryReadVersion(footerBuffer.AsSpan(0, SsTableFormat.FooterLength));
                if (formatVersion != SsTableFormat.LegacyVersion)
                {
                    contentLength -= SsTableFormat.FooterLength;
                }
            }

            if (contentLength < BloomFilterPersistence.LegacyFooterLength)
            {
                throw new InvalidDataException("The SST is too short to contain a Bloom filter footer.");
            }

            stream.Seek(contentLength - BloomFilterPersistence.LegacyFooterLength, SeekOrigin.Begin);
            await stream.ReadExactlyAsync(footerBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
            var serializedK = BinaryPrimitives.ReadUInt32LittleEndian(footerBuffer);
            await stream.ReadExactlyAsync(footerBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
            var bloomFilterOffset = BinaryPrimitives.ReadUInt32LittleEndian(footerBuffer);

            var footerLength = BloomFilterPersistence.LegacyFooterLength;
            var algorithmVersion = 0;

            if (serializedK == BloomFilterPersistence.VersionedSentinel
                && bloomFilterOffset != contentLength - BloomFilterPersistence.LegacyFooterLength
                && contentLength >= BloomFilterPersistence.VersionedFooterLength)
            {
                stream.Seek(contentLength - 3 * sizeof(uint), SeekOrigin.Begin);
                await stream.ReadExactlyAsync(footerBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
                var marker = BinaryPrimitives.ReadUInt32LittleEndian(footerBuffer);

                if (BloomFilterPersistence.TryDecodeMarker(marker, out algorithmVersion))
                {
                    stream.Seek(contentLength - BloomFilterPersistence.VersionedFooterLength, SeekOrigin.Begin);
                    await stream.ReadExactlyAsync(footerBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
                    serializedK = BinaryPrimitives.ReadUInt32LittleEndian(footerBuffer);
                    footerLength = BloomFilterPersistence.VersionedFooterLength;
                }
            }

            if (serializedK > int.MaxValue)
            {
                throw new InvalidDataException("The SST contains an invalid Bloom filter hash count.");
            }

            var contentEnd = contentLength - footerLength;
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
            await stream.ReadExactlyAsync(footerBuffer.AsMemory(0, sizeof(uint)), cancellationToken);
            var metaBlockOffset = BinaryPrimitives.ReadUInt32LittleEndian(footerBuffer);
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
                blockMetadata = tableEncoder.DecodeMetadata(buffer.AsMemory(0, (int)metadataLength), 0, formatVersion);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            ValidateBlockMetadata(blockMetadata, metaBlockOffset, formatVersion);

            success = true;
            return new SsTable(id ?? IdGenerator.GetNextId(), stream, filename, blockMetadata, metaBlockOffset, blockBuilder, bloomFilter);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(footerBuffer);
            if (!success)
            {
                stream.Dispose();
            }
        }
    }

    private static void ValidateBlockMetadata(IReadOnlyList<BlockMetadata> metadata, long metadataOffset, int formatVersion)
    {
        if (metadata.Count == 0)
        {
            throw new InvalidDataException("The SST does not contain any data blocks.");
        }

        for (var i = 0; i < metadata.Count; i++)
        {
            var block = metadata[i];
            var end = i + 1 < metadata.Count ? metadata[i + 1].Offset : metadataOffset;

            if (block.Offset < 0
                || block.Offset >= end
                || end > metadataOffset
                || (i == 0 && block.Offset != 0))
            {
                throw new InvalidDataException("The SST contains invalid block offsets.");
            }

            if (formatVersion >= 1)
            {
                var storedLength = end - block.Offset;
                if (block.UncompressedLength <= 0
                    || (block.Compression != SstCompression.None
                        && block.UncompressedLength > SsTableFormat.MaxCompressedBlockUncompressedLength)
                    || (block.Compression == SstCompression.None && block.UncompressedLength != storedLength))
                {
                    throw new InvalidDataException("The SST contains invalid block compression lengths.");
                }
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
