using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Buffers;
using Silex.Ownership;
using Silex.Serialization;
using System.Buffers;
using System.IO;
using System.IO.Hashing;

namespace Silex.Tables;

/// <summary>
/// Uses are predefined sized buffer before flushing blocks to disk. Default buffer size is 32KiB.
/// </summary>
internal sealed class BufferedSsTableBuilderFactory : ISsTableBuilderFactory
{
    public ISsTableBuilder CreateSsTableBuilder(
        string path,
        ISsTableEncoder tableEncoder,
        IBlockEncoder blockEncoder,
        IBloomFilterFactory bloomFilterFactory,
        int count,
        SstCompression compression,
        int compressionLevel,
        double minimumCompressionSavingsPercent)
    {
        return new BufferedSsTableBuilder(
            path,
            tableEncoder,
            blockEncoder,
            bloomFilterFactory,
            count,
            compression,
            compressionLevel,
            minimumCompressionSavingsPercent);
    }
}

internal sealed class BufferedSsTableBuilder : ISsTableBuilder
{

    private readonly string _filename;
    private readonly string _tempFilename;
    private readonly ISsTableEncoder _tableEncoder;
    private long _offset;
    private bool _isFirstKey = true;
    [OwnedResource]
    private OwnedByteSlice? _firstKey = default;
    [OwnedResource]
    private OwnedByteSlice? _lastKey = default;
    private readonly IBloomFilter _bloomFilter;
    private readonly IBloomFilterFactory _bloomFilterFactory;
    private readonly int _entryEstimate;
    private IBloomFilter? _versionPrefixBloom;
    private readonly BlockBuilder _blockBuilder;
    private readonly BlockCompressor _blockCompressor;
    private readonly int _formatVersion;
    private List<BlockMetadata>? _metadata;
    private List<OwnedByteSlice>? _metadataKeyOwners;
    private BlockMetadataStore? _flattenedMetadata;
    private bool _disposed;
    private bool _ownsBuiltResources = true;
    private readonly FileStream _stream;

    private static readonly int _flushBufferSize = (int)32.KiB();
    private readonly PooledArrayBufferWriter<byte> _bufferWriter = new(_flushBufferSize);

    public BufferedSsTableBuilder(
        string filename,
        ISsTableEncoder tableEncoder,
        IBlockEncoder blockEncoder,
        IBloomFilterFactory bloomFilterFactory,
        int count,
        SstCompression compression = SstCompression.None,
        int compressionLevel = 0,
        double minimumCompressionSavingsPercent = 12.5,
        int formatVersion = SsTableFormat.CurrentVersion)
    {
        _filename = filename;
        // Write to a temporary file and atomically rename to the final ".sst" name on a successful
        // build. This guarantees a crash never leaves a partial ".sst" that recovery would try to load
        // (and, for a flush, mistake for a completed table and drop the still-needed WAL).
        _tempFilename = filename + ".tmp";
        _tableEncoder = tableEncoder;
        _bloomFilter = bloomFilterFactory.CreateBloomFilter(count, 0.01);
        _bloomFilterFactory = bloomFilterFactory;
        _entryEstimate = count;
        _blockBuilder = new BlockBuilder(blockEncoder);
        _blockCompressor = new BlockCompressor(compression, compressionLevel, minimumCompressionSavingsPercent);
        if (formatVersion is < SsTableFormat.LegacyVersion or > SsTableFormat.CurrentVersion
            || (formatVersion == SsTableFormat.LegacyVersion && compression != SstCompression.None))
        {
            throw new ArgumentOutOfRangeException(nameof(formatVersion), formatVersion, "The requested SST format version does not support these options.");
        }
        _formatVersion = formatVersion;

        // Disable dotnet buffer as we are handling it
        var bufferSize = 0;

        // No write-through: the flush/compaction commit synchronizes each new SST once before the manifest
        // publishes it, whereas write-through paid a device flush on every write (~10 ms per small SST on Windows).
        var options =
            FileOptions.Asynchronous // All operation are async
            ;

        _stream = File.Create(_tempFilename, bufferSize, options);
    }

    internal ValueTask RolloverBlockForRawAsync(CancellationToken cancellationToken)
    {
        return new ValueTask(FinishBlockAsync(cancellationToken));
    }

    internal bool TryAddRawFast(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone)
    {
        if (_isFirstKey)
        {
            _firstKey = OwnedByteSlice.CopyFrom(key);
            _isFirstKey = false;
        }

        if (_blockBuilder.AddRaw(key, value, isTombstone))
        {
            AddBloomKeys();
            return true;
        }

        return false;
    }

    public async Task AddAsync(ByteSlice key, ByteSlice value, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_isFirstKey)
        {
            _firstKey = OwnedByteSlice.CopyFrom(key.Span);
            _isFirstKey = false;
        }

        // Is the key added in the current block?
        if (_blockBuilder.Add(key, value))
        {
            // The key was encoded once by the block builder; reuse those exact bytes for the bloom filter.
            AddBloomKeys();
            return;
        }

        // No, then there was not enough space in the current block for the data, start a new one
        await FinishBlockAsync(cancellationToken);

        if (!_blockBuilder.Add(key, value))
        {
            // The block builder has to accept this entry since it's empty, even if
            // the size is over the block size
            throw new InvalidOperationException("The data was not successfully added to a block.");
        }

        AddBloomKeys();

        _firstKey = OwnedByteSlice.CopyFrom(key.Span);
    }

    public long EstimatedSize => _offset;

    private void AddBloomKeys()
    {
        var key = _blockBuilder.LastEncodedKey;
        _bloomFilter.Add(key);
        if (_formatVersion >= 2 && VersionPrefixBloom.IsVersionKey(key))
        {
            _versionPrefixBloom ??= _bloomFilterFactory.CreateBloomFilter(_entryEstimate, 0.01);
            _versionPrefixBloom.Add(key[..^sizeof(long)]);
        }
    }

    private async Task FinishBlockAsync(CancellationToken cancellationToken)
    {
        if (!_blockBuilder.HasEntries)
        {
            return;
        }

        // Release the block's memory as soon as we have copied its content.
        using var block = _blockBuilder.BuildBlock();

        // Write the SST content in memory as blocks are getting created.
        // Use a buffer writer as it will handle the growth of the SST buffer automatically.

        // Flush before compression so no span over the source or reusable compression buffer crosses
        // the await. Compression never stores more bytes than the original block.
        if (_bufferWriter.FreeCapacity < block.Memory.Span.Length && _bufferWriter.WrittenCount > 0)
        {
            await FLushBufferToDiskAsync(cancellationToken);
        }

        var storedBlock = _blockCompressor.Compress(block.Memory.Span);

        _metadata ??= [];
        _metadataKeyOwners ??= [];
        var firstKey = _firstKey!;
        var lastKey = _lastKey ?? OwnedByteSlice.CopyFrom(_blockBuilder.LastEncodedKey);
        _metadata.Add(new BlockMetadata(
            _metadata.Count,
            _offset,
            storedBlock.UncompressedLength,
            storedBlock.Compression,
            XxHash32.HashToUInt32(storedBlock.Data),
            firstKey.Memory,
            lastKey.Memory));
        _metadataKeyOwners.Add(firstKey);
        _metadataKeyOwners.Add(lastKey);

        // If the buffer is too small, it will grow automatically, and this new size will be kept
        // as the new threshold until the end of the SST
        _bufferWriter.Write(storedBlock.Data);

        _offset += storedBlock.Data.Length;

        _firstKey = default;
        _lastKey = default;
        _isFirstKey = true;
        _blockBuilder.Clear();

        return;
    }

    private void ReplaceLastKey(ByteSlice key)
    {
        _lastKey?.Dispose();
        _lastKey = OwnedByteSlice.CopyFrom(key.Span);
    }

    /// <remarks>
    /// The returned SST has an open file handle. The result must be disposed to close the handle.
    /// </summary>
    public async Task<SsTable> BuildAsync(CancellationToken cancellationToken = default)
    {
        await FinishBlockAsync(cancellationToken);

        if (_metadata == null || _bufferWriter == null)
        {
            throw new InvalidOperationException("Nothing to store in SsTable.");
        }

        _flattenedMetadata = BlockMetadataStore.Create(_metadata);

        // Flush the blocks data before we use the buffer for metadata and bloom filter
        if (_bufferWriter.WrittenCount > 0)
        {
            await FLushBufferToDiskAsync(cancellationToken);
        }

        var metadataOffset = _offset;
        var binaryWriter = new EncoderBinaryWriter(_bufferWriter);
        _tableEncoder.EncodeMetadata(ref binaryWriter, _metadata, metadataOffset, _formatVersion);

        var bloomFilterOffset = _offset + binaryWriter.BytesWritten;

        WriteBloomFilter(ref binaryWriter, bloomFilterOffset);
        if (_versionPrefixBloom is not null)
        {
            var prefixOffset = _offset + binaryWriter.BytesWritten;
            var bytes = _versionPrefixBloom.GetBytes();
            Span<byte> descriptor = stackalloc byte[16];
            VersionPrefixBloom.WriteDescriptor(descriptor, _versionPrefixBloom.K, _versionPrefixBloom.AlgorithmVersion, prefixOffset);
            binaryWriter.WriteRaw(bytes);
            binaryWriter.WriteRaw(descriptor);
            binaryWriter.WriteUInt32(VersionPrefixBloom.Checksum(bytes, descriptor));
            binaryWriter.WriteUInt32(VersionPrefixBloom.Magic);
        }
        if (_formatVersion >= 1)
        {
            SsTableFormat.WriteFooter(ref binaryWriter, _versionPrefixBloom is null ? 1 : _formatVersion);
        }

        binaryWriter.Flush();

        await FLushBufferToDiskAsync(cancellationToken);
        await _stream.DisposeAsync();

        // The temporary file is now complete and closed; publish it under the final name atomically so
        // readers and recovery only ever see a fully written SST.
        File.Move(_tempFilename, _filename, overwrite: true);

        var flattenedMetadata = _flattenedMetadata!;
        var readStream = SsTable.OpenRead(_filename);
        SsTable table;

        try
        {
            table = new SsTable(IdGenerator.GetNextId(), readStream, _filename, flattenedMetadata, metadataOffset, _blockBuilder, _bloomFilter);
            table.VersionPrefixBloomFilter = _versionPrefixBloom;
        }
        catch
        {
            readStream.Dispose();
            throw;
        }

        // Ownership of the block builder and packed metadata transfers to the SsTable.
        _flattenedMetadata = null;
        _metadata.Clear();
        _metadata = null;
        DisposeMetadataKeyOwners();
        _ownsBuiltResources = false;

        return table;
    }

    private async Task FLushBufferToDiskAsync(CancellationToken cancellationToken = default)
    {
        await _stream.WriteAsync(_bufferWriter.WrittenMemory, cancellationToken);
        await _stream.FlushAsync(cancellationToken);
        _bufferWriter.Clear();
    }

    private void WriteBloomFilter(ref EncoderBinaryWriter writer, long bloomFilterOffset)
    {
        writer.WriteRaw(_bloomFilter.GetBytes());

        if (_bloomFilter.AlgorithmVersion > 0)
        {
            writer.WriteUInt32((uint)_bloomFilter.K);
            writer.WriteUInt32(BloomFilterPersistence.EncodeMarker(_bloomFilter.AlgorithmVersion));
            writer.WriteUInt32(BloomFilterPersistence.VersionedSentinel);
        }
        else
        {
            writer.WriteUInt32((uint)_bloomFilter.K);
        }

        writer.WriteUInt32((uint)bloomFilterOffset);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        DisposeInternal();
        GC.SuppressFinalize(this);

        _disposed = true;
    }

    private void DisposeInternal()
    {
        // Defensive null checks: the constructor can fail (for example File.Create throwing when the
        // directory was removed), leaving a partially constructed instance. The underlying FileStream
        // releases its OS handle through its own finalizer, so no native resource leaks even then.
        _stream?.Dispose();
        _bufferWriter?.Dispose();
        _blockCompressor?.Dispose();
        _firstKey?.Dispose();
        _lastKey?.Dispose();
        _firstKey = null;
        _lastKey = null;

        // After a successful build, the block builder and packed metadata are owned by the SsTable.
        if (_ownsBuiltResources)
        {
            _blockBuilder?.Dispose();
            _flattenedMetadata?.Dispose();
            _flattenedMetadata = null;
            DisposeMetadataKeyOwners();
            _metadata?.Clear();
            _metadata = null;

            // The build never completed (otherwise the temp file was renamed to the final name), so
            // remove the abandoned partial temp file rather than leaving it behind.
            TryDeleteTempFile();
        }
    }

    private void DisposeMetadataKeyOwners()
    {
        if (_metadataKeyOwners is null)
        {
            return;
        }

        foreach (var owner in _metadataKeyOwners)
        {
            owner.Dispose();
        }

        _metadataKeyOwners.Clear();
        _metadataKeyOwners = null;
    }

    private void TryDeleteTempFile()
    {
        try
        {
            File.Delete(_tempFilename);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}
