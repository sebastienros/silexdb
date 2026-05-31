using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Buffers;
using Silex.Serialization;
using System.Buffers;
using System.IO;

namespace Silex.Tables;

/// <summary>
/// Uses are predefined sized buffer before flushing blocks to disk. Default buffer size is 32KiB.
/// </summary>
public sealed class BufferedSsTableBuilderFactory : ISsTableBuilderFactory
{
    public ISsTableBuilder<TKey, TValue> CreateSsTableBuilder<TKey, TValue>(string path, ISsTableEncoder<TKey, TValue> tableEncoder, IBlockEncoder<TKey, TValue> blockEncoder, IBloomFilterFactory bloomFilterFactory, int count)
    {
        return new BufferedSsTableBuilder<TKey, TValue>(path, tableEncoder, blockEncoder, bloomFilterFactory, count);
    }
}

internal sealed class BufferedSsTableBuilder<TKey, TValue> : ISsTableBuilder<TKey, TValue>
{

    private readonly string _filename;
    private readonly string _tempFilename;
    private readonly ISsTableEncoder<TKey, TValue> _tableEncoder;
    private long _offset;
    private bool _isFirstKey = true;
    private TKey? _firstKey = default;
    private TKey? _lastKey = default;
    private readonly IBloomFilter _bloomFilter;
    private readonly BlockBuilder<TKey, TValue> _blockBuilder;
    private List<BlockMetadata<TKey>>? _metadata;
    private bool _disposed;
    private bool _ownsBuiltResources = true;
    private readonly FileStream _stream;

    private static readonly int _flushBufferSize = (int)32.KiB();
    private readonly PooledArrayBufferWriter<byte> _bufferWriter = new(_flushBufferSize);

    public BufferedSsTableBuilder(string filename, ISsTableEncoder<TKey, TValue> tableEncoder, IBlockEncoder<TKey, TValue> blockEncoder, IBloomFilterFactory bloomFilterFactory, int count)
    {
        _filename = filename;
        // Write to a temporary file and atomically rename to the final ".sst" name on a successful
        // build. This guarantees a crash never leaves a partial ".sst" that recovery would try to load
        // (and, for a flush, mistake for a completed table and drop the still-needed WAL).
        _tempFilename = filename + ".tmp";
        _tableEncoder = tableEncoder;
        _bloomFilter = bloomFilterFactory.CreateBloomFilter(count, 0.01);
        _blockBuilder = new BlockBuilder<TKey, TValue>(blockEncoder);

        // Disable dotnet buffer as we are handling it
        var bufferSize = 0;

        var options =
            FileOptions.WriteThrough | // Skip OS cache
            FileOptions.Asynchronous // All operation are async
            ;

        _stream = File.Create(_tempFilename, bufferSize, options);
    }

    public async Task AddAsync(TKey key, TValue value, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_isFirstKey)
        {
            _firstKey = key;
            _isFirstKey = false;
        }

        // Is the key added in the current block?
        if (_blockBuilder.Add(key, value))
        {
            // The key was encoded once by the block builder; reuse those exact bytes for the bloom filter.
            _bloomFilter.Add(_blockBuilder.LastEncodedKey);
            _lastKey = key;
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

        _bloomFilter.Add(_blockBuilder.LastEncodedKey);

        _firstKey = key;
        _lastKey = key;
    }

    public long EstimatedSize => _offset;

    private async Task FinishBlockAsync(CancellationToken cancellationToken)
    {
        if (!_blockBuilder.HasEntries)
        {
            return;
        }

        // Release the block's memory as soon as we have copied its content.
        using var block = _blockBuilder.BuildBlock();

        _metadata ??= [];

        var m = new BlockMetadata<TKey>()
        {
            Index = _metadata.Count,
            Offset = _offset,
            FirstKey = _firstKey!,
            LastKey = _lastKey!
        };

        _metadata.Add(m);

        // Write the SST content in memory as blocks are getting created.
        // Use a buffer writer as it will handle the growth of the SST buffer automatically.

        // If the block is larger than the free capacity of the buffer then
        // flush to disk
        if (_bufferWriter.FreeCapacity < block.Memory.Span.Length && _bufferWriter.WrittenCount > 0)
        {
            await FLushBufferToDiskAsync(cancellationToken);
        }

        // If the buffer is too small, it will grow automatically, and this new size will be kept
        // as the new threshold until the end of the SST
        _bufferWriter.Write(block.Memory.Span);

        _offset += block.Memory.Length;

        _firstKey = default;
        _lastKey = default;
        _blockBuilder.Clear();

        return;
    }

    /// <remarks>
    /// The returned SST has an open file handle. The result must be disposed to close the handle.
    /// </summary>
    public async Task<SsTable<TKey, TValue>> BuildAsync(CancellationToken cancellationToken = default)
    {
        await FinishBlockAsync(cancellationToken);

        if (_metadata == null || _bufferWriter == null)
        {
            throw new InvalidOperationException("Nothing to store in SsTable.");
        }

        // Flush the blocks data before we use the buffer for metadata and bloom filter
        if (_bufferWriter.WrittenCount > 0)
        {
            await FLushBufferToDiskAsync(cancellationToken);
        }

        var metadataOffset = _offset;
        var binaryWriter = new EncoderBinaryWriter(_bufferWriter);
        _tableEncoder.EncodeMetadata(ref binaryWriter, _metadata, metadataOffset);

        var bloomFilterOffset = _offset + binaryWriter.BytesWritten;

        WriteBloomFilter(ref binaryWriter, bloomFilterOffset);

        binaryWriter.Flush();

        await FLushBufferToDiskAsync(cancellationToken);
        await _stream.DisposeAsync();

        // The temporary file is now complete and closed; publish it under the final name atomically so
        // readers and recovery only ever see a fully written SST.
        File.Move(_tempFilename, _filename, overwrite: true);

        // Ownership of the block builder and metadata transfers to the SsTable, which uses them to
        // decode and locate blocks, so this builder must no longer dispose or clear them.
        _ownsBuiltResources = false;

        return new SsTable<TKey, TValue>(IdGenerator.GetNextId(), File.OpenRead(_filename), _filename, _metadata, metadataOffset, _blockBuilder, _bloomFilter);
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
        writer.WriteUInt32((uint)_bloomFilter.K);
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

        // After a successful build, the block builder and metadata are owned by the SsTable, so the
        // builder must not dispose the block builder nor clear the metadata list the SsTable now uses.
        if (_ownsBuiltResources)
        {
            _blockBuilder?.Dispose();
            _metadata?.Clear();
            _metadata = null;

            // The build never completed (otherwise the temp file was renamed to the final name), so
            // remove the abandoned partial temp file rather than leaving it behind.
            TryDeleteTempFile();
        }
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
