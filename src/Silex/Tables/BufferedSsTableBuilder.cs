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

    private static readonly IBinaryEncoder<TKey> _keySerializer = BinaryEncoderFactory<TKey>.BinarySerializer;
    private readonly string _filename;
    private readonly ISsTableEncoder<TKey, TValue> _tableEncoder;
    private long _offset;
    private bool _isFirstKey = true;
    private TKey? _firstKey = default;
    private TKey? _lastKey = default;
    private readonly IBloomFilter _bloomFilter;
    private readonly BlockBuilder<TKey, TValue> _blockBuilder;
    private List<BlockMetadata<TKey>>? _metadata;
    private bool _disposed;
    private readonly FileStream _stream;

    private static readonly int _flushBufferSize = (int)32.KiB();
    private readonly PooledArrayBufferWriter<byte> _bufferWriter = new(_flushBufferSize);

    // Used to serialize the key contents as they are added to the bloom filter. At max it will contain the biggest key 
    // for the current block. After each block the buffer is returned. Between two values the buffer is cleared().
    private readonly PooledArrayBufferWriter<byte> _valueWriter = new();

    public BufferedSsTableBuilder(string filename, ISsTableEncoder<TKey, TValue> tableEncoder, IBlockEncoder<TKey, TValue> blockEncoder, IBloomFilterFactory bloomFilterFactory, int count)
    {
        _filename = filename;
        _tableEncoder = tableEncoder;
        _bloomFilter = bloomFilterFactory.CreateBloomFilter(count, 0.01);
        _blockBuilder = new BlockBuilder<TKey, TValue>(blockEncoder);

        // Disable dotnet buffer as we are handling it
        var bufferSize = 0;

        var options =
            FileOptions.WriteThrough | // Skip OS cache
            FileOptions.Asynchronous // All operation are async
            ;

        _stream = File.Create(filename, bufferSize, options);
    }

    public async Task AddAsync(TKey key, TValue value)
    {
        if (_isFirstKey)
        {
            _firstKey = key;
            _isFirstKey = false;
        }

        var binaryWriter = new EncoderBinaryWriter(_valueWriter);
        _keySerializer.Encode(key, ref binaryWriter);
        _bloomFilter.Add(_valueWriter.WrittenMemory.Span);

        // Clear this buffer to reuse it for next key/value pair
        _valueWriter.Clear();

        // Is the key added in the current block?
        if (_blockBuilder.Add(key, value))
        {
            _lastKey = key;
            return;
        }

        // No, then there was not enough space in the current block for the data, start a new one
        await FinishBlockAsync();

        if (!_blockBuilder.Add(key, value))
        {
            // The block builder has to accept this entry since it's empty, even if
            // the size is over the block size
            throw new InvalidOperationException("The data was not successfully added to a block.");
        }

        _firstKey = key;
        _lastKey = key;
    }

    public long EstimatedSize => _offset;

    private async Task FinishBlockAsync()
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
            await FLushBufferToDiskAsync();
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
        await FinishBlockAsync();

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
        _stream.Dispose();
        _bufferWriter.Dispose();
        _valueWriter.Dispose();
        _metadata?.Clear();
        _metadata = null;
    }

    ~BufferedSsTableBuilder()
    {
        DisposeInternal();
    }
}
