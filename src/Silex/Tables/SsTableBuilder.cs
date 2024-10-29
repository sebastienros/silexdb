using Microsoft.IO;
using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Buffers;
using Silex.Serialization;

namespace Silex.Tables;

public class SsTableBuilder<TKey, TValue> : IDisposable
{
    private static readonly IBinaryEncoder<TKey> _keySerializer = BinaryEncoderFactory<TKey>.BinarySerializer;

    private readonly ISsTableEncoder<TKey, TValue> _tableEncoder;
    private long _offset;

    private bool _isFirstKey = true;
    private TKey? _firstKey = default;
    private TKey? _lastKey = default;
    private readonly IBloomFilter _bloomFilter;
    private readonly BlockBuilder<TKey, TValue> _blockBuilder;
    private List<BlockMetadata<TKey>>? _metadata;
    private bool _disposed;

    // This accumulates the SST blocks in memory as they are created. Ultimately all the SST will be help in memory
    // before being flushed to disk.
    private readonly RecyclableMemoryStream _bufferWriter = RecyclableMemoryStreamFactory.Shared.GetStream();

    // Used to serialize the key contents as they are added to the bloom filter. At max it will contain the biggest key 
    // for the current block. After each block the buffer is returned. Between two values the buffer is cleared().
    private readonly PooledArrayBufferWriter<byte> _valueWriter = new();

    public SsTableBuilder(ISsTableEncoder<TKey, TValue> tableEncoder, IBlockEncoder<TKey, TValue> blockEncoder, IBloomFilterFactory bloomFilterFactory, int count)
    {
        _tableEncoder = tableEncoder;
        _bloomFilter = bloomFilterFactory.CreateBloomFilter(count, 0.01);
        _blockBuilder = new BlockBuilder<TKey, TValue>(blockEncoder);
    }

    public void Add(TKey key, TValue value)
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

        if (_blockBuilder.Add(key, value))
        {
            _lastKey = key;
            return;
        }

        // There was not enough space in the current block for the data, start a new one
        FinishBlock();

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

    private void FinishBlock()
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

        _bufferWriter.Write(block.Memory.Span);
        
        _offset += block.Memory.Length;

        _firstKey = default;
        _lastKey = default;
        _blockBuilder.Clear();
    }

    public async Task<SsTable<TKey, TValue>> BuildAsync(string filename, CancellationToken cancellationToken = default)
    {
        FinishBlock();

        if (_metadata == null || _bufferWriter == null)
        {
            throw new InvalidOperationException("Nothing to store in SsTable.");
        }

        var stream = File.Create(filename, (int)4.KiB(), FileOptions.WriteThrough | FileOptions.Asynchronous);
        
        var metadataOffset = _offset;
        var binaryWriter = new EncoderBinaryWriter(_bufferWriter);
        _tableEncoder.EncodeMetadata(ref binaryWriter, _metadata, metadataOffset);

        var bloomFilterOffset = _offset + binaryWriter.BytesWritten;

        WriteBloomFilter(ref binaryWriter, bloomFilterOffset);

        binaryWriter.Flush();

        try
        {
            var sequence = _bufferWriter.GetReadOnlySequence();

            foreach (var s in sequence)
            {
                await stream.WriteAsync(s, cancellationToken);
            }
        }
        finally
        {
            await stream.FlushAsync(cancellationToken);
            await stream.DisposeAsync();
        }

        var table = new SsTable<TKey, TValue>(IdGenerator.GetNextId(), File.OpenRead(filename), filename, _metadata, metadataOffset, _blockBuilder, _bloomFilter);

        _metadata = null;
        _offset = 0;

        return table;
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
        _metadata?.Clear();
        _metadata = null;
    }

    private void DisposeInternal()
    {
        _bufferWriter.Dispose();
        _valueWriter.Dispose();
    }

    ~SsTableBuilder()
    {
        DisposeInternal();
    }
}
