using Silex.Blocks;
using Silex.Buffers;
using System.Buffers;

namespace Silex.Tables;

public class SsTableBuilder<TKey, TValue>
{
    private readonly ISsTableEncoder<TKey, TValue> _tableEncoder;
    private long _offset;

    private bool _isFirstKey = true;
    private TKey? _firstKey = default;
    private TKey? _lastKey = default;
    private readonly BlockBuilder<TKey, TValue> _blockBuilder;
    private List<BlockMetadata<TKey>>? _metadata;
    
    private RecyclableArrayBufferWriter<byte>? _bufferWriter;

    public SsTableBuilder(ISsTableEncoder<TKey, TValue> tableEncoder, IBlockEncoder<TKey, TValue> blockEncoder)
    {
        _tableEncoder = tableEncoder;
        _blockBuilder = new BlockBuilder<TKey, TValue>(blockEncoder);
    }

    public void Add(TKey key, TValue value)
    {
        if (_isFirstKey)
        {
            _firstKey = key;
            _isFirstKey = false;
        }

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
            throw new InvalidOperationException("The data was not be successfully added to a block.");
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

        var blockLength = block.Memory.Length;

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

        if (_bufferWriter == null)
        {
            _bufferWriter = new(); ;
            _bufferWriter.GetMemory(blockLength);
        }

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

        using var stream = File.Create(filename);

        var binaryWriter = new EncoderBinaryWriter(_bufferWriter);
        _tableEncoder.EncodeMetadata(binaryWriter, _metadata, _offset);

        try
        {
            var memory = _bufferWriter.GetCommittedMemory();
            await stream.WriteAsync(memory, cancellationToken);
        }
        finally
        {
            await stream.DisposeAsync();
        }

        var table = new SsTable<TKey, TValue>(IdGenerator.GetNextId(), filename, _metadata, _offset, _blockBuilder);

        _bufferWriter.Dispose();
        _bufferWriter = null;
        _metadata = null;
        _offset = 0;

        return table;
    }

    ~SsTableBuilder()
    {
        _bufferWriter?.Dispose();
    }
}
