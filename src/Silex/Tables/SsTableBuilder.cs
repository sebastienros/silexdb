using Silex.Blocks;
using Silex.Buffers;
using System.Buffers;

namespace Silex.Tables;

public class SsTableBuilder
{
    private readonly ISsTableEncoder _tableEncoder;
    private long _offset;

    private ReadOnlyMemory<byte> _firstKey = ReadOnlyMemory<byte>.Empty;
    private ReadOnlyMemory<byte> _lastKey = ReadOnlyMemory<byte>.Empty;
    private readonly BlockBuilder _blockBuilder;
    private List<BlockMetadata>? _metadata;
    
    private RecyclableArrayBufferWriter<byte>? _bufferWriter;

    public SsTableBuilder(ISsTableEncoder tableEncoder, IBlockEncoder blockEncoder)
    {
        _tableEncoder = tableEncoder;
        _blockBuilder = new BlockBuilder(blockEncoder);
    }

    public void Add(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        if (_firstKey.IsEmpty)
        {
            _firstKey = key;
        }

        _blockBuilder.Add(key, value);

        _lastKey = key;
        
        if (_blockBuilder.EstimatedSize >= _tableEncoder.BlockSize)
        {
            FinishBlock();
        }
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

        var m = new BlockMetadata()
        {
            Index = _metadata.Count,
            Offset = _offset,
            FirstKey = _firstKey,
            LastKey = _lastKey
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

        _firstKey = ReadOnlyMemory<byte>.Empty;
        _lastKey = ReadOnlyMemory<byte>.Empty;
        _blockBuilder.Clear();
    }

    public async Task<SsTable> BuildAsync(string filename, CancellationToken cancellationToken = default)
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

        var table = new SsTable(filename, _metadata, _offset, _blockBuilder);

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
