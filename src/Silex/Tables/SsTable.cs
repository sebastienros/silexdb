using Silex.Blocks;
using System.Buffers;
using System.Buffers.Binary;

namespace Silex.Tables;

public class SsTable
{
    private readonly string _filename;
    private readonly Bytes _firstKey;
    private readonly Bytes _lastKey;
    private readonly BlockBuilder _blockBuilder;

    public SsTable(string filename, IReadOnlyList<BlockMetadata> blockMetadata, long metadataBlockOffset, BlockBuilder blockBuilder)
    {
        _filename = filename;
        BlockMetadata = blockMetadata;
        MetaBlockOffset = metadataBlockOffset;
        _blockBuilder = blockBuilder;

        if (blockMetadata.Count > 0)
        {
            _firstKey = BlockMetadata[0].FirstKey;
            _lastKey = BlockMetadata[BlockMetadata.Count - 1].LastKey;
        }
    }

    public IReadOnlyList<BlockMetadata> BlockMetadata { get; } = [];

    public long MetaBlockOffset { get; }

    public string Filename => _filename;

    public Bytes FirstKey => _firstKey;

    public Bytes LastKey => _lastKey;

    public async Task<Block> ReadBlockAsync(int index, CancellationToken cancellationToken = default)
    {
        using var stream = File.OpenRead(Filename);
        var offset = BlockMetadata[index].Offset;
        
        // If there is a single block it ends at the metadata block
        var offsetEnd = BlockMetadata.Count > index + 1
            ? BlockMetadata[index + 1].Offset
            : MetaBlockOffset;

        var length = (int)(offsetEnd - offset);

        var buffer = ArrayPool<byte>.Shared.Rent(length)!;

        stream.Seek(offset, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(buffer, 0, length, cancellationToken);
        
        var block = _blockBuilder.Decode(new ReadOnlyMemory<byte>(buffer, 0, length));

        ArrayPool<byte>.Shared.Return(buffer);

        return block;
    }

    public static async Task<SsTable> LoadSsTableAsync(string filename, ISsTableEncoder tableEncoder, BlockBuilder blockBuilder, CancellationToken cancellationToken = default)
    {
        using var stream = File.OpenRead(filename);

        // Read the metadata block offset in the last four bytes
        stream.Seek(stream.Length - 4, SeekOrigin.Begin);
        var buffer = ArrayPool<byte>.Shared.Rent(4);
        Memory<byte> memory = buffer.AsMemory(0, 4);
        await stream.ReadExactlyAsync(memory, cancellationToken);
        var metaBlockOffset = BinaryPrimitives.ReadUInt32LittleEndian(buffer);

        // Read the metadata block content
        var metadataLength = stream.Length - 4 - metaBlockOffset;

        buffer = ArrayPool<byte>.Shared.Rent((int)metadataLength);
        stream.Seek(metaBlockOffset, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(buffer, 0, (int)metadataLength, cancellationToken);
        var blockMetadata = tableEncoder.DecodeMetadata(buffer, 0);
        ArrayPool<byte>.Shared.Return(buffer);

        var table = new SsTable(filename, blockMetadata, metaBlockOffset, blockBuilder);
        
        return table;
    }
}
