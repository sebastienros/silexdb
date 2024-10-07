using Silex.Blocks;
using System.Buffers;

namespace Silex.Tables;

public class SsTable
{
    private readonly string _filename;
    private readonly BlockBuilder _blockBuilder;

    public SsTable(string filename, IReadOnlyList<BlockMetadata> blockMetadata, long metadataBlockOffset, BlockBuilder blockBuilder)
    {
        _filename = filename;
        BlockMetadata = blockMetadata;
        MetaBlockOffset = metadataBlockOffset;
        _blockBuilder = blockBuilder;
    }

    public IReadOnlyList<BlockMetadata> BlockMetadata { get; } = [];

    public long MetaBlockOffset { get; }

    public string Filename => _filename;

    public async Task<Block> LoadBlockAsync(int index, CancellationToken cancellationToken = default)
    {
        using var stream = File.OpenRead(Filename);
        var offset = BlockMetadata[index].Offset;
        
        // If there is a single block it ends at the metadata block
        var length = BlockMetadata.Count > index + 1
            ? BlockMetadata[index + 1].Offset - offset
            : MetaBlockOffset - offset
            ;

        var buffer = new byte[length];

        stream.Seek(offset, SeekOrigin.Begin);
        await stream.ReadExactlyAsync(buffer, 0, buffer.Length, cancellationToken);
        
        return _blockBuilder.Decode(buffer);
    }

    public static async Task<SsTable> LoadSsTableAsync(string filename, ISsTableEncoder tableEncoder, BlockBuilder blockBuilder, CancellationToken cancellationToken = default)
    {
        using var stream = File.OpenRead(filename);

        // Read the metadata block offset in the last four bytes
        stream.Seek(stream.Length - 4, SeekOrigin.Begin);
        var buffer = ArrayPool<byte>.Shared.Rent(4);
        Memory<byte> memory = buffer.AsMemory(0, 4);
        await stream.ReadExactlyAsync(memory, cancellationToken);
        var metaBlockOffset = BitConverter.ToUInt32(buffer);

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
