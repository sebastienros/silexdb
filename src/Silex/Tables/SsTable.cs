using Silex.Blocks;
using System.Buffers;

namespace Silex.Tables;

public class SsTable
{
    private readonly string _filename;
    private readonly IBlockEncoder _blockEncoder;

    public SsTable(string filename, IReadOnlyList<BlockMetadata> blockMetadata, long metadataBlockOffset, IBlockEncoder blockEncoder)
    {
        _filename = filename;
        _blockEncoder = blockEncoder;
        BlockMetadata = blockMetadata;
        MetaBlockOffset = metadataBlockOffset;
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
        
        return _blockEncoder.Decode(buffer);
    }

    public static async Task<SsTable> LoadSsTableAsync(string filename, ISsTableEncoder tableEncoder, IBlockEncoder blockEncoder, CancellationToken cancellationToken = default)
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

        var table = new SsTable(filename, blockMetadata, metaBlockOffset, blockEncoder);
        
        return table;
    }
}
