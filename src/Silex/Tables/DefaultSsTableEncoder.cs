using Silex.Buffers;
using System.Buffers;

namespace Silex.Tables;

/// <summary>
/// The table encoding format is as follows:
/// 
/// -------------------------------------------------------------------------------------------
/// |         Block Section         |          Meta Section         |          Extra          |
/// -------------------------------------------------------------------------------------------
/// | data block | ... | data block |            metadata           | meta block offset(u32)  |
/// -------------------------------------------------------------------------------------------
/// 
/// -------------------------------------------------------------------------------------------------------------------------------
/// |                           Meta Section                                                                                | ... |
/// -------------------------------------------------------------------------------------------------------------------------------
/// | num_blocks (7b) | offset (7b) | first_key_len (7b) | key (first_key_len) | last_key_len (u16) | key (last_key_len) | ... |
/// -------------------------------------------------------------------------------------------------------------------------------
/// 
/// </summary>
public class DefaultSsTableEncoder : ISsTableEncoder
{
    public IReadOnlyList<BlockMetadata> DecodeMetadata(ReadOnlyMemory<byte> buffer, int offset)
    {
        var binaryReader = new EncoderBinaryReader(buffer, offset);

        // Read total number of blocks
        var numBlocks = binaryReader.Read7BitEncodedInt();

        // Read each block metadata

        var result = new List<BlockMetadata>(numBlocks);

        for (var i = 0; i < numBlocks; i++)
        {
            var blockOffset = binaryReader.Read7BitEncodedInt();
            var firstKeyLen = binaryReader.Read7BitEncodedInt();
            var firstKey = (Bytes)binaryReader.ReadBytesMemory(firstKeyLen);
            var lastKeyLen = binaryReader.Read7BitEncodedInt();
            var lastKey = (Bytes)binaryReader.ReadBytesMemory(lastKeyLen);

            result.Add(new BlockMetadata { Index = i, Offset = blockOffset, FirstKey = firstKey, LastKey = lastKey });
        }

        return result;
    }

    public void EncodeMetadata(EncoderBinaryWriter writer, IReadOnlyList<BlockMetadata> blockMetadata, long metadataOffset)
    {
        writer.Write7BitEncodedInt(blockMetadata.Count);

        foreach (var block in blockMetadata)
        {
            writer.Write7BitEncodedInt64(block.Offset);
            writer.Write7BitEncodedInt(block.FirstKey.Length);
            writer.WriteRaw(block.FirstKey.Span);
            writer.Write7BitEncodedInt(block.LastKey.Length);
            writer.WriteRaw(block.LastKey.Span);
        }

        writer.WriteUInt32((uint)metadataOffset);
        writer.Flush();
    }

    public int EstimateMetadataSize(IReadOnlyList<BlockMetadata> blockMetadata)
    {
        int estimate = 0;

        estimate += sizeof(uint);

        foreach (var block in blockMetadata)
        {
            estimate += sizeof(uint);
            estimate += sizeof(uint);
            estimate += block.FirstKey.Length;

            estimate += sizeof(uint);
            estimate += sizeof(uint);
            estimate += block.LastKey.Length;
        }

        estimate += sizeof(uint);

        return estimate;
    }
}
