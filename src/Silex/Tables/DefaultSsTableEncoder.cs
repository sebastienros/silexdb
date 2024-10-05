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
    public ushort BlockSize => (ushort)4.KiB();

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
            var firstKey = binaryReader.ReadBytesMemory(firstKeyLen);
            var lastKeyLen = binaryReader.Read7BitEncodedInt();
            var lastKey = binaryReader.ReadBytesMemory(lastKeyLen);

            result.Add(new BlockMetadata { Index = i, Offset = blockOffset, FirstKey = firstKey, LastKey = lastKey });
        }

        return result;
    }

    public (IMemoryOwner<byte>, int) EncodeMetadata(IReadOnlyList<BlockMetadata> blockMetadata, long metadataOffset)
    {
        
        var buffer = new RecyclableArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(buffer);

        var size = EstimateMetadataSize(blockMetadata);

        try
        {
            buffer.GetMemory(size);

            writer.Write7BitEncodedInt(blockMetadata.Count);

            foreach (var block in blockMetadata)
            {
                writer.Write7BitEncodedInt64(block.Offset);
                writer.Write7BitEncodedInt(block.FirstKey.Length);
                writer.WriteRaw(block.FirstKey.Span);
                writer.Write7BitEncodedInt(block.LastKey.Length);
                writer.WriteRaw(block.LastKey.Span);
            }

            writer.WriteRaw(BitConverter.GetBytes((uint)metadataOffset));
            writer.Flush();

            var memory = buffer.GetCommittedMemory();

            var memoryOwner = MemoryPool<byte>.Shared.Rent(memory.Length);
            memory.CopyTo(memoryOwner.Memory);

            return (memoryOwner, memory.Length);
        }
        finally
        {
            buffer.Dispose();
        }
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
