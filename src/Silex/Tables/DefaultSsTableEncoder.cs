using Silex.Buffers;
using Silex.Serialization;

namespace Silex.Tables;

/// <summary>
/// The table encoding format is as follows:
/// 
/// ------------------------------------------------------------------------------------------------------------------------------------
/// |         Block Section         |                   Meta Section                                                                   |
/// ------------------------------------------------------------------------------------------------------------------------------------
/// | data block | ... | data block | metadata (varlen) | meta block offset (u32) | bloom filter (varlen) | bloom filter offset (u32)  |
/// ------------------------------------------------------------------------------------------------------------------------------------
/// 
/// ----------------------------------------------------------------------------------------------------------------------------
/// |                           Metadata Section                                                                         | ... |
/// ----------------------------------------------------------------------------------------------------------------------------
/// | num_blocks (7b) | offset (7b) | first_key_len (7b) | key (first_key_len) | last_key_len (u16) | key (last_key_len) | ... |
/// ----------------------------------------------------------------------------------------------------------------------------
/// 
/// </summary>
public class DefaultSsTableEncoder<TKey, TValue> : ISsTableEncoder<TKey, TValue>
{
    private static readonly IBinaryEncoder<TKey> _keySerializer = BinaryEncoderFactory<TKey>.BinarySerializer;

    public IReadOnlyList<BlockMetadata<TKey>> DecodeMetadata(ReadOnlyMemory<byte> buffer, int offset)
    {
        var binaryReader = new EncoderBinaryReader(buffer, offset);

        // Read total number of blocks
        var numBlocks = binaryReader.Read7BitEncodedInt();

        // Read each block metadata

        var result = new List<BlockMetadata<TKey>>(numBlocks);

        for (var i = 0; i < numBlocks; i++)
        {
            var blockOffset = binaryReader.Read7BitEncodedInt();

            var firstKeyLen = binaryReader.Read7BitEncodedInt();
            var firstKeyData = binaryReader.ReadBytesSpan(firstKeyLen);
            var firstKey = _keySerializer.Decode(firstKeyData);

            var lastKeyLen = binaryReader.Read7BitEncodedInt();
            var lastKeyData = binaryReader.ReadBytesSpan(lastKeyLen);
            var lastKey = _keySerializer.Decode(lastKeyData);

            result.Add(new BlockMetadata<TKey> { Index = i, Offset = blockOffset, FirstKey = firstKey, LastKey = lastKey });
        }

        return result;
    }

    public void EncodeMetadata(ref EncoderBinaryWriter writer, IReadOnlyList<BlockMetadata<TKey>> blockMetadata, long metadataOffset)
    {
        writer.Write7BitEncodedInt(blockMetadata.Count);

        foreach (var block in blockMetadata)
        {
            writer.Write7BitEncodedInt64(block.Offset);

            writer.Write7BitEncodedInt(_keySerializer.GetLength(block.FirstKey));
            _keySerializer.Encode(block.FirstKey, ref writer);

            writer.Write7BitEncodedInt(_keySerializer.GetLength(block.LastKey));
            _keySerializer.Encode(block.LastKey, ref writer);
        }

        writer.WriteUInt32((uint)metadataOffset);
    }

    public int EstimateMetadataSize(IReadOnlyList<BlockMetadata<TKey>> blockMetadata)
    {
        int estimate = 0;

        estimate += sizeof(uint);

        foreach (var block in blockMetadata)
        {
            estimate += sizeof(uint);
            estimate += sizeof(uint);
            estimate += _keySerializer.GetLength(block.FirstKey);

            estimate += sizeof(uint);
            estimate += sizeof(uint);
            estimate += _keySerializer.GetLength(block.LastKey);
        }

        estimate += sizeof(uint);

        return estimate;
    }
}
