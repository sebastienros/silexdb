using Silex.Buffers;
using Silex.Serialization;

namespace Silex.Tables;

/// <summary>
/// The table encoding format is as follows:
/// 
/// ------------------------------------------------------------------------------------------------------------------------------------
/// |         Block Section         |                   Meta Section                                                                   |
/// ------------------------------------------------------------------------------------------------------------------------------------
/// | data block | ... | metadata (varlen) | meta offset (u32) | bloom (varlen) | bloom footer | "SILEXSST" (u64) | version (u32) |
/// ------------------------------------------------------------------------------------------------------------------------------------
/// 
/// ----------------------------------------------------------------------------------------------------------------------------
/// |                           Metadata Section                                                                         | ... |
/// ----------------------------------------------------------------------------------------------------------------------------
/// | num_blocks (7b) | offset (7b) | raw_len (7b) | codec (u8) | checksum (u32) | first_key_len (7b) | key | last_key_len (7b) | key | ... |
/// ----------------------------------------------------------------------------------------------------------------------------
/// 
/// </summary>
internal sealed class DefaultSsTableEncoder : ISsTableEncoder
{
    private static readonly IBinaryEncoder<ByteSlice> _keySerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;

    public IReadOnlyList<BlockMetadata> DecodeMetadata(ReadOnlyMemory<byte> buffer, int offset, int formatVersion)
    {
        var binaryReader = new EncoderBinaryReader(buffer, offset);

        // Read total number of blocks
        var numBlocks = binaryReader.Read7BitEncodedInt();

        // Read each block metadata

        var result = new List<BlockMetadata>(numBlocks);

        for (var i = 0; i < numBlocks; i++)
        {
            var blockOffset = binaryReader.Read7BitEncodedInt();
            var uncompressedLength = 0;
            var compression = SstCompression.None;
            var checksum = 0u;

            if (formatVersion >= 1)
            {
                uncompressedLength = binaryReader.Read7BitEncodedInt();
                compression = (SstCompression)binaryReader.ReadByte();
                checksum = binaryReader.ReadUInt32();

                if (uncompressedLength <= 0 || !Enum.IsDefined(compression))
                {
                    throw new InvalidDataException("The SST contains invalid block compression metadata.");
                }
            }

            var firstKeyLen = binaryReader.Read7BitEncodedInt();
            var firstKey = OwnedByteSlice.CopyFrom(binaryReader.ReadBytesSpan(firstKeyLen));

            var lastKeyLen = binaryReader.Read7BitEncodedInt();
            var lastKey = OwnedByteSlice.CopyFrom(binaryReader.ReadBytesSpan(lastKeyLen));

            result.Add(new BlockMetadata
            {
                Index = i,
                Offset = blockOffset,
                UncompressedLength = uncompressedLength,
                Compression = compression,
                Checksum = checksum,
                FirstKeyOwner = firstKey,
                LastKeyOwner = lastKey
            });
        }

        return result;
    }

    public void EncodeMetadata(ref EncoderBinaryWriter writer, IReadOnlyList<BlockMetadata> blockMetadata, long metadataOffset, int formatVersion)
    {
        writer.Write7BitEncodedInt(blockMetadata.Count);

        foreach (var block in blockMetadata)
        {
            writer.Write7BitEncodedInt64(block.Offset);

            if (formatVersion >= 1)
            {
                writer.Write7BitEncodedInt(block.UncompressedLength);
                writer.Write((byte)block.Compression);
                writer.WriteUInt32(block.Checksum);
            }

            writer.Write7BitEncodedInt(_keySerializer.GetLength(block.FirstKey));
            _keySerializer.Encode(block.FirstKey, ref writer);

            writer.Write7BitEncodedInt(_keySerializer.GetLength(block.LastKey));
            _keySerializer.Encode(block.LastKey, ref writer);
        }

        writer.WriteUInt32((uint)metadataOffset);
    }

    public int EstimateMetadataSize(IReadOnlyList<BlockMetadata> blockMetadata, int formatVersion)
    {
        int estimate = 0;

        estimate += sizeof(uint);

        foreach (var block in blockMetadata)
        {
            estimate += sizeof(uint);

            if (formatVersion >= 1)
            {
                estimate += sizeof(uint);
                estimate += sizeof(byte);
                estimate += sizeof(uint);
            }

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
