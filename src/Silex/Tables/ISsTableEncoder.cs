using Silex.Buffers;

namespace Silex.Tables;

/// <summary>
/// Encodes or decodes a <see cref="SsTable"/>. 
/// </summary>
internal interface ISsTableEncoder
{
    void EncodeMetadata(ref EncoderBinaryWriter writer, IReadOnlyList<BlockMetadata> blockMetadata, long metadataOffset, int formatVersion);

    IReadOnlyList<BlockMetadata> DecodeMetadata(ReadOnlyMemory<byte> buffer, int offset, int formatVersion);

    int EstimateMetadataSize(IReadOnlyList<BlockMetadata> blockMetadata, int formatVersion);
}
