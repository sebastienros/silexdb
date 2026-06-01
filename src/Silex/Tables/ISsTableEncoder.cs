using Silex.Buffers;

namespace Silex.Tables;

/// <summary>
/// Encodes or decodes a <see cref="SsTable"/>. 
/// </summary>
internal interface ISsTableEncoder
{
    void EncodeMetadata(ref EncoderBinaryWriter writer, IReadOnlyList<BlockMetadata> blockMetadata, long metadataOffset);

    IReadOnlyList<BlockMetadata> DecodeMetadata(ReadOnlyMemory<byte> buffer, int offset);

    int EstimateMetadataSize(IReadOnlyList<BlockMetadata> blockMetadata);
}
