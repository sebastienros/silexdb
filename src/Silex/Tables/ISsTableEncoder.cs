using Silex.Buffers;

namespace Silex.Tables;

/// <summary>
/// Encodes or decodes a <see cref="SsTable"/>. 
/// </summary>
public interface ISsTableEncoder<TKey>
{
    void EncodeMetadata(ref EncoderBinaryWriter writer, IReadOnlyList<BlockMetadata<TKey>> blockMetadata, long metadataOffset);

    IReadOnlyList<BlockMetadata<TKey>> DecodeMetadata(ReadOnlyMemory<byte> buffer, int offset);

    int EstimateMetadataSize(IReadOnlyList<BlockMetadata<TKey>> blockMetadata);
}
