using Silex.Blocks;
using Silex.Buffers;

namespace Silex.Tables;

/// <summary>
/// Encodes or decodes a <see cref="SsTable"/>. 
/// </summary>
public interface ISsTableEncoder
{
    void EncodeMetadata(EncoderBinaryWriter writer, IReadOnlyList<BlockMetadata> blockMetadata, long metadataOffset);

    IReadOnlyList<BlockMetadata> DecodeMetadata(ReadOnlyMemory<byte> buffer, int offset);

    /// <summary>
    /// The size that a <see cref="Block"/> takes on the disk for this <see cref="ISsTableEncoder" />.
    /// </summary>
    ushort BlockSize { get; }

    int EstimateMetadataSize(IReadOnlyList<BlockMetadata> blockMetadata);
}
