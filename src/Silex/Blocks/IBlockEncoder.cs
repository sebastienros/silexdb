using System.Buffers;

namespace Silex.Blocks;

/// <summary>
/// Encodes or decodes a <see cref="Block"/>. 
/// </summary>
public interface IBlockEncoder
{
    Block Encode(IReadOnlyList<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> entries);

    /// <summary>
    /// Creates a <see cref="Block"/> instance that will hold the original memory block saved on this.
    /// </summary>
    /// <param name="buffer"></param>
    /// <returns></returns>
    Block Decode(ReadOnlyMemory<byte> buffer);

    BlockEntry DecodeEntry(ReadOnlyMemory<byte> data, int offset);

    ReadOnlyMemory<byte> DecodeValue(ReadOnlyMemory<byte> data, int offset, int length);


    int EstimateSize(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value);
}
