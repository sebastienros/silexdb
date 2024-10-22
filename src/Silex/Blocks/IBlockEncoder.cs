namespace Silex.Blocks;

/// <summary>
/// Encodes or decodes a <see cref="Block"/>. 
/// </summary>
public interface IBlockEncoder<TKey, TValue>
{
    Block<TKey, TValue> Encode(IReadOnlyList<KeyValuePair<TKey, TValue>> entries);

    /// <summary>
    /// Creates a <see cref="Block"/> instance that will hold the original memory block saved on this.
    /// </summary>
    /// <param name="buffer"></param>
    /// <returns></returns>
    Block<TKey, TValue> Decode(ReadOnlyMemory<byte> buffer);

    RecordLocation<TKey> DecodeEntry(ReadOnlyMemory<byte> data, int offset);

    ReadOnlyMemory<byte> DecodeValue(ReadOnlyMemory<byte> data, int offset, int length);

    /// <summary>
    /// The size that a <see cref="Block"/> takes on the disk for this <see cref="ISsTableEncoder" />.
    /// </summary>
    ushort BlockSize { get; }

    int EstimateSize(TKey key, TValue value);
}
