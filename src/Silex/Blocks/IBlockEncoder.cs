namespace Silex.Blocks;

/// <summary>
/// Encodes or decodes a <see cref="Block"/>. 
/// </summary>
public interface IBlockEncoder<TKey, TValue>
{
    /// <summary>
    /// Encodes the given entries into a <see cref="Block"/>. The keys are read from <paramref name="encodedKeys"/>
    /// using the offsets and lengths carried by each <see cref="BlockEntry{TValue}"/>, so they are not re-encoded.
    /// </summary>
    /// <param name="encodedKeys">The buffer holding every encoded key referenced by <paramref name="entries"/>.</param>
    /// <param name="entries">The entries to encode, in ascending key order.</param>
    Block<TKey, TValue> Encode(ReadOnlyMemory<byte> encodedKeys, IReadOnlyList<BlockEntry<TValue>> entries);

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

    /// <summary>
    /// Estimates the size, in bytes, that an entry with the given encoded key length and value takes in a block.
    /// </summary>
    int EstimateSize(int encodedKeyLength, TValue value);
}
