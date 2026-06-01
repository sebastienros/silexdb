using System.Buffers;

namespace Silex.Blocks;

/// <summary>
/// Encodes or decodes a <see cref="Block"/>. 
/// </summary>
internal interface IBlockEncoder
{
    /// <summary>
    /// Encodes the given entries into a <see cref="Block"/>. The keys and values are read from the supplied
    /// buffers using the offsets and lengths carried by each <see cref="BlockEntry"/>, so borrowed input slices
    /// are not retained by the block builder.
    /// </summary>
    /// <param name="encodedKeys">The buffer holding every encoded key referenced by <paramref name="entries"/>.</param>
    /// <param name="values">The buffer holding every value referenced by <paramref name="entries"/>.</param>
    /// <param name="entries">The entries to encode, in ascending key order.</param>
    Block Encode(ReadOnlyMemory<byte> encodedKeys, ReadOnlyMemory<byte> values, IReadOnlyList<BlockEntry> entries);

    /// <summary>
    /// Creates a <see cref="Block"/> instance that will hold the original memory block saved on this.
    /// </summary>
    /// <param name="buffer"></param>
    /// <returns></returns>
    Block Decode(ReadOnlyMemory<byte> buffer);

    /// <summary>
    /// Builds a <see cref="Block"/> that takes ownership of <paramref name="owner"/> directly, scanning the
    /// offset section in place. Unlike <see cref="Decode(ReadOnlyMemory{byte})"/> this avoids renting a second
    /// buffer and copying the block bytes, so the data read from disk becomes the block's backing memory as-is.
    /// </summary>
    /// <param name="owner">The buffer holding the block bytes; the returned block disposes it.</param>
    /// <param name="length">The number of valid bytes at the start of <paramref name="owner"/>.</param>
    Block Decode(IMemoryOwner<byte> owner, int length);

    RecordLocation DecodeEntry(ReadOnlyMemory<byte> data, int offset);

    ReadOnlyMemory<byte> DecodeValue(ReadOnlyMemory<byte> data, int offset, int length);

    /// <summary>
    /// The size that a <see cref="Block"/> takes on the disk for this <see cref="ISsTableEncoder" />.
    /// </summary>
    ushort BlockSize { get; }

    /// <summary>
    /// Estimates the size, in bytes, that an entry with the given encoded key and value lengths takes in a block.
    /// </summary>
    int EstimateSize(int encodedKeyLength, int valueLength);
}
