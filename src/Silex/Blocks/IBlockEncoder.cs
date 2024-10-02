using System;
using System.Buffers;

namespace Silex.Blocks;

/// <summary>
/// Encodes or decodes a <see cref="Block"/>. 
/// </summary>
public interface IBlockEncoder
{
    Block Encode(IBufferWriter<byte> buffer, IReadOnlyList<BlockEntry> entries);

    Block Decode(ReadOnlyMemory<byte> buffer);

    BlockEntry DecodeEntry(ReadOnlyMemory<byte> data, int offset);
}
