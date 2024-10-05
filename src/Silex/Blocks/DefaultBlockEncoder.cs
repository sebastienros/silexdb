using Silex.Buffers;
using System;
using System.Buffers;

namespace Silex.Blocks;

/// <summary>
/// The block encoding format is as follows:
/// 
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
/// 
/// Each entry is a key-value pair.
/// 
/// -----------------------------------------------------------------------
/// |                           Entry #1                            | ... |
/// -----------------------------------------------------------------------
/// | key_len (u16) | key (keylen) | value_len (u16) | value (varlen) | ... |
/// -----------------------------------------------------------------------
/// 
/// Key length and value length are 7 bits encoded since each entry position is 
/// recorded in the offsets sections so we don't have to calculate them.
///  
/// We assume that keys will never be empty, and values can be empty.
/// An empty value means that the corresponding key has been deleted in the view 
/// of other parts of the system.
/// 
/// At the end of each block, we will store the offsets of each entry and the total number 
/// of entries. For example, if the first entry is at 0th position of the block, 
/// and the second entry is at 12th position of the block.
/// 
/// -------------------------------
/// |offset|offset| total_entries |
/// -------------------------------
/// |   0  |  12  |       2       |
/// -------------------------------
/// 
/// The footer of the block will be as above. Each of the number is stored as ushort (UInt16).
/// </summary>
public class DefaultBlockEncoder : IBlockEncoder
{
    public Block Decode(ReadOnlyMemory<byte> buffer)
    {
        var span = buffer.Span;

        // Read the last two bytes
        var totalEntries = BitConverter.ToUInt16(span.Slice(span.Length - 2, 2));

        // Read the offsets position
        var offsetPosition = span.Length - (totalEntries + 1) * 2;
        var current = offsetPosition;

        var offsets = new List<ushort>(totalEntries);

        for (var i = 0; i < totalEntries; i++)
        {
            var bytes = span.Slice(current, 2);
            offsets.Add(BitConverter.ToUInt16(bytes));
            current += 2;
        }

        var memoryOwner = MemoryPool<byte>.Shared.Rent(buffer.Length);
        buffer.CopyTo(memoryOwner.Memory);

        return new Block(this, memoryOwner, buffer.Length, offsets);
    }

    public BlockEntry DecodeEntry(ReadOnlyMemory<byte> data, int offset)
    {
        var binaryReader = new EncoderBinaryReader(data, offset);
        var keyLength = binaryReader.Read7BitEncodedInt();
        var key = binaryReader.ReadBytesMemory(keyLength);
        var valueLength = binaryReader.Read7BitEncodedInt();

        return new BlockEntry
        {
            Key = key,
            Offset = binaryReader.Offset,
            Length = valueLength
        };
    }

    public ReadOnlyMemory<byte> DecodeValue(ReadOnlyMemory<byte> data, int offset, int length)
    {
        return data.Slice(offset, length);
    }

    public Block Encode(IReadOnlyList<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> entries)
    {
        var size = entries.Sum(x => EstimateSize(x.Key, x.Value)) + sizeof(ushort);

        // This buffer can extend its memory dynamically using an ArrayPool<byte> as we keep
        // writing on it. Once the buffer is finalized we can copy its content to a locally 
        // allocated array that we are free to dispose when necessary.

        var buffer = new RecyclableArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(buffer);

        try
        {
            var offsets = new List<ushort>(entries.Count);

            foreach (var entry in entries)
            {
                offsets.Add((ushort)writer.BytesWritten);

                writer.Write7BitEncodedInt(entry.Key.Length);
                writer.WriteRaw(entry.Key.Span);

                writer.Write7BitEncodedInt(entry.Value.Length);
                writer.WriteRaw(entry.Value.Span);
            }

            foreach (var offset in offsets)
            {
                // 2 bytes for each offset (ushort)
                writer.WriteRaw(BitConverter.GetBytes(offset));
            }

            // 2 bytes for the number of elements
            writer.WriteRaw(BitConverter.GetBytes((ushort)offsets.Count));

            writer.Flush();

            var memory = buffer.GetCommittedMemory();

            var memoryOwner = MemoryPool<byte>.Shared.Rent(memory.Length);
            memory.CopyTo(memoryOwner.Memory);

            return new Block(this, memoryOwner, memory.Length, offsets);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public int EstimateSize(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        return 2 // key length
            + key.Length
            + 2 // value length
            + value.Length
            + 2 // offset
            ;
    }
}
