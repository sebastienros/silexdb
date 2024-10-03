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
/// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
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
        var block = new Block(this);
        block.Data = buffer;

        var span = buffer.Span;

        // Read the last two bytes
        var totalEntries = BitConverter.ToUInt16(span.Slice(buffer.Span.Length - 2, 2));

        // Read the offsets position
        var offsetPosition = buffer.Length - (totalEntries + 1) * 2;
        var current = offsetPosition;

        for (var i = 0; i < totalEntries; i++)
        {
            var bytes = span.Slice(current, 2);
            block.Offsets.Add(BitConverter.ToUInt16(bytes));
            current += 2;
        }

        return block;
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
        var binaryReader = new EncoderBinaryReader(data, offset);
        return data.Slice(offset, length);
    }

    public Block Encode(IBufferWriter<byte> buffer, IReadOnlyList<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> entries)
    {
        var block = new Block(this);

        var writer = new EncoderBinaryWriter(buffer);

        foreach (var entry in entries)
        {
            block.Offsets.Add((ushort)writer.Offset);

            writer.Write7BitEncodedInt(entry.Key.Length);
            writer.WriteRaw(entry.Key.Span);

            writer.Write7BitEncodedInt(entry.Value.Length);
            writer.WriteRaw(entry.Value.Span);
        }

        foreach (var offset in block.Offsets)
        {
            // 2 bytes for each offset (ushort)
            writer.WriteRaw(BitConverter.GetBytes(offset));
        }

        // 2 bytes for the number of elements
        writer.WriteRaw(BitConverter.GetBytes((ushort)block.Offsets.Count));

        writer.Flush();
        return block;
    }
}
