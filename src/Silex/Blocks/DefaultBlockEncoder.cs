using Silex.Buffers;
using Silex.Serialization;
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
public class DefaultBlockEncoder<TKey, TValue> : IBlockEncoder<TKey, TValue>
{
    private static readonly IBinaryEncoder<TKey> _keySerializer = BinaryEncoderFactory<TKey>.BinarySerializer;
    private static readonly IBinaryEncoder<TValue> _valueSerializer = BinaryEncoderFactory<TValue>.BinarySerializer;

    public ushort BlockSize => (ushort)4.KiB();

    public Block<TKey, TValue> Decode(ReadOnlyMemory<byte> buffer)
    {
        var binaryReader = new EncoderBinaryReader(buffer, 0);

        // Read the last two bytes
        binaryReader.Seek(buffer.Length - 2);
        var totalEntries = binaryReader.ReadUInt16();

        // Read the offsets position
        var offsetPosition = buffer.Length - (totalEntries + 1) * 2;
        binaryReader.Seek(offsetPosition);

        var offsets = new List<ushort>(totalEntries);

        for (var i = 0; i < totalEntries; i++)
        {
            offsets.Add(binaryReader.ReadUInt16());
        }

        var memoryOwner = MemoryPool<byte>.Shared.Rent(buffer.Length);
        buffer.CopyTo(memoryOwner.Memory);

        return new Block<TKey, TValue>(this, memoryOwner, buffer.Length, offsets);
    }

    public RecordLocation<TKey> DecodeEntry(ReadOnlyMemory<byte> data, int offset)
    {
        var binaryReader = new EncoderBinaryReader(data, offset);
        var keyLength = binaryReader.Read7BitEncodedInt();
        
        if (keyLength == 0)
        {
            throw new InvalidOperationException("Unexpected zero-length key was stored");

        }
        var keyData = binaryReader.ReadBytesSpan(keyLength);
        var key = _keySerializer.Decode(keyData);
        var valueLength = binaryReader.Read7BitEncodedInt();

        return new RecordLocation<TKey>
        {
            Key = key,
            BlockOffset = binaryReader.Offset,
            Length = valueLength
        };
    }

    public ReadOnlyMemory<byte> DecodeValue(ReadOnlyMemory<byte> data, int offset, int length)
    {
        return data.Slice(offset, length);
    }

    public Block<TKey, TValue> Encode(IReadOnlyList<KeyValuePair<TKey, TValue>> entries)
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

                writer.Write7BitEncodedInt(_keySerializer.GetLength(entry.Key));
                _keySerializer.Encode(entry.Key, ref writer);

                writer.Write7BitEncodedInt(_valueSerializer.GetLength(entry.Value));
                _valueSerializer.Encode(entry.Value, ref writer);
            }

            foreach (var offset in offsets)
            {
                // 2 bytes for each offset (ushort)
                writer.WriteUInt16(offset);
            }

            // 2 bytes for the number of elements
            writer.WriteUInt16((ushort)offsets.Count);

            writer.Flush();

            // The internal array could be kept if buffer was not disposed,
            // but by using MemoryPool and copy the value we ensure that all 
            // array allocations are effectively pooled.

            var memory = buffer.GetCommittedMemory();

            var memoryOwner = MemoryPool<byte>.Shared.Rent(memory.Length);
            memory.CopyTo(memoryOwner.Memory);

            return new Block<TKey, TValue>(this, memoryOwner, memory.Length, offsets);
        }
        finally
        {
            buffer.Dispose();
        }
    }

    public int EstimateSize(TKey key, TValue value)
    {
        return 2 // key length
            + _keySerializer.GetLength(key)
            + 2 // value length
            + _valueSerializer.GetLength(value)
            + 2 // offset
            ;
    }
}
