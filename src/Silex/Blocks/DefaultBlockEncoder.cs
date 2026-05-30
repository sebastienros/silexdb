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

        var offsets = new ushort[totalEntries];

        for (var i = 0; i < totalEntries; i++)
        {
            offsets[i] = binaryReader.ReadUInt16();
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

    public Block<TKey, TValue> Encode(ReadOnlyMemory<byte> encodedKeys, IReadOnlyList<BlockEntry<TValue>> entries)
    {
        var size = 0;

        for (var i = 0; i < entries.Count; i++)
        {
            size += EstimateSize(entries[i].KeyLength, entries[i].Value);
        }

        size += sizeof(ushort);

        // This buffer can extend its memory dynamically using an ArrayPool<byte> as we keep
        // writing on it. We pass it along as an IMemoryOwner so it can be disposed once used.

        var buffer = RecyclableMemoryStreamFactory.Shared.GetStream(tag: null, size);

        var writer = new EncoderBinaryWriter(buffer);

        var offsets = new List<ushort>(entries.Count);

        var keysSpan = encodedKeys.Span;

        foreach (var entry in entries)
        {
            offsets.Add((ushort)writer.BytesWritten);

            if (entry.KeyLength <= 0)
            {
                throw new InvalidOperationException($"Invalid key length: {entry.KeyLength}");
            }

            writer.Write7BitEncodedInt(entry.KeyLength);

            // The key was already encoded when it was added to the block, so write its bytes as-is.
            writer.WriteRaw(keysSpan.Slice(entry.KeyOffset, entry.KeyLength));

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

        // The internal array could be kept if buffer was not disposed,
        // but by using MemoryPool and copy the value we ensure that all 
        // array allocations are effectively pooled.

        var memory = new MemoryStreamOwner(buffer);

        writer.Flush();
           
        return new Block<TKey, TValue>(this, memory, (int)buffer.Length, offsets);
    }

    public int EstimateSize(int encodedKeyLength, TValue value)
    {
        return 2 // key length
            + encodedKeyLength
            + 2 // value length
            + _valueSerializer.GetLength(value)
            + 2 // offset
            ;
    }
}
