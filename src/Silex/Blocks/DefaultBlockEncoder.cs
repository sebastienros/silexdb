using Silex.Buffers;
using Silex.Serialization;
using System.Buffers;
using System.Buffers.Binary;

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
/// | key_len (7b) | key (keylen) | value_len_code (7b) | value (varlen) | ... |
/// -----------------------------------------------------------------------
/// 
/// Key length and value length are 7 bits encoded since each entry position is 
/// recorded in the offsets sections so we don't have to calculate them.
///  
/// We assume that keys will never be empty. A zero value length code is a tombstone,
/// <see cref="RecordValueEncoding.EmptyValueLengthCode"/> is a live empty value, and every other code
/// is the value's byte length.
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
internal sealed class DefaultBlockEncoder : IBlockEncoder
{
    private static readonly IBinaryEncoder<ByteSlice> _keySerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;
    private readonly ushort _blockSize;

    public DefaultBlockEncoder()
        : this((ushort)4.KiB())
    {
    }

    public DefaultBlockEncoder(ushort blockSize)
    {
        if (blockSize == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(blockSize), blockSize, "Block size must be positive.");
        }

        _blockSize = blockSize;
    }

    public ushort BlockSize => _blockSize;

    public Block Decode(ReadOnlyMemory<byte> buffer)
    {
        var count = ReadEntryCount(buffer, buffer.Length);

        var memoryOwner = MemoryPool<byte>.Shared.Rent(buffer.Length);
        buffer.CopyTo(memoryOwner.Memory);

        return new Block(this, memoryOwner, buffer.Length, count);
    }

    public Block Decode(IMemoryOwner<byte> owner, int length)
    {
        // The block bytes were already read into the owned buffer, so build the block over it directly
        // instead of renting a second buffer and copying. The offset section is read in place by the block.
        var count = ReadEntryCount(owner.Memory, length);

        return new Block(this, owner, length, count);
    }

    private static int ReadEntryCount(ReadOnlyMemory<byte> buffer, int length)
    {
        // The total number of entries is stored as the trailing ushort of the block.
        return BinaryPrimitives.ReadUInt16LittleEndian(buffer.Span.Slice(length - sizeof(ushort), sizeof(ushort)));
    }

    public RecordLocation DecodeEntry(ReadOnlyMemory<byte> data, int offset)
    {
        var binaryReader = new EncoderBinaryReader(data, offset);
        var keyLength = binaryReader.Read7BitEncodedInt();
        
        if (keyLength == 0)
        {
            throw new InvalidOperationException("Unexpected zero-length key was stored");
        }

        var key = ByteSlice.FromMemory(binaryReader.ReadBytesMemory(keyLength));
        var valueLength = RecordValueEncoding.DecodeLength(binaryReader.Read7BitEncodedInt(), out var isTombstone);

        return new RecordLocation
        {
            Key = key,
            BlockOffset = binaryReader.Offset,
            Length = isTombstone ? -1 : valueLength,
        };
    }

    public ReadOnlyMemory<byte> DecodeValue(ReadOnlyMemory<byte> data, int offset, int length)
    {
        return data.Slice(offset, length);
    }

    public Block Encode(ReadOnlyMemory<byte> encodedKeys, ReadOnlyMemory<byte> values, IReadOnlyList<BlockEntry> entries)
    {
        var size = 0;

        for (var i = 0; i < entries.Count; i++)
        {
            size += EstimateSize(entries[i].KeyLength, entries[i].StoredValueLength, entries[i].IsTombstone);
        }

        size += sizeof(ushort);

        // This buffer can extend its memory dynamically using an ArrayPool<byte> as we keep
        // writing on it. We pass it along as an IMemoryOwner so it can be disposed once used.

        var buffer = RecyclableMemoryStreamFactory.Shared.GetStream(tag: null, size);

        var writer = new EncoderBinaryWriter(buffer);

        var offsets = new List<ushort>(entries.Count);

        var keysSpan = encodedKeys.Span;
        var valuesSpan = values.Span;

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

            writer.Write7BitEncodedInt(RecordValueEncoding.EncodeLength(entry.StoredValueLength, entry.IsTombstone));
            writer.WriteRaw(valuesSpan.Slice(entry.ValueOffset, entry.StoredValueLength));
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
           
        return new Block(this, memory, (int)buffer.Length, offsets.Count);
    }

    public int EstimateSize(int encodedKeyLength, int valueLength, bool isTombstone)
    {
        return 2 // key length
            + encodedKeyLength
            + RecordValueEncoding.GetEncodedLengthSize(valueLength, isTombstone)
            + valueLength
            + 2 // offset
            ;
    }
}
