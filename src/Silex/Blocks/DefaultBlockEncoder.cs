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
/// 
/// When the encoder writes key windows (<see cref="WritesKeyWindows"/>), a key-window index is inserted
/// between the data section and the offset section, and the high bit of num_of_elements
/// (<see cref="BlockKeyWindows.CountFlag"/>) is set:
/// 
/// --------------------------------------------------------------------------------------------------
/// | Data Section | shared_prefix | prefix_len (1 byte) | Window #1 | ... | Window #N | Offsets | Extra |
/// --------------------------------------------------------------------------------------------------
/// 
/// shared_prefix is the prefix every key of the block starts with (at most 255 bytes) and each window is
/// a little-endian ulong holding the eight key bytes that follow it, read big-endian and zero-padded (see
/// <see cref="BlockKeyWindows"/>). Blocks without the flag use the original layout, so both kinds are
/// readable by the same code. SSTs that may contain flagged blocks use a newer footer version
/// (<see cref="Tables.SsTableFormat.GetFooterVersion"/>) so that older readers reject them.
/// </summary>
internal sealed class DefaultBlockEncoder : IBlockEncoder
{
    private static readonly IBinaryEncoder<ByteSlice> _keySerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;
    private readonly ushort _blockSize;
    private readonly bool _writeKeyWindows;

    public DefaultBlockEncoder()
        : this((ushort)4.KiB())
    {
    }

    public DefaultBlockEncoder(ushort blockSize, bool writeKeyWindows = false)
    {
        if (blockSize == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(blockSize), blockSize, "Block size must be positive.");
        }

        _blockSize = blockSize;
        _writeKeyWindows = writeKeyWindows;
    }

    public ushort BlockSize => _blockSize;

    public bool WritesKeyWindows => _writeKeyWindows;

    public Block Decode(ReadOnlyMemory<byte> buffer)
    {
        var count = ReadEntryCount(buffer.Span, buffer.Length, out var hasKeyWindows);

        var memoryOwner = MemoryPool<byte>.Shared.Rent(buffer.Length);
        buffer.CopyTo(memoryOwner.Memory);

        return new Block(this, memoryOwner, buffer.Length, count, hasKeyWindows);
    }

    public Block Decode(IMemoryOwner<byte> owner, int length)
    {
        // The block bytes were already read into the owned buffer, so build the block over it directly
        // instead of renting a second buffer and copying. The offset section is read in place by the block.
        var count = ReadEntryCount(owner.Memory.Span, length, out var hasKeyWindows);

        return new Block(this, owner, length, count, hasKeyWindows);
    }

    /// <summary>
    /// Reads the number of entries stored as the trailing ushort of a block, and whether the block carries a
    /// key-window index (flagged in the high bit of that ushort).
    /// </summary>
    internal static int ReadEntryCount(ReadOnlySpan<byte> block, int length, out bool hasKeyWindows)
    {
        var raw = BinaryPrimitives.ReadUInt16LittleEndian(block.Slice(length - sizeof(ushort), sizeof(ushort)));
        hasKeyWindows = (raw & BlockKeyWindows.CountFlag) != 0;
        return raw & ~BlockKeyWindows.CountFlag;
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

    public void DecodeRawEntry(
        ReadOnlyMemory<byte> data,
        int offset,
        out ReadOnlyMemory<byte> key,
        out ReadOnlyMemory<byte> value,
        out bool isTombstone)
    {
        var reader = new EncoderBinaryReader(data, offset);
        var keyLength = reader.Read7BitEncodedInt();
        key = reader.ReadBytesMemory(keyLength);
        var valueLength = RecordValueEncoding.DecodeLength(reader.Read7BitEncodedInt(), out isTombstone);
        value = reader.ReadBytesMemory(valueLength);
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

        var count = offsets.Count;
        var hasKeyWindows = _writeKeyWindows && count > 0;
        if (hasKeyWindows)
        {
            if (count >= BlockKeyWindows.CountFlag)
            {
                throw new InvalidOperationException($"A block with a key-window index cannot hold {count} entries.");
            }

            var firstKey = keysSpan.Slice(entries[0].KeyOffset, entries[0].KeyLength);
            var lastKey = keysSpan.Slice(entries[count - 1].KeyOffset, entries[count - 1].KeyLength);
            var prefixLength = BlockKeyWindows.GetPrefixLength(firstKey, lastKey);

            writer.WriteRaw(firstKey[..prefixLength]);
            writer.Write((byte)prefixLength);

            foreach (var entry in entries)
            {
                writer.WriteUInt64(BlockKeyWindows.GetWindow(keysSpan.Slice(entry.KeyOffset, entry.KeyLength), prefixLength));
            }
        }

        foreach (var offset in offsets)
        {
            // 2 bytes for each offset (ushort)
            writer.WriteUInt16(offset);
        }

        // 2 bytes for the number of elements, flagged when the block carries a key-window index
        writer.WriteUInt16((ushort)(hasKeyWindows ? count | BlockKeyWindows.CountFlag : count));

        // The internal array could be kept if buffer was not disposed,
        // but by using MemoryPool and copy the value we ensure that all 
        // array allocations are effectively pooled.

        // GetBuffer may promote the stream; commit pending bytes before it copies them.
        writer.Flush();

        var memory = new MemoryStreamOwner(buffer);

        return new Block(this, memory, (int)buffer.Length, count, hasKeyWindows);
    }

    public int EstimateSize(int encodedKeyLength, int valueLength, bool isTombstone)
    {
        return 2 // key length
            + encodedKeyLength
            + RecordValueEncoding.GetEncodedLengthSize(valueLength, isTombstone)
            + valueLength
            + 2 // offset
            + (_writeKeyWindows ? BlockKeyWindows.WindowLength : 0)
            ;
    }
}
