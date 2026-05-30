using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Silex.Serialization;

public sealed class UInt32Encoder : IBinaryEncoder<uint>
{    
    // Encoded big-endian so a bytewise comparison of the encoded bytes matches the numeric ordering.
    public uint Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(uint));
        return BinaryPrimitives.ReadUInt32BigEndian(data);
    }

    public int GetLength(uint value) => sizeof(uint);

    public uint GetTombstoneValue() => uint.MaxValue;

    public bool IsTombstoneValue(uint value) => value == uint.MaxValue;

    public int Encode(uint value, ref EncoderBinaryWriter writer)
    {
        var length = sizeof(uint);
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt32BigEndian(span, value);
        writer.WriteRaw(span);

        return length;
    }
}
