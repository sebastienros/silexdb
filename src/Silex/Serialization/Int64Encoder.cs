using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Silex.Serialization;

public sealed class Int64Encoder : IBinaryEncoder<long>
{    
    // Keys are encoded in an order-preserving form: the sign bit is flipped and the result is written
    // big-endian, so a plain bytewise (lexicographic) comparison of the encoded bytes matches the
    // signed numeric ordering of the values. This lets the engine compare keys as raw bytes.
    private const ulong SignMask = 0x8000_0000_0000_0000ul;

    public long Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(long));
        return (long)(BinaryPrimitives.ReadUInt64BigEndian(data) ^ SignMask);
    }

    public int GetLength(long value) => sizeof(long);

    public long GetTombstoneValue() => long.MaxValue;

    public bool IsTombstoneValue(long value) => value == long.MaxValue;

    public int Encode(long value, ref EncoderBinaryWriter writer)
    {
        var length = sizeof(long);
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt64BigEndian(span, (ulong)value ^ SignMask);
        writer.WriteRaw(span);

        return length;
    }
}
