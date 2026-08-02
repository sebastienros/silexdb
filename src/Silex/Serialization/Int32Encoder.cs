using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Silex.Serialization;

public sealed class Int32Encoder : IBinaryEncoder<int>
{    
    // Keys are encoded in an order-preserving form: the sign bit is flipped and the result is written
    // big-endian, so a plain bytewise (lexicographic) comparison of the encoded bytes matches the
    // signed numeric ordering of the values. This lets the engine compare keys as raw bytes.
    private const uint SignMask = 0x8000_0000u;

    public int Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(int));
        return (int)(BinaryPrimitives.ReadUInt32BigEndian(data) ^ SignMask);
    }

    public int GetLength(int value) => sizeof(int);

    public int Encode(int value, ref EncoderBinaryWriter writer)
    {
        var length = sizeof(int);
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt32BigEndian(span, (uint)value ^ SignMask);
        writer.WriteRaw(span);

        return length;
    }
}
