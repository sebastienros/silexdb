using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Silex.Serialization;

public sealed class UInt16Serializer : IBinaryEncoder<ushort>
{
    // Encoded big-endian so a bytewise comparison of the encoded bytes matches the numeric ordering.
    public ushort Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(ushort));
        return BinaryPrimitives.ReadUInt16BigEndian(data);
    }

    public int GetLength(ushort value) => sizeof(ushort);

    public int Encode(ushort value, ref EncoderBinaryWriter writer)
    {
        var length = sizeof(ushort);
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt16BigEndian(span, value);
        writer.WriteRaw(span);

        return length;
    }
}
