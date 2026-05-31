using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Silex.Serialization;

public sealed class UInt64Encoder : IBinaryEncoder<ulong>
{
    // Encoded big-endian so a bytewise comparison of the encoded bytes matches the numeric ordering.
    public ulong Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(ulong));
        return BinaryPrimitives.ReadUInt64BigEndian(data);
    }

    public int GetLength(ulong value) => sizeof(ulong);

    public ulong GetTombstoneValue() => ulong.MaxValue;

    public bool IsTombstoneValue(ulong value) => value == ulong.MaxValue;

    public int Encode(ulong value, ref EncoderBinaryWriter writer)
    {
        var length = sizeof(ulong);
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt64BigEndian(span, value);
        writer.WriteRaw(span);

        return length;
    }
}
