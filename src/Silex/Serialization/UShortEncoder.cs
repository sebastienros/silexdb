using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Silex.Serialization;

public sealed class UInt16Serializer : IBinaryEncoder<ushort>
{
    public ushort Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(ushort));
        return BinaryPrimitives.ReadUInt16LittleEndian(data);
    }

    public int GetLength(ushort value) => sizeof(ushort);

    public ushort GetTombstoneValue() => ushort.MaxValue;

    public bool IsTombstoneValue(ushort value) => value == ushort.MaxValue;

    public int Encode(ushort value, ref EncoderBinaryWriter writer)
    {
        var length = sizeof(ushort);
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt16LittleEndian(span, value);
        writer.WriteRaw(span);

        return length;
    }
}
