using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Silex.Serialization;

public sealed class Int64Encoder : IBinaryEncoder<long>
{    
    public long Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(int));
        return BinaryPrimitives.ReadInt64LittleEndian(data);
    }

    public int GetLength(long value) => sizeof(long);

    public long GetTombstoneValue() => long.MaxValue;

    public bool IsTombstoneValue(long value) => value == long.MaxValue;

    public int Encode(long value, ref EncoderBinaryWriter writer)
    {
        var length = sizeof(long);
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteInt64LittleEndian(span, value);
        writer.WriteRaw(span);

        return length;
    }
}
