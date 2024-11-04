using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Silex.Serialization;

public sealed class Int32Encoder : IBinaryEncoder<int>
{    
    public int Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(int));
        return BinaryPrimitives.ReadInt32LittleEndian(data);
    }

    public int GetLength(int value) => sizeof(int);

    public int GetTombstoneValue() => int.MaxValue;

    public bool IsTombstoneValue(int value) => value == int.MaxValue;

    public int Encode(int value, ref EncoderBinaryWriter writer)
    {
        var length = sizeof(int);
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteInt32LittleEndian(span, value);
        writer.WriteRaw(span);

        return length;
    }
}
