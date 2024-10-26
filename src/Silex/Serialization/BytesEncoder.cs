using Silex.Buffers;
using System.Diagnostics;

namespace Silex.Serialization;

public sealed class BytesEncoder : IBinaryEncoder<Bytes>
{
    public IComparer<Bytes> Comparer => Bytes.Comparer;

    public Bytes Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(int));
        return new Bytes(data);
    }

    public int GetLength(Bytes value) => value.Length;

    public Bytes GetTombstoneValue() => Bytes.Empty;

    public bool IsTombstoneValue(Bytes value) => value == Bytes.Empty;

    public int Encode(Bytes value, ref EncoderBinaryWriter writer)
    {
        writer.WriteRaw(value.Span);
        return value.Length;
    }

}
