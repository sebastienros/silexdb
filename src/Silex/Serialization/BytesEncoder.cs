using Silex.Buffers;

namespace Silex.Serialization;

public sealed class BytesEncoder : IBinaryEncoder<Bytes>
{
    public IComparer<Bytes> Comparer => Bytes.Comparer;

    public Bytes Decode(ReadOnlySpan<byte> data)
    {
        return new Bytes(data);
    }

    public int GetLength(Bytes value) => value.Length;

    public Bytes GetTombstoneValue() => Bytes.Empty;

    public bool IsTombstoneValue(Bytes value) => value.IsEmpty;

    public int Encode(Bytes value, ref EncoderBinaryWriter writer)
    {
        writer.WriteRaw(value.Span);
        return value.Length;
    }

}
