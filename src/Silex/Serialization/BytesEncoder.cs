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

    public bool IsTombstoneValue(Bytes value) => value == Bytes.Empty;

    // Empty values (which include the tombstone) collapse to the canonical empty Bytes; non-empty
    // values are copied into a fresh pooled buffer so the engine never aliases caller-owned memory.
    public Bytes Copy(Bytes value) => value.IsEmpty ? Bytes.Empty : new Bytes(value.Span);

    public int Encode(Bytes value, ref EncoderBinaryWriter writer)
    {
        writer.WriteRaw(value.Span);
        return value.Length;
    }

}
