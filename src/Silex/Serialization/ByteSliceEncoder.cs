using Silex.Buffers;

namespace Silex.Serialization;

internal sealed class ByteSliceEncoder : IBinaryEncoder<ByteSlice>
{
    public IComparer<ByteSlice> Comparer => ByteSlice.Comparer;

    public ByteSlice Decode(ReadOnlySpan<byte> data)
    {
        throw new NotSupportedException($"{nameof(ByteSlice)} is a non-owning view. Decode from a memory-backed owner explicitly instead.");
    }

    public bool UsesEmptyTombstone => true;

    public bool TryGetRawBytes(ByteSlice value, out ReadOnlySpan<byte> bytes)
    {
        bytes = value.Span;
        return true;
    }

    public int GetLength(ByteSlice value) => value.Length;

    public ByteSlice GetTombstoneValue() => ByteSlice.Empty;

    public bool IsTombstoneValue(ByteSlice value) => value.IsEmpty;

    public int Encode(ByteSlice value, ref EncoderBinaryWriter writer)
    {
        writer.WriteRaw(value.Span);
        return value.Length;
    }

}
