namespace Silex.Serialization;

using Silex.Buffers;

public interface IBinaryEncoder<T>
{
    int Encode(T value, ref EncoderBinaryWriter writer);
    T Decode(ReadOnlySpan<byte> data);
    int GetLength(T value);
    T GetTombstoneValue();
    bool IsTombstoneValue(T value);
    IComparer<T> Comparer => Comparer<T>.Default;
    IEqualityComparer<T> EqualityComparer => EqualityComparer<T>.Default;

    /// <summary>
    /// When <c>true</c>, a zero-length encoded value is reserved as the tombstone marker and empty live
    /// values are not representable. This lets the raw read path treat any empty byte representation as a
    /// deletion without decoding. When <c>false</c> the encoder uses a sentinel tombstone value and the raw
    /// path must decode to recognise it.
    /// </summary>
    bool UsesEmptyTombstone => false;

    /// <summary>
    /// Attempts to expose the value's already-encoded bytes without copying. Returns <c>true</c> and sets
    /// <paramref name="bytes"/> to a borrow of the value's own memory when the encoded form is identical to
    /// that memory (for example <see cref="byte"/>[] or <see cref="Bytes"/>); returns <c>false</c> when the
    /// value must be encoded through <see cref="Encode"/> to obtain its bytes.
    /// </summary>
    bool TryGetRawBytes(T value, out ReadOnlySpan<byte> bytes)
    {
        bytes = default;
        return false;
    }
}
