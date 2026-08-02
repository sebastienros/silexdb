namespace Silex.Serialization;

using Silex.Buffers;

public interface IBinaryEncoder<T>
{
    int Encode(T value, ref EncoderBinaryWriter writer);
    T Decode(ReadOnlySpan<byte> data);
    int GetLength(T value);
    IComparer<T> Comparer => Comparer<T>.Default;
    IEqualityComparer<T> EqualityComparer => EqualityComparer<T>.Default;

    /// <summary>
    /// Attempts to expose the value's already-encoded bytes without copying. Returns <c>true</c> and sets
    /// <paramref name="bytes"/> to a borrow of the value's own memory when the encoded form is identical to
    /// that memory (for example <see cref="byte"/>[] or <see cref="ByteSlice"/>); returns <c>false</c> when the
    /// value must be encoded through <see cref="Encode"/> to obtain its bytes.
    /// </summary>
    bool TryGetRawBytes(T value, out ReadOnlySpan<byte> bytes)
    {
        bytes = default;
        return false;
    }
}
