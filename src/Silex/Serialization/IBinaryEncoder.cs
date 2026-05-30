namespace Silex.Serialization;

using Silex.Buffers;

public interface IBinaryEncoder<T>
{
    int Encode(T value, ref EncoderBinaryWriter writer);
    T Decode(ReadOnlySpan<byte> data);
    int GetLength(T value);
    T GetTombstoneValue();
    bool IsTombstoneValue(T value);

    /// <summary>
    /// Returns a value that the engine can own independently of the caller's memory.
    /// </summary>
    /// <remarks>
    /// The default returns the value unchanged, which is correct for value types and immutable
    /// reference types (e.g. <see cref="int"/>, <see cref="string"/>). Encoders backed by mutable
    /// or pooled memory (e.g. <see cref="byte"/>[], <see cref="Bytes"/>) must return an independent
    /// copy so the engine never aliases caller memory on writes and callers never receive a reference
    /// to engine-owned memory on reads.
    /// </remarks>
    T Copy(T value) => value;
    IComparer<T> Comparer => Comparer<T>.Default;
    IEqualityComparer<T> EqualityComparer => EqualityComparer<T>.Default;
}
