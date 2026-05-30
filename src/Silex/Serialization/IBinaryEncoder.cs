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
}
