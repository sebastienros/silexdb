namespace Silex.Serialization;

using Silex.Buffers;

public sealed class ByteArrayEncoder : IBinaryEncoder<byte[]>
{
    private static readonly Comparer _comparer = new();

    IComparer<byte[]> IBinaryEncoder<byte[]>.Comparer => _comparer;

    public byte[] Decode(ReadOnlySpan<byte> data)
    {
        return data.ToArray();
    }

    public int GetLength(byte[] value) => value.Length;

    public byte[] GetTombstoneValue() => Array.Empty<byte>();

    public bool IsTombstoneValue(byte[] value) => value == Array.Empty<byte>();

    public int Encode(byte[] value, ref EncoderBinaryWriter writer)
    {
        writer.WriteRaw(value.AsSpan());
        return value.Length;
    }

    private sealed class Comparer : IComparer<byte[]>
    {
        public int Compare(byte[]? x, byte[]? y)
        {
            if (x == y) return 0;
            if (x == null) return 1;
            if (y == null) return -1;
            
            return x.AsSpan().SequenceCompareTo(y.AsSpan());
        }
    }
}
