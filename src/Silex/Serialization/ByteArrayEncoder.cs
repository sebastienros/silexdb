using Silex.Buffers;

namespace Silex.Serialization;

public sealed class ByteArrayEncoder : IBinaryEncoder<byte[]>
{
    private static readonly Comparer _comparer = new();

    IComparer<byte[]> IBinaryEncoder<byte[]>.Comparer => _comparer;

    IEqualityComparer<byte[]> IBinaryEncoder<byte[]>.EqualityComparer => _comparer;

    public byte[] Decode(ReadOnlySpan<byte> data)
    {
        return data.ToArray();
    }

    public bool UsesEmptyTombstone => true;

    public bool TryGetRawBytes(byte[] value, out ReadOnlySpan<byte> bytes)
    {
        bytes = value;
        return true;
    }

    public int GetLength(byte[] value) => value.Length;

    public byte[] GetTombstoneValue() => Array.Empty<byte>();

    public bool IsTombstoneValue(byte[] value) => value.Length == 0;

    public int Encode(byte[] value, ref EncoderBinaryWriter writer)
    {
        writer.WriteRaw(value.AsSpan());
        return value.Length;
    }

    private sealed class Comparer : IComparer<byte[]>, IEqualityComparer<byte[]>
    {
        public int Compare(byte[]? x, byte[]? y)
        {
            if (x == y) return 0;
            if (x == null) return 1;
            if (y == null) return -1;
            
            return x.AsSpan().SequenceCompareTo(y.AsSpan());
        }

        public bool Equals(byte[]? x, byte[]? y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x == null || y == null) return false;

            return x.AsSpan().SequenceEqual(y.AsSpan());
        }

        public int GetHashCode(byte[] obj)
        {
            var hash = new HashCode();
            hash.AddBytes(obj);
            return hash.ToHashCode();
        }
    }
}
