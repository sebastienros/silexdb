namespace Silex;

public class ByteArrayComparer : EqualityComparer<ReadOnlyMemory<byte>>, IComparer<ReadOnlyMemory<byte>>
{
    public static readonly ByteArrayComparer Instance = new();

    public int Compare(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return x.Span.SequenceCompareTo(y.Span);
    }

    public override bool Equals(ReadOnlyMemory<byte> x, ReadOnlyMemory<byte> y)
    {
        return x.Span.SequenceEqual(y.Span);
    }

    public override int GetHashCode(ReadOnlyMemory<byte> obj)
    {
        var hash = new HashCode();
        hash.AddBytes(obj.Span);
        return hash.ToHashCode();
    }
}
