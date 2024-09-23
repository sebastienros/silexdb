namespace Silex;

public class ByteArrayComparer : EqualityComparer<ReadOnlyMemory<byte>>
{
    public static readonly ByteArrayComparer Instance = new();

    public override bool Equals(ReadOnlyMemory<byte> first, ReadOnlyMemory<byte> second)
    {
        if (first.Length != second.Length)
        {
            return false;
        }

        return first.Span.SequenceEqual(second.Span);
    }

    public override int GetHashCode(ReadOnlyMemory<byte> obj)
    {
        var hash = new HashCode();
        hash.AddBytes(obj.Span);
        return hash.ToHashCode();
    }
}