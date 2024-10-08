namespace Silex.MemTables;

using MemTableRecord = KeyValuePair<Bytes, MemTableEntry>;

internal class MemTableRecordComparer : EqualityComparer<MemTableRecord>, IComparer<MemTableRecord>
{
    public static readonly MemTableRecordComparer Instance = new();

    public int Compare(MemTableRecord x, MemTableRecord y)
    {
        return x.Key.Span.SequenceCompareTo(y.Key.Span);
    }

    public override bool Equals(MemTableRecord x, MemTableRecord y)
    {
        return x.Key.Span.SequenceEqual(y.Key.Span);
    }

    public override int GetHashCode(MemTableRecord obj)
    {
        var hash = new HashCode();
        hash.AddBytes(obj.Key.Span);
        return hash.ToHashCode();
    }
}
