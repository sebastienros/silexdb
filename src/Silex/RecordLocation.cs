namespace Silex;

using Silex.Serialization;

/// <summary>
/// Represents the information stored in a Block for a specific record.
/// It doesn't contain the value itself, but all the information to be able
/// to read the value from the block it is saved in.
/// </summary>
public readonly struct RecordLocation<TKey> : IComparable<RecordLocation<TKey>>
{
    private static readonly IComparer<TKey> _keyComparer = BinaryEncoderFactory<TKey>.BinarySerializer.Comparer;

    public string SsTableFilename { get; init; }
    public TKey Key { get; init; }
    public int BlockOffset { get; init; }
    public int Length { get; init; }
    public bool IsTombstone { get; init; }

    public int CompareTo(RecordLocation<TKey> other)
    {
        return _keyComparer.Compare(Key, other.Key);
    }
}
