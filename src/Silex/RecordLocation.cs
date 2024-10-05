namespace Silex;

/// <summary>
/// Represents the information stored in a Block for a specific record.
/// It doesn't contain the value itself, but all the information to be able
/// to read the value from the block it is saved in.
/// </summary>
public readonly struct RecordLocation : IComparable<RecordLocation>
{
    public string SsTableFilename { get; init; }
    public ReadOnlyMemory<byte> Key { get; init; }
    public int BlockOffset { get; init; }
    public int Length { get; init; }

    public int CompareTo(RecordLocation other)
    {
        return ByteArrayComparer.Instance.Compare(Key, other.Key);
    }
}
