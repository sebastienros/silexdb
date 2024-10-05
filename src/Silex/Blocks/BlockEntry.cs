namespace Silex.Blocks;

/// <summary>
/// Represents the information stored in a Block for a specific key/value.
/// It doesn't contain the value itself, but all the information to be able
/// to read the value from the block it is saved in.
/// </summary>
public readonly struct BlockEntry : IComparable<BlockEntry>
{
    public ReadOnlyMemory<byte> Key { get; init; }

    public int Offset { get; init; }
    public int Length { get; init; }

    public int CompareTo(BlockEntry other)
    {
        return ByteArrayComparer.Instance.Compare(Key, other.Key);
    }
}
