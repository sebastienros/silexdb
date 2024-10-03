namespace Silex.Blocks;

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
