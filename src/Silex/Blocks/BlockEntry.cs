namespace Silex.Blocks;

public readonly struct BlockEntry
{
    public ReadOnlyMemory<byte> Key { get; init; }

    public ReadOnlyMemory<byte> Value { get; init; }
}
