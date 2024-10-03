namespace Silex.Blocks;

public interface IBlockIterator
{
    IEnumerable<BlockEntry> Enumerate();

    IEnumerable<BlockEntry> Enumerate(ReadOnlyMemory<byte> afterKey);
}
