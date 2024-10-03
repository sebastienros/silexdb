namespace Silex.Blocks;

internal sealed class BlockIterator : IBlockIterator
{
    private readonly Block _block;

    private readonly BlockEntry[] _entries;

    public BlockIterator(Block block)
    {
        _block = block;
        _entries = new BlockEntry[_block.Offsets.Count];

        var index = 0;
        foreach (var offset in _block.Offsets)
        {
            _entries[index++] = _block.GetEntry(offset);
        }
    }

    public IEnumerable<BlockEntry> Enumerate()
    {
        return Enumerate(ReadOnlyMemory<byte>.Empty);
    }

    public IEnumerable<BlockEntry> Enumerate(ReadOnlyMemory<byte> afterKey)
    {
        var index = 0;

        if (!afterKey.IsEmpty)
        {
            var compare = Array.BinarySearch(_entries, new BlockEntry { Key = afterKey });

            index = compare >= 0 ? compare : ~compare;
        }

        while (true)
        {
            if (index >= _block.Offsets.Count)
            {
                yield break;
            }

            yield return _block.GetEntry(_block.Offsets[index++]);
        }
    }
}
