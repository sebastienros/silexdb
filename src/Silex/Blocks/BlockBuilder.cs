namespace Silex.Blocks;

public class BlockBuilder<TKey, TValue>
{
    private readonly IBlockEncoder<TKey, TValue> _blockEncoder;
    private readonly List<KeyValuePair<TKey, TValue>> _blockEntries = [];
    private int _estimatedSize;

    public BlockBuilder(IBlockEncoder<TKey, TValue> blockEncoder)
    {
        _blockEncoder = blockEncoder;
    }

    public void Clear()
    {
        _blockEntries.Clear();
        _estimatedSize = 0;
    }

    /// <summary>
    /// Tries to add an entry to the block. If the new entry doesn't fit in the free space of 
    /// the block, and if the block already has entries then it will fail and return <see langword="false"/>.
    /// </summary>
    /// <param name="key">The key of the entry.</param>
    /// <param name="value">The value of the entry.</param>
    /// <returns><see langword="true"/> if the value was added, <see langword="false"/> otherwise.</returns>
    public bool Add(TKey key, TValue value)
    {
        var size = _blockEncoder.EstimateSize(key, value);

        // If the block already has other entries and the next value doesn't fit, refuse it.
        if (_estimatedSize > 0 && _estimatedSize + size > _blockEncoder.BlockSize)
        {
            return false;
        }

        // If the block is new, accept any value size.

        _blockEntries.Add(new(key, value));
        _estimatedSize += size;

        return true;    
    }

    public bool HasEntries => _blockEntries.Count > 0;

    public int EstimatedSize => _estimatedSize;

    public Block<TKey, TValue> BuildBlock()
    {
        return _blockEncoder.Encode(_blockEntries);
    }

    public Block<TKey, TValue> Decode(ReadOnlyMemory<byte> data)
    {
        return _blockEncoder.Decode(data);
    }
}
