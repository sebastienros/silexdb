namespace Silex.Blocks;

using System;

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

    public bool Add(TKey key, TValue value)
    {
        var size = _blockEncoder.EstimateSize(key, value);

        // A fresh new block has to accept an entry even if it's over its max size
        if (_estimatedSize > 0 && _estimatedSize + size > _blockEncoder.BlockSize)
        {
            return false;
        }

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
