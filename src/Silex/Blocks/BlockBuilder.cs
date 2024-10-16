namespace Silex.Blocks;
using System;

public class BlockBuilder
{
    private readonly IBlockEncoder _blockEncoder;
    private readonly List<KeyValuePair<Bytes, Bytes>> _blockEntries = [];
    private int _estimatedSize;
    
    public BlockBuilder(IBlockEncoder blockEncoder)
    {
        _blockEncoder = blockEncoder;
    }

    public void Clear()
    {
        _blockEntries.Clear();
        _estimatedSize = 0;
    }

    public bool Add(Bytes key, Bytes value)
    {
        var size = _blockEncoder.EstimateSize(key, value);

        if (_estimatedSize + size > _blockEncoder.BlockSize)
        {
            return false;
        }

        _blockEntries.Add(new(key, value));
        _estimatedSize += size;

        return true;    
    }

    public bool HasEntries => _blockEntries.Count > 0;

    public int EstimatedSize => _estimatedSize;

    public Block BuildBlock()
    {
        return _blockEncoder.Encode(_blockEntries);
    }

    public Block Decode(ReadOnlyMemory<byte> data)
    {
        return _blockEncoder.Decode(data);
    }
}
