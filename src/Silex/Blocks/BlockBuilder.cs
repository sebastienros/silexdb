namespace Silex.Blocks;
public class BlockBuilder
{
    private readonly IBlockEncoder _blockEncoder;
    private readonly List<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> _blockEntries = [];
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

    public void AddEntry(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value)
    {
        _blockEntries.Add(new(key, value));
        _estimatedSize += _blockEncoder.EstimateSize(key, value);
    }

    public bool HasEntries => _blockEntries.Count > 0;

    public int EstimatedSize => _estimatedSize;

    public Block BuildBlock()
    {
        return _blockEncoder.Encode(_blockEntries);
    }
}
