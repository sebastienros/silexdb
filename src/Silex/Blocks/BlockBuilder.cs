namespace Silex.Blocks;

public class BlockBuilder
{
    private readonly IBlockEncoder _blockEncoder;
    private readonly int _blockSizeBytes;
    private readonly List<BlockEntry> _blockEntries = [];
    
    public BlockBuilder(IBlockEncoder blockEncoder, ushort blockSizeBytes)
    {
        _blockEncoder = blockEncoder;
        _blockSizeBytes = blockSizeBytes;
    }

    public void Clear()
    {
        _blockEntries.Clear();
    }

    public void AddEntry(BlockEntry entry)
    {
        _blockEntries.Add(entry);
    }

    public Block BuildBlock()
    {
        var buffer = new RecyclableArrayBufferWriter<byte>();

        try
        {
            buffer.GetMemory(_blockSizeBytes);
            var block = _blockEncoder.Encode(buffer, _blockEntries);
            block.Data = buffer.GetCommittedMemory();
            return block;
        }
        finally
        {
            buffer.Dispose();
        }
    }
}
