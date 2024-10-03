namespace Silex.Blocks;

public class Block
{
    private readonly IBlockEncoder _encoder;

    public Block(IBlockEncoder encoder)
    {
        _encoder = encoder;
    }

    public ReadOnlyMemory<byte> Data { get; set; }

    public IList<ushort> Offsets { get; } = [];

    public BlockEntry GetEntry(int offset)
    {
        return _encoder.DecodeEntry(Data, offset);
    }

    public ReadOnlyMemory<byte> GetValue(BlockEntry entry)
    {
        return _encoder.DecodeValue(Data, entry.Offset, entry.Length);
    }
}
