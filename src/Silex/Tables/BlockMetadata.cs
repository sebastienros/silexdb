namespace Silex.Tables;

public class BlockMetadata
{
    public int Index { get; set; }
    public long Offset { get; set; }
    public ReadOnlyMemory<byte> FirstKey { get; set; }
    public ReadOnlyMemory<byte> LastKey { get; set; }
}
