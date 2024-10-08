namespace Silex.Tables;

public class BlockMetadata
{
    public int Index { get; set; }
    public long Offset { get; set; }
    public Bytes FirstKey { get; set; }
    public Bytes LastKey { get; set; }
}
