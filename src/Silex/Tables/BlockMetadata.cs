namespace Silex.Tables;

public class BlockMetadata<TKey>
{
    public int Index { get; set; }
    public long Offset { get; set; }
    public required TKey FirstKey { get; set; }
    public required TKey LastKey { get; set; }
}
