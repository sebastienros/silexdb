namespace Silex.Blocks;

public class DefaultBlockEncoderFactory : IBlockEncoderFactory
{
    public IBlockEncoder<TKey, TValue> Create<TKey, TValue>() => new DefaultBlockEncoder<TKey, TValue>();
}
