namespace Silex.Blocks;

/// <summary>
/// Creates an <see cref="IBlockEncoder{T, U}"/> instance.
/// </summary>
public interface IBlockEncoderFactory
{
    public IBlockEncoder<TKey, TValue> Create<TKey, TValue>();
}
