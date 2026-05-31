namespace Silex.Blocks;

/// <summary>
/// Creates an <see cref="IBlockEncoder{T}"/> instance.
/// </summary>
public interface IBlockEncoderFactory
{
    public IBlockEncoder<TKey> Create<TKey>();
}
