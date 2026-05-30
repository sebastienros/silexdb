namespace Silex.Blocks;

public class DefaultBlockEncoderFactory : IBlockEncoderFactory
{
    private readonly ushort _blockSize;

    public DefaultBlockEncoderFactory()
        : this((ushort)4.KiB())
    {
    }

    public DefaultBlockEncoderFactory(ushort blockSize)
    {
        if (blockSize == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(blockSize), blockSize, "Block size must be positive.");
        }

        _blockSize = blockSize;
    }

    public IBlockEncoder<TKey, TValue> Create<TKey, TValue>() => new DefaultBlockEncoder<TKey, TValue>(_blockSize);
}
