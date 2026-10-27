namespace Silex.Blocks;

internal sealed class DefaultBlockEncoderFactory : IBlockEncoderFactory
{
    private readonly ushort _blockSize;
    private readonly bool _writeKeyWindows;

    public DefaultBlockEncoderFactory()
        : this((ushort)4.KiB())
    {
    }

    public DefaultBlockEncoderFactory(ushort blockSize, bool writeKeyWindows = false)
    {
        if (blockSize == 0)
        {
            throw new ArgumentOutOfRangeException(nameof(blockSize), blockSize, "Block size must be positive.");
        }

        _blockSize = blockSize;
        _writeKeyWindows = writeKeyWindows;
    }

    public IBlockEncoder Create() => new DefaultBlockEncoder(_blockSize, _writeKeyWindows);
}
