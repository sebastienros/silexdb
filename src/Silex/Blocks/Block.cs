namespace Silex.Blocks;

using System;
using System.Buffers;
using System.Collections.Generic;

public class Block : IDisposable
{
    private readonly IBlockEncoder _encoder;
    private readonly IMemoryOwner<byte> _blockData;
    private bool _disposed;

    public Block(IBlockEncoder encoder, IMemoryOwner<byte> blockData, int length, IReadOnlyList<ushort> offsets)
    {
        _encoder = encoder;
        _blockData = blockData;
        Memory = _blockData.Memory[..length];
        Offsets = offsets;
    }

    public ReadOnlyMemory<byte> Memory { get; }
    public IReadOnlyList<ushort> Offsets { get; }

    /// <summary>
    /// Returns a descriptor of a key/value in a block.
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    public BlockEntry GetEntry(int offset)
    {
        return _encoder.DecodeEntry(Memory, offset);
    }

    /// <summary>
    /// Returns a block of memory containing the value associated with the specified <see cref="BlockEntry"/>.
    /// </summary>
    /// <param name="entry"></param>
    /// <returns></returns>
    public ReadOnlySpan<byte> GetValue(BlockEntry entry)
    {
        return _encoder.DecodeValue(Memory, entry.Offset, entry.Length).Span;
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        DisposeInternal();
    }

    private void DisposeInternal()
    {
        if (_disposed)
        {
            return;
        }

        _blockData.Dispose();

        _disposed = true;
    }

    ~Block()
    {
        DisposeInternal();
    }
}
