namespace Silex.Blocks;

using Silex;
using System;
using System.Buffers;
using System.Collections.Generic;
using static System.Runtime.InteropServices.JavaScript.JSType;

public class Block : IDisposable
{
    private readonly IBlockEncoder _encoder;
    private readonly IMemoryOwner<byte>? _memoryOwner;
    private bool _disposed;

    public Block(IBlockEncoder encoder, IMemoryOwner<byte> blockData, int length, IReadOnlyList<ushort> offsets)
    {
        _encoder = encoder;
        _memoryOwner = blockData;
        Memory = _memoryOwner.Memory[..length];
        Offsets = offsets;
    }

    public Block(IBlockEncoder encoder, ReadOnlyMemory<byte> blockData, int length, IReadOnlyList<ushort> offsets)
    {
        _encoder = encoder;
        _memoryOwner = null;
        Memory = blockData[..length];
        Offsets = offsets;
    }

    public ReadOnlyMemory<byte> Memory { get; }
    public IReadOnlyList<ushort> Offsets { get; }

    /// <summary>
    /// Returns a descriptor of a key/value in a block.
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    public RecordLocation GetEntry(int offset)
    {
        return _encoder.DecodeEntry(Memory, offset);
    }

    public ReadOnlySpan<byte> GetValue(Bytes key)
    {
        double start = 0;
        var end = Offsets.Count - 1;

        while (start <= end)
        {
            var m = (int)Math.Round((start + end) / 2);

            var entry = GetEntry(Offsets[m]);

            switch (Bytes.Comparer.Compare(key, entry.Key))
            {
                case 0:
                    return GetValue(entry);
                case > 0:
                    start = m + 1;
                    break;
                case < 0:
                    end = m - 1;
                    break;
            }
        }

        return default;
    }

    /// <summary>
    /// Returns a block of memory containing the value associated with the specified <see cref="RecordLocation"/>.
    /// </summary>
    /// <param name="entry"></param>
    /// <returns></returns>
    public ReadOnlySpan<byte> GetValue(RecordLocation entry)
    {
        return _encoder.DecodeValue(Memory, entry.BlockOffset, entry.Length).Span;
    }

    public void Dispose()
    {
        if (_memoryOwner == null)
        {
            return;
        }

        GC.SuppressFinalize(this);
        DisposeInternal();
    }

    private void DisposeInternal()
    {
        if (_disposed)
        {
            return;
        }

        _memoryOwner?.Dispose();
        _disposed = true;
    }

    ~Block()
    {
        if (_memoryOwner == null)
        {
            return;
        }

        DisposeInternal();
    }
}
