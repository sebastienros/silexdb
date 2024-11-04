using Silex.Serialization;
using System.Buffers;

namespace Silex.Blocks;

public class Block<TKey, TValue> : IDisposable
{
    private static readonly IComparer<TKey> _keyComparer = BinaryEncoderFactory<TKey>.BinarySerializer.Comparer;

    private readonly IBlockEncoder<TKey, TValue> _encoder;
    private readonly IMemoryOwner<byte>? _memoryOwner;
    private bool _disposed;

    public Block(IBlockEncoder<TKey, TValue> encoder, IMemoryOwner<byte> blockData, int length, IReadOnlyList<ushort> offsets)
    {
        _encoder = encoder;
        _memoryOwner = blockData;
        Memory = _memoryOwner.Memory[..length];
        Offsets = offsets;
    }

    public Block(IBlockEncoder<TKey, TValue> encoder, ReadOnlyMemory<byte> blockData, int length, IReadOnlyList<ushort> offsets)
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
    public RecordLocation<TKey> GetEntry(int offset)
    {
        return _encoder.DecodeEntry(Memory, offset);
    }

    public ReadOnlySpan<byte> GetValue(TKey key)
    {
        double start = 0;
        var end = Offsets.Count - 1;

        while (start <= end)
        {
            var m = (int)Math.Round((start + end) / 2);

            var entry = GetEntry(Offsets[m]);

            switch (_keyComparer.Compare(key, entry.Key))
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
    public ReadOnlySpan<byte> GetValue(RecordLocation<TKey> entry)
    {
        return _encoder.DecodeValue(Memory, entry.BlockOffset, entry.Length).Span;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);
        DisposeInternal();

        _disposed = true;
    }

    private void DisposeInternal()
    {
        _memoryOwner?.Dispose();
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
