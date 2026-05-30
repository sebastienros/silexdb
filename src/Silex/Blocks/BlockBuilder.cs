using Silex.Buffers;
using Silex.Serialization;
using System.Diagnostics;

namespace Silex.Blocks;

public class BlockBuilder<TKey, TValue> : IDisposable
{
    private static readonly IBinaryEncoder<TKey> _keySerializer = BinaryEncoderFactory<TKey>.BinarySerializer;

    private readonly IBlockEncoder<TKey, TValue> _blockEncoder;
    private readonly List<BlockEntry<TValue>> _blockEntries = [];

    // Keys are encoded once, when they are added, and their bytes are reused both for the SST bloom
    // filter and for the final block encoding instead of being serialized a second time.
    private readonly PooledArrayBufferWriter<byte> _keyBuffer = new();

    private int _estimatedSize;
    private int _lastKeyOffset;
    private int _lastKeyLength;
    private bool _disposed;

    public BlockBuilder(IBlockEncoder<TKey, TValue> blockEncoder)
    {
        _blockEncoder = blockEncoder;
    }

    public void Clear()
    {
        _blockEntries.Clear();
        _keyBuffer.Clear();
        _estimatedSize = 0;
        _lastKeyOffset = 0;
        _lastKeyLength = 0;
    }

    /// <summary>
    /// Tries to add an entry to the block. If the new entry doesn't fit in the free space of 
    /// the block, and if the block already has entries then it will fail and return <see langword="false"/>.
    /// </summary>
    /// <param name="key">The key of the entry.</param>
    /// <param name="value">The value of the entry.</param>
    /// <returns><see langword="true"/> if the value was added, <see langword="false"/> otherwise.</returns>
    public bool Add(TKey key, TValue value)
    {
        var keyLength = _keySerializer.GetLength(key);
        var size = _blockEncoder.EstimateSize(keyLength, value);

        // If the block already has other entries and the next value doesn't fit, refuse it.
        // The size is checked before encoding so the key buffer never needs to be rolled back.
        if (_estimatedSize > 0 && _estimatedSize + size > _blockEncoder.BlockSize)
        {
            return false;
        }

        // If the block is new, accept any value size.

        var keyOffset = _keyBuffer.WrittenCount;
        var keyWriter = new EncoderBinaryWriter(_keyBuffer);
        _keySerializer.Encode(key, ref keyWriter);
        keyWriter.Flush();

        var actualKeyLength = _keyBuffer.WrittenCount - keyOffset;
        Debug.Assert(actualKeyLength == keyLength, $"Encoded key length {actualKeyLength} does not match the estimated length {keyLength}.");

        _blockEntries.Add(new BlockEntry<TValue>(keyOffset, actualKeyLength, value));
        _estimatedSize += size;
        _lastKeyOffset = keyOffset;
        _lastKeyLength = actualKeyLength;

        return true;    
    }

    /// <summary>
    /// Gets the encoded bytes of the key from the most recent successful <see cref="Add"/>.
    /// </summary>
    public ReadOnlySpan<byte> LastEncodedKey => _keyBuffer.WrittenMemory.Span.Slice(_lastKeyOffset, _lastKeyLength);

    public bool HasEntries => _blockEntries.Count > 0;

    public int EstimatedSize => _estimatedSize;

    public Block<TKey, TValue> BuildBlock()
    {
        return _blockEncoder.Encode(_keyBuffer.WrittenMemory, _blockEntries);
    }

    public Block<TKey, TValue> Decode(ReadOnlyMemory<byte> data)
    {
        return _blockEncoder.Decode(data);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);
        _keyBuffer.Dispose();

        _disposed = true;
    }
}
