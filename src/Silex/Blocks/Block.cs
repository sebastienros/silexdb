using Silex.Buffers;
using Silex.Serialization;
using System.Buffers;

namespace Silex.Blocks;

public class Block<TKey, TValue> : IDisposable
{
    private static readonly IComparer<TKey> _keyComparer = BinaryEncoderFactory<TKey>.BinarySerializer.Comparer;

    private readonly IBlockEncoder<TKey, TValue> _encoder;
    private readonly IMemoryOwner<byte>? _memoryOwner;
    private bool _disposed;

    public Block(IBlockEncoder<TKey, TValue> encoder, IMemoryOwner<byte> blockData, int length, int count)
    {
        _encoder = encoder;
        _memoryOwner = blockData;
        Memory = _memoryOwner.Memory[..length];
        Offsets = new BlockOffsets(Memory, length - (count + 1) * sizeof(ushort), count);
    }

    public Block(IBlockEncoder<TKey, TValue> encoder, ReadOnlyMemory<byte> blockData, int length, int count)
    {
        _encoder = encoder;
        _memoryOwner = null;
        Memory = blockData[..length];
        Offsets = new BlockOffsets(Memory, length - (count + 1) * sizeof(ushort), count);
    }

    public ReadOnlyMemory<byte> Memory { get; }
    public BlockOffsets Offsets { get; }

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
        return TryGetValue(key, out var value) ? value : default;
    }

    /// <summary>
    /// Looks up <paramref name="key"/> and reports whether it is present, distinguishing a genuine miss
    /// from a key stored with an empty value (a tombstone for empty-tombstone encoders).
    /// </summary>
    public bool TryGetValue(TKey key, out ReadOnlySpan<byte> value)
    {
        var start = 0;
        var end = Offsets.Count - 1;

        while (start <= end)
        {
            var m = start + (end - start) / 2;

            var entry = GetEntry(Offsets[m]);

            switch (_keyComparer.Compare(key, entry.Key))
            {
                case 0:
                    value = GetValue(entry);
                    return true;
                case > 0:
                    start = m + 1;
                    break;
                case < 0:
                    end = m - 1;
                    break;
            }
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Looks up a key by its already-encoded bytes, doing a binary search directly over the block bytes
    /// without materializing a <typeparamref name="TKey"/> per visited entry. This is the zero-allocation
    /// hot path for point lookups; it is correct because key encoders are order-preserving, so a bytewise
    /// comparison of encoded keys matches the typed key comparison. As with the typed overload, a returned
    /// empty span is a key stored with an empty value (a tombstone for empty-tombstone encoders).
    /// </summary>
    public bool TryGetValue(ReadOnlySpan<byte> encodedKey, out ReadOnlySpan<byte> value)
    {
        var memory = Memory;

        var start = 0;
        var end = Offsets.Count - 1;

        while (start <= end)
        {
            var m = start + (end - start) / 2;

            var reader = new EncoderBinaryReader(memory, Offsets[m]);
            var keyLength = reader.Read7BitEncodedInt();
            var entryKey = reader.ReadBytesSpan(keyLength);

            var cmp = encodedKey.SequenceCompareTo(entryKey);

            if (cmp == 0)
            {
                var valueLength = reader.Read7BitEncodedInt();
                value = reader.ReadBytesSpan(valueLength);
                return true;
            }

            if (cmp > 0)
            {
                start = m + 1;
            }
            else
            {
                end = m - 1;
            }
        }

        value = default;
        return false;
    }

    internal bool ForEachRaw<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, bool skipEmptyValues)
    {
        var memory = Memory;

        for (var i = 0; i < Offsets.Count; i++)
        {
            var blockReader = new EncoderBinaryReader(memory, Offsets[i]);
            var keyLength = blockReader.Read7BitEncodedInt();
            var key = blockReader.ReadBytesSpan(keyLength);
            var valueLength = blockReader.Read7BitEncodedInt();
            var value = blockReader.ReadBytesSpan(valueLength);

            if (skipEmptyValues && value.IsEmpty)
            {
                continue;
            }

            if (!reader(arg, key, value))
            {
                return false;
            }
        }

        return true;
    }

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
