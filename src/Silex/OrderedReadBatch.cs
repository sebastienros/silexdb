using System.Buffers;

namespace Silex;

/// <summary>Owns entry payloads until disposed, independently of its cursor and storage.</summary>
/// <remarks>Do not dispose while using returned memory. PayloadBytes excludes descriptors and pool slack.</remarks>
public sealed class OrderedReadBatch : IDisposable
{
    private byte[]? _bytes;
    private Entry[]? _entries;
    private int _capacity;
    private readonly int _maxEntries;
    private readonly int _maxBytes;

    internal OrderedReadBatch(int maxEntries, int maxBytes)
    {
        _maxEntries = maxEntries;
        _maxBytes = maxBytes;
        _capacity = Math.Min(16, maxEntries);
        _entries = ArrayPool<Entry>.Shared.Rent(_capacity);
        _bytes = ArrayPool<byte>.Shared.Rent(Math.Min(256, maxBytes));
    }

    public int Count { get; private set; }
    public int PayloadBytes { get; private set; }

    /// <summary>True only after traversal proves the range exhausted; a full batch may require another advance.</summary>
    public bool IsExhausted { get; internal set; }

    public KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>> this[int index]
    {
        get
        {
            ObjectDisposedException.ThrowIf(_bytes is null, this);
            ArgumentOutOfRangeException.ThrowIfNegative(index);
            if (index >= Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            var entry = _entries![index];
            return new(_bytes.AsMemory(entry.Offset, entry.KeyLength),
                _bytes.AsMemory(entry.Offset + entry.KeyLength, entry.ValueLength));
        }
    }

    internal bool Full { get; private set; }

    internal bool Add(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        var payloadLength = (long)key.Length + value.Length;
        if (payloadLength > _maxBytes - PayloadBytes)
        {
            if (Count == 0)
            {
                throw new ArgumentException($"Entry payload ({payloadLength} bytes) exceeds maxBytes ({_maxBytes}).", "maxBytes");
            }
            Full = true;
            return false;
        }
        var length = (int)payloadLength;
        if (Count == _capacity)
        {
            _capacity = (int)Math.Min((long)_capacity * 2, _maxEntries);
            var entries = ArrayPool<Entry>.Shared.Rent(_capacity);
            _entries.AsSpan(0, Count).CopyTo(entries);
            ArrayPool<Entry>.Shared.Return(_entries!);
            _entries = entries;
        }
        if (length > _bytes!.Length - PayloadBytes)
        {
            var size = (int)Math.Min(Math.Max((long)_bytes.Length * 2, (long)PayloadBytes + length), _maxBytes);
            var bytes = ArrayPool<byte>.Shared.Rent(size);
            _bytes.AsSpan(0, PayloadBytes).CopyTo(bytes);
            ArrayPool<byte>.Shared.Return(_bytes);
            _bytes = bytes;
        }
        key.CopyTo(_bytes.AsSpan(PayloadBytes));
        value.CopyTo(_bytes.AsSpan(PayloadBytes + key.Length));
        _entries![Count++] = new Entry(PayloadBytes, key.Length, value.Length);
        PayloadBytes += length;
        Full = Count == _maxEntries || PayloadBytes == _maxBytes;
        return !Full;
    }

    public void Dispose()
    {
        if (_bytes is { } bytes)
        {
            _bytes = null;
            ArrayPool<byte>.Shared.Return(bytes);
            ArrayPool<Entry>.Shared.Return(_entries!);
            _entries = null;
        }
    }

    private readonly record struct Entry(int Offset, int KeyLength, int ValueLength);
}
