using System.Buffers;

namespace Silex;

/// <summary>Owns the values of a bounded batched point read until disposed, independently of storage.</summary>
/// <remarks>
/// Results correspond by position to the requested keys. Do not dispose while using returned memory.
/// PayloadBytes counts found values only, excluding descriptors and pool slack.
/// </remarks>
public sealed class PointReadBatch : IDisposable
{
    private byte[]? _bytes;
    private Entry[]? _entries;
    private readonly int _maxBytes;
    private bool _overflow;

    internal PointReadBatch(int keyCount, int maxBytes)
    {
        _maxBytes = maxBytes;
        _entries = ArrayPool<Entry>.Shared.Rent(keyCount);
        _bytes = ArrayPool<byte>.Shared.Rent(Math.Min(256, maxBytes));
        RequestedCount = keyCount;
    }

    /// <summary>Gets the number of leading requested keys resolved by this batch.</summary>
    public int Count { get; private set; }

    /// <summary>Gets the number of requested keys, including any left for a follow-up call.</summary>
    public int RequestedCount { get; }

    /// <summary>Gets the number of value bytes held.</summary>
    public int PayloadBytes { get; private set; }

    /// <summary>True when every requested key was resolved; otherwise retry the remaining suffix.</summary>
    public bool IsComplete => Count == RequestedCount;

    /// <summary>Gets the value for the key at <paramref name="index"/>, or <c>false</c> when missing or deleted.</summary>
    public bool TryGetValue(int index, out ReadOnlyMemory<byte> value)
    {
        ObjectDisposedException.ThrowIf(_bytes is null, this);
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        if (index >= Count)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        var entry = _entries![index];
        if (entry.Length < 0)
        {
            value = default;
            return false;
        }

        value = _bytes.AsMemory(entry.Offset, entry.Length);
        return true;
    }

    internal bool Overflowed => _overflow;

    internal void AddMissing() => _entries![Count++] = new Entry(PayloadBytes, -1);

    internal void AddValue(ReadOnlySpan<byte> value)
    {
        if (value.Length > _maxBytes - PayloadBytes)
        {
            _overflow = true;
            return;
        }

        if (value.Length > _bytes!.Length - PayloadBytes)
        {
            var size = (int)Math.Min(Math.Max((long)_bytes.Length * 2, (long)PayloadBytes + value.Length), _maxBytes);
            var bytes = ArrayPool<byte>.Shared.Rent(size);
            _bytes.AsSpan(0, PayloadBytes).CopyTo(bytes);
            ArrayPool<byte>.Shared.Return(_bytes);
            _bytes = bytes;
        }

        value.CopyTo(_bytes.AsSpan(PayloadBytes));
        _entries![Count++] = new Entry(PayloadBytes, value.Length);
        PayloadBytes += value.Length;
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

    private readonly record struct Entry(int Offset, int Length);
}
