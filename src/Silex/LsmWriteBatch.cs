namespace Silex;

/// <summary>
/// A reusable collection of byte-oriented mutations that can be written with one WAL write and flush.
/// </summary>
/// <remarks>
/// Keys and values are copied into contiguous reusable storage, so callers may reuse their input buffers
/// immediately without creating one managed array per key and value. A committed batch is one WAL recovery
/// unit: replay applies either every mutation or none of them.
/// </remarks>
public sealed class LsmWriteBatch
{
    private const int InitialByteCapacity = 256;
    private const int InitialEntryCapacity = 16;

    private byte[] _bytes = GC.AllocateUninitializedArray<byte>(InitialByteCapacity);
    private LsmWriteBatchEntry[] _entries = GC.AllocateUninitializedArray<LsmWriteBatchEntry>(InitialEntryCapacity);
    private int _byteCount;
    private int _count;

    /// <summary>
    /// Gets the number of mutations in this batch.
    /// </summary>
    public int Count => _count;

    /// <summary>
    /// Adds or replaces a value.
    /// </summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        var requiredBytes = checked(key.Length + value.Length);
        EnsureCapacity(requiredBytes);

        var keyOffset = _byteCount;
        key.CopyTo(_bytes.AsSpan(keyOffset));
        _byteCount += key.Length;

        var valueOffset = _byteCount;
        value.CopyTo(_bytes.AsSpan(valueOffset));
        _byteCount += value.Length;

        _entries[_count++] = new LsmWriteBatchEntry(
            keyOffset,
            key.Length,
            valueOffset,
            value.Length,
            IsTombstone: false);
    }

    /// <summary>
    /// Adds a deletion.
    /// </summary>
    public void Delete(ReadOnlySpan<byte> key)
    {
        EnsureCapacity(key.Length);

        var keyOffset = _byteCount;
        key.CopyTo(_bytes.AsSpan(keyOffset));
        _byteCount += key.Length;

        _entries[_count++] = new LsmWriteBatchEntry(
            keyOffset,
            key.Length,
            ValueOffset: 0,
            ValueLength: 0,
            IsTombstone: true);
    }

    /// <summary>
    /// Removes every mutation while retaining the buffers for reuse.
    /// </summary>
    public void Clear()
    {
        _byteCount = 0;
        _count = 0;
    }

    internal void GetEntry(
        int index,
        out ReadOnlySpan<byte> key,
        out ReadOnlySpan<byte> value,
        out bool isTombstone)
    {
        if ((uint)index >= (uint)_count)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        ref readonly var entry = ref _entries[index];
        key = _bytes.AsSpan(entry.KeyOffset, entry.KeyLength);
        value = _bytes.AsSpan(entry.ValueOffset, entry.ValueLength);
        isTombstone = entry.IsTombstone;
    }

    private void EnsureCapacity(int additionalBytes)
    {
        if (_count == _entries.Length)
        {
            var entries = GC.AllocateUninitializedArray<LsmWriteBatchEntry>(checked(_entries.Length * 2));
            _entries.AsSpan().CopyTo(entries);
            _entries = entries;
        }

        var requiredBytes = checked(_byteCount + additionalBytes);
        if (requiredBytes <= _bytes.Length)
        {
            return;
        }

        var bytes = GC.AllocateUninitializedArray<byte>(Math.Max(requiredBytes, checked(_bytes.Length * 2)));
        _bytes.AsSpan(0, _byteCount).CopyTo(bytes);
        _bytes = bytes;
    }
}

internal readonly record struct LsmWriteBatchEntry(
    int KeyOffset,
    int KeyLength,
    int ValueOffset,
    int ValueLength,
    bool IsTombstone);
