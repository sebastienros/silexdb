using Silex.Collections;
using System.Buffers;

namespace Silex.MemTables;

/// <summary>
/// An instance of <see cref="MemTable"/> contains a sorted list of key value pairs of bytes to be stored.
/// The memory that is passed is not duplicated, but deallocated when not used anymore. The ownership is delegated.
/// </summary>
/// <remarks>
/// The current implementation is not thread-safe when writes are involved. Thread-safety is handled in <see cref="LsmStorageInner"/>
/// as it knows when a MemTable is frozen or used concurrently in read/write.
/// The dictionary supports multiple readers concurrently, as long as the collection is not modified, meaning the 
/// higher-level component needs to lock reads during writes.
/// 
/// A MemTable doesn't hold an entry that was read from the store. It is not a reads cache.
/// 
/// A MemTable usually has a size limit and it will be frozen to an immutable MemTable when it reaches the size limit.
/// This logic is part of <see cref="LsmStorageInner"/>.
/// </remarks>
internal sealed class MemTable : IDisposable, IMemTable
{
    private readonly SkipList<ReadOnlyMemory<byte>, MemTableEntry> _map = new(ByteArrayComparer.Instance);
    private long _size;
    private bool _disposing;
    private readonly int _id;

    public MemTable(int id)
    {
        _id = id;
    }

    /// <summary>
    /// The identifier of the <see cref="MemTable"/>. Used for debugging purpose.
    /// </summary>
    public int Id => _id;

    /// <inheritdocs />
    public long Size => _size;

    /// <inheritdocs />
    public bool TryGet(ReadOnlyMemory<byte> key, out ReadOnlyMemory<byte> result)
    {
        ObjectDisposedException.ThrowIf(_disposing, this);

        if (_map.TryGetValue(key, out var memoryOwner))
        {
            result = memoryOwner.Memory;
            return true;
        }

        result = ReadOnlyMemory<byte>.Empty;
        return false;
    }

    public void Put(ReadOnlyMemory<byte> key, Memory<byte> value)
    {
        Put(key, new MemoryOwner(value), value.Length);
    }

    /// <inheritdocs />
    public void Put(ReadOnlyMemory<byte> key, IMemoryOwner<byte> memoryOwner, int bufferSize)
    {
        ObjectDisposedException.ThrowIf(_disposing, this);

        // This method could be called concurrently, while the items in the store
        // and the size need to be consistent.
        // This also needs to handle when the same key is updated concurrently

        var memoryEntry = new MemTableEntry
        {
            MemoryOwner = memoryOwner,
            Size = bufferSize
        };

        // Retrieve the previous value to keep its size consistent.
        if (_map.TryRemove(key, out var previousValue))
        {
            _size -= previousValue.Size + key.Length;
            previousValue.MemoryOwner.Dispose();
        }

        _map.Add(key, memoryEntry);
        _size += bufferSize + key.Length;
        return;
    }

    public void Dispose()
    {
        if (_disposing)
        {
            return;
        }

        _disposing = true;

        GC.SuppressFinalize(this);
        DisposeInternal();
    }

    private void DisposeInternal()
    {
        foreach (var entry in _map)
        {
            entry.Value.MemoryOwner.Dispose();
        }

        _map.Clear();
    }

    public IEnumerable<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> Scan(ReadOnlyMemory<byte> minValue = default, ReadOnlyMemory<byte> maxValue = default)
    {
        // We don't handle concurrency, like for instance doing a snapshot of the keys. The caller is responsible for preventing writes while there is a
        // scan if it knows this MemTable is the mutable.
        // Use a read lock when for the mutable MemTable such that no other key is added while we clone it.
        // Deleted records need to be returned such that the merge iterator can decide if the entry should
        // be skipped.

        // We can't use a Array.BinarySearch without needing to clone the keys in an array since SortedDictionary can't use a position based
        // index, only be enumerated. A SkipList would be faster.

        foreach (var entry in _map)
        {
            if (!minValue.IsEmpty && minValue.Span.SequenceCompareTo(entry.Key.Span) > 0)
            {
                continue;
            }

            if (!maxValue.IsEmpty && maxValue.Span.SequenceCompareTo(entry.Key.Span) < 0)
            {
                // No more elements since the list is ordered
                yield break;
            }

            yield return new(entry.Key, entry.Value.Memory);
        }
    }

    ~MemTable()
    {
        DisposeInternal();
    }
}
