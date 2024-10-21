using Silex.Collections;
using Silex.Tables;
using System.Runtime.CompilerServices;

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
internal sealed class MemTable : IMemTable
{
    private readonly SkipList<Bytes, Bytes> _map = new(Bytes.Comparer);
    private long _size;
    private bool _disposing;
    private readonly long _id;

    public MemTable(long id)
    {
        _id = id;
    }

    /// <summary>
    /// The identifier of the <see cref="MemTable"/>. Used for debugging purpose.
    /// </summary>
    public long Id => _id;

    /// <inheritdocs />
    public long Size => _size;

    /// <inheritdocs />
    public bool TryGet(Bytes key, out Bytes result)
    {
        ObjectDisposedException.ThrowIf(_disposing, this);

        if (_map.TryGetValue(key, out var memoryOwner))
        {
            result = (Bytes)memoryOwner.Memory;
            return true;
        }

        result = Bytes.Empty;
        return false;
    }

    /// <inheritdocs />
    public void Put(Bytes key, Bytes value)
    {
        ObjectDisposedException.ThrowIf(_disposing, this);

        // This method could be called concurrently, while the items in the store
        // and the size need to be consistent.
        // This also needs to handle when the same key is updated concurrently

        // Retrieve the previous value to keep its size consistent.
        if (_map.TryRemove(key, out var previousValue))
        {
            _size -= previousValue.Length + key.Length + sizeof(int);
            previousValue.Dispose();
        }

        _map.Add(key, value);
        _size += value.Length + key.Length + sizeof(int);
        return;
    }

    public IStorageIterator CreateIterator()
    {
        return new MemTableIterator(this);
    }

    public void Flush(SsTableBuilder builder)
    {
        foreach (var entry in _map)
        {
            builder.Add(entry.Key, entry.Value);
        }
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
            entry.Value.Dispose();
        }

        _map.Clear();
    }

    ~MemTable()
    {
        DisposeInternal();
    }

    private sealed class MemTableIterator : IStorageIterator
    {
        private readonly MemTable _table;
        
        public MemTableIterator(MemTable table)
        {
            _table = table;
        }

        public IAsyncEnumerable<RecordLocation> EnumerateAsync(CancellationToken cancellationToken = default)
        {
            return EnumerateAsync(Bytes.Empty, cancellationToken);
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async IAsyncEnumerable<RecordLocation> EnumerateAsync(Bytes afterKey, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            if (afterKey.IsEmpty)
            {
                foreach (var kvp in _table._map.Enumerate())
                {
                    yield return new RecordLocation { Key = kvp.Key, Length = kvp.Value.Memory.Length };
                }

                yield break;
            }

            foreach (var kvp in _table._map.Enumerate(afterKey))
            {
                yield return new RecordLocation { Key = kvp.Key, Length = kvp.Value.Memory.Length };
            }
        }
    }
}
