using Silex.Serialization;
using Silex.Tables;
using System.Diagnostics.CodeAnalysis;
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
internal sealed class MemTable<TKey, TValue> : IMemTable<TKey, TValue> where TKey : notnull
{
    private static readonly IBinaryEncoder<TKey> _keySerializer = BinaryEncoderFactory<TKey>.BinarySerializer;
    private static readonly IBinaryEncoder<TValue> _valueSerializer = BinaryEncoderFactory<TValue>.BinarySerializer;

    private IDictionary<TKey, TValue> _map = new Dictionary<TKey, TValue>(_keySerializer.EqualityComparer);

    private long _size;
    private bool _disposing;
    private bool _keysAreOrdered;
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
    public bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue result)
    {
        ObjectDisposedException.ThrowIf(_disposing, this);

        if (_map.TryGetValue(key, out result))
        {
            return true;
        }

        result = default!;
        return false;
    }

    /// <inheritdocs />
    public void Put(TKey key, TValue value)
    {
        ObjectDisposedException.ThrowIf(_disposing, this);

        // This method could be called concurrently, while the items in the store
        // and the size need to be consistent.
        // This also needs to handle when the same key is updated concurrently

        var keyLength = _keySerializer.GetLength(key);

        // Retrieve the previous value to keep its size consistent.
        if (_map.Remove(key, out var previousValue))
        {
            _size -= _valueSerializer.GetLength(previousValue) + keyLength + sizeof(int);
        }

        _map.Add(key, value);

        _size += _valueSerializer.GetLength(value) + keyLength + sizeof(int);
        return;
    }

    public IStorageIterator<TKey, TValue> CreateIterator()
    {
        return new MemTableIterator(this);
    }

    public void Flush(SsTableBuilder<TKey, TValue> builder)
    {
        EnsureSortedMap();

        foreach (var entry in _map)
        {
            builder.Add(entry.Key, entry.Value);
        }
    }

    private void EnsureSortedMap()
    {
        if (_keysAreOrdered)
        {
            return;
        }

        _map = new SortedDictionary<TKey, TValue>(_map, _keySerializer.Comparer);

        _keysAreOrdered = true;
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
            if (entry is IDisposable d)
            {
                d.Dispose();
            }
        }

        _map.Clear();
    }

    ~MemTable()
    {
        DisposeInternal();
    }

    private sealed class MemTableIterator : IStorageIterator<TKey, TValue>
    {
        private readonly MemTable<TKey, TValue> _table;
        
        public MemTableIterator(MemTable<TKey, TValue> table)
        {
            _table = table;
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async IAsyncEnumerable<RecordLocation<TKey>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            _table.EnsureSortedMap();

            foreach (var entry in _table._map)
            {
                yield return new RecordLocation<TKey> { Key = entry.Key, Length = 0, IsTombstone = _valueSerializer.IsTombstoneValue(entry.Value) };
            }
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async IAsyncEnumerable<RecordLocation<TKey>> EnumerateAsync(TKey afterKey, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            _table.EnsureSortedMap();

            // TODO: [PERF] Optimize the iteration by seeking the index
            // of the key without cloning the collection

            var orderedKeys = new List<TKey>(_table._map.Keys);
            
            var index = orderedKeys.BinarySearch(0, _table._map.Count, afterKey, _keySerializer.Comparer);

            if (index < 0)
            {
                index = ~index;
            }

            for (var i = index; i < orderedKeys.Count; i++)
            {
                var key = orderedKeys[i];

                // TODO: [PERF] Optimize the iteration by preventing
                // a lookup for each key

                if (_table._map.TryGetValue(key, out var value))
                {
                    yield return new RecordLocation<TKey> { Key = key, Length = 0, IsTombstone = _valueSerializer.IsTombstoneValue(value) };
                }
            }
        }
    }
}
