using Silex.Collections;
using Silex.Serialization;
using Silex.Tables;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Silex.MemTables;

/// <summary>
/// An instance of <see cref="MemTable"/> contains a sorted list of key value pairs of bytes to be stored.
/// The default collection is a dictionary, and mutates to a custom implementation of <see cref="SortedDictionary{TKey, TValue}"/> once
/// the table is enumerated. We use a custom implementation in order to add Enumerate(from, to) without needing to 
/// clone the keys collection.
/// </summary>
/// <remarks>
/// The current implementation is not thread-safe when writes are involved. Thread-safety is handled in <see cref="LsmStorageInner"/>
/// as it knows when a MemTable is frozen or used concurrently in read/write.
/// The dictionary supports multiple concurrent readers, as long as the collection is not modified, meaning the 
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

    private Dictionary<TKey, TValue>? _dic = new(_keySerializer.EqualityComparer);
    private SortedDictionary<TKey, TValue>? _sorted;

    private long _size;
    private bool _disposing;
    private readonly long _id;

    [MemberNotNullWhen(true, nameof(_sorted))]
    [MemberNotNullWhen(false, nameof(_dic))]
    private bool IsSortedDictionary => _sorted != null;

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

        if (_dic != null)
        {
            if (_dic.TryGetValue(key, out result))
            {
                return true;
            }
        }
        else
        {
            if (_sorted!.TryGetValue(key, out result))
            {
                return true;
            }
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

        if (!IsSortedDictionary)
        {
            // Retrieve the previous value to keep its size consistent.
            if (_dic.Remove(key, out var previousValue))
            {
                _size -= _valueSerializer.GetLength(previousValue) + keyLength + sizeof(int);
            }

            _dic.Add(key, value);
        }
        else
        {
            // Retrieve the previous value to keep its size consistent.
            if (_sorted.Remove(key, out var previousValue))
            {
                _size -= _valueSerializer.GetLength(previousValue) + keyLength + sizeof(int);
            }

            _sorted.Add(key, value);
        }

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

        IDictionary<TKey, TValue> store = _dic != null ? _dic : _sorted!;

        foreach (var entry in store)
        {
            builder.Add(entry.Key, entry.Value);
        }
    }

    [MemberNotNull(nameof(_sorted))]
    private void EnsureSortedMap()
    {
        if (IsSortedDictionary)
        {
            return;
        }

        var dic = _dic;
        _sorted = new SortedDictionary<TKey, TValue>(dic, _keySerializer.Comparer);
        _dic = null;
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
        IDictionary<TKey, TValue> store = IsSortedDictionary ? _sorted : _dic;

        foreach (var entry in store)
        {
            if (entry.Value is IDisposable d)
            {
                d.Dispose();
            }
        }

        store.Clear();
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
        public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            _table.EnsureSortedMap();

            // _map is a SortedDictionary at this point

            foreach (var entry in _table._sorted)
            {
                yield return entry;
            }
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(TKey afterKey, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            _table.EnsureSortedMap();

            var items = _table._sorted.Enumerate(afterKey, default!, true, false);

            foreach (var item in items)
            {
                yield return item;
            }
        }
    }
}
