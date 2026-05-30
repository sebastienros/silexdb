using Silex.Collections;
using Silex.Serialization;
using Silex.Tables;
using Silex.Wal;
using System.Diagnostics;
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

    private volatile Dictionary<TKey, TValue>? _dic = new(_keySerializer.EqualityComparer);
    private volatile SortedDictionary<TKey, TValue>? _sorted;

    private long _size;
    private bool _disposed;
    private readonly long _id;
    private readonly WriteAheadLog<TKey, TValue>? _wal;

    public MemTable(long id, WriteAheadLog<TKey, TValue>? wal = null)
    {
        _id = id;
        _wal = wal;
    }

    /// <summary>
    /// The identifier of the <see cref="MemTable"/>. Used for debugging purpose.
    /// </summary>
    public long Id => _id;

    /// <inheritdocs />
    public long Size => _size;

    /// <inheritdocs />
    public int Count
    {
        get
        {
            var dic = _dic;
            return dic == null ? _sorted!.Count : dic.Count;
        }
    }

    /// <inheritdocs />
    public bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue result)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var dic = _dic;
        if (dic == null)
        {
            Debug.Assert(_sorted != null);

            if (_sorted.TryGetValue(key, out result))
            {
                return true;
            }
        }
        else
        {
            if (dic.TryGetValue(key, out result))
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
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Journal the mutation before applying it in memory so a crash can't leave an applied write
        // that isn't recoverable. The write lock held by the caller serializes appends.
        _wal?.Append(key, value);

        // This method could be called concurrently, while the items in the store
        // and the size need to be consistent.
        // This also needs to handle when the same key is updated concurrently

        var keyLength = _keySerializer.GetLength(key);

        var dic = _dic;
        if (dic != null)
        {
            // Retrieve the previous value to keep its size consistent.
            if (dic.Remove(key, out var previousValue))
            {
                _size -= _valueSerializer.GetLength(previousValue) + keyLength + sizeof(int);
            }

            dic.Add(key, value);
        }
        else
        {
            Debug.Assert(_sorted != null);
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

    public async Task FlushAsync(ISsTableBuilder<TKey, TValue> builder)
    {
        EnsureSortedMap();

        IDictionary<TKey, TValue> store = _dic != null ? _dic : _sorted;

        foreach (var entry in store)
        {
            await builder.AddAsync(entry.Key, entry.Value);
        }
    }

    [MemberNotNull(nameof(_sorted))]
    private void EnsureSortedMap()
    {
        var dic = _dic;

        if (dic == null)
        {
            Debug.Assert(_sorted != null);

            return;
        }

        lock (dic)
        {
            _sorted = new SortedDictionary<TKey, TValue>(dic, _keySerializer.Comparer);
            _dic = null;
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);
        DisposeInternal();

        // Close the write-ahead log handle (keeps the file). Done only on deterministic disposal, never
        // from the finalizer: an abandoned (crashed) memtable must leave its WAL on disk for recovery.
        _wal?.Dispose();

        _disposed = true;
    }

    private void DisposeInternal()
    {
        var dic = _dic;
        IDictionary<TKey, TValue> store = dic == null ? _sorted! : dic;

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
