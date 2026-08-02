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
/// The default collection is a dictionary, and mutates to a custom implementation of <see cref="SortedDictionary{ByteSlice, ByteSlice}"/> once
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
internal sealed class MemTable : IMemTable, IRawBytesMemTable
{
    private static readonly IBinaryEncoder<ByteSlice> _keySerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;

    private volatile Dictionary<ByteSlice, ByteSlice>? _dic = new(_keySerializer.EqualityComparer);
    private volatile SortedDictionary<ByteSlice, ByteSlice>? _sorted;

    private long _size;
    private bool _disposed;
    private readonly long _id;
    private readonly WriteAheadLog? _wal;
    private readonly MemTableArena? _arena;

    public MemTable(long id, WriteAheadLog? wal = null, int arenaBlockSize = 32 * 1024)
    {
        _id = id;
        _wal = wal;
        _arena = new MemTableArena(arenaBlockSize);
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
    public bool TryGet(ByteSlice key, [MaybeNullWhen(false)] out ByteSlice result)
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
    public void Put(ByteSlice key, ByteSlice value)
    {
        if (value.IsTombstone)
        {
            DeleteRaw(key.Span);
        }
        else
        {
            PutRaw(key.Span, value.Span);
        }
    }

    public void PutRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        PutRawCore(key, value, isTombstone: false);
    }

    public void DeleteRaw(ReadOnlySpan<byte> key)
    {
        PutRawCore(key, default, isTombstone: true);
    }

    private void PutRawCore(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var arena = _arena ?? throw new ObjectDisposedException(nameof(MemTable));

        if (isTombstone)
        {
            _wal?.AppendDeleteRaw(key);
        }
        else
        {
            _wal?.AppendRaw(key, value);
        }

        var ownedKey = arena.Copy(key);
        var ownedValue = isTombstone ? ByteSlice.Tombstone : arena.Copy(value);

        var dic = _dic;
        if (dic != null)
        {
            if (dic.ContainsKey(ownedKey))
            {
                dic.Remove(ownedKey);
            }

            dic.Add(ownedKey, ownedValue);
        }
        else
        {
            Debug.Assert(_sorted != null);

            if (_sorted.ContainsKey(ownedKey))
            {
                _sorted.Remove(ownedKey);
            }

            _sorted.Add(ownedKey, ownedValue);
        }

        _size += key.Length + value.Length + sizeof(int);
    }

    public IStorageIterator CreateIterator()
    {
        return new MemTableIterator(this);
    }

    public async Task FlushAsync(ISsTableBuilder builder, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureSortedMap();

        IDictionary<ByteSlice, ByteSlice> store = _dic != null ? _dic : _sorted;

        foreach (var entry in store)
        {
            await builder.AddAsync(entry.Key, entry.Value, cancellationToken);
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
            _sorted = new SortedDictionary<ByteSlice, ByteSlice>(dic, _keySerializer.Comparer);
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
        IDictionary<ByteSlice, ByteSlice> store = dic == null ? _sorted! : dic;

        store.Clear();
        _arena?.Dispose();
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

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
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
        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(ByteSlice afterKey, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            _table.EnsureSortedMap();

            var items = _table._sorted.Enumerate(afterKey, default!, true, false);

            foreach (var item in items)
            {
                yield return item;
            }
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync([EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            _table.EnsureSortedMap();

            foreach (var item in _table._sorted.EnumerateBackwards(default!, default!, false, false))
            {
                yield return item;
            }
        }

#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(ByteSlice from, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
        {
            _table.EnsureSortedMap();

            foreach (var item in _table._sorted.EnumerateBackwards(default!, from, false, true))
            {
                yield return item;
            }
        }
    }
}
