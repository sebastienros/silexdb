using Microsoft.Extensions.Caching.Memory;
using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Buffers;
using Silex.MemTables;
using Silex.Serialization;
using Silex.Tables;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Silex;

/// <summary>
/// The inner storage engine. It handles thread-safety for the <see cref="StorageState"/>.
/// </summary>
internal sealed class LsmStorageInner<TKey, TValue> : IDisposable where TKey : notnull
{
    private static readonly IBinaryEncoder<TValue> _valueSerializer = BinaryEncoderFactory<TValue>.BinarySerializer;
    private static readonly IBinaryEncoder<TKey> _keySerializer= BinaryEncoderFactory<TKey>.BinarySerializer;
    private static readonly IComparer<TKey> _keyComparer = BinaryEncoderFactory<TKey>.BinarySerializer.Comparer;

    // Use different locks for each type of manipulated data such that we can lock them individually.
    // For instance updating the MemTable should be synchronized, but not blocked by compaction.
    // Moreover, some locks are asynchronous (level0) while other are synchronous (mem tables).

    private readonly ReaderWriterLockSlim _currentMemTableLock = new();
    private readonly ReaderWriterLockSlim _immutableMemTablesLock = new();
    private readonly AsyncReaderWriterLock _level0Lock = new();
    private readonly AsyncReaderWriterLock _leveledTablesLock = new();

    internal StorageState<TKey, TValue> _state;
    private bool _disposed;
    private readonly IBlockEncoder<TKey, TValue> _blockEncoder;
    private readonly ISsTableEncoder<TKey, TValue> _ssTableEncoder;
    private readonly ISsTableBuilderFactory _ssTableBuilderFactory;
    private readonly IBloomFilterFactory _bloomFilterFactory;
    private readonly long _memTableSizeLimit;
    private readonly IMemoryCache _blockCache;
    private readonly MemoryCacheEntryOptions _cacheEntryOptions;

    public string StoragePath { get; }

    /// <summary>
    /// Creates an empty new <see cref="LsmStorageInner"/> that will store its tables to the specified existing path.
    /// Any existing tables in the path are ignored, use <see cref="OpenAsync(string, StorageOptions, CancellationToken)"/> to load
    /// an existing folder instead.
    /// </summary>
    /// <param name="path"></param>
    /// <param name="options"></param>
    internal LsmStorageInner(string path, StorageOptions options)
    {
        StoragePath = path;
        _state = new StorageState<TKey, TValue>() { CurrentMemTable = new MemTable<TKey, TValue>(IdGenerator.GetNextId()) };
        _blockEncoder = options.BlockEncoderFactory.Create<TKey, TValue>();
        _ssTableEncoder = options.SsTableEncoderFactory.Create<TKey, TValue>();
        _ssTableBuilderFactory = options.SsTableBuilderFactory;
        _bloomFilterFactory = options.BloomFilterFactory;
        _memTableSizeLimit = options.MemTableSizeLimit;
        _blockCache = new MemoryCache(new MemoryCacheOptions { SizeLimit = options.BlockCacheSizeLimit });
        _cacheEntryOptions = new MemoryCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = options.BlockCacheAbsoluteExpiration == TimeSpan.Zero ? null : options.BlockCacheAbsoluteExpiration,
            SlidingExpiration = options.BlockCacheSlidingExpiration == TimeSpan.Zero ? null : options.BlockCacheSlidingExpiration
        };
    }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns><see cref="TValue"/> if the key was not found.</returns>
    public async ValueTask<TValue> GetAsync(TKey key, CancellationToken cancellationToken = default)
    {
        // The immutable MemTables can be accessed without lock since they are frozen (read-only),
        // and the collection won't be changed FreezeMemTable substitute the collection when it's altered.

        // Access the current and immutable MemTables in a read-lock to ensure no
        // other transaction is creating a new MemTable while we are reading variables. This is only
        // to ensure the mutable MemTable and immutable ones are coherent.

        _currentMemTableLock.EnterReadLock();

        var snapshot = _state.Clone();

        try
        {
            // CurrentMemTable is the only thing that needs to be locked
            // since all other collections are immutable
            if (snapshot.CurrentMemTable.TryGet(key, out var result))
            {
                return result;
            }
        }
        finally 
        { 
            _currentMemTableLock.ExitReadLock(); 
        }

        // If any new immutable MemTable(s) was created after this call then we just ignore it, as 
        // the newly created MemTable(s).

        try
        {
            _immutableMemTablesLock.EnterReadLock();

            foreach (var memTable in snapshot.ImmutableMemTables)
            {
                if (memTable.TryGet(key, out var result))
                {
                    return result;
                }
            }
        }
        finally
        {
            _immutableMemTablesLock.ExitReadLock();
        }

        // Process L0 tables in reverse order since the last one to be flush is at the end

        var keyLength = _keySerializer.GetLength(key);
        var bufferWriter = new PooledArrayBufferWriter<byte>(keyLength);
        var writer = new EncoderBinaryWriter(bufferWriter);
        _keySerializer.Encode(key, ref writer);
        var keyMemory = bufferWriter.WrittenMemory;
        
        try
        {
            await _level0Lock.EnterReadLockAsync();

            var l0 = snapshot.LevelZeroTables;
            
            // TODO: this can be parallelized, multiple table could return the value, but the one 
            // from the most recent table would be used.

            for (var i = l0.Count - 1; i >= 0; i--)
            {
                var table = l0[i];

                // The key could be in this table, if not will go to the next one
                if (_keyComparer.Compare(key, table.FirstKey) < 0 || _keyComparer.Compare(key, table.LastKey) > 0)
                {
                    continue;
                }

                // Check if the bloom filter tells us to skip this table
                if (!table.BloomFilter.Probe(keyMemory.Span))
                {
                    continue; 
                }

                foreach (var metadata in table.BlockMetadata)
                {
                    if (_keyComparer.Compare(key, metadata.FirstKey) < 0 || _keyComparer.Compare(key, metadata.LastKey) > 0)
                    {
                        continue;
                    }

                    var block = await table.ReadBlockCachedAsync(metadata.Index, _blockCache, _cacheEntryOptions, cancellationToken);

                    if (block != null)
                    {
                        var value = block.GetValue(key);
                                
                        if (!value.IsEmpty)
                        {
                            return _valueSerializer.Decode(value);
                        }
                    }
                }

                return default!;
            }
        }
        finally
        {
            // Return the key buffer
            bufferWriter.Dispose();

            _level0Lock.ExitReadLock();
        }

        return default!;
    }

    /// <summary>
    /// Puts a value with the specified key in the current <see cref="IMemTable">. If one already exists it is replaced.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    public void Put(TKey key, TValue value)
    {
        _currentMemTableLock.EnterWriteLock();

        try
        {
            _state.CurrentMemTable.Put(key, value);

            if (_state.CurrentMemTable.Size >= _memTableSizeLimit)
            {
                FreezeMemTable();
            }
        }
        finally
        {
            _currentMemTableLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Adds a delete operation for the specified key.
    /// </summary>
    /// <param name="key"></param>
    public void Delete(TKey key)
    {
        _currentMemTableLock.EnterWriteLock();

        try
        {
            _state.CurrentMemTable.Put(key, _valueSerializer.GetTombstoneValue());

            if (_state.CurrentMemTable.Size >= _memTableSizeLimit)
            {
                FreezeMemTable();
            }
        }
        finally
        {
            _currentMemTableLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Force freeze the current MemTable to an immutable MemTable.
    /// </summary>
    /// <remarks>
    /// Once a MemTable reaches the limit, we call ForceFreezeMemTable to freeze the MemTable and create a new one.
    /// </remarks>
    public void ForceFreezeMemTable()
    {
        _currentMemTableLock.EnterReadLock();

        try
        {
            if (_state.CurrentMemTable.Size == 0)
            {
                return;
            }
        }
        finally
        {
            _currentMemTableLock.ExitReadLock();
        }

        _currentMemTableLock.EnterWriteLock();

        try
        {
            FreezeMemTable();
        }
        finally
        {
            _currentMemTableLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Force flush the earliest created MemTable to disk.
    /// </summary>
    public async Task ForceFlushNextImmutableMemTableAsync(CancellationToken cancellationToken = default)
    {
        if (_state.ImmutableMemTables.IsEmpty)
        {
            return;
        }

        _immutableMemTablesLock.EnterWriteLock();

        IMemTable<TKey, TValue> memTableToFlush;

        try
        {
            // double-check lock
            if (_state.ImmutableMemTables.IsEmpty)
            {
                return;
            }

            _state.ImmutableMemTables = _state.ImmutableMemTables.Dequeue(out memTableToFlush);
        }
        finally
        {
            _immutableMemTablesLock.ExitWriteLock();
        }

        var sstFilename = GetSstPath(memTableToFlush.Id);
        using var builder = _ssTableBuilderFactory.CreateSsTableBuilder(sstFilename, _ssTableEncoder, _blockEncoder, _bloomFilterFactory, memTableToFlush.Count);
        await memTableToFlush.FlushAsync(builder);

        var ssTable = await builder.BuildAsync(cancellationToken);

        await _level0Lock.EnterWriteLockAsync();

        try
        {
            _state.LevelZeroTables.Add(ssTable);
        }
        finally
        {
            _level0Lock.ExitWriteLock();

            memTableToFlush.Dispose();
        }
    }

    public string GetSstPath(long id)
    {
        return Path.Combine(StoragePath, $"{id.ToString(CultureInfo.InvariantCulture)}.sst");
    }

    public IStorageIterator<TKey, TValue> CreateIterator()
    {
        return new LsmStorageIterator(this);
    }

    /// <summary>
    /// Freezes the current MemTable to an immutable MemTable. This method is not synchronized and should
    /// only be called by other synchronized methods.
    /// </summary>
    private void FreezeMemTable()
    {
        Debug.Assert(_currentMemTableLock.IsWriteLockHeld);

        _immutableMemTablesLock.EnterWriteLock();

        try
        {
            var _previousMemTable = _state.CurrentMemTable;

            _state = new StorageState<TKey, TValue>
            {
                CurrentMemTable = new MemTable<TKey, TValue>(IdGenerator.GetNextId()),
                ImmutableMemTables = _state.ImmutableMemTables.Enqueue(_previousMemTable),
                LevelZeroTables = _state.LevelZeroTables,
                LeveledSsTables = _state.LeveledSsTables
            };
        }
        finally
        {
            _immutableMemTablesLock.ExitWriteLock();
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

        _disposed = true;
    }

    public void DisposeInternal()
    {
        _state.CurrentMemTable.Dispose();

        foreach (var memTable in _state.ImmutableMemTables)
        {
            memTable.Dispose();
        }

        foreach (var table in _state.LevelZeroTables)
        {
            table.Dispose();
        }

        foreach (var table in _state.LeveledSsTables.SelectMany(x => x))
        {
            table.Dispose();
        }
    }

    ~LsmStorageInner()
    {
        DisposeInternal();
    }

    private sealed class LsmStorageIterator : IStorageIterator<TKey, TValue>
    {
        private readonly ReaderWriterLockSlim _memTableLock;
        private readonly StorageState<TKey, TValue> _state;

        public LsmStorageIterator(LsmStorageInner<TKey, TValue> storage)
        {
            _memTableLock = storage._currentMemTableLock;
            _state = storage._state;
        }

        public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            List<IAsyncEnumerator<KeyValuePair<TKey, TValue>>> iterators = [];

            // In theory only the current MemTable needs to be synchronized,
            // but we need to keep the mutable MemTable iterator around to compare with immutable ones.
            // A solution to shorten the read-lock would be to copy the mutable MemTable keys. 

            _memTableLock.EnterReadLock();

            try
            {
                var currentIterator = _state.CurrentMemTable.CreateIterator();

                var currentEnumerator = currentIterator.EnumerateAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);

                if (await currentEnumerator.MoveNextAsync())
                {
                    iterators.Add(currentEnumerator);
                }

                foreach (var memTable in _state.ImmutableMemTables)
                {
                    var iterator = memTable.CreateIterator();

                    var enumerator = iterator.EnumerateAsync(cancellationToken).GetAsyncEnumerator(cancellationToken);

                    if (await enumerator.MoveNextAsync())
                    {
                        iterators.Add(enumerator);
                    }
                }

                while (iterators.Count > 0)
                {
                    // Assume the smallest is the element from the first iterator
                    var smallest = iterators[0].Current;

                    var smallestIndex = 0;

                    for (var i = 1; i < iterators.Count; i++)
                    {
                        var iterator = iterators[i];

                        var current = iterator.Current;

                        switch (_keyComparer.Compare(smallest.Key, current.Key))
                        {
                            // Discard the entry since there is the same key from a more recent table
                            case 0:
                                if (!await iterator.MoveNextAsync())
                                {
                                    iterators.RemoveAt(i);
                                    i--;
                                }
                                break;

                            case > 0:
                                smallestIndex = i;
                                smallest = current;
                                break;

                            default:
                                break;
                        }
                    }

                    // Consume the smallest element
                    if (!await iterators[smallestIndex].MoveNextAsync())
                    {
                        iterators.RemoveAt(smallestIndex);
                    }

                    if (!_valueSerializer.IsTombstoneValue(smallest.Value))
                    {
                        yield return smallest;
                    }
                }
            }
            finally
            {
                _memTableLock.ExitReadLock();
            }
        }

        /// <summary>
        /// Returns all the values currently stored in memory.
        /// </summary>
        /// <remarks>Uses a merge iterator.</remarks>
        /// <returns></returns>
        public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(TKey from, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            List<IAsyncEnumerator<KeyValuePair<TKey, TValue>>> iterators = [];

            // In theory only the current MemTable needs to be synchronized,
            // but we need to keep the mutable MemTable iterator around to compare with immutable ones.
            // A solution to shorten the read-lock would be to copy the mutable MemTable keys. 

            _memTableLock.EnterReadLock();

            try
            {
                var currentIterator = _state.CurrentMemTable.CreateIterator();

                var currentEnumerator = currentIterator.EnumerateAsync(from, cancellationToken).GetAsyncEnumerator(cancellationToken);

                if (await currentEnumerator.MoveNextAsync())
                {
                    iterators.Add(currentEnumerator);
                }

                foreach (var memTable in _state.ImmutableMemTables)
                {
                    var iterator = memTable.CreateIterator();

                    var enumerator = iterator.EnumerateAsync(from, cancellationToken).GetAsyncEnumerator(cancellationToken);

                    if (await enumerator.MoveNextAsync())
                    {
                        iterators.Add(enumerator);
                    }
                }

                while (iterators.Count > 0)
                {
                    // Assume the smallest is the element from the first iterator
                    var smallest = iterators[0].Current;

                    var smallestIndex = 0;

                    for (var i = 1; i < iterators.Count; i++)
                    {
                        var iterator = iterators[i];

                        var current = iterator.Current;

                        switch (_keyComparer.Compare(smallest.Key, current.Key))
                        {
                            // Discard the entry since there is the same key from a more recent table
                            case 0:
                                if (!await iterator.MoveNextAsync())
                                {
                                    iterators.RemoveAt(i);
                                    i--;
                                }
                                break;

                            case > 0:
                                smallestIndex = i;
                                smallest = current;
                                break;
                            
                            default:
                                break;
                        }
                    }

                    // Consume the smallest element
                    if (!await iterators[smallestIndex].MoveNextAsync())
                    {
                        iterators.RemoveAt(smallestIndex);
                    }

                    // Don't return tombstones
                    if (!_valueSerializer.IsTombstoneValue(smallest.Value))
                    {
                        yield return smallest;
                    }
                }
            }
            finally
            {
                _memTableLock.ExitReadLock();
            }
        }
    }
}
