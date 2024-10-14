using Silex.Blocks;
using Silex.MemTables;
using Silex.Tables;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Silex;

/// <summary>
/// The inner storage engine. It handles thread-safety for the <see cref="StorageState"/>.
/// </summary>
public sealed class LsmStorageInner : IDisposable
{
    private static readonly ReadOnlyMemory<byte> _tombStone = new([]);

    // Use different locks for each type of manipulated data such that we can lock them individually.
    // For instance updating the MemTable should be synchronized, but not blocked by compaction.
    // Moreover, some locks are asynchronous (level0) while other are synchronous (mem tables).

    private readonly ReaderWriterLockSlim _currentMemTableLock = new();
    private readonly ReaderWriterLockSlim _immutableMemTablesLock = new();
    private readonly AsyncReaderWriterLock _level0Lock = new();

    internal StorageState _state;
    private bool _disposed;
    private readonly IBlockEncoder _blockEncoder;
    private readonly ISsTableEncoder _ssTableEncoder;
    private readonly long _memTableSizeLimit;

    public string StoragePath { get; }

    internal LsmStorageInner(string path, StorageOptions options)
    {
        StoragePath = path;
        _state = new StorageState(options) { CurrentMemTable = new MemTable(IdGenerator.GetNextId()) };
        _blockEncoder = options.BlockEncoder;
        _ssTableEncoder = options.SsTableEncoder;
        _memTableSizeLimit = options.MemTableSizeLimit;
    }

    public static async Task<LsmStorageInner> OpenAsync(string path, StorageOptions options, CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }

        var instance = new LsmStorageInner(path, options);

        var sstFilenames = Directory.EnumerateFiles(path, "*.sst");

        var ssTables = new List<SsTable>();

        // TODO: [PERF] Can be parallelized
        foreach (var sstFilename in sstFilenames)
        {
            var blockBuilder = new BlockBuilder(options.BlockEncoder);
            var ssTable = await SsTable.LoadSsTableAsync(sstFilename, options.SsTableEncoder, blockBuilder, cancellationToken);
            ssTables.Add(ssTable);
        }

        // TODO: For now we only load l0 SSTs
        instance._state.SsTables= [ssTables];

        return instance;
    }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns><c>true</c> if the key was found, <c>false</c> otherwise.</returns>
    public bool TryGet(Bytes key, out Bytes value)
    {
        // The immutable MemTables can be accessed without lock since they are frozen (read-only),
        // and the collection won't be changed FreezeMemTable substitute the collection when it's altered.

        // Access the current and immutable MemTables in a read-lock to ensure no
        // other transaction is creating a new MemTable while we are reading variables. This is only
        // to ensure the mutable MemTable and immutable ones are consistent together.

        _currentMemTableLock.EnterReadLock();

        var snapshot = _state.Clone();

        try
        {
            // CurrentMemTable is the only thing that needs to be locked
            // since all other collections are immutable
            if (snapshot.CurrentMemTable.TryGet(key, out value))
            {
                return true;
            }
        }
        finally 
        { 
            _currentMemTableLock.ExitReadLock(); 
        }

        // If any new immutable MemTable(s) was created after this call then we just ignore it, as 
        // the newly created MemTable(s).

        foreach (var memTable in snapshot.ImmutableMemTables)
        {
            if (memTable.TryGet(key, out value))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Puts a value with the specified key in the current <see cref="IMemTable">. If one already exists it is replaced.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    public void Put(Bytes key, Bytes value)
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
    public void Delete(Bytes key)
    {
        _currentMemTableLock.EnterWriteLock();

        try
        {
            _state.CurrentMemTable.Put(key, _tombStone);

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

        await _level0Lock.EnterWriteLock();

        try
        {
            await FlushNextImmutableMemTableAsync(cancellationToken);
        }
        finally
        {
            await _level0Lock.ExitWriteLock();
        }
    }

    /// <remarks>
    /// This method is not synchronized and should only be called when the state is write-locked.
    /// </remarks>
    private async Task FlushNextImmutableMemTableAsync(CancellationToken token = default)
    {
        Debug.Assert(_level0Lock.IsWriteLockHeld);
        
        _state.ImmutableMemTables = _state.ImmutableMemTables.Dequeue(out var memTableToFlush);

        var builder = new SsTableBuilder(_ssTableEncoder, _blockEncoder);
        memTableToFlush.Flush(builder);
        memTableToFlush.Dispose();

        var sstFilename = GetSstPath(memTableToFlush.Id);
        var ssTable = await builder.BuildAsync(sstFilename, token);

        _state.SsTables[0].Add(ssTable);
    }

    public string GetSstPath(long id)
    {
        return Path.Combine(StoragePath, $"{id.ToString(CultureInfo.InvariantCulture)}.sst");
    }

    public IStorageIterator CreateIterator()
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

            _state = new StorageState
            {
                CurrentMemTable = new MemTable(IdGenerator.GetNextId()),
                ImmutableMemTables = _state.ImmutableMemTables.Enqueue(_previousMemTable),
                SsTables = _state.SsTables
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
    }

    ~LsmStorageInner()
    {
        DisposeInternal();
    }

    private class LsmStorageIterator : IStorageIterator
    {
        private readonly ReaderWriterLockSlim _memTableLock;
        private readonly StorageState _state;

        public LsmStorageIterator(LsmStorageInner storage)
        {
            _memTableLock = storage._currentMemTableLock;
            _state = storage._state;
        }

        public IAsyncEnumerable<RecordLocation> EnumerateAsync(CancellationToken cancellationToken = default)
        {
            return EnumerateAsync(Bytes.Empty, cancellationToken);
        }

        /// <summary>
        /// Returns all the values currently stored in memory.
        /// </summary>
        /// <remarks>Uses a merge iterator.</remarks>
        /// <returns></returns>
        public async IAsyncEnumerable<RecordLocation> EnumerateAsync(Bytes minValue, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            List<IAsyncEnumerator<RecordLocation>> iterators = [];

            // In theory only the current MemTable needs to be synchronized,
            // but we need to keep the mutable MemTable iterator around to compare with immutable ones.
            // A solution to shorten the read-lock would be to copy the mutable MemTable keys. 

            _memTableLock.EnterReadLock();

            try
            {
                var currentIterator = _state.CurrentMemTable.CreateIterator();

                var currentEnumerator = currentIterator.EnumerateAsync(minValue, cancellationToken).GetAsyncEnumerator(cancellationToken);

                if (await currentEnumerator.MoveNextAsync())
                {
                    iterators.Add(currentEnumerator);
                }

                foreach (var memTable in _state.ImmutableMemTables)
                {
                    var iterator = memTable.CreateIterator();

                    var enumerator = iterator.EnumerateAsync(minValue, cancellationToken).GetAsyncEnumerator(cancellationToken);

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

                        switch (smallest.Key.CompareTo(current.Key))
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

                    if (smallest.Length != 0)
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
