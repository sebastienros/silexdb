using Silex.MemTables;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Silex;

using StorageRecord = KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>;

/// <summary>
/// The inner storage engine. It handles thread-safety for the <see cref="StorageState"/>.
/// </summary>
public sealed class LsmStorageInner : IDisposable
{
    private static readonly MemoryOwner _tombStone = new(Memory<byte>.Empty);

    private readonly ReaderWriterLockSlim _rwLock = new();
    internal StorageState _state;
    private readonly long _memTableSizeLimit;
    private int _memTableId = 0;

    private int NextMemTableId() => Interlocked.Increment(ref _memTableId);

    public LsmStorageInner(StorageOptions options)
    {
        _state = new StorageState(options) { CurrentMemTable = new MemTable(0) };
        _memTableSizeLimit = options.MemTableSizeLimit;
    }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns><c>true</c> if the key was found, <c>false</c> otherwise.</returns>
    public bool TryGet(ReadOnlyMemory<byte> key, out ReadOnlyMemory<byte> value)
    {
        // The immutable MemTables can be accessed without lock since they are frozen (read-only),
        // and the collection won't be changed FreezeMemTable substitute the collection when it's altered.

        // Access the current and immutable MemTables in a read-lock to ensure no
        // other transaction is creating a new MemTable while we are reading variables. This is only
        // to ensure the mutable MemTable and immutable ones are consistent together.

        _rwLock.EnterReadLock();

        var snapshot = _state.Clone();

        try
        {
            if (snapshot.CurrentMemTable.TryGet(key, out value))
            {
                return true;
            }
        }
        finally 
        { 
            _rwLock.ExitReadLock(); 
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
    public void Put(ReadOnlyMemory<byte> key, Memory<byte> value)
    {
        Put(key, new MemoryOwner(value), value.Length);
    }

    /// <summary>
    /// Puts a value with the specified key in the current <see cref="IMemTable">. If one already exists it is replaced.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    void Put(ReadOnlyMemory<byte> key, IMemoryOwner<byte> value, int bufferSize)
    {
        _rwLock.EnterWriteLock();

        try
        {
            _state.CurrentMemTable.Put(key, value, bufferSize);

            if (_state.CurrentMemTable.Size >= _memTableSizeLimit)
            {
                FreezeMemTable();
            }
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Adds a delete operation for the specified key.
    /// </summary>
    /// <param name="key"></param>
    public void Delete(ReadOnlyMemory<byte> key)
    {
        _rwLock.EnterWriteLock();

        try
        {
            _state.CurrentMemTable.Put(key, _tombStone, 0);

            if (_state.CurrentMemTable.Size >= _memTableSizeLimit)
            {
                FreezeMemTable();
            }
        }
        finally
        {
            _rwLock.ExitWriteLock();
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
        _rwLock.EnterWriteLock();

        try
        {
            FreezeMemTable();
        }
        finally
        {
            _rwLock.ExitWriteLock();
        }
    }

    public IStorageIterator CreateIterator()
    {
        return new Iterator(this);
    }

    /// <summary>
    /// Freeze the current MemTable to an immutable MemTable. This method is not synchronized and should be called
    /// by other synchronized methods.
    /// </summary>
    private void FreezeMemTable()
    {
        var _previousMemTable = _state.CurrentMemTable;
        _state = new StorageState
        {
            CurrentMemTable = new MemTable(NextMemTableId()),
            ImmutableMemTables = _state.ImmutableMemTables.Push(_previousMemTable)
        };
    }

    public void Dispose()
    {
        _rwLock.Dispose();
    }

    private class Iterator : IStorageIterator
    {
        private readonly ReaderWriterLockSlim _rwLock;
        private readonly StorageState _state;

        public Iterator(LsmStorageInner storage)
        {
            _rwLock = storage._rwLock;
            _state = storage._state;
        }

        public IAsyncEnumerable<RecordLocation> EnumerateAsync(CancellationToken cancellationToken = default)
        {
            return EnumerateAsync(ReadOnlyMemory<byte>.Empty, cancellationToken);
        }

        /// <summary>
        /// Returns all the values currently stored in memory.
        /// </summary>
        /// <remarks>Uses a merge iterator.</remarks>
        /// <returns></returns>
        public async IAsyncEnumerable<RecordLocation> EnumerateAsync(ReadOnlyMemory<byte> minValue, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            List<IAsyncEnumerator<RecordLocation>> iterators = [];

            // In theory only the current MemTable needs to be synchronized,
            // but we need to keep the mutable MemTable iterator around to compare with immutable ones.
            // A solution to shorten the read-lock would be to copy the mutable MemTable keys. 

            _rwLock.EnterReadLock();

            try
            {
                var currentIterator = _state.CurrentMemTable.CreateIterator();

                var currentEnumerator = currentIterator.EnumerateAsync(minValue, cancellationToken).GetAsyncEnumerator();

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

                        switch (ByteArrayComparer.Instance.Compare(smallest.Key, current.Key))
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
                _rwLock.ExitReadLock();
            }
        }
    }
}
