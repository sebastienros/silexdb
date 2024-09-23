using System.Buffers;

namespace Silex;

/// <summary>
/// The inner storage engine. It handles thread-safety for the <see cref="StorageState"/>.
/// </summary>
public sealed class LsmStorageInner : IDisposable
{
    private static readonly MemoryOwner _tombStone = new(Memory<byte>.Empty);

    private readonly ReaderWriterLockSlim _rwLock = new();
    internal readonly StorageState _state;
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
        
        var snapshot = _state.ImmutableMemTables;
        var currentMemTable = _state.CurrentMemTable;

        try
        {
            if (currentMemTable!.TryGet(key, out value))
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

        foreach (var memTable in snapshot)
        {
            if (memTable.TryGet(key, out value))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Puts a value with the specified key. If one already exists it is replaced.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    public void Put(ReadOnlyMemory<byte> key, Memory<byte> value)
    {
        Put(key, new MemoryOwner(value), value.Length);
    }

    /// <summary>
    /// Puts a value with the specified key. If one already exists it is replaced.
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
    /// Deletes the specified key.
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

    /// <summary>
    /// Freeze the current MemTable to an immutable MemTable. This method is not synchronized and should be called
    /// by other synchronized methods.
    /// </summary>
    private void FreezeMemTable()
    {
        var _previousMemTable = _state.CurrentMemTable;
        _state.CurrentMemTable = new MemTable(NextMemTableId());
        _state.ImmutableMemTables = _state.ImmutableMemTables.Push(_previousMemTable);
    }

    public void Dispose()
    {
        _rwLock.Dispose();
    }
}
