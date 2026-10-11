namespace Silex;

/// <summary>
/// An optimistic MVCC transaction with snapshot isolation.
/// </summary>
/// <remarks>
/// Writes are buffered until commit and participate in write-write conflict detection. Use
/// <see cref="GetForUpdateRawAsync"/> for reads whose values influence writes; those keys also participate in
/// conflict detection. Range scans are not tracked unless the caller registers the interval with
/// <see cref="TrackReadRange"/>; otherwise this type does not prevent phantom reads.
/// Transaction objects are single-consumer: no operations, including reads, may overlap.
/// </remarks>
public sealed class MvccTransaction : IDisposable
{
    private MvccStorage? _storage;
    private readonly MvccWriteBuffer _writes;

    internal MvccTransaction(MvccStorage storage, long sequence, TransactionLimits limits)
    {
        _storage = storage;
        _writes = new MvccWriteBuffer(limits: limits);
        Sequence = sequence;
    }

    /// <summary>
    /// Gets the snapshot sequence captured when the transaction started.
    /// </summary>
    public long Sequence { get; }

    /// <summary>Gets the first detected commit conflict, retained after this transaction is consumed.</summary>
    /// <remarks>
    /// Null before commit, after success, and on cancellation or storage failure. Only inspect after the
    /// commit operation completes; transaction operations must not overlap. Retry and replay safety belong
    /// to the caller.
    /// </remarks>
    public MvccConflict? Conflict { get; private set; }

    /// <summary>Gets the accounted retained bytes as defined by <see cref="StorageOptions.MvccTransactionMaxBytes"/>.</summary>
    public long BufferedBytes { get { GetStorage(); return _writes.BufferedBytes; } }

    /// <summary>Gets the number of distinct staged keys, including tombstones.</summary>
    public int MutationCount { get { GetStorage(); return _writes.MutationCount; } }

    /// <summary>Gets the number of distinct tracked point-read dependencies.</summary>
    public int TrackedReadCount { get { GetStorage(); return _writes.TrackedReadCount; } }

    /// <summary>Gets the number of registered range-read dependencies.</summary>
    public int TrackedReadRangeCount { get { GetStorage(); return _writes.ReadRangeCount; } }

    /// <summary>Registers an encoded byte-key interval as a commit-time read dependency.</summary>
    /// <remarks>
    /// Commit fails if any key inside the interval, including a key absent from the starting snapshot or
    /// deleted afterwards, was written by a commit after <see cref="Sequence"/>. This covers insertions into
    /// empty ranges, updates, deletions and gaps between returned keys. The caller must register every interval
    /// that influenced its decisions; this does not by itself provide end-to-end serializability, and the engine
    /// knows nothing about predicates. Empty intervals are ignored. Bounds are copied. This transaction's own
    /// writes never conflict. Validation cost is proportional to the physical versions in the interval.
    /// </remarks>
    /// <exception cref="TransactionLimitException">A transaction resource budget would be exceeded.</exception>
    public void TrackReadRange(OrderedReadRange range)
    {
        ArgumentNullException.ThrowIfNull(range);
        GetStorage();
        _writes.TrackReadRange(range);
    }

    /// <summary>Creates a checkpoint of the current staged writes and tracked point and range reads.</summary>
    /// <remarks>Like all transaction operations, savepoint operations must not overlap other operations.</remarks>
    public MvccSavepoint CreateSavepoint()
    {
        GetStorage();
        return _writes.CreateSavepoint();
    }

    /// <summary>Restores staged writes and tracked reads to a savepoint, keeping that savepoint reusable.</summary>
    /// <remarks>
    /// Newer savepoints are invalidated. Reads and ranges tracked after the target are removed; earlier dependencies
    /// remain. The starting snapshot and already-created cursors are unchanged. No committed writes are undone.
    /// </remarks>
    /// <exception cref="ArgumentException">The handle is not active in this transaction.</exception>
    public void RollbackToSavepoint(MvccSavepoint savepoint)
    {
        GetStorage();
        _writes.RollbackToSavepoint(savepoint);
    }

    /// <summary>Releases a savepoint and all newer savepoints without changing staged writes or tracked reads.</summary>
    /// <exception cref="ArgumentException">The handle is not active in this transaction.</exception>
    public void ReleaseSavepoint(MvccSavepoint savepoint)
    {
        GetStorage();
        _writes.ReleaseSavepoint(savepoint);
    }

    /// <summary>Creates a bounded cursor over the starting snapshot and the current pending writes.</summary>
    /// <remarks>
    /// Pending writes are copied and frozen at creation; later mutations are invisible to this cursor.
    /// The cursor independently owns its overlay and retains the exact starting sequence, including
    /// after transaction disposal or any commit outcome. Reads do not register conflict dependencies.
    /// Creation, mutation, reads, commit and disposal of a transaction must not overlap.
    /// </remarks>
    public OrderedReadCursor CreateOrderedReadCursor(OrderedReadRange? range = null,
        OrderedReadDirection direction = OrderedReadDirection.Forward)
    {
        var storage = GetStorage();
        if (direction is not (OrderedReadDirection.Forward or OrderedReadDirection.Reverse))
        {
            throw new ArgumentOutOfRangeException(nameof(direction));
        }
        range ??= new OrderedReadRange();
        var writes = OrderedWriteSnapshot.Create(_writes, range);
        try
        {
            return storage.CreateOrderedReadCursorAt(Sequence, range, direction, writes);
        }
        catch
        {
            writes?.Dispose();
            throw;
        }
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        GetStorage();
        var mutationIndex = _writes.FindMutation(key);
        if (mutationIndex < 0)
        {
            _writes.AddMutation(key, value, isTombstone: false);
        }
        else
        {
            _writes.SetValue(mutationIndex, value, isTombstone: false);
        }
    }

    public void Delete(ReadOnlySpan<byte> key)
    {
        GetStorage();
        var mutationIndex = _writes.FindMutation(key);
        if (mutationIndex < 0)
        {
            _writes.AddMutation(key, default, isTombstone: true);
        }
        else
        {
            _writes.SetValue(mutationIndex, default, isTombstone: true);
        }
    }

    public ValueTask<int> GetRawAsync(ReadOnlySpan<byte> key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        var storage = GetStorage();
        var mutationIndex = _writes.FindMutation(key);
        if (mutationIndex < 0)
        {
            return storage.GetRawAtAsync(key, Sequence, destination, cancellationToken);
        }

        var mutation = _writes.GetMutation(mutationIndex);
        if (mutation.IsTombstone)
        {
            return ValueTask.FromResult(-1);
        }

        var value = _writes.GetValue(mutation);
        if (value.Length <= destination.Length)
        {
            value.Span.CopyTo(destination.Span);
        }

        return ValueTask.FromResult(value.Length);
    }

    /// <summary>
    /// Reads a key and includes it in commit-time conflict detection.
    /// </summary>
    public ValueTask<int> GetForUpdateRawAsync(
        ReadOnlySpan<byte> key,
        Memory<byte> destination,
        CancellationToken cancellationToken = default)
    {
        GetStorage();
        TrackRead(key);
        return GetRawAsync(key, destination, cancellationToken);
    }

    /// <summary>
    /// Attempts to commit all buffered writes atomically.
    /// </summary>
    /// <returns>
    /// <c>false</c> when a tracked or written key, or a key in a tracked range, changed after this transaction
    /// started. Inspect <see cref="Conflict"/> for the first detected dependency conflict.
    /// </returns>
    /// <remarks>
    /// Cancellation is observed before the final write starts. Once writing starts, it completes without
    /// polling cancellation. A <see cref="StorageOperationException"/> can report an indeterminate or
    /// committed outcome even though this call throws; inspect the owning store's health before retrying.
    /// With group commit enabled, cancellation ends at acceptance after ordered validation and batch
    /// preparation. Accepted transactions wait for their group's WAL synchronization and serialized
    /// publication, even if cancellation is subsequently requested. Transactions are single-consumer;
    /// do not mutate or dispose one while its commit is in flight.
    /// </remarks>
    public async ValueTask<bool> TryCommitAsync(CancellationToken cancellationToken = default)
    {
        var storage = GetStorage();

        try
        {
            Conflict = await storage.TryCommitAsync(Sequence, _writes, cancellationToken).ConfigureAwait(false);
            return Conflict is null;
        }
        finally
        {
            Complete();
        }
    }

    /// <summary>
    /// Commits all buffered writes atomically, throwing on a conflict.
    /// </summary>
    /// <exception cref="MvccConflictException">A dependency conflict, with owned diagnostics in its Conflict property.</exception>
    public async ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        if (!await TryCommitAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new MvccConflictException(Conflict!);
        }
    }

    public void Dispose() => Complete();

    private void TrackRead(ReadOnlySpan<byte> key)
    {
        if (_writes.FindMutation(key) < 0)
        {
            _writes.TrackRead(key);
        }
    }

    private MvccStorage GetStorage()
    {
        return _storage ?? throw new ObjectDisposedException(nameof(MvccTransaction));
    }

    private void Complete()
    {
        var storage = Interlocked.Exchange(ref _storage, null);
        if (storage is not null)
        {
            _writes.Dispose();
            storage.ReleaseSnapshot(Sequence);
        }
    }
}
