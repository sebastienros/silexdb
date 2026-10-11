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
        => CreateOrderedReadCursorCore(range, direction, false);

    private OrderedReadCursor CreateOrderedReadCursorCore(OrderedReadRange? range,
        OrderedReadDirection direction, bool keysOnly)
    {
        var storage = GetStorage();
        if (direction is not (OrderedReadDirection.Forward or OrderedReadDirection.Reverse))
        {
            throw new ArgumentOutOfRangeException(nameof(direction));
        }
        range ??= new OrderedReadRange();
        var writes = OrderedWriteSnapshot.Create(_writes, range, keysOnly);
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

    /// <summary>Stages point deletions for up to maxEntries keys visible in an encoded range, in ascending order.</summary>
    /// <remarks>
    /// This is logical deletion only: it adds ordinary MVCC tombstones to this transaction, and nothing becomes
    /// visible or durable until commit. Physical data stays readable by older snapshots and is reclaimed only by
    /// MVCC garbage collection and compaction after retention allows it; no files are deleted here. Keys are
    /// those visible at <see cref="Sequence"/> plus this transaction's own pending writes (pending puts are deleted,
    /// already-deleted keys are skipped). The scanned span (the range's lower bound through the last processed key,
    /// or the whole range when complete) is registered as a read dependency, so commit conflicts with any later
    /// commit that wrote, inserted into, or deleted from it, including gaps. Keys in the unprocessed
    /// <see cref="RangeDeleteResult.Remaining"/> suffix are not tracked. Call repeatedly, each time in a new bounded
    /// transaction, with Remaining until complete; a retry after conflict simply resumes from the same range.
    /// The call is all-or-nothing: on cancellation, a <see cref="TransactionLimitException"/> or any other failure, staged
    /// writes and dependencies are restored. It temporarily uses one savepoint slot and cannot be combined with
    /// overlapping transaction operations. Traversal batches copy only keys (at most 256 keys and 64 KiB of key
    /// payload, or one larger key alone); pending overlays copy keys and markers, not values. Ordinary SST block
    /// loading, overlay keys, bounds and staged writes/undo are additional to the batch working set.
    /// </remarks>
    /// <exception cref="TransactionLimitException">Staging the deletions or tracking the range would exceed a budget.</exception>
    public async ValueTask<RangeDeleteResult> DeleteRangeAsync(OrderedReadRange range, int maxEntries,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(range);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEntries);
        GetStorage();
        cancellationToken.ThrowIfCancellationRequested();
        if (range.IsEmpty)
        {
            return new RangeDeleteResult(0, null);
        }

        var savepoint = _writes.CreateSavepoint();
        try
        {
            var result = await DeleteRangeCoreAsync(range, maxEntries, cancellationToken).ConfigureAwait(false);
            _writes.ReleaseSavepoint(savepoint);
            return result;
        }
        catch
        {
            _writes.RollbackToSavepoint(savepoint);
            _writes.ReleaseSavepoint(savepoint);
            throw;
        }
    }

    private async ValueTask<RangeDeleteResult> DeleteRangeCoreAsync(OrderedReadRange range, int maxEntries,
        CancellationToken cancellationToken)
    {
        const int ChunkEntries = 256;
        const int ChunkKeyBytes = 64 * 1024;
        using var cursor = CreateOrderedReadCursorCore(range, OrderedReadDirection.Forward, true);
        byte[]? last = null;
        var deleted = 0;
        var complete = false;
        while (true)
        {
            var remaining = maxEntries - deleted;
            using var batch = await cursor.ReadKeyBatchAsync(remaining == 0 ? 1 : Math.Min(remaining, ChunkEntries),
                ChunkKeyBytes, cancellationToken).ConfigureAwait(false);
            if (remaining == 0)
            {
                complete = batch.Count == 0;
                break;
            }
            for (var i = 0; i < batch.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                Delete(batch[i].Key.Span);
                deleted++;
            }
            if (batch.Count != 0)
            {
                last = batch[batch.Count - 1].Key.ToArray();
            }
            if (batch.IsExhausted)
            {
                complete = true;
                break;
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        if (complete)
        {
            TrackReadRange(range);
            return new RangeDeleteResult(deleted, null);
        }
        TrackReadRange(new OrderedReadRange(range.Lower, range.LowerInclusive, last, upperInclusive: true));
        return new RangeDeleteResult(deleted,
            new OrderedReadRange(last, lowerInclusive: false, range.Upper, range.UpperInclusive));
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
    /// Reads many keys from the starting snapshot overlaid with this transaction's pending writes.
    /// </summary>
    /// <remarks>
    /// Equivalent to <see cref="GetRawAsync"/> per key: reads are not tracked for conflict detection, so use
    /// <see cref="GetForUpdateRawAsync"/> for keys that must be dependencies. Like other transaction operations,
    /// it must not overlap mutation, commit or disposal. See <see cref="MvccStorage.ReadManyRawAsync"/> for
    /// the byte-budget behavior.
    /// </remarks>
    public ValueTask<PointReadBatch> ReadManyRawAsync(
        ReadOnlyMemory<ReadOnlyMemory<byte>> keys,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        return GetStorage().ReadManyAtAsync(keys, Sequence, maxBytes, _writes, cancellationToken);
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
