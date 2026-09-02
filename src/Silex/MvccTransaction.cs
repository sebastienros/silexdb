namespace Silex;

/// <summary>
/// An optimistic MVCC transaction with snapshot isolation.
/// </summary>
/// <remarks>
/// Writes are buffered until commit and participate in write-write conflict detection. Use
/// <see cref="GetForUpdateRawAsync"/> for reads whose values influence writes; those keys also participate in
/// conflict detection. Range scans are not tracked, so this type does not prevent phantom reads.
/// </remarks>
public sealed class MvccTransaction : IDisposable
{
    private MvccStorage? _storage;
    private readonly MvccWriteBuffer _writes = new();

    internal MvccTransaction(MvccStorage storage, long sequence)
    {
        _storage = storage;
        Sequence = sequence;
    }

    /// <summary>
    /// Gets the snapshot sequence captured when the transaction started.
    /// </summary>
    public long Sequence { get; }

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
    /// <returns><c>false</c> when a tracked or written key changed after this transaction started.</returns>
    public async ValueTask<bool> TryCommitAsync(CancellationToken cancellationToken = default)
    {
        var storage = GetStorage();

        try
        {
            return await storage.TryCommitAsync(Sequence, _writes, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Complete();
        }
    }

    /// <summary>
    /// Commits all buffered writes atomically, throwing on a conflict.
    /// </summary>
    public async ValueTask CommitAsync(CancellationToken cancellationToken = default)
    {
        if (!await TryCommitAsync(cancellationToken).ConfigureAwait(false))
        {
            throw new MvccConflictException();
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
