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
    private readonly List<MvccMutation> _mutations = [];
    private readonly List<byte[]> _trackedReads = [];

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
        var mutation = FindMutation(key);
        if (mutation is null)
        {
            _mutations.Add(new MvccMutation(key.ToArray(), value.ToArray()));
        }
        else
        {
            mutation.Value = value.ToArray();
        }
    }

    public void Delete(ReadOnlySpan<byte> key)
    {
        GetStorage();
        var mutation = FindMutation(key);
        if (mutation is null)
        {
            _mutations.Add(new MvccMutation(key.ToArray(), null));
        }
        else
        {
            mutation.Value = null;
        }
    }

    public ValueTask<int> GetRawAsync(ReadOnlySpan<byte> key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        var storage = GetStorage();
        var mutation = FindMutation(key);
        if (mutation is null)
        {
            return storage.GetRawAtAsync(key, Sequence, destination, cancellationToken);
        }

        if (mutation.Value is null)
        {
            return ValueTask.FromResult(-1);
        }

        if (mutation.Value.Length <= destination.Length)
        {
            mutation.Value.CopyTo(destination);
        }

        return ValueTask.FromResult(mutation.Value.Length);
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
            return await storage.TryCommitAsync(Sequence, _mutations, _trackedReads, cancellationToken).ConfigureAwait(false);
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

    private MvccMutation? FindMutation(ReadOnlySpan<byte> key)
    {
        for (var i = _mutations.Count - 1; i >= 0; i--)
        {
            if (key.SequenceEqual(_mutations[i].Key))
            {
                return _mutations[i];
            }
        }

        return null;
    }

    private void TrackRead(ReadOnlySpan<byte> key)
    {
        for (var i = 0; i < _trackedReads.Count; i++)
        {
            if (key.SequenceEqual(_trackedReads[i]))
            {
                return;
            }
        }

        _trackedReads.Add(key.ToArray());
    }

    private MvccStorage GetStorage()
    {
        return _storage ?? throw new ObjectDisposedException(nameof(MvccTransaction));
    }

    private void Complete()
    {
        var storage = Interlocked.Exchange(ref _storage, null);
        storage?.ReleaseSnapshot(Sequence);
    }
}
