namespace Silex;

/// <summary>
/// A stable read-only MVCC view.
/// </summary>
public sealed class MvccSnapshot : IDisposable
{
    private MvccStorage? _storage;

    internal MvccSnapshot(MvccStorage storage, long sequence)
    {
        _storage = storage;
        Sequence = sequence;
    }

    /// <summary>
    /// Gets the published sequence captured by this snapshot.
    /// </summary>
    public long Sequence { get; }

    public ValueTask<int> GetRawAsync(ReadOnlySpan<byte> key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        return GetStorage().GetRawAtAsync(key, Sequence, destination, cancellationToken);
    }

    public ValueTask<bool> TryReadRawAsync<TArg>(
        ReadOnlySpan<byte> key,
        TArg arg,
        ReadValueAction<TArg> reader,
        CancellationToken cancellationToken = default)
    {
        return GetStorage().TryReadRawAtAsync(key, Sequence, arg, reader, cancellationToken);
    }

    public ValueTask<long> ScanRawAsync<TArg>(
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries = long.MaxValue,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxEntries);
        return GetStorage().ScanAtAsync(Sequence, arg, reader, maxEntries, cancellationToken);
    }

    public ValueTask<long> SeekRawAsync<TArg>(
        ReadOnlySpan<byte> from,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries = long.MaxValue,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxEntries);
        return GetStorage().SeekAtAsync(from, Sequence, arg, reader, maxEntries, cancellationToken);
    }

    public void Dispose()
    {
        var storage = Interlocked.Exchange(ref _storage, null);
        storage?.ReleaseSnapshot(Sequence);
    }

    private MvccStorage GetStorage()
    {
        return _storage ?? throw new ObjectDisposedException(nameof(MvccSnapshot));
    }
}
