namespace Silex;

/// <summary>A resumable ordered read. Dispose to release its independently retained MVCC snapshot.</summary>
/// <remarks>
/// Advance and disposal are single-consumer operations; overlapping calls throw. No storage lock is
/// held between advances. Failed/canceled advances do not move the position and may be retried.
/// </remarks>
public sealed class OrderedReadCursor : IDisposable
{
    private readonly LsmStorage? _lsm;
    private readonly MvccStorage? _mvcc;
    private readonly OrderedReadRange _range;
    private byte[]? _resume;
    private int _busy;
    private bool _disposed;
    private bool _exhausted;

    internal OrderedReadCursor(LsmStorage? lsm, MvccStorage? mvcc, long? sequence,
        OrderedReadRange range, OrderedReadDirection direction)
    {
        _lsm = lsm;
        _mvcc = mvcc;
        Sequence = sequence;
        _range = range;
        Direction = direction;
        _exhausted = range.IsEmpty;
    }

    public OrderedReadDirection Direction { get; }
    public OrderedReadConsistency Consistency => _mvcc is null ? OrderedReadConsistency.Live : OrderedReadConsistency.Snapshot;
    public long? Sequence { get; }

    /// <summary>Returns at most maxEntries and maxBytes of key-plus-value payload. Oversized first entries throw.</summary>
    public async ValueTask<OrderedReadBatch> ReadBatchAsync(int maxEntries, int maxBytes,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxEntries);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBytes);
        Enter();
        OrderedReadBatch? batch = null;
        try
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            cancellationToken.ThrowIfCancellationRequested();
            batch = new OrderedReadBatch(maxEntries, maxBytes);
            // Even exhausted cursors reject advances after storage shutdown or failure.
            if (_mvcc is not null)
            {
                await _mvcc.ReadOrderedBatchAsync(_range, Direction, _resume, Sequence!.Value,
                    batch, _exhausted, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _lsm!.ReadOrderedBatchAsync(_range, Direction, _resume,
                    batch, _exhausted, cancellationToken).ConfigureAwait(false);
            }
            cancellationToken.ThrowIfCancellationRequested();
            if (batch.Count != 0)
            {
                _resume = batch[batch.Count - 1].Key.ToArray();
            }
            _exhausted = batch.IsExhausted;
            return batch;
        }
        catch
        {
            batch?.Dispose();
            throw;
        }
        finally
        {
            Volatile.Write(ref _busy, 0);
        }
    }

    public void Dispose()
    {
        Enter();
        try
        {
            if (!_disposed)
            {
                _disposed = true;
                _resume = null;
                if (_mvcc is not null)
                {
                    _mvcc.ReleaseSnapshot(Sequence!.Value);
                }
            }
        }
        finally
        {
            Volatile.Write(ref _busy, 0);
        }
    }

    private void Enter()
    {
        if (Interlocked.CompareExchange(ref _busy, 1, 0) != 0)
        {
            throw new InvalidOperationException("A cursor advance or disposal is already in progress.");
        }
    }
}
