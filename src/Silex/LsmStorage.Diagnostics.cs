namespace Silex;

public sealed partial class LsmStorage
{
    /// <summary>
    /// Returns cumulative counters and a consistent view of the physical structure. Safe to call frequently;
    /// it never waits for the WAL, never reads data blocks and still works on a faulted instance so the fault
    /// and its pressure signals can be observed.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The instance is disposed or closing.</exception>
    public Task<LsmStorageStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default)
    {
        _inner.Lifetime.CheckOpen();
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _inner.GetStatisticsAsync(cancellationToken);
    }

    /// <summary>
    /// Estimates the physical size of the SST blocks that may hold keys in <paramref name="range"/> using only
    /// block-index boundaries. The result is approximate physical bytes, not an entry count or selectivity.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The instance is disposed or closing.</exception>
    public Task<StorageRangeEstimate> EstimateRangeAsync(OrderedReadRange? range = null, CancellationToken cancellationToken = default)
    {
        _inner.Lifetime.CheckOpen();
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _inner.EstimateRangeAsync(range ?? new OrderedReadRange(), cancellationToken);
    }
}
