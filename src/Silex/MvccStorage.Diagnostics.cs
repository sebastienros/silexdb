using System.Diagnostics;

namespace Silex;

public sealed partial class MvccStorage
{
    private readonly struct SnapshotEntry(int count, long pinnedAt)
    {
        public int Count { get; } = count;
        public long PinnedAt { get; } = pinnedAt;
    }

    private long _openSequence;
    private long _gateAcquisitions;
    private long _gateWaits;
    private int _gateWaiters;
    private long _gateWaitTicks;
    private long _gateWaitMaxTicks;
    private long _gateWaitLastTicks;
    private long _commitGroups;
    private long _groupedCommits;
    private int _garbageCollectionRunning;
    private readonly object _garbageCollectionStatsLock = new();
    private long _gcCompleted;
    private long _gcFailed;
    private long _gcCanceled;
    private long _gcExamined;
    private long _gcDeleted;
    private long _gcTicks;
    private long _gcMaxTicks;
    private long _gcLastTicks;
    private MvccGarbageCollectionResult? _gcLastResult;
    private Exception? _gcLastFailure;
    private DateTimeOffset? _gcLastFailureAt;

    private void EnterCommitGate()
    {
        Interlocked.Increment(ref _gateAcquisitions);
        if (_commitGate.Wait(0))
        {
            return;
        }

        Interlocked.Increment(ref _gateWaiters);
        var started = Stopwatch.GetTimestamp();
        try
        {
            _commitGate.Wait();
        }
        finally
        {
            Interlocked.Decrement(ref _gateWaiters);
        }
        RecordGateWait(started);
    }

    private ValueTask EnterCommitGateAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Interlocked.Increment(ref _gateAcquisitions);
        if (_commitGate.Wait(0))
        {
            return ValueTask.CompletedTask;
        }

        return WaitForCommitGateAsync(cancellationToken);
    }

    private async ValueTask WaitForCommitGateAsync(CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _gateWaiters);
        var started = Stopwatch.GetTimestamp();
        try
        {
            await _commitGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref _gateWaiters);
        }
        RecordGateWait(started);
    }

    private void RecordGateWait(long started)
    {
        var ticks = Stopwatch.GetTimestamp() - started;
        Interlocked.Increment(ref _gateWaits);
        Interlocked.Add(ref _gateWaitTicks, ticks);
        StorageMetrics.AtomicMax(ref _gateWaitMaxTicks, ticks);
        Interlocked.Exchange(ref _gateWaitLastTicks, ticks);
    }

    private async ValueTask<MvccGarbageCollectionResult> TrackGarbageCollectionAsync(
        MvccGarbageCollectionOptions options, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref _garbageCollectionRunning);
        try
        {
            var result = await RunGarbageCollectionAsync(options, cancellationToken).ConfigureAwait(false);
            lock (_garbageCollectionStatsLock)
            {
                var ticks = (long)(result.Elapsed.TotalSeconds * Stopwatch.Frequency);
                _gcCompleted++;
                _gcTicks += ticks;
                _gcMaxTicks = Math.Max(_gcMaxTicks, ticks);
                _gcLastTicks = ticks;
                _gcLastResult = result;
            }
            return result;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            lock (_garbageCollectionStatsLock)
            {
                _gcCanceled++;
            }
            throw;
        }
        catch (ObjectDisposedException)
        {
            throw;
        }
        catch (Exception exception)
        {
            lock (_garbageCollectionStatsLock)
            {
                _gcFailed++;
                _gcLastFailure = exception;
                _gcLastFailureAt = DateTimeOffset.UtcNow;
            }
            throw;
        }
        finally
        {
            Interlocked.Decrement(ref _garbageCollectionRunning);
        }
    }

    private void RecordGarbageCollectionChunk(long examined, long deleted)
    {
        lock (_garbageCollectionStatsLock)
        {
            _gcExamined += examined;
            _gcDeleted += deleted;
        }
    }

    /// <summary>
    /// Returns cumulative counters, the retention window pinned by read views, and the engine's physical state.
    /// Works on a faulted instance so the fault and its pressure signals remain observable. Policy for
    /// admission control or timeouts is deliberately left to the caller.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The instance is disposed or closing.</exception>
    public async Task<MvccStorageStatistics> GetStatisticsAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeState) != 0, this);

        int views;
        int distinct;
        long? oldest = null;
        long oldestPinnedAt = 0;
        long published;
        lock (_snapshotLock)
        {
            published = Volatile.Read(ref _publishedSequence);
            views = 0;
            distinct = _activeSnapshots.Count;
            foreach (var (sequence, entry) in _activeSnapshots)
            {
                views += entry.Count;
                if (oldest is null || sequence < oldest)
                {
                    oldest = sequence;
                    oldestPinnedAt = entry.PinnedAt;
                }
            }
        }

        var engine = await _storage.GetStatisticsAsync(cancellationToken).ConfigureAwait(false);
        var versionStorage = await EstimateVersionKeyspaceAsync(cancellationToken).ConfigureAwait(false);

        MvccGarbageCollectionStatistics gc;
        lock (_garbageCollectionStatsLock)
        {
            gc = new MvccGarbageCollectionStatistics(
                Volatile.Read(ref _garbageCollectionRunning) != 0, _gcCompleted, _gcFailed, _gcCanceled,
                _gcExamined, _gcDeleted,
                DurationStatistics.FromTicks(_gcCompleted, _gcTicks, _gcMaxTicks, _gcLastTicks),
                _gcLastResult, _gcLastFailure, _gcLastFailureAt);
        }

        var waits = Interlocked.Read(ref _gateWaits);
        return new MvccStorageStatistics
        {
            PublishedSequence = published,
            ActiveReadViews = views,
            DistinctPinnedSequences = distinct,
            OldestRetainedSequence = oldest,
            RetainedSequenceSpan = oldest is null ? null : Math.Max(0, published - oldest.Value),
            OldestRetainedViewAge = oldest is null ? null : Stopwatch.GetElapsedTime(oldestPinnedAt),
            CommitsPublished = Math.Max(0, published - Interlocked.Read(ref _openSequence)),
            VersionStorage = versionStorage,
            CommitGateAcquisitions = Interlocked.Read(ref _gateAcquisitions),
            CommitGateWaiters = Volatile.Read(ref _gateWaiters),
            CommitGateWait = DurationStatistics.FromTicks(
                waits, Interlocked.Read(ref _gateWaitTicks),
                Interlocked.Read(ref _gateWaitMaxTicks), Interlocked.Read(ref _gateWaitLastTicks)),
            GroupCommitEnabled = _commitCoordinator is not null,
            CommitGroups = Interlocked.Read(ref _commitGroups),
            GroupedCommits = Interlocked.Read(ref _groupedCommits),
            PendingCommitRequests = _commitCoordinator?.PendingCount,
            GarbageCollection = gc,
            Engine = engine,
        };
    }

    /// <summary>
    /// Estimates the physical size of the stored versions (live, retained history and tombstones) for user
    /// keys in <paramref name="range"/>. Like <see cref="LsmStorage.EstimateRangeAsync"/> this is approximate
    /// physical bytes from block boundaries: not a count of keys or versions, and not a selectivity estimate.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The instance is disposed or closing.</exception>
    public async Task<StorageRangeEstimate> EstimateRangeAsync(OrderedReadRange? range = null, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeState) != 0, this);
        range ??= new OrderedReadRange();
        using var lower = range.Lower is { } low
            ? EncodeVersionKey(low.Span, range.LowerInclusive ? long.MaxValue : 0) : null;
        using var upper = range.Upper is { } high
            ? EncodeVersionKey(high.Span, range.UpperInclusive ? 0 : long.MaxValue) : null;
        var physical = range.IsEmpty
            ? new OrderedReadRange(new byte[] { 2 }, true, new byte[] { 1 })
            : new OrderedReadRange(
                lower is null ? new byte[] { DataPrefix } : lower.Slice.Memory, lower is null || range.LowerInclusive,
                upper is null ? new byte[] { 2 } : upper.Slice.Memory,
                upper is not null && range.UpperInclusive);
        return await _storage.EstimateRangeAsync(physical, cancellationToken).ConfigureAwait(false);
    }

    private Task<StorageRangeEstimate> EstimateVersionKeyspaceAsync(CancellationToken cancellationToken) =>
        _storage.EstimateRangeAsync(
            new OrderedReadRange(new byte[] { DataPrefix }, true, new byte[] { DataPrefix + 1 }, false), cancellationToken);
}
