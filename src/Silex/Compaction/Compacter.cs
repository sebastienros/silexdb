using System.Diagnostics;

namespace Silex.Compaction;

/// <summary>
/// Coordinates MemTable flushes and SST compaction on regular intervals.
/// </summary>
internal class Compacter : IAsyncDisposable
{
    private readonly LsmStorageInner _storage;
    private readonly PeriodicTimer? _flushTimer;
    private Task? _flushTask;
    private CancellationTokenSource? _flushTaskCts;
    private readonly ushort _memTableTableMaxCount;

    public Compacter(LsmStorageInner storage, TimeProvider timeProvider, StorageOptions options)
    {
        _storage = storage;
        _flushTimer = options.FlushPeriod == TimeSpan.Zero ? null : new PeriodicTimer(options.FlushPeriod, timeProvider);
        _memTableTableMaxCount = options.MemTableMaxCount;
    }

    public void StartBackgroundFlush()
    {
        if (_flushTimer == null || _flushTask != null)
        {
            return;
        }

        _flushTaskCts = new();
        _flushTask = BackgroundFlushAsync(_flushTaskCts.Token);
    }

    public async Task StopBackgroundFlushAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_flushTask is null || _flushTaskCts is null)
        {
            return;
        }

        _flushTaskCts.Cancel();
        await _flushTask.WaitAsync(cancellationToken);
        _flushTaskCts.Dispose();
        _flushTask = null;
        _flushTaskCts = null;
    }

    private async Task BackgroundFlushAsync(CancellationToken cancellationToken)
    {
        Debug.Assert(_flushTimer != null);

        try
        {
            while (await _flushTimer.WaitForNextTickAsync(cancellationToken))
            {
                await TriggerFlushAsync(cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Ignore as this is thrown when the LsmStorage is closed.
        }
    }

    private async Task TriggerFlushAsync(CancellationToken cancellationToken)
    {
        if (_storage._state.ImmutableMemTables.Count() + 1 > _memTableTableMaxCount)
        {
            await _storage.ForceFlushNextImmutableMemTableAsync(cancellationToken);
        }

        // Flush and compaction are driven from this single loop so they never overlap; that ordering keeps
        // tiered compaction's L0 recency order stable while each structural change is committed.
        await _storage.TryTieredCompactionAsync(cancellationToken);
        await _storage.TryLeveledCompactionAsync(cancellationToken);

    }

    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        return StopBackgroundFlushAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync();
    }
}
