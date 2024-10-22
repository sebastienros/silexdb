namespace Silex;

using System.Diagnostics;

internal class Compacter<TKey, TValue> : IAsyncDisposable where TKey : notnull
{
    private readonly LsmStorageInner<TKey, TValue> _storage;
    private readonly PeriodicTimer? _flushTimer;
    private Task? _flushTask;
    private CancellationTokenSource? _flushTaskCts;
    private readonly ushort _memTableTableMaxCount;

    public Compacter(LsmStorageInner<TKey, TValue> storage, TimeProvider timeProvider, StorageOptions options)
    {
        _storage = storage;
        _flushTimer = options.FlushPeriod == TimeSpan.Zero ? null : new PeriodicTimer(options.FlushPeriod, timeProvider);
        _memTableTableMaxCount = options.MemTableMaxCount;
    }

    public void StartBackgroundFlush()
    {
        if (_flushTimer == null)
        {
            return;
        }

        _flushTask ??= BackgroundFlushAsync();
    }

    public async Task StopBackgroundFlushAsync()
    {
        if (_flushTask is null || _flushTaskCts is null)
        {
            return;
        }

        _flushTaskCts.Cancel();
        await _flushTask;
        _flushTaskCts.Dispose();
        _flushTask = null;
        _flushTaskCts = null;
    }

    private async Task BackgroundFlushAsync()
    {
        Debug.Assert(_flushTimer != null);

        _flushTaskCts = new();

        try
        {
            while (await _flushTimer.WaitForNextTickAsync(_flushTaskCts.Token))
            {
                await TriggerFlushAsync();
            }
        }
        catch (OperationCanceledException)
        {

        }
    }

    private Task TriggerFlushAsync()
    {
        if (_storage._state.ImmutableMemTables.Count() + 1 > _memTableTableMaxCount)
        {
            return _storage.ForceFlushNextImmutableMemTableAsync();
        }

        return Task.CompletedTask;
    }

    public async Task CloseAsync()
    {
        await StopBackgroundFlushAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await CloseAsync();
    }
}
