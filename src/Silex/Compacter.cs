namespace Silex;
internal class Compacter : IAsyncDisposable
{
    private readonly LsmStorageInner _storage;
    private readonly PeriodicTimer _flushTimer;
    private Task? _flushTask;
    private CancellationTokenSource? _flushTaskCts;
    private readonly ushort _memTableTableMaxCount;

    public Compacter(LsmStorageInner storage, TimeProvider timeProvider, StorageOptions options)
    {
        _storage = storage;
        _flushTimer = new PeriodicTimer(options.FlushPeriod, timeProvider);
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
