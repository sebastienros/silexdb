namespace Silex;

internal sealed class StorageLifetime
{
    private const int Closing = int.MinValue;
    private int _state;
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal void CheckOpen() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _state) < 0, this);

    internal Lease Enter()
    {
        var state = Volatile.Read(ref _state);
        while (true)
        {
            ObjectDisposedException.ThrowIf(state < 0, this);
            var previous = Interlocked.CompareExchange(ref _state, state + 1, state);
            if (previous == state)
            {
                return new Lease(this);
            }
            state = previous;
        }
    }

    internal Task CloseAsync()
    {
        var state = Interlocked.Or(ref _state, Closing);
        if ((state & int.MaxValue) == 0)
        {
            _drained.TrySetResult();
        }
        return _drained.Task;
    }

    internal readonly struct Lease(StorageLifetime lifetime) : IDisposable
    {
        public void Dispose()
        {
            if (Interlocked.Decrement(ref lifetime._state) == Closing)
            {
                lifetime._drained.TrySetResult();
            }
        }
    }
}
