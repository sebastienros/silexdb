namespace Silex;

internal sealed class StorageLifetime
{
    private const int StripeCount = 64;
    private const int StrideInts = 16;

    private readonly int[] _stripes = new int[StripeCount * StrideInts];
    private int _closing;
    private readonly TaskCompletionSource _drained = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal void CheckOpen() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _closing) != 0, this);

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    internal Lease Enter()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _closing) != 0, this);
        var slot = StripedCounter.GetThreadStripeSlot() * 2;
        Interlocked.Increment(ref _stripes[slot]);
        if (Volatile.Read(ref _closing) == 0)
        {
            return new Lease(this, slot);
        }

        Exit(slot);
        throw new ObjectDisposedException(nameof(StorageLifetime));
    }

    internal Task CloseAsync()
    {
        Interlocked.Exchange(ref _closing, 1);
        TryCompleteClose();
        return _drained.Task;
    }

    private void Exit(int slot)
    {
        if (Interlocked.Decrement(ref _stripes[slot]) == 0 && Volatile.Read(ref _closing) != 0)
        {
            TryCompleteClose();
        }
    }

    private void TryCompleteClose()
    {
        for (var i = 0; i < StripeCount; i++)
        {
            if (Volatile.Read(ref _stripes[i * StrideInts]) != 0)
            {
                return;
            }
        }

        _drained.TrySetResult();
    }

    internal readonly struct Lease(StorageLifetime lifetime, int slot) : IDisposable
    {
        public void Dispose() => lifetime.Exit(slot);
    }
}
