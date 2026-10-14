namespace Silex;

internal sealed class AsyncReaderWriterLock
{
    private const int StripeCount = 64;
    private const int StrideInts = 16;

    private readonly object _gate = new();
    private readonly int[] _readerStripes = new int[StripeCount * StrideInts];
    private Waiter? _queueHead;
    private Waiter? _queueTail;
    private int _activeReaders;
    // Writers close the gate with a full fence before checking the reader count. Readers increment first,
    // then recheck the gate, so neither side can miss an acquisition racing on another core.
    private int _readerGate;
    private bool _activeWriter;

    public bool IsWriteLockHeld
    {
        get
        {
            lock (_gate)
            {
                return _activeWriter;
            }
        }
    }

    internal int ActiveReaderCount
    {
        get
        {
            var total = Volatile.Read(ref _activeReaders);
            for (var i = 0; i < StripeCount; i++)
            {
                total += Volatile.Read(ref _readerStripes[i * StrideInts]);
            }
            return total;
        }
    }

    public bool TryEnterReadLock()
    {
        if (Volatile.Read(ref _readerGate) == 0)
        {
            Interlocked.Increment(ref _activeReaders);
            if (Volatile.Read(ref _readerGate) == 0)
            {
                return true;
            }

            ExitReadLock();
        }

        return false;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public bool TryEnterReadLock(out int stripeSlot)
    {
        if (Volatile.Read(ref _readerGate) == 0)
        {
            stripeSlot = StripedCounter.GetThreadStripeSlot() * 2;
            Interlocked.Increment(ref _readerStripes[stripeSlot]);
            if (Volatile.Read(ref _readerGate) == 0)
            {
                return true;
            }

            ExitReadLock(stripeSlot);
        }

        stripeSlot = -1;
        return false;
    }

    public void ExitReadLock(int stripeSlot)
    {
        var remaining = Interlocked.Decrement(ref _readerStripes[stripeSlot]);
        if (remaining < 0)
        {
            Interlocked.Increment(ref _readerStripes[stripeSlot]);
            throw new SynchronizationLockException($"Invalid usage of {nameof(ExitReadLock)}(), expected {nameof(TryEnterReadLock)}() first.");
        }

        if (remaining != 0 || Volatile.Read(ref _readerGate) == 0 || ActiveReaderCount != 0)
        {
            return;
        }

        Waiter? granted;
        lock (_gate)
        {
            granted = PromoteWaiters();
            UpdateReaderGate();
        }

        CompleteGrantedWaiters(granted);
    }

    public Task EnterReadLockAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        if (Volatile.Read(ref _readerGate) == 0)
        {
            Interlocked.Increment(ref _activeReaders);

            if (Volatile.Read(ref _readerGate) == 0)
            {
                return Task.CompletedTask;
            }

            ExitReadLock();
        }

        return QueueWaiter(EnterLockType.Read, cancellationToken);
    }

    public void ExitReadLock()
    {
        var remaining = Interlocked.Decrement(ref _activeReaders);
        if (remaining < 0)
        {
            Interlocked.Increment(ref _activeReaders);
            throw new SynchronizationLockException($"Invalid usage of {nameof(ExitReadLock)}(), expected {nameof(EnterReadLockAsync)}() first.");
        }

        if (remaining != 0 || Volatile.Read(ref _readerGate) == 0 || ActiveReaderCount != 0)
        {
            return;
        }

        Waiter? granted;
        lock (_gate)
        {
            granted = PromoteWaiters();
            UpdateReaderGate();
        }

        CompleteGrantedWaiters(granted);
    }

    public Task EnterWriteLockAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        Waiter? waiter = null;
        var canceled = false;

        lock (_gate)
        {
            Interlocked.Exchange(ref _readerGate, 1);

            if (!_activeWriter && ActiveReaderCount == 0 && _queueHead is null)
            {
                _activeWriter = true;
                return Task.CompletedTask;
            }

            waiter = new Waiter(this, EnterLockType.Write, cancellationToken);
            if (cancellationToken.CanBeCanceled)
            {
                waiter.CancellationRegistration = cancellationToken.UnsafeRegister(static state =>
                {
                    var queuedWaiter = (Waiter)state!;
                    queuedWaiter.Owner.CancelWaiter(queuedWaiter);
                }, waiter);
            }

            if (waiter.Status == WaiterStatus.Canceled)
            {
                canceled = true;
            }
            else
            {
                waiter.Status = WaiterStatus.Queued;
                Enqueue(waiter);
            }

            UpdateReaderGate();
        }

        if (canceled)
        {
            waiter.CancellationRegistration.Dispose();
        }

        return waiter.CompletionSource.Task;
    }

    /// <summary>
    /// Attempts to acquire a pass-through lock, that is it will only acquire the lock if there
    /// are no other active or pending readers or writers.
    /// </summary>
    /// <returns><see langword="true"/> if the lock was acquired, <see langword="false"/> otherwise.</returns>
    public ValueTask<bool> TryEnterWriteLockAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<bool>(cancellationToken);
        }

        lock (_gate)
        {
            Interlocked.Exchange(ref _readerGate, 1);

            if (!_activeWriter && ActiveReaderCount == 0 && _queueHead is null)
            {
                _activeWriter = true;
                return ValueTask.FromResult(true);
            }

            UpdateReaderGate();
        }

        return ValueTask.FromResult(false);
    }

    public void ExitWriteLock()
    {
        Waiter? granted;

        lock (_gate)
        {
            if (!_activeWriter)
            {
                throw new SynchronizationLockException($"Invalid usage of {nameof(ExitWriteLock)}(), expected {nameof(EnterWriteLockAsync)}() first.");
            }

            _activeWriter = false;
            granted = PromoteWaiters();
            UpdateReaderGate();
        }

        CompleteGrantedWaiters(granted);
    }

    private Task QueueWaiter(EnterLockType enterLockType, CancellationToken cancellationToken)
    {
        var waiter = new Waiter(this, enterLockType, cancellationToken);

        if (cancellationToken.CanBeCanceled)
        {
            waiter.CancellationRegistration = cancellationToken.UnsafeRegister(static state =>
            {
                var queuedWaiter = (Waiter)state!;
                queuedWaiter.Owner.CancelWaiter(queuedWaiter);
            }, waiter);
        }

        var granted = false;
        var canceled = false;

        lock (_gate)
        {
            if (enterLockType == EnterLockType.Write)
            {
                Interlocked.Exchange(ref _readerGate, 1);
            }

            if (waiter.Status == WaiterStatus.Canceled)
            {
                canceled = true;
            }
            else if (CanEnterImmediately(enterLockType))
            {
                GrantWaiter(waiter);
                granted = true;
            }
            else
            {
                waiter.Status = WaiterStatus.Queued;
                Enqueue(waiter);
            }

            UpdateReaderGate();
        }

        if (canceled)
        {
            waiter.CancellationRegistration.Dispose();
        }
        else if (granted)
        {
            CompleteGrantedWaiters(waiter);
        }

        return waiter.CompletionSource.Task;
    }

    private bool CanEnterImmediately(EnterLockType enterLockType)
    {
        if (_queueHead is not null || _activeWriter)
        {
            return false;
        }

        return enterLockType == EnterLockType.Read || ActiveReaderCount == 0;
    }

    private void CancelWaiter(Waiter waiter)
    {
        Waiter? granted = null;
        var canceled = false;

        lock (_gate)
        {
            switch (waiter.Status)
            {
                case WaiterStatus.Initial:
                    waiter.Status = WaiterStatus.Canceled;
                    canceled = true;
                    break;
                case WaiterStatus.Queued:
                    Remove(waiter);
                    waiter.Status = WaiterStatus.Canceled;
                    canceled = true;
                    granted = PromoteWaiters();
                    break;
            }

            UpdateReaderGate();
        }

        if (canceled)
        {
            waiter.CompletionSource.TrySetCanceled(waiter.CancellationToken);
        }

        CompleteGrantedWaiters(granted);
    }

    // Returns an intrusive list of waiters to complete after releasing _gate.
    private Waiter? PromoteWaiters()
    {
        if (_activeWriter || _queueHead is null)
        {
            return null;
        }

        if (_queueHead.EnterLockType == EnterLockType.Write)
        {
            if (ActiveReaderCount != 0)
            {
                return null;
            }

            var writer = Dequeue();
            GrantWaiter(writer);
            return writer;
        }

        Waiter? grantedHead = null;
        Waiter? grantedTail = null;

        while (_queueHead?.EnterLockType == EnterLockType.Read)
        {
            var reader = Dequeue();
            GrantWaiter(reader);

            if (grantedHead is null)
            {
                grantedHead = reader;
            }
            else
            {
                grantedTail!.Next = reader;
            }

            grantedTail = reader;
        }

        return grantedHead;
    }

    private void GrantWaiter(Waiter waiter)
    {
        waiter.Status = WaiterStatus.Granted;

        if (waiter.EnterLockType == EnterLockType.Read)
        {
            Interlocked.Increment(ref _activeReaders);
        }
        else
        {
            _activeWriter = true;
        }
    }

    private void UpdateReaderGate()
    {
        Volatile.Write(ref _readerGate, _activeWriter || _queueHead is not null ? 1 : 0);
    }

    private void Enqueue(Waiter waiter)
    {
        waiter.Previous = _queueTail;

        if (_queueTail is null)
        {
            _queueHead = waiter;
        }
        else
        {
            _queueTail.Next = waiter;
        }

        _queueTail = waiter;
    }

    private Waiter Dequeue()
    {
        var waiter = _queueHead!;
        var next = waiter.Next;

        _queueHead = next;
        if (next is null)
        {
            _queueTail = null;
        }
        else
        {
            next.Previous = null;
        }

        waiter.Next = null;
        waiter.Previous = null;
        return waiter;
    }

    private void Remove(Waiter waiter)
    {
        if (waiter.Previous is null)
        {
            _queueHead = waiter.Next;
        }
        else
        {
            waiter.Previous.Next = waiter.Next;
        }

        if (waiter.Next is null)
        {
            _queueTail = waiter.Previous;
        }
        else
        {
            waiter.Next.Previous = waiter.Previous;
        }

        waiter.Next = null;
        waiter.Previous = null;
    }

    private static void CompleteGrantedWaiters(Waiter? waiter)
    {
        while (waiter is not null)
        {
            var next = waiter.Next;
            waiter.Next = null;
            waiter.CancellationRegistration.Dispose();
            waiter.CompletionSource.TrySetResult();
            waiter = next;
        }
    }

    private sealed class Waiter(AsyncReaderWriterLock owner, EnterLockType enterLockType, CancellationToken cancellationToken)
    {
        public readonly AsyncReaderWriterLock Owner = owner;
        public readonly EnterLockType EnterLockType = enterLockType;
        public readonly CancellationToken CancellationToken = cancellationToken;
        public readonly TaskCompletionSource CompletionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public CancellationTokenRegistration CancellationRegistration;
        public WaiterStatus Status;
        public Waiter? Previous;
        public Waiter? Next;
    }

    private enum EnterLockType : byte
    {
        Read,
        Write,
    }

    private enum WaiterStatus : byte
    {
        Initial,
        Queued,
        Granted,
        Canceled,
    }
}

internal sealed class StripedReaderWriterLock : IDisposable
{
    private const int StripeCount = 64;
    private const int StrideInts = 16;

    private readonly object _writeLock = new();
    private readonly object _readerDrainGate = new();
    private readonly int[] _readerStripes = new int[StripeCount * StrideInts];
    private int _writerGate;
    private int _waitingWriters;
    private int _blockedReaders;
    private int _anyReaderEntered;

    public bool IsWriteLockHeld => Monitor.IsEntered(_writeLock);

    public int WaitingReadCount => Volatile.Read(ref _blockedReaders);

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public void EnterReadLock()
    {
        if (Volatile.Read(ref _anyReaderEntered) == 0)
        {
            Interlocked.Exchange(ref _anyReaderEntered, 1);
        }
        var slot = StripedCounter.GetThreadStripeSlot() * 2;
        if (Volatile.Read(ref _writerGate) == 0)
        {
            Interlocked.Increment(ref _readerStripes[slot]);
            if (Volatile.Read(ref _writerGate) == 0)
            {
                return;
            }

            if (Interlocked.Decrement(ref _readerStripes[slot]) == 0)
            {
                NotifyWriterIfDrained();
            }
        }

        EnterReadLockSlow(slot);
    }

    private void EnterReadLockSlow(int slot)
    {
        var spinner = new SpinWait();
        while (Volatile.Read(ref _writerGate) != 0 && !spinner.NextSpinWillYield)
        {
            spinner.SpinOnce();
        }

        lock (_readerDrainGate)
        {
            Interlocked.Increment(ref _blockedReaders);
            try
            {
                while (true)
                {
                    while (Volatile.Read(ref _writerGate) != 0)
                    {
                        Monitor.Wait(_readerDrainGate);
                    }
                    Interlocked.Increment(ref _readerStripes[slot]);
                    if (Volatile.Read(ref _writerGate) == 0)
                    {
                        return;
                    }
                    if (Interlocked.Decrement(ref _readerStripes[slot]) == 0 && ActiveReaders == 0)
                    {
                        Monitor.PulseAll(_readerDrainGate);
                    }
                }
            }
            finally
            {
                Interlocked.Decrement(ref _blockedReaders);
            }
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public void ExitReadLock()
    {
        var slot = StripedCounter.GetThreadStripeSlot() * 2;
        if (Interlocked.Decrement(ref _readerStripes[slot]) == 0 && Volatile.Read(ref _writerGate) != 0)
        {
            NotifyWriterIfDrained();
        }
    }

    private void NotifyWriterIfDrained()
    {
        lock (_readerDrainGate)
        {
            if (Volatile.Read(ref _writerGate) != 0 && ActiveReaders == 0)
            {
                Monitor.PulseAll(_readerDrainGate);
            }
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public bool TryEnterWriteLock(int millisecondsTimeout)
    {
        if (Volatile.Read(ref _writerGate) != 0 || !Monitor.TryEnter(_writeLock))
        {
            return false;
        }

        Interlocked.Exchange(ref _writerGate, 1);
        if (ActiveReaders == 0)
        {
            return true;
        }

        if (Volatile.Read(ref _waitingWriters) == 0)
        {
            Volatile.Write(ref _writerGate, 0);
        }
        Monitor.Exit(_writeLock);
        if (Volatile.Read(ref _blockedReaders) != 0 && Volatile.Read(ref _waitingWriters) == 0)
        {
            lock (_readerDrainGate)
            {
                Monitor.PulseAll(_readerDrainGate);
            }
        }
        return false;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public void EnterWriteLock()
    {
        Interlocked.Increment(ref _waitingWriters);
        Interlocked.Exchange(ref _writerGate, 1);
        Monitor.Enter(_writeLock);
        Interlocked.Decrement(ref _waitingWriters);
        Interlocked.Exchange(ref _writerGate, 1);

        if (ActiveReaders != 0)
        {
            WaitForReadersToDrain();
        }
    }

    private void WaitForReadersToDrain()
    {
        var spinner = new SpinWait();
        while (ActiveReaders != 0 && !spinner.NextSpinWillYield)
        {
            spinner.SpinOnce();
        }

        if (ActiveReaders == 0)
        {
            return;
        }

        lock (_readerDrainGate)
        {
            while (ActiveReaders != 0)
            {
                Monitor.Wait(_readerDrainGate);
            }
        }
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public void ExitWriteLock()
    {
        if (Volatile.Read(ref _waitingWriters) == 0)
        {
            Volatile.Write(ref _writerGate, 0);
        }
        Monitor.Exit(_writeLock);
        if (Volatile.Read(ref _blockedReaders) != 0 && Volatile.Read(ref _waitingWriters) == 0)
        {
            lock (_readerDrainGate)
            {
                Monitor.PulseAll(_readerDrainGate);
            }
        }
    }

    private int ActiveReaders
    {
        get
        {
            if (Volatile.Read(ref _anyReaderEntered) == 0)
            {
                return 0;
            }
            var total = 0;
            var max = StripedCounter.MaxActiveStripes;
            for (var i = 0; i < max; i++)
            {
                total += Volatile.Read(ref _readerStripes[i * StrideInts]);
            }
            return total;
        }
    }

    public void Dispose()
    {
    }
}
