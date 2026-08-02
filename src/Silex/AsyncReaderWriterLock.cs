namespace Silex;

internal sealed class AsyncReaderWriterLock
{
    private readonly object _gate = new();
    private Waiter? _queueHead;
    private Waiter? _queueTail;
    private int _activeReaders;
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
            lock (_gate)
            {
                return _activeReaders;
            }
        }
    }

    public Task EnterReadLockAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        lock (_gate)
        {
            if (!_activeWriter && _queueHead is null)
            {
                _activeReaders++;
                return Task.CompletedTask;
            }
        }

        return QueueWaiter(EnterLockType.Read, cancellationToken);
    }

    public void ExitReadLock()
    {
        Waiter? granted;

        lock (_gate)
        {
            if (_activeReaders == 0)
            {
                throw new SynchronizationLockException($"Invalid usage of {nameof(ExitReadLock)}(), expected {nameof(EnterReadLockAsync)}() first.");
            }

            _activeReaders--;
            granted = PromoteWaiters();
        }

        CompleteGrantedWaiters(granted);
    }

    public Task EnterWriteLockAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        lock (_gate)
        {
            if (!_activeWriter && _activeReaders == 0 && _queueHead is null)
            {
                _activeWriter = true;
                return Task.CompletedTask;
            }
        }

        return QueueWaiter(EnterLockType.Write, cancellationToken);
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
            if (!_activeWriter && _activeReaders == 0 && _queueHead is null)
            {
                _activeWriter = true;
                return ValueTask.FromResult(true);
            }
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

        return enterLockType == EnterLockType.Read || _activeReaders == 0;
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
            if (_activeReaders != 0)
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
            _activeReaders++;
        }
        else
        {
            _activeWriter = true;
        }
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
