namespace Silex;

using System.Collections.Immutable;

// _state contains the consistent state of the lock, that is all readers, writers and queued locks. This field can't be updated directly, only a copy
// of it can be updated, and once all the changes have been done on this copy does it have its reference swapped with the copy. This means that _state can be used 
// in readonly too, assuming a copy of its reference was done before.
// Example: the code below uses a consistent state, that is its Readers and Writers values act like an atomic read since one can't be changed while we are reading the two.
// var state = _state
// if (state.Readers == 0 && state.Writers == 0)
//
// To do the atomic reference switch we use Interlocked.CompareExchange over the value we changed and the current _state. If no other swap was done
// our copy of the reference should be the same, and then the swap will happen.

internal class AsyncReaderWriterLock
{
    internal volatile State _state = new(0, 0, []);

    public AsyncReaderWriterLock()
    {
    }

    public bool IsWriteLockHeld => _state.Writers != 0;

    public Task EnterReadLockAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        LockCompletionSource? ticket = null;

        // Loop until we have managed to update the state
        while (true)
        {
            // Make local copy of the state. Use the local copy since
            // another thread may have changed the value;
            var oldState = _state;
            var state = oldState.Clone();
            state.Readers++;

            // There are no writers, we can add a reader without locking
            if (state.Writers == 0)
            {
                // If the swap was successful, return the result
                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    return Task.CompletedTask;
                }
                else
                {
                    // There was a concurrent update, retry
                    continue;
                }
            }
            else
            {
                // Enqueue a ticket so we can trigger the readers
                // when the writers are done

                ticket ??= new LockCompletionSource(EnterLockType.Read);
                state.LocksQueue = state.LocksQueue.Enqueue(ticket);

                // If the swap was successful, return the result
                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    return WaitForLockAsync(ticket, cancellationToken);
                }
            }
        }
    }

    public void ExitReadLock()
    {
        // Loop until we have managed to update the state
        while (true)
        {
            // Make local copy of the state. Use the local copy since
            // another thread may have changed the value;
            var oldState = _state;

            if (oldState.Readers == 0)
            {
                throw new SynchronizationLockException($"Invalid usage of {nameof(ExitReadLock)}(), expected {nameof(EnterReadLockAsync)}() first.");
            }

            var state = oldState.Clone();
            state.Readers--;
            
            // If there is nothing to unblock, return asap
            if (state.LocksQueue.IsEmpty)
            {
                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    return;
                }
                else
                {
                    // There was a concurrent update, retry
                    continue;
                }
            }

            // Fetch the next consumer in line
            state.LocksQueue = state.LocksQueue.Dequeue(out var next);

            // If the swap was successful, return the result
            if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
            {            
                // The tcs might already be set if the previous locks was a writer
                if (next != null && !next.TaskCompletionSource.Task.IsCompleted)
                {
                    next.TaskCompletionSource.SetResult();
                }

                return;
            }

            // Retry
        }
    }

    public Task EnterWriteLockAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromCanceled(cancellationToken);
        }

        LockCompletionSource? ticket = null;

        // Loop until we have managed to update the state
        while (true)
        {
            // Make local copy of the state. Use the local copy since
            // another thread may have changed the value;
            var oldState = _state;
            var state = oldState.Clone();

            // If there are no consumers return without updating the queue
            if (state.Readers == 0 && state.Writers == 0)
            {
                state.Writers = 1;

                // If the swap was successful, return the result
                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    return Task.CompletedTask;
                }
            }
            else
            {
                state.Writers++;

                ticket ??= new LockCompletionSource(EnterLockType.Write);
                state.LocksQueue = state.LocksQueue.Enqueue(ticket);
                
                // If the swap was successful, return the result
                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    return WaitForLockAsync(ticket, cancellationToken);
                }
            }
        }
    }

    /// <summary>
    /// Attempts to acquire a pass-through lock, that is it will only acquire the lock if there
    /// are no other pending readers or writers. This can be used to check if a resource is busy and execute some
    /// code when no other clients are active.
    /// </summary>
    /// <returns><see langword="true"/> if the lock was acquired, <see langword="false"/> otherwise.</returns>
    public ValueTask<bool> TryEnterWriteLockAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return new ValueTask<bool>(Task.FromCanceled<bool>(cancellationToken));
        }

        // Make local copy of the state. Use the local copy since
        // another thread may have changed the value;
        var oldState = _state;

        // Only succeed if there are no active consumers
        if (oldState.Readers == 0 && oldState.Writers == 0)
        {
            var state = oldState.Clone();
            state.Writers = 1;

            // If the swap was successful, return the result
            if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
            {
                return ValueTask.FromResult(true);
            }
        }

        return ValueTask.FromResult(false);
    }

    private Task WaitForLockAsync(LockCompletionSource ticket, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled || ticket.TaskCompletionSource.Task.IsCompleted)
        {
            return ticket.TaskCompletionSource.Task;
        }

        return WaitForLockSlowAsync(ticket, cancellationToken);
    }

    private async Task WaitForLockSlowAsync(LockCompletionSource ticket, CancellationToken cancellationToken)
    {
        using var registration = cancellationToken.Register(static state =>
        {
            var context = (CancellationRegistrationContext)state!;
            context.Owner.CancelQueuedLock(context.Ticket, context.CancellationToken);
        }, new CancellationRegistrationContext(this, ticket, cancellationToken));

        await ticket.TaskCompletionSource.Task.ConfigureAwait(false);
    }

    private void CancelQueuedLock(LockCompletionSource ticket, CancellationToken cancellationToken)
    {
        while (true)
        {
            if (ticket.TaskCompletionSource.Task.IsCompleted)
            {
                return;
            }

            var oldState = _state;
            var state = oldState.Clone();
            state.LocksQueue = RemoveQueuedLock(state.LocksQueue, ticket, out var removed);

            if (!removed)
            {
                return;
            }

            if (ticket.EnterLockType == EnterLockType.Read)
            {
                state.Readers--;
            }
            else if (ticket.EnterLockType == EnterLockType.Write)
            {
                state.Writers--;
            }

            if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
            {
                ticket.TaskCompletionSource.TrySetCanceled(cancellationToken);
                return;
            }
        }
    }

    private static ImmutableQueue<LockCompletionSource> RemoveQueuedLock(ImmutableQueue<LockCompletionSource> queue, LockCompletionSource ticket, out bool removed)
    {
        removed = false;
        var result = ImmutableQueue<LockCompletionSource>.Empty;

        foreach (var queued in queue)
        {
            if (!removed && ReferenceEquals(queued, ticket))
            {
                removed = true;
                continue;
            }

            result = result.Enqueue(queued);
        }

        return result;
    }

    public void ExitWriteLock()
    {
        // Loop until we have managed to update the state
        while (true)
        {
            // Make local copy of the state. Use the local copy since
            // another thread may have changed the value;
            var oldState = _state;
            var state = oldState.Clone();

            if (state.Writers == 0)
            {
                throw new SynchronizationLockException($"Invalid usage of {nameof(ExitWriteLock)}(), expected {nameof(EnterWriteLockAsync)}() first.");
            }

            state.Writers--;

            // If there is nothing to unblock, return asap
            if (state.LocksQueue.IsEmpty)
            {
                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    return;
                }
                else
                {
                    // There was a concurrent update, retry
                    continue;
                }
            }
            
            state.LocksQueue = state.LocksQueue.Dequeue(out var ticket);

            // If we unblocked a reader, unblock the following ones too
            if (!state.LocksQueue.IsEmpty && ticket.EnterLockType == EnterLockType.Read)
            {
                List<LockCompletionSource>? readers = [ticket];

                ticket = state.LocksQueue.Peek();

                var iterator = state.LocksQueue.GetEnumerator();

                while (iterator.MoveNext() && iterator.Current.EnterLockType == EnterLockType.Read)
                {
                    readers.Add(iterator.Current);
                }                    

                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    foreach (var r in readers)
                    {
                        if (!r.TaskCompletionSource.Task.IsCompleted)
                        {
                            r.TaskCompletionSource.SetResult();
                        }
                    }

                    return;
                }

                // If we couldn't update the state, try again
            }
            else
            {
                // Next is a write or the last to unblock
                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    if (!ticket.TaskCompletionSource.Task.IsCompleted)
                    {
                        ticket.TaskCompletionSource.SetResult();
                    }

                    return;
                }

                // If failure, retry
            }
        }
    }

    internal class LockCompletionSource(EnterLockType enterLockType)
    {
        public static readonly LockCompletionSource Empty = new(EnterLockType.None);
        public EnterLockType EnterLockType = enterLockType;
        public TaskCompletionSource TaskCompletionSource = new();
    }

    private sealed class CancellationRegistrationContext(AsyncReaderWriterLock owner, LockCompletionSource ticket, CancellationToken cancellationToken)
    {
        public AsyncReaderWriterLock Owner { get; } = owner;
        public LockCompletionSource Ticket { get; } = ticket;
        public CancellationToken CancellationToken { get; } = cancellationToken;
    }

    internal enum EnterLockType
    {
        None,
        Read,
        Write,
    }

    internal class State(uint readers, uint writers, ImmutableQueue<LockCompletionSource> queue)
    {
        public uint Readers = readers;
        public uint Writers = writers;
        public ImmutableQueue<LockCompletionSource> LocksQueue = queue;

        public State Clone() => new(Readers, Writers, LocksQueue);
    }
}
