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

    public Task EnterReadLock()
    {
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
                    return ticket.TaskCompletionSource.Task;
                }
            }
        }
    }

    public Task ExitReadLock()
    {
        // Loop until we have managed to update the state
        while (true)
        {
            // Make local copy of the state. Use the local copy since
            // another thread may have changed the value;
            var oldState = _state;

            if (oldState.Readers == 0)
            {
                throw new SynchronizationLockException($"Invalid usage of {nameof(ExitReadLock)}(), expected {nameof(EnterReadLock)}() first.");
            }

            var state = oldState.Clone();
            state.Readers--;
            
            // If there is nothing to unblock, return asap
            if (state.LocksQueue.IsEmpty)
            {
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

                return Task.CompletedTask;
            }

            // Retry
        }
    }

    public Task EnterWriteLock()
    {
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
                    return ticket.TaskCompletionSource.Task;
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
    public ValueTask<bool> TryEnterWriteLock()
    {
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

    public Task ExitWriteLock()
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
                throw new SynchronizationLockException($"Invalid usage of {nameof(ExitWriteLock)}(), expected {nameof(EnterWriteLock)}() first.");
            }

            state.Writers--;

            // If there is nothing to unblock, return asap
            if (state.LocksQueue.IsEmpty)
            {
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

                    return Task.CompletedTask;
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
                    return Task.CompletedTask;
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
