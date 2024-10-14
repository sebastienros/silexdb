namespace Silex;

using System.Collections.Immutable;

internal class AsyncReaderWriterLock
{
    internal volatile State _state = new(0, 0, []);

    public AsyncReaderWriterLock()
    {
    }

    public bool IsWriteLockHeld => _state.Writers != 0;

    public Task EnterReadLock()
    {
        Ticket? ticket = null;

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

                ticket ??= new Ticket(EnterLockType.Read);
                state.Queue = state.Queue.Enqueue(ticket);

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
            var state = oldState.Clone();

            if (state.Readers == 0)
            {
                throw new SynchronizationLockException($"Invalid usage of {nameof(ExitReadLock)}(), expected {nameof(EnterReadLock)}() first.");
            }

            state.Readers--;
            
            // If there is nothing to unblock, return asap
            if (state.Queue.IsEmpty)
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
            state.Queue = state.Queue.Dequeue(out var next);

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
        Ticket? ticket = null;

        // Loop until we have managed to update the state
        while (true)
        {
            // Make local copy of the state. Use the local copy since
            // another thread may have changed the value;
            var oldState = _state;
            var state = oldState.Clone();
            state.Writers++;

            // If there are no consumers return without updating the queue
            if (state.Readers == 0 && state.Writers == 1)
            {
                // If the swap was successful, return the result
                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    return Task.CompletedTask;
                }
            }
            else
            {
                ticket ??= new Ticket(EnterLockType.Write);
                state.Queue = state.Queue.Enqueue(ticket);
                
                // If the swap was successful, return the result
                if (Interlocked.CompareExchange(ref _state, state, oldState) == oldState)
                {
                    return ticket.TaskCompletionSource.Task;
                }
            }
        }
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
            if (state.Queue.IsEmpty)
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
            
            state.Queue = state.Queue.Dequeue(out var ticket);

            // If we unblocked a reader, unblock the following ones too
            if (!state.Queue.IsEmpty && ticket.EnterLockType == EnterLockType.Read)
            {
                List<Ticket>? readers = [ticket];

                ticket = state.Queue.Peek();

                var iterator = state.Queue.GetEnumerator();

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

    internal class Ticket(EnterLockType enterLockType)
    {
        public static readonly Ticket Empty = new(EnterLockType.None);
        public EnterLockType EnterLockType = enterLockType;
        public TaskCompletionSource TaskCompletionSource = new();
    }

    internal enum EnterLockType
    {
        None,
        Read,
        Write,
    }

    internal class State(uint readers, uint writers, ImmutableQueue<Ticket> queue)
    {
        public uint Readers = readers;
        public uint Writers = writers;
        public ImmutableQueue<Ticket> Queue = queue;

        public State Clone() => new(Readers, Writers, Queue);
    }
}
