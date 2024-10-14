namespace Silex;

using Silex.Collections;

internal class AsyncReaderWriterLock
{
    LockFreeQueue<Ticket> _queue = new LockFreeQueue<Ticket>();

    private static readonly Task<Ticket> _noTcsReaderTicket = Task.FromResult(new Ticket(EnterLockType.Read, new TaskCompletionSource()));
    private static readonly Task<Ticket> _noTcsWriterTicket = Task.FromResult(new Ticket(EnterLockType.Write, new TaskCompletionSource()));

    // Number of active readers
    private uint _readers;
    
    // Number of active writers
    private uint _writers;

    // When only readers are acquiring the lock we can skip the _locks table
    private uint _readersOnly = 1;

    public AsyncReaderWriterLock()
    {
        
    }

    public Task EnterReadLock()
    {
        // TODO: Adding only readers (from an empty list) should not add Ticket instances to _locks
        
        var readers = Interlocked.Increment(ref _readers);

        var canSkipLock = Interlocked.CompareExchange(ref _readersOnly, 0, 1);
        
        // There are no writers, we can add a reader without locking
        if (canSkipLock == 1)
        {
            return _noTcsReaderTicket;
        }

        // There is a writer, prevent the writers from being
        // updated while we are registering a reader
        // Double lock in case the writer was released while
        // we were starting this method
        if (_writers == 0)
        {
            return _noTcsReaderTicket;
        }

        // Record the ticket so we can trigger the readers
        // when the writers are done
        var tcs = new TaskCompletionSource();
        var ticket = new Ticket(EnterLockType.Read, tcs);
        _queue.Enqueue(ticket);
        return tcs.Task;
    }

    public Task ExitReadLock()
    {
        var readers = Interlocked.Decrement(ref _readers);
        
        if (readers == 0)
        {
            Interlocked.Exchange(ref _readersOnly, 1);
        }

        Ticket? next;

        if (_queue.IsEmpty)
        {
            if (_readers == uint.MaxValue)
            {
                throw new SynchronizationLockException($"Invalid usage of {nameof(ExitReadLock)}(), expected {nameof(EnterReadLock)}() first.");
            }

            return Task.CompletedTask;
        }

        // Fetch the next consumer in line
        next = _queue.Dequeue();

        // The tcs might already be set if the previous locks was a writer
        if (next != null && !next.TaskCompletionSource.Task.IsCompleted)
        {
            next.TaskCompletionSource.SetResult();
        }

        return Task.CompletedTask;
    }

    public Task EnterWriteLock()
    {
        Interlocked.Increment(ref _writers);

        var canWrite = Interlocked.CompareExchange(ref _readersOnly, 0, 1);

        // There are no writers, we can add one
        if (canWrite == 1)
        {
            return _noTcsWriterTicket;
        }

        var tcs = new TaskCompletionSource();
        var ticket = new Ticket(EnterLockType.Write, tcs);
        _queue.Enqueue(ticket);
        return tcs.Task;
    }

    public Task ExitWriteLock()
    {
        var writers = Interlocked.Decrement(ref _writers);

        Ticket? next = _queue.Dequeue();

        if (next == null)
        {
            if (writers == uint.MaxValue)
            {
                throw new SynchronizationLockException($"Invalid usage of {nameof(ExitWriteLock)}(), expected {nameof(EnterWriteLock)}() first.");
            }

            return Task.CompletedTask;
        }

        next.TaskCompletionSource.SetResult();

        // If we unblocked a reader, unblock other ones too
        if (next.EnterLockType == EnterLockType.Read)
        {
            var node = Volatile.Read(ref _queue._head).Next;
                
            while (node != null)
            {
                if (node.Item?.EnterLockType != EnterLockType.Read)
                {
                    break;
                }

                node.Item.TaskCompletionSource.SetResult();
                node = Volatile.Read(ref node.Next);
            }
        }

        return Task.CompletedTask;
    }


    internal class Ticket(EnterLockType enterLockType, TaskCompletionSource taskCompletionSource)
    {
        public static readonly Ticket Empty = new Ticket(EnterLockType.None, new TaskCompletionSource());
        public EnterLockType EnterLockType { get; } = enterLockType;
        public TaskCompletionSource TaskCompletionSource { get; } = taskCompletionSource;
    }

internal enum EnterLockType
    {
        None,
        Read,
        Write,
    }

}
