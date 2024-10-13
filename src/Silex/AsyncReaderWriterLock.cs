namespace Silex;

internal class AsyncReaderWriterLock
{
    private readonly Queue<Ticket> _locks = [];

    private readonly Task<Ticket> _noTcsReaderTicket;
    private readonly Task<Ticket> _noTcsWriterTicket;

    private int _readers;
    private int _writers;
    private int _canWrite = 1; // 1 when there are no writers or readers yet, 0 otherwise

    public AsyncReaderWriterLock()
    {
        _noTcsReaderTicket = Task.FromResult(new Ticket(true, new TaskCompletionSource<Ticket>(null), this));
        _noTcsWriterTicket = Task.FromResult(new Ticket(false, new TaskCompletionSource<Ticket>(null), this));
    }

    public Task EnterReadLock()
    {
        // TODO: Adding only readers (from an empty list) should not add Ticket instances to _locks

        var canWrite = Interlocked.CompareExchange(ref _canWrite, 0, 1);
        var readers = Interlocked.Increment(ref _readers);

        // There are no writers, we can add a reader without locking
        if (canWrite == 1)
        {
            return _noTcsReaderTicket;
        }

        // There is a writer, prevent the writers from being
        // updated while we are registering a reader
        lock (_locks) 
        {
            // Double lock in case the writer was released while
            // we were starting this method
            if (_writers == 0)
            {
                return _noTcsReaderTicket;
            }

            // Record the ticket so we can trigger the readers
            // when the writers are done
            var tcs = new TaskCompletionSource<Ticket>();
            var ticket = new Ticket(true, tcs, this);
            _locks.Enqueue(ticket);
            return tcs.Task;
        }
    }

    public Task ExitReadLock()
    {
        var readers = Interlocked.Decrement(ref _readers);
        
        if (readers == 0)
        {
            Interlocked.Exchange(ref _canWrite, 1);
        }

        Ticket next;

        lock (_locks)
        {
            if (_locks.Count == 0)
            {
                if (_readers == -1)
                {
                    throw new SynchronizationLockException($"Invalid usage of {nameof(ExitReadLock)}(), expected {nameof(EnterReadLock)}() first.");
                }

                return Task.CompletedTask;
            }

            // Fetch the next consumer in line
            next = _locks.Dequeue();
        }

        // The tcs might already be set if the previous locks was a writer
        if (!next.TaskCompletionSource.Task.IsCompleted)
        {
            next.TaskCompletionSource.SetResult(next);
        }

        return Task.CompletedTask;
    }

    public Task EnterWriteLock()
    {
        Interlocked.Increment(ref _writers);

        var canWrite = Interlocked.CompareExchange(ref _canWrite, 0, 1);

        // There are no writers, we can add one
        if (canWrite == 1)
        {
            return _noTcsWriterTicket;
        }

        lock (_locks)
        {
            var tcs = new TaskCompletionSource<Ticket>();
            var ticket = new Ticket(false, tcs, this);
            _locks.Enqueue(ticket);
            return tcs.Task;
        }
    }

    public Task ExitWriteLock()
    {
        var writers = Interlocked.Decrement(ref _writers);

        Ticket next;

        lock (_locks)
        {
            if (_locks.Count == 0)
            {
                if (writers == -1)
                {
                    throw new SynchronizationLockException($"Invalid usage of {nameof(ExitWriteLock)}(), expected {nameof(EnterWriteLock)}() first.");
                }

                return Task.CompletedTask;
            }

            // Fetch the next consumer in line
            next = _locks.Dequeue();

            next.TaskCompletionSource.SetResult(next);

            // If we unblocked a reader, unblock other ones too
            if (next.IsReader)
            {
                foreach (var n in _locks)
                {
                    if (!n.IsReader)
                    {
                        break;
                    }

                    n.TaskCompletionSource.SetResult(n);
                }
            }
        }

        return Task.CompletedTask;
    }

    private class Ticket(bool isReader, TaskCompletionSource<Ticket> taskCompletionSource, AsyncReaderWriterLock rwLock)
    {
        public bool IsReader { get; private set; } = isReader;
        public TaskCompletionSource<Ticket> TaskCompletionSource { get; private set; } = taskCompletionSource;
        public AsyncReaderWriterLock Lock { get; private set; } = rwLock;

        public Task Exit()
        {   
            return IsReader
                ? Lock.ExitReadLock()
                : Lock.ExitWriteLock();
        }
    }
}
