namespace Silex.Test;

using Silex.Collections;
using static Silex.AsyncReaderWriterLock;

public class AsyncReaderWriterLockTests
{
    private TimeSpan _timeout = TimeSpan.FromSeconds(1);

    [Fact]
    public void ShouldQueueAndDequeueItems()
    {
        var queue = new LockFreeQueue<int>();

        Assert.True(queue.IsEmpty);

        queue.Enqueue(0);

        Assert.False(queue.IsEmpty);

        queue.Enqueue(1);

        Assert.False(queue.IsEmpty);

        Assert.Equal(0, queue.Dequeue());

        Assert.False(queue.IsEmpty);
        
        Assert.Equal(1, queue.Dequeue());

        Assert.True(queue.IsEmpty);
    }

    [Fact]
    public void DequeueEmptyShouldReturnDefaultTicket()
    {
        var queue = new LockFreeQueue<int>();

        Assert.Equal(0, queue.Dequeue());
        Assert.Equal(0, queue.Dequeue());

        queue.Enqueue(1);
        Assert.Equal(1, queue.Dequeue());
        Assert.Equal(0, queue.Dequeue());
    }

    [Fact]
    public async Task ReadersShouldExit()
    {
        var arwl = new AsyncReaderWriterLock();

        await arwl.EnterReadLock().WaitAsync(_timeout);
        await arwl.ExitReadLock().WaitAsync(_timeout);
    }

    [Fact]
    public async Task ReadersShouldNotBeBlocked()
    {
        var arwl = new AsyncReaderWriterLock();

        await arwl.EnterReadLock().WaitAsync(_timeout);
        await arwl.EnterReadLock().WaitAsync(_timeout);
        await arwl.ExitReadLock().WaitAsync(_timeout);
        await arwl.ExitReadLock().WaitAsync(_timeout);
    }

    [Fact]
    public async Task WritersShouldExit()
    {
        var arwl = new AsyncReaderWriterLock();

        await arwl.EnterWriteLock().WaitAsync(_timeout);
        await arwl.ExitWriteLock().WaitAsync(_timeout);
    }

    [Fact]
    public async Task WritersShouldBlockWriters()
    {
        var arwl = new AsyncReaderWriterLock();

        var lock1 = arwl.EnterWriteLock().WaitAsync(_timeout);

        Assert.True(lock1.IsCompletedSuccessfully);

        var lock2 = arwl.EnterWriteLock();

        Assert.False(lock2.IsCompletedSuccessfully);

        await arwl.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(lock2.IsCompletedSuccessfully);

        await lock2;
    }

    [Fact]
    public async Task WritersShouldBlockReaders()
    {
        var arwl = new AsyncReaderWriterLock();

        var lock1 = arwl.EnterWriteLock().WaitAsync(_timeout);

        Assert.True(lock1.IsCompletedSuccessfully);

        var lock2 = arwl.EnterReadLock();

        Assert.False(lock2.IsCompletedSuccessfully);

        await arwl.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(lock2.IsCompletedSuccessfully);

        await lock2;
    }

    [Fact]
    public async Task ReadersShouldBlockWriter()
    {
        var arwl = new AsyncReaderWriterLock();

        var lock1 = arwl.EnterReadLock().WaitAsync(_timeout);

        Assert.True(lock1.IsCompletedSuccessfully);

        var lock2 = arwl.EnterWriteLock();

        Assert.False(lock2.IsCompletedSuccessfully);

        await arwl.ExitReadLock().WaitAsync(_timeout);

        Assert.True(lock2.IsCompletedSuccessfully);

        await lock2;
    }

    [Fact]
    public async Task WriterShouldUnblockWriterInOrder()
    {
        var arwl = new AsyncReaderWriterLock();

        var t1 = arwl.EnterWriteLock();
        var t2 = arwl.EnterWriteLock();
        var t3 = arwl.EnterWriteLock();

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.False(t2.IsCompletedSuccessfully);
        Assert.False(t3.IsCompletedSuccessfully);

        await arwl.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.True(t2.IsCompletedSuccessfully);
        Assert.False(t3.IsCompletedSuccessfully);

        await arwl.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.True(t2.IsCompletedSuccessfully);
        Assert.True(t3.IsCompletedSuccessfully);

        await arwl.ExitWriteLock().WaitAsync(_timeout);
    }

    [Fact]
    public async Task WriterShouldUnblockAllReadersInOrder()
    {
        var arwl = new AsyncReaderWriterLock();

        var t1 = arwl.EnterWriteLock();
        var t2 = arwl.EnterReadLock();
        var t3 = arwl.EnterReadLock();
        var t4 = arwl.EnterWriteLock();

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.False(t2.IsCompletedSuccessfully);
        Assert.False(t3.IsCompletedSuccessfully);
        Assert.False(t4.IsCompletedSuccessfully);

        await arwl.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.True(t2.IsCompletedSuccessfully);
        Assert.True(t3.IsCompletedSuccessfully);
        Assert.False(t4.IsCompletedSuccessfully);

        await arwl.ExitReadLock().WaitAsync(_timeout);

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.True(t2.IsCompletedSuccessfully);
        Assert.True(t3.IsCompletedSuccessfully);
        Assert.False(t4.IsCompletedSuccessfully);

        await arwl.ExitReadLock().WaitAsync(_timeout);

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.True(t2.IsCompletedSuccessfully);
        Assert.True(t3.IsCompletedSuccessfully);
        Assert.True(t4.IsCompletedSuccessfully);

        await arwl.ExitWriteLock().WaitAsync(_timeout);
    }

    [Fact]
    public async Task ExitReadLockThrowExceptionWhenUnexpected()
    {
        var arwl = new AsyncReaderWriterLock();

        await arwl.EnterWriteLock().WaitAsync(_timeout);
        
        await Assert.ThrowsAsync<SynchronizationLockException>(arwl.ExitReadLock);
    }

    [Fact]
    public async Task ExitWriteLockThrowExceptionWhenUnexpected()
    {
        var arwl = new AsyncReaderWriterLock();

        await arwl.EnterReadLock().WaitAsync(_timeout);

        await Assert.ThrowsAsync<SynchronizationLockException>(arwl.ExitWriteLock);
    }
}
