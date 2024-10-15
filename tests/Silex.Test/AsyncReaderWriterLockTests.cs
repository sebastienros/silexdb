namespace Silex.Test;

using Xunit.Abstractions;

public class AsyncReaderWriterLockTests
{
    private readonly TimeSpan _timeout = TimeSpan.FromSeconds(1);

    private readonly ITestOutputHelper? _output;

    public AsyncReaderWriterLockTests(ITestOutputHelper _)
    {
        // Uncomment to debug tests
        //_output = output;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(10)]
    public async Task ShouldHandleConcurrentConsumers(int levelOfConcurrency)
    {
        var loq = new AsyncReaderWriterLock();
        var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(1)).Token;
        var ids = 0;
        await Parallel.ForAsync(0, levelOfConcurrency, async (i, cancellationToken) =>
        {
            await Work(loq, timeout);
        });

        async Task Work(AsyncReaderWriterLock loq, CancellationToken stopToken)
        {
            var id = Interlocked.Increment(ref ids);

            while (!stopToken.IsCancellationRequested)
            {
                var delay = TimeSpan.FromMilliseconds(Random.Shared.Next(20));
                var operation = Random.Shared.Next(2);
                var timeout = TimeSpan.FromSeconds(2);

                Task t;

                switch (operation)
                {
                    case 0: // Read
                        _output?.WriteLine($"Read({id}) r:{loq._state.Readers},w:{loq._state.Writers}");
                        t = loq.EnterReadLock().WaitAsync(timeout);
                        if (t == Task.CompletedTask) _output?.WriteLine($"Go {id} r:{loq._state.Readers},w:{loq._state.Writers}"); else _output?.WriteLine($"Wait {id} r:{loq._state.Readers},w:{loq._state.Writers}");
                        await t;
                        _output?.WriteLine($"Do({id})");
                        await Task.Delay(delay);
                        t = loq.ExitReadLock();
                        _output?.WriteLine($"~Read({id}) r:{loq._state.Readers},w:{loq._state.Writers}");
                        await t;
                        break;

                    case 1: // Write
                        _output?.WriteLine($"Write({id}) r:{loq._state.Readers},w:{loq._state.Writers}");
                        t = loq.EnterWriteLock().WaitAsync(timeout);
                        if (t == Task.CompletedTask) _output?.WriteLine($"Go {id} r:{loq._state.Readers},w:{loq._state.Writers}"); else _output?.WriteLine($"Wait {id} r:{loq._state.Readers},w:{loq._state.Writers}");
                        await t;
                        _output?.WriteLine($"Do({id})");
                        await Task.Delay(delay);
                        t = loq.ExitWriteLock();
                        _output?.WriteLine($"~Write({id}) r:{loq._state.Readers},w:{loq._state.Writers}");
                        await t;
                        break;
                }

                await Task.Delay(delay);
            }
        }
    }

    [Fact]
    public async Task ReadersShouldExit()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLock().WaitAsync(_timeout);
        await loq.ExitReadLock().WaitAsync(_timeout);
    }

    [Fact]
    public async Task TryEnterWriteLockShouldWork()
    {
        var loq = new AsyncReaderWriterLock();

        Assert.True(await loq.TryEnterWriteLock().AsTask().WaitAsync(_timeout));
        await loq.ExitWriteLock().WaitAsync(_timeout);

        await loq.EnterReadLock().WaitAsync(_timeout);
        Assert.False(await loq.TryEnterWriteLock().AsTask().WaitAsync(_timeout));
        
        await loq.ExitReadLock().WaitAsync(_timeout);
        Assert.True(await loq.TryEnterWriteLock().AsTask().WaitAsync(_timeout));
        await loq.ExitWriteLock().WaitAsync(_timeout);

        await loq.EnterWriteLock().WaitAsync(_timeout);
        Assert.False(await loq.TryEnterWriteLock().AsTask().WaitAsync(_timeout));

        await loq.ExitWriteLock().WaitAsync(_timeout);
        Assert.True(await loq.TryEnterWriteLock().AsTask().WaitAsync(_timeout));
    }

    [Fact]
    public async Task WriteFollowingRead()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLock().WaitAsync(_timeout);
        Assert.Equal((uint)1, loq._state.Readers);
        await loq.ExitReadLock().WaitAsync(_timeout);
        Assert.Equal((uint)0, loq._state.Readers);
        await loq.EnterWriteLock().WaitAsync(_timeout);
        Assert.Equal((uint)1, loq._state.Writers);
        await loq.ExitWriteLock().WaitAsync(_timeout);
        Assert.Equal((uint)0, loq._state.Writers);
    }

    [Fact]
    public async Task ReadersShouldNotBeBlocked()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLock().WaitAsync(_timeout);
        await loq.EnterReadLock().WaitAsync(_timeout);
        await loq.ExitReadLock().WaitAsync(_timeout);
        await loq.ExitReadLock().WaitAsync(_timeout);
    }

    [Fact]
    public void WriteWriteRead()
    {
        var loq = new AsyncReaderWriterLock();

        var w3 = loq.EnterWriteLock().WaitAsync(_timeout);
        Assert.True(w3.IsCompleted);

        var w1 = loq.EnterWriteLock().WaitAsync(_timeout);
        Assert.False(w1.IsCompleted);

        var r2 = loq.EnterReadLock().WaitAsync(_timeout);
        Assert.False(r2.IsCompleted);

        w3 = loq.ExitWriteLock().WaitAsync(_timeout);
        Assert.True(w3.IsCompleted);
        Assert.True(w1.IsCompleted);
        Assert.False(r2.IsCompleted);

        w1 = loq.ExitWriteLock().WaitAsync(_timeout);
        Assert.True(w3.IsCompleted);
        Assert.True(w1.IsCompleted);
        Assert.True(r2.IsCompleted);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(10)]
    public async Task WritersShouldExit(int count)
    {
        var loq = new AsyncReaderWriterLock();

        var tasks = new List<Task>();
        for (int i = 0; i < count; i++)
        {
            tasks.Add(loq.EnterWriteLock());
        }

        for (int i = 0; i < count; i++)
        {
            tasks.Add(loq.ExitWriteLock());
        }

        await Task.WhenAll(tasks.ToArray());
    }

    [Fact]
    public async Task WritersShouldBlockWriters()
    {
        var loq = new AsyncReaderWriterLock();

        var lock1 = loq.EnterWriteLock().WaitAsync(_timeout);

        Assert.True(lock1.IsCompletedSuccessfully);

        var lock2 = loq.EnterWriteLock();

        Assert.False(lock2.IsCompletedSuccessfully);

        await loq.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(lock2.IsCompletedSuccessfully);

        await lock2;
    }

    [Fact]
    public async Task WritersShouldBlockWritersAwaited()
    {
        var loq = new AsyncReaderWriterLock();
        var delay = TimeSpan.FromMilliseconds(5);

        await loq.EnterWriteLock().WaitAsync(_timeout);
        await Task.Delay(delay);
        await loq.ExitWriteLock().WaitAsync(_timeout);

        await loq.EnterWriteLock().WaitAsync(_timeout);
        await Task.Delay(delay);
        await loq.ExitWriteLock().WaitAsync(_timeout);
    }

    [Fact]
    public async Task WritersShouldBlockReaders()
    {
        var loq = new AsyncReaderWriterLock();

        var lock1 = loq.EnterWriteLock().WaitAsync(_timeout);

        Assert.True(lock1.IsCompletedSuccessfully);

        var lock2 = loq.EnterReadLock();

        Assert.False(lock2.IsCompletedSuccessfully);

        await loq.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(lock2.IsCompletedSuccessfully);

        await lock2;
    }

    [Fact]
    public async Task ReadersShouldBlockWriter()
    {
        var loq = new AsyncReaderWriterLock();

        var lock1 = loq.EnterReadLock().WaitAsync(_timeout);

        Assert.True(lock1.IsCompletedSuccessfully);

        var lock2 = loq.EnterWriteLock();

        Assert.False(lock2.IsCompletedSuccessfully);

        await loq.ExitReadLock().WaitAsync(_timeout);

        Assert.True(lock2.IsCompletedSuccessfully);

        await lock2;
    }

    [Fact]
    public async Task WriterShouldUnblockWriterInOrder()
    {
        var loq = new AsyncReaderWriterLock();

        var t1 = loq.EnterWriteLock();
        var t2 = loq.EnterWriteLock();
        var t3 = loq.EnterWriteLock();

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.False(t2.IsCompletedSuccessfully);
        Assert.False(t3.IsCompletedSuccessfully);

        await loq.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.True(t2.IsCompletedSuccessfully);
        Assert.False(t3.IsCompletedSuccessfully);

        await loq.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.True(t2.IsCompletedSuccessfully);
        Assert.True(t3.IsCompletedSuccessfully);

        await loq.ExitWriteLock().WaitAsync(_timeout);
    }

    [Fact]
    public async Task WriterShouldUnblockAllReadersInOrder()
    {
        var loq = new AsyncReaderWriterLock();

        var w1 = loq.EnterWriteLock();
        var r2 = loq.EnterReadLock();
        var r3 = loq.EnterReadLock();
        var w4 = loq.EnterWriteLock();

        Assert.True(w1.IsCompletedSuccessfully);
        Assert.False(r2.IsCompletedSuccessfully);
        Assert.False(r3.IsCompletedSuccessfully);
        Assert.False(w4.IsCompletedSuccessfully);

        await loq.ExitWriteLock().WaitAsync(_timeout);

        Assert.True(w1.IsCompletedSuccessfully);
        Assert.True(r2.IsCompletedSuccessfully);
        Assert.True(r3.IsCompletedSuccessfully);
        Assert.False(w4.IsCompletedSuccessfully);

        await loq.ExitReadLock().WaitAsync(_timeout);

        Assert.True(w1.IsCompletedSuccessfully);
        Assert.True(r2.IsCompletedSuccessfully);
        Assert.True(r3.IsCompletedSuccessfully);
        Assert.False(w4.IsCompletedSuccessfully);

        await loq.ExitReadLock().WaitAsync(_timeout);

        Assert.True(w1.IsCompletedSuccessfully);
        Assert.True(r2.IsCompletedSuccessfully);
        Assert.True(r3.IsCompletedSuccessfully);
        Assert.True(w4.IsCompletedSuccessfully);

        await loq.ExitWriteLock().WaitAsync(_timeout);
    }

    [Fact]
    public async Task ExitReadLockThrowExceptionWhenUnexpected()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterWriteLock().WaitAsync(_timeout);
        
        await Assert.ThrowsAsync<SynchronizationLockException>(loq.ExitReadLock);
    }

    [Fact]
    public async Task ExitWriteLockThrowExceptionWhenUnexpected()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLock().WaitAsync(_timeout);

        await Assert.ThrowsAsync<SynchronizationLockException>(loq.ExitWriteLock);
    }
}
