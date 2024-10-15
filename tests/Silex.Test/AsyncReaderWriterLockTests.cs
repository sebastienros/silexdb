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
                        t = loq.EnterReadLockAsync().WaitAsync(timeout);
                        if (t == Task.CompletedTask) _output?.WriteLine($"Go {id} r:{loq._state.Readers},w:{loq._state.Writers}"); else _output?.WriteLine($"Wait {id} r:{loq._state.Readers},w:{loq._state.Writers}");
                        await t;
                        _output?.WriteLine($"Do({id})");
                        await Task.Delay(delay);
                        loq.ExitReadLock();
                        _output?.WriteLine($"~Read({id}) r:{loq._state.Readers},w:{loq._state.Writers}");
                        break;

                    case 1: // Write
                        _output?.WriteLine($"Write({id}) r:{loq._state.Readers},w:{loq._state.Writers}");
                        t = loq.EnterWriteLockAsync().WaitAsync(timeout);
                        if (t == Task.CompletedTask) _output?.WriteLine($"Go {id} r:{loq._state.Readers},w:{loq._state.Writers}"); else _output?.WriteLine($"Wait {id} r:{loq._state.Readers},w:{loq._state.Writers}");
                        await t;
                        _output?.WriteLine($"Do({id})");
                        await Task.Delay(delay);
                        loq.ExitWriteLock();
                        _output?.WriteLine($"~Write({id}) r:{loq._state.Readers},w:{loq._state.Writers}");
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

        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        loq.ExitReadLock();
    }

    [Fact]
    public async Task TryEnterWriteLockShouldWork()
    {
        var loq = new AsyncReaderWriterLock();

        Assert.True(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout));
        loq.ExitWriteLock();

        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        Assert.False(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout));
        
        loq.ExitReadLock();
        Assert.True(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout));
        loq.ExitWriteLock();

        await loq.EnterWriteLockAsync().WaitAsync(_timeout);
        Assert.False(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout));

        loq.ExitWriteLock();
        Assert.True(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout));
    }

    [Fact]
    public async Task WriteFollowingRead()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        Assert.Equal((uint)1, loq._state.Readers);
        loq.ExitReadLock();
        Assert.Equal((uint)0, loq._state.Readers);
        await loq.EnterWriteLockAsync().WaitAsync(_timeout);
        Assert.Equal((uint)1, loq._state.Writers);
        loq.ExitWriteLock();
        Assert.Equal((uint)0, loq._state.Writers);
    }

    [Fact]
    public async Task ReadersShouldNotBeBlocked()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        loq.ExitReadLock();
        loq.ExitReadLock();
    }

    [Fact]
    public void WriteWriteRead()
    {
        var loq = new AsyncReaderWriterLock();

        var w3 = loq.EnterWriteLockAsync().WaitAsync(_timeout);
        Assert.True(w3.IsCompleted);

        var w1 = loq.EnterWriteLockAsync().WaitAsync(_timeout);
        Assert.False(w1.IsCompleted);

        var r2 = loq.EnterReadLockAsync().WaitAsync(_timeout);
        Assert.False(r2.IsCompleted);

        loq.ExitWriteLock();
        Assert.True(w1.IsCompleted);
        Assert.False(r2.IsCompleted);

        loq.ExitReadLock();
        Assert.True(w3.IsCompleted);
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
            tasks.Add(loq.EnterWriteLockAsync());
        }

        for (int i = 0; i < count; i++)
        {
            loq.ExitWriteLock();
        }

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task WritersShouldBlockWriters()
    {
        var loq = new AsyncReaderWriterLock();

        var lock1 = loq.EnterWriteLockAsync().WaitAsync(_timeout);

        Assert.True(lock1.IsCompletedSuccessfully);

        var lock2 = loq.EnterWriteLockAsync();

        Assert.False(lock2.IsCompletedSuccessfully);

        loq.ExitWriteLock();

        Assert.True(lock2.IsCompletedSuccessfully);

        await lock2;
    }

    [Fact]
    public async Task WritersShouldBlockWritersAwaited()
    {
        var loq = new AsyncReaderWriterLock();
        var delay = TimeSpan.FromMilliseconds(5);

        await loq.EnterWriteLockAsync().WaitAsync(_timeout);
        await Task.Delay(delay);
        loq.ExitWriteLock();

        await loq.EnterWriteLockAsync().WaitAsync(_timeout);
        await Task.Delay(delay);
        loq.ExitWriteLock();
    }

    [Fact]
    public async Task WritersShouldBlockReaders()
    {
        var loq = new AsyncReaderWriterLock();

        var lock1 = loq.EnterWriteLockAsync().WaitAsync(_timeout);

        Assert.True(lock1.IsCompletedSuccessfully);

        var lock2 = loq.EnterReadLockAsync();

        Assert.False(lock2.IsCompletedSuccessfully);

        loq.ExitWriteLock();

        Assert.True(lock2.IsCompletedSuccessfully);

        await lock2;
    }

    [Fact]
    public async Task ReadersShouldBlockWriter()
    {
        var loq = new AsyncReaderWriterLock();

        var lock1 = loq.EnterReadLockAsync().WaitAsync(_timeout);

        Assert.True(lock1.IsCompletedSuccessfully);

        var lock2 = loq.EnterWriteLockAsync();

        Assert.False(lock2.IsCompletedSuccessfully);

        loq.ExitReadLock();

        Assert.True(lock2.IsCompletedSuccessfully);

        await lock2;
    }

    [Fact]
    public void WriterShouldUnblockWriterInOrder()
    {
        var loq = new AsyncReaderWriterLock();

        var t1 = loq.EnterWriteLockAsync();
        var t2 = loq.EnterWriteLockAsync();
        var t3 = loq.EnterWriteLockAsync();

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.False(t2.IsCompletedSuccessfully);
        Assert.False(t3.IsCompletedSuccessfully);

        loq.ExitWriteLock();

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.True(t2.IsCompletedSuccessfully);
        Assert.False(t3.IsCompletedSuccessfully);

        loq.ExitWriteLock();

        Assert.True(t1.IsCompletedSuccessfully);
        Assert.True(t2.IsCompletedSuccessfully);
        Assert.True(t3.IsCompletedSuccessfully);

        loq.ExitWriteLock();
    }

    [Fact]
    public void WriterShouldUnblockAllReadersInOrder()
    {
        var loq = new AsyncReaderWriterLock();

        var w1 = loq.EnterWriteLockAsync();
        var r2 = loq.EnterReadLockAsync();
        var r3 = loq.EnterReadLockAsync();
        var w4 = loq.EnterWriteLockAsync();

        Assert.True(w1.IsCompletedSuccessfully);
        Assert.False(r2.IsCompletedSuccessfully);
        Assert.False(r3.IsCompletedSuccessfully);
        Assert.False(w4.IsCompletedSuccessfully);

        loq.ExitWriteLock();

        Assert.True(w1.IsCompletedSuccessfully);
        Assert.True(r2.IsCompletedSuccessfully);
        Assert.True(r3.IsCompletedSuccessfully);
        Assert.False(w4.IsCompletedSuccessfully);

        loq.ExitReadLock();

        Assert.True(w1.IsCompletedSuccessfully);
        Assert.True(r2.IsCompletedSuccessfully);
        Assert.True(r3.IsCompletedSuccessfully);
        Assert.False(w4.IsCompletedSuccessfully);

        loq.ExitReadLock();

        Assert.True(w1.IsCompletedSuccessfully);
        Assert.True(r2.IsCompletedSuccessfully);
        Assert.True(r3.IsCompletedSuccessfully);
        Assert.True(w4.IsCompletedSuccessfully);

        loq.ExitWriteLock();
    }

    [Fact]
    public async Task ExitReadLockThrowExceptionWhenUnexpected()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterWriteLockAsync().WaitAsync(_timeout);
        
        Assert.Throws<SynchronizationLockException>(loq.ExitReadLock);
    }

    [Fact]
    public async Task ExitWriteLockThrowExceptionWhenUnexpected()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLockAsync().WaitAsync(_timeout);

        Assert.Throws<SynchronizationLockException>(loq.ExitWriteLock);
    }
}
