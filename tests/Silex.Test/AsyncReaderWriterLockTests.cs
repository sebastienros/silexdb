namespace Silex.Test;


public class AsyncReaderWriterLockTests
{
    private readonly TimeSpan _timeout = TimeSpan.FromSeconds(1);

    private readonly TextWriter? _output = null;

    [Test]
    [Arguments(1)]
    [Arguments(3)]
    [Arguments(10)]
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

    [Test]
    public async Task ReadersShouldExit()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        loq.ExitReadLock();
    }

    [Test]
    public async Task TryEnterWriteLockShouldWork()
    {
        var loq = new AsyncReaderWriterLock();

        await Assert.That(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout)).IsTrue();
        loq.ExitWriteLock();

        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        await Assert.That(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout)).IsFalse();
        
        loq.ExitReadLock();
        await Assert.That(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout)).IsTrue();
        loq.ExitWriteLock();

        await loq.EnterWriteLockAsync().WaitAsync(_timeout);
        await Assert.That(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout)).IsFalse();

        loq.ExitWriteLock();
        await Assert.That(await loq.TryEnterWriteLockAsync().AsTask().WaitAsync(_timeout)).IsTrue();
    }

    [Test]
    public async Task WriteFollowingRead()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        await Assert.That(loq._state.Readers).IsEqualTo((uint)1);
        loq.ExitReadLock();
        await Assert.That(loq._state.Readers).IsEqualTo((uint)0);
        await loq.EnterWriteLockAsync().WaitAsync(_timeout);
        await Assert.That(loq._state.Writers).IsEqualTo((uint)1);
        loq.ExitWriteLock();
        await Assert.That(loq._state.Writers).IsEqualTo((uint)0);
    }

    [Test]
    public async Task ReadersShouldNotBeBlocked()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        await loq.EnterReadLockAsync().WaitAsync(_timeout);
        loq.ExitReadLock();
        loq.ExitReadLock();
    }

    [Test]
    public async Task WriteWriteRead()
    {
        var loq = new AsyncReaderWriterLock();

        var w3 = loq.EnterWriteLockAsync().WaitAsync(_timeout);
        await Assert.That(w3.IsCompleted).IsTrue();

        var w1 = loq.EnterWriteLockAsync().WaitAsync(_timeout);
        await Assert.That(w1.IsCompleted).IsFalse();

        var r2 = loq.EnterReadLockAsync().WaitAsync(_timeout);
        await Assert.That(r2.IsCompleted).IsFalse();

        loq.ExitWriteLock();
        await Assert.That(w1.IsCompleted).IsTrue();
        await Assert.That(r2.IsCompleted).IsFalse();

        loq.ExitReadLock();
        await Assert.That(w3.IsCompleted).IsTrue();
        await Assert.That(r2.IsCompleted).IsTrue();
    }

    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(10)]
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

    [Test]
    public async Task WritersShouldBlockWriters()
    {
        var loq = new AsyncReaderWriterLock();

        var lock1 = loq.EnterWriteLockAsync().WaitAsync(_timeout);

        await Assert.That(lock1.IsCompletedSuccessfully).IsTrue();

        var lock2 = loq.EnterWriteLockAsync();

        await Assert.That(lock2.IsCompletedSuccessfully).IsFalse();

        loq.ExitWriteLock();

        await Assert.That(lock2.IsCompletedSuccessfully).IsTrue();

        await lock2;
    }

    [Test]
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

    [Test]
    public async Task WritersShouldBlockReaders()
    {
        var loq = new AsyncReaderWriterLock();

        var lock1 = loq.EnterWriteLockAsync().WaitAsync(_timeout);

        await Assert.That(lock1.IsCompletedSuccessfully).IsTrue();

        var lock2 = loq.EnterReadLockAsync();

        await Assert.That(lock2.IsCompletedSuccessfully).IsFalse();

        loq.ExitWriteLock();

        await Assert.That(lock2.IsCompletedSuccessfully).IsTrue();

        await lock2;
    }

    [Test]
    public async Task ReadersShouldBlockWriter()
    {
        var loq = new AsyncReaderWriterLock();

        var lock1 = loq.EnterReadLockAsync().WaitAsync(_timeout);

        await Assert.That(lock1.IsCompletedSuccessfully).IsTrue();

        var lock2 = loq.EnterWriteLockAsync();

        await Assert.That(lock2.IsCompletedSuccessfully).IsFalse();

        loq.ExitReadLock();

        await Assert.That(lock2.IsCompletedSuccessfully).IsTrue();

        await lock2;
    }

    [Test]
    public async Task WriterShouldUnblockWriterInOrder()
    {
        var loq = new AsyncReaderWriterLock();

        var t1 = loq.EnterWriteLockAsync();
        var t2 = loq.EnterWriteLockAsync();
        var t3 = loq.EnterWriteLockAsync();

        await Assert.That(t1.IsCompletedSuccessfully).IsTrue();
        await Assert.That(t2.IsCompletedSuccessfully).IsFalse();
        await Assert.That(t3.IsCompletedSuccessfully).IsFalse();

        loq.ExitWriteLock();

        await Assert.That(t1.IsCompletedSuccessfully).IsTrue();
        await Assert.That(t2.IsCompletedSuccessfully).IsTrue();
        await Assert.That(t3.IsCompletedSuccessfully).IsFalse();

        loq.ExitWriteLock();

        await Assert.That(t1.IsCompletedSuccessfully).IsTrue();
        await Assert.That(t2.IsCompletedSuccessfully).IsTrue();
        await Assert.That(t3.IsCompletedSuccessfully).IsTrue();

        loq.ExitWriteLock();
    }

    [Test]
    public async Task WriterShouldUnblockAllReadersInOrder()
    {
        var loq = new AsyncReaderWriterLock();

        var w1 = loq.EnterWriteLockAsync();
        var r2 = loq.EnterReadLockAsync();
        var r3 = loq.EnterReadLockAsync();
        var w4 = loq.EnterWriteLockAsync();

        await Assert.That(w1.IsCompletedSuccessfully).IsTrue();
        await Assert.That(r2.IsCompletedSuccessfully).IsFalse();
        await Assert.That(r3.IsCompletedSuccessfully).IsFalse();
        await Assert.That(w4.IsCompletedSuccessfully).IsFalse();

        loq.ExitWriteLock();

        await Assert.That(w1.IsCompletedSuccessfully).IsTrue();
        await Assert.That(r2.IsCompletedSuccessfully).IsTrue();
        await Assert.That(r3.IsCompletedSuccessfully).IsTrue();
        await Assert.That(w4.IsCompletedSuccessfully).IsFalse();

        loq.ExitReadLock();

        await Assert.That(w1.IsCompletedSuccessfully).IsTrue();
        await Assert.That(r2.IsCompletedSuccessfully).IsTrue();
        await Assert.That(r3.IsCompletedSuccessfully).IsTrue();
        await Assert.That(w4.IsCompletedSuccessfully).IsFalse();

        loq.ExitReadLock();

        await Assert.That(w1.IsCompletedSuccessfully).IsTrue();
        await Assert.That(r2.IsCompletedSuccessfully).IsTrue();
        await Assert.That(r3.IsCompletedSuccessfully).IsTrue();
        await Assert.That(w4.IsCompletedSuccessfully).IsTrue();

        loq.ExitWriteLock();
    }

    [Test]
    public async Task ExitReadLockThrowExceptionWhenUnexpected()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterWriteLockAsync().WaitAsync(_timeout);
        
        await Assert.That(loq.ExitReadLock).Throws<SynchronizationLockException>();
    }

    [Test]
    public async Task ExitWriteLockThrowExceptionWhenUnexpected()
    {
        var loq = new AsyncReaderWriterLock();

        await loq.EnterReadLockAsync().WaitAsync(_timeout);

        await Assert.That(loq.ExitWriteLock).Throws<SynchronizationLockException>();
    }
}
