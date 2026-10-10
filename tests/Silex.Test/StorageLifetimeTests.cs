namespace Silex.Test;

public class StorageLifetimeTests
{
    private static StorageOptions Options => new() { FlushPeriod = TimeSpan.Zero, CompactionStrategy = CompactionStrategy.None };
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(20);

    [Test]
    [Arguments("normal")]
    [Arguments("salvage")]
    [Arguments("mvcc")]
    public async Task AllHandlesExclusivelyOwnDirectory(string mode)
    {
        using var folder = TempFolder.Create();
        IAsyncDisposable owner = mode switch
        {
            "salvage" => await LsmStorage.OpenSalvageAsync(folder, Options),
            "mvcc" => await MvccStorage.OpenAsync(folder, Options),
            _ => await LsmStorage.OpenAsync(folder, Options)
        };
        await using (owner)
        {
            await AssertRejectedAsync(folder);
            await AssertRejectedAsync(Path.Combine(folder, "."));
            foreach (var attempt in new[] { "normal", "salvage", "integrity" })
            {
                await DirectoryOwnershipTestProcess.AssertRejectedAsync(folder, attempt);
            }
        }
        await Assert.That(File.Exists(Path.Combine(folder, DirectoryOwnership.FileName))).IsTrue();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
    }

    [Test]
    public async Task ProcessExitReleasesOwnershipWithoutDeletingLockFile()
    {
        using var folder = TempFolder.Create();
        await CrashRecoveryTestProcess.WriteBatchAndExitWithoutDisposalAsync(folder, 2);
        await Assert.That(File.Exists(Path.Combine(folder, DirectoryOwnership.FileName))).IsTrue();
        await using var storage = await LsmStorage.OpenAsync(folder, Options);
        await Assert.That(await storage.ScanRawAsync(0, static (_, _, _) => true)).IsEqualTo(2L);
    }

    [Test]
    public async Task IntegrityCheckOwnsDirectoryAndReleasesAfterFailure()
    {
        using var folder = TempFolder.Create();
        await using (var storage = await LsmStorage.OpenAsync(folder, Options))
        {
            storage.Put([1], [11]);
        }
        var options = Options;
        var factory = new BlockingEncoderFactory();
        options.BlockEncoderFactory = factory;
        var verification = Task.Run(() => LsmStorage.VerifyIntegrityAsync(folder, options));
        try
        {
            await factory.Entered.Task.WaitAsync(Timeout);
            await AssertRejectedAsync(folder);
        }
        finally
        {
            factory.Release.Set();
        }
        await Assert.That(async () => await verification).Throws<InvalidOperationException>();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
    }

    [Test]
    [Arguments(false, "point")]
    [Arguments(true, "point")]
    [Arguments(false, "scan")]
    [Arguments(true, "scan")]
    [Arguments(true, "seek")]
    [Arguments(true, "salvage")]
    public async Task ShutdownDrainsBorrowedReadsAndRejectsNewWork(bool onDisk, string mode)
    {
        using var folder = TempFolder.Create();
        var storage = await LsmStorage.OpenAsync(folder, Options);
        storage.Put([1], [11]);
        if (onDisk)
        {
            await storage.FlushAndCompactAsync();
        }
        if (mode == "salvage")
        {
            await storage.DisposeAsync();
            storage = await LsmStorage.OpenSalvageAsync(folder, Options);
        }
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var read = Task.Run(async () =>
        {
            var state = (entered, release);
            if (mode is "point" or "salvage")
            {
                return await storage.TryReadRawAsync(new byte[] { 1 }, state, Observe);
            }
            var count = mode == "scan"
                ? await storage.ScanRawAsync(state, static (s, _, value) => { Observe(s, value); return true; })
                : await storage.SeekRawAsync(new byte[] { 1 }, state, static (s, _, value) => { Observe(s, value); return true; });
            return count == 1;
        });
        Task? close = null;
        Task? synchronousDispose = null;
        try
        {
            await entered.Task.WaitAsync(Timeout);
            close = storage.CloseAsync();
            synchronousDispose = Task.Run(storage.Dispose);
            await Assert.That(close.IsCompleted).IsFalse();
            await Assert.That(ReferenceEquals(close, storage.CloseAsync())).IsTrue();
            await AssertNewWorkRejectedAsync(storage);
            await AssertRejectedAsync(folder);
        }
        finally
        {
            release.Set();
        }
        await Assert.That(await read.WaitAsync(Timeout)).IsTrue();
        await close!.WaitAsync(Timeout);
        await synchronousDispose!.WaitAsync(Timeout);
        await storage.DisposeAsync();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Disposed);
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        var value = new byte[1];
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);

        static void Observe((TaskCompletionSource entered, ManualResetEventSlim release) state, ReadOnlySpan<byte> value)
        {
            state.entered.SetResult();
            if (!state.release.Wait(Timeout))
            {
                throw new TimeoutException("Shutdown did not release read.");
            }
            if (value[0] != 11)
            {
                throw new InvalidOperationException("Borrowed memory changed during shutdown.");
            }
        }
    }

    [Test]
    [Arguments("write")]
    [Arguments("manual")]
    [Arguments("background")]
    public async Task ShutdownDrainsWritesAndMaintenance(string mode)
    {
        using var folder = TempFolder.Create();
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var options = Options;
        if (mode == "background")
        {
            options.FlushPeriod = TimeSpan.FromMilliseconds(1);
            options.MemTableSizeLimit = 1;
            options.MemTableMaxCount = 1;
        }
        var armed = false;
        options.FaultInjector = point =>
        {
            if (Volatile.Read(ref armed) && point == (mode == "write"
                    ? StorageFaultPoint.BatchBeforeApply : StorageFaultPoint.FlushBeforeBuild))
            {
                entered.TrySetResult();
                if (!release.Wait(Timeout))
                {
                    throw new TimeoutException("Shutdown did not release mutation.");
                }
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        Task work;
        if (mode == "write")
        {
            Volatile.Write(ref armed, true);
            work = Task.Run(() => storage.Put([1], [11]));
        }
        else
        {
            if (mode == "background")
            {
                Volatile.Write(ref armed, true);
            }
            storage.Put([1], [11]);
            Volatile.Write(ref armed, true);
            work = mode == "manual" ? Task.Run(() => storage.FlushAndCompactAsync()) : Task.CompletedTask;
        }
        Task? close = null;
        Task secondMaintenance = Task.CompletedTask;
        try
        {
            await entered.Task.WaitAsync(Timeout);
            if (mode == "manual")
            {
                secondMaintenance = storage.FlushAndCompactAsync();
                await Assert.That(secondMaintenance.IsCompleted).IsFalse();
            }
            close = storage.CloseAsync();
            await Assert.That(close.IsCompleted).IsFalse();
            await AssertNewWorkRejectedAsync(storage);
            await AssertRejectedAsync(folder);
        }
        finally
        {
            release.Set();
        }
        await Task.WhenAll(work, secondMaintenance).WaitAsync(Timeout);
        await close!.WaitAsync(Timeout);
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        var value = new byte[1];
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);
    }

    [Test]
    public async Task ShutdownFailureRetainsOwnershipUntilRecoveryPreservingCleanup()
    {
        using var folder = TempFolder.Create();
        var options = Options;
        options.FaultInjector = point =>
        {
            if (point == StorageFaultPoint.FlushBeforeBuild)
            {
                throw new IOException("Injected shutdown failure.");
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        storage.Put([1], [11]);
        await Assert.That(async () => await storage.CloseAsync()).Throws<IOException>();
        await Assert.That(storage.Health).IsEqualTo(StorageHealth.Faulted);
        await AssertRejectedAsync(folder);
        await AssertNewWorkRejectedAsync(storage);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, new byte[1])).IsEqualTo(1);
    }

    [Test]
    public async Task CloseCancellationIsAdmissionOnly()
    {
        using var folder = TempFolder.Create();
        var storage = await LsmStorage.OpenAsync(folder, Options);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await storage.CloseAsync(cancellation.Token)).Throws<OperationCanceledException>();
        storage.Put([1], [11]);
        await storage.DisposeAsync();
        await using var reopened = await LsmStorage.OpenAsync(folder, Options);
    }

    [Test]
    public async Task ConcurrentMvccDisposersDrainAnAdmittedCommit()
    {
        using var folder = TempFolder.Create();
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var armed = false;
        var options = Options;
        options.FaultInjector = point =>
        {
            if (Volatile.Read(ref armed) && point == StorageFaultPoint.MvccBeforePublication)
            {
                entered.SetResult();
                if (!release.Wait(Timeout))
                {
                    throw new TimeoutException("MVCC commit was not released.");
                }
            }
        };
        var storage = await MvccStorage.OpenAsync(folder, options);
        Volatile.Write(ref armed, true);
        var write = Task.Run(() => storage.Put([1], [11]));
        Task? close = null;
        Task? secondClose = null;
        try
        {
            await entered.Task.WaitAsync(Timeout);
            close = storage.DisposeAsync().AsTask();
            secondClose = storage.DisposeAsync().AsTask();
            await Assert.That(ReferenceEquals(close, secondClose)).IsTrue();
            await Assert.That(close.IsCompleted).IsFalse();
            await Assert.That(() => storage.CreateSnapshot()).Throws<ObjectDisposedException>();
            await Assert.That(() => storage.Put([2], [22])).Throws<ObjectDisposedException>();
            await AssertRejectedAsync(folder);
        }
        finally
        {
            release.Set();
        }
        await write.WaitAsync(Timeout);
        await Task.WhenAll(close!, secondClose!).WaitAsync(Timeout);
        await using var reopened = await MvccStorage.OpenAsync(folder, Options);
        await Assert.That(reopened.PublishedSequence).IsEqualTo(1L);
        await Assert.That(await reopened.GetRawAsync(new byte[] { 1 }, new byte[1])).IsEqualTo(1);
    }

    [Test]
    public async Task PointReadAdmissionDoesNotAllocateOnTheHotPath()
    {
        using var folder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(folder, Options);
        byte[] key = [1];
        byte[] value = [11];
        storage.Put(key, value);
        for (var i = 0; i < 1000; i++)
        {
            storage.GetRawAsync(key, value).GetAwaiter().GetResult();
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10000; i++)
        {
            storage.GetRawAsync(key, value).GetAwaiter().GetResult();
        }
        allocated = GC.GetAllocatedBytesForCurrentThread() - allocated;
        await Assert.That(allocated).IsEqualTo(0L);
    }

    [Test]
    public async Task IteratorLeaseDrainsUntilEnumeratorIsDisposed()
    {
        using var folder = TempFolder.Create();
        var storage = await LsmStorage.OpenAsync<int, int>(folder, Options);
        storage.Put(1, 11);
        var iterator = storage.CreateIterator();
        var enumerator = iterator.EnumerateAsync().GetAsyncEnumerator();
        await Assert.That(await enumerator.MoveNextAsync()).IsTrue();
        var close = storage.CloseAsync();
        await Assert.That(close.IsCompleted).IsFalse();
        await Assert.That(async () => await storage.GetAsync(1)).Throws<ObjectDisposedException>();
        await enumerator.DisposeAsync();
        await close.WaitAsync(Timeout);
        await Assert.That(async () =>
        {
            await foreach (var entry in iterator.EnumerateAsync())
            {
                GC.KeepAlive(entry);
            }
        }).Throws<ObjectDisposedException>();
        await using var reopened = await LsmStorage.OpenAsync<int, int>(folder, Options);
        await Assert.That(await reopened.GetAsync(1)).IsEqualTo(11);
    }

    private static async Task AssertRejectedAsync(string path)
    {
        await Assert.That(async () => await LsmStorage.OpenAsync(path, Options)).Throws<IOException>();
        await Assert.That(async () => await LsmStorage.OpenSalvageAsync(path, Options)).Throws<IOException>();
        await Assert.That(async () => await LsmStorage.VerifyIntegrityAsync(path, Options)).Throws<IOException>();
    }

    private static async Task AssertNewWorkRejectedAsync(LsmStorage storage)
    {
        await Assert.That(() => storage.Put([2], [22])).Throws<ObjectDisposedException>();
        await Assert.That(() => storage.Delete([1])).Throws<ObjectDisposedException>();
        await Assert.That(() => storage.Write(new LsmWriteBatch())).Throws<ObjectDisposedException>();
        await Assert.That(async () => await storage.GetRawAsync(new byte[] { 1 }, new byte[1])).Throws<ObjectDisposedException>();
        await Assert.That(async () => await storage.ScanRawAsync(0, static (_, _, _) => true)).Throws<ObjectDisposedException>();
        await Assert.That(async () => await storage.FlushAndCompactAsync()).Throws<ObjectDisposedException>();
    }

    private sealed class BlockingEncoderFactory : Silex.Blocks.IBlockEncoderFactory
    {
        internal readonly TaskCompletionSource Entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal readonly ManualResetEventSlim Release = new();

        public Silex.Blocks.IBlockEncoder Create()
        {
            Entered.SetResult();
            if (!Release.Wait(Timeout))
            {
                throw new TimeoutException("Integrity check was not released.");
            }
            throw new InvalidOperationException("Injected verification failure.");
        }
    }
}
