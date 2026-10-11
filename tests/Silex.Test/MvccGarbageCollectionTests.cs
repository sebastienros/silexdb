namespace Silex.Test;

public class MvccGarbageCollectionTests
{
    private static readonly StorageOptions _options = new()
    {
        UseWriteAheadLog = false,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
    };

    private static async Task<byte> ReadAsync(MvccSnapshot snapshot, byte key)
    {
        var buffer = new byte[1];
        await Assert.That(await snapshot.GetRawAsync([key], buffer)).IsEqualTo(1);
        return buffer[0];
    }

    private static async Task<byte> ReadAsync(MvccStorage storage, byte key)
    {
        var buffer = new byte[1];
        await Assert.That(await storage.GetRawAsync([key], buffer)).IsEqualTo(1);
        return buffer[0];
    }

    private static async Task<int> ReadIntAsync(MvccSnapshot snapshot, int key)
    {
        var buffer = new byte[4];
        await Assert.That(await snapshot.GetRawAsync([(byte)key], buffer)).IsEqualTo(4);
        return BitConverter.ToInt32(buffer);
    }

    [Test]
    public async Task ChunkedCollectionMatchesUnboundedCollection()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        for (byte version = 1; version <= 5; version++)
        {
            for (byte key = 0; key < 6; key++)
            {
                storage.Put([key], [version]);
            }
        }

        var progress = new List<MvccGarbageCollectionProgress>();
        var result = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions
        {
            ChunkSize = 4,
            Progress = new SyncProgress<MvccGarbageCollectionProgress>(progress.Add),
        });

        await Assert.That(result.IsCompleted).IsTrue();
        await Assert.That(result.Resume).IsNull();
        await Assert.That(result.VersionsExamined).IsEqualTo(30);
        await Assert.That(result.VersionsDeleted).IsEqualTo(24);
        await Assert.That(progress.Count).IsEqualTo(8);
        await Assert.That(progress[^1].Resume).IsNull();
        await Assert.That(await storage.CollectGarbageAsync()).IsEqualTo(0);
        for (byte key = 0; key < 6; key++)
        {
            await Assert.That(await ReadAsync(storage, key)).IsEqualTo((byte)5);
        }
    }

    [Test]
    public async Task ResumingMidKeyPreservesEverySnapshotVersion()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        var snapshots = new List<(MvccSnapshot Snapshot, byte Version)>();
        for (byte version = 1; version <= 10; version++)
        {
            storage.Put([1], [version]);
            storage.Put([2], [version]);
            if (version is 3 or 7)
            {
                snapshots.Add((storage.CreateSnapshot(), version));
            }
        }

        // Two versions per call guarantees resumes land inside a key's version chain.
        var calls = 0;
        long deleted = 0;
        MvccGarbageCollectionCursor? cursor = null;
        while (true)
        {
            var result = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions
            {
                MaxVersionsExamined = 2,
                ChunkSize = 1,
                Resume = cursor,
            });
            calls++;
            deleted += result.VersionsDeleted;
            await Assert.That(result.VersionsExamined).IsLessThanOrEqualTo(2);
            if (result.IsCompleted)
            {
                break;
            }
            await Assert.That(result.StopReason).IsEqualTo(MvccGarbageCollectionStopReason.ExaminedLimit);
            cursor = result.Resume;
            await Assert.That(cursor).IsNotNull();
        }

        // Per key: retain versions 10 (published), 7 and 3 (snapshots); delete the other seven.
        await Assert.That(deleted).IsEqualTo(14);
        await Assert.That(calls).IsEqualTo(10);
        foreach (var (snapshot, version) in snapshots)
        {
            await Assert.That(await ReadAsync(snapshot, 1)).IsEqualTo(version);
            await Assert.That(await ReadAsync(snapshot, 2)).IsEqualTo(version);
            snapshot.Dispose();
        }
        await Assert.That(await storage.CollectGarbageAsync()).IsEqualTo(4);
        await Assert.That(await ReadAsync(storage, 1)).IsEqualTo((byte)10);
    }

    [Test]
    public async Task DeleteLimitStopsAndResumes()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        for (byte version = 1; version <= 6; version++)
        {
            storage.Put([1], [version]);
        }

        var first = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { MaxVersionsDeleted = 2 });
        await Assert.That(first.StopReason).IsEqualTo(MvccGarbageCollectionStopReason.DeletedLimit);
        await Assert.That(first.VersionsDeleted).IsEqualTo(2);
        var second = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { Resume = first.Resume });
        await Assert.That(second.IsCompleted).IsTrue();
        await Assert.That(second.VersionsDeleted).IsEqualTo(3);
        await Assert.That(await ReadAsync(storage, 1)).IsEqualTo((byte)6);
    }

    [Test]
    public async Task TimeBudgetStillMakesProgressAndEventuallyCompletes()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        for (byte version = 1; version <= 20; version++)
        {
            storage.Put([1], [version]);
        }

        long deleted = 0;
        MvccGarbageCollectionCursor? cursor = null;
        for (var calls = 0; calls < 1000; calls++)
        {
            var result = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions
            {
                MaxDuration = TimeSpan.FromTicks(1),
                ChunkSize = 1,
                Resume = cursor,
            });
            await Assert.That(result.VersionsExamined).IsGreaterThan(0);
            deleted += result.VersionsDeleted;
            if (result.IsCompleted)
            {
                await Assert.That(deleted).IsEqualTo(19);
                return;
            }
            await Assert.That(result.StopReason).IsEqualTo(MvccGarbageCollectionStopReason.TimeBudget);
            cursor = result.Resume;
        }
        throw new InvalidOperationException("Collection did not complete.");
    }

    [Test]
    public async Task CommitsAndSnapshotsProceedWhileChunksAreChosenButNotApplied()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        for (byte version = 1; version <= 3; version++)
        {
            storage.Put([1], [version]);
        }

        MvccSnapshot? during = null;
        storage.GarbageCollectionBeforeApply = (chunk, _) =>
        {
            if (chunk == 0)
            {
                // The gate is free here: a synchronous commit would deadlock otherwise.
                storage.Put([1], [4]);
                during = storage.CreateSnapshot();
                storage.Put([1], [5]);
            }
            return ValueTask.CompletedTask;
        };

        var result = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions());
        storage.GarbageCollectionBeforeApply = null;

        // Versions 1 and 2 were obsolete when chosen; 3 is the horizon's newest; 4 and 5 postdate the horizon.
        await Assert.That(result.VersionsDeleted).IsEqualTo(2);
        await Assert.That(await ReadAsync(during!, 1)).IsEqualTo((byte)4);
        await Assert.That(await ReadAsync(storage, 1)).IsEqualTo((byte)5);
        during!.Dispose();
        await Assert.That(await storage.CollectGarbageAsync()).IsEqualTo(2);
        await Assert.That(await ReadAsync(storage, 1)).IsEqualTo((byte)5);
    }

    [Test]
    public async Task SnapshotCreatedBetweenChunksKeepsItsView()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        for (byte version = 1; version <= 4; version++)
        {
            storage.Put([1], [version]);
            storage.Put([2], [version]);
        }

        MvccSnapshot? between = null;
        storage.GarbageCollectionAfterApply = (chunk, _) =>
        {
            if (chunk == 0)
            {
                storage.Put([2], [9]);
                between = storage.CreateSnapshot();
                storage.Put([1], [8]);
            }
            return ValueTask.CompletedTask;
        };

        await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { ChunkSize = 2 });
        storage.GarbageCollectionAfterApply = null;
        await storage.CollectGarbageAsync();

        await Assert.That(await ReadAsync(between!, 1)).IsEqualTo((byte)4);
        await Assert.That(await ReadAsync(between!, 2)).IsEqualTo((byte)9);
        await Assert.That(await ReadAsync(storage, 1)).IsEqualTo((byte)8);
        await Assert.That(await ReadAsync(storage, 2)).IsEqualTo((byte)9);
        between!.Dispose();
    }

    [Test]
    public async Task ConcurrentWritersAndSnapshotsRemainConsistentDuringCollection()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        const int keys = 8;
        for (byte key = 0; key < keys; key++)
        {
            storage.Put([key], BitConverter.GetBytes(0));
        }

        using var stop = new CancellationTokenSource();
        var writer = Task.Run(() =>
        {
            var version = 0;
            while (!stop.IsCancellationRequested)
            {
                version++;
                for (byte key = 0; key < keys; key++)
                {
                    storage.Put([key], BitConverter.GetBytes(version));
                }
            }
        });
        var reader = Task.Run(async () =>
        {
            while (!stop.IsCancellationRequested)
            {
                using var snapshot = storage.CreateSnapshot();
                var first = await ReadIntAsync(snapshot, 0);
                await Task.Yield();
                await Assert.That(await ReadIntAsync(snapshot, 0)).IsEqualTo(first);
                // Keys are written in order, so a snapshot never sees a later key behind an earlier one.
                await Assert.That(await ReadIntAsync(snapshot, keys - 1)).IsLessThanOrEqualTo(first);
            }
        });

        for (var i = 0; i < 20; i++)
        {
            await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { ChunkSize = 5 });
        }
        await stop.CancelAsync();
        await writer;
        await reader;
        await storage.CollectGarbageAsync();
        await Assert.That(await storage.CollectGarbageAsync()).IsEqualTo(0);
    }

    [Test]
    public async Task CancellationKeepsAppliedChunksAndResumes()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        for (byte version = 1; version <= 8; version++)
        {
            storage.Put([1], [version]);
        }

        using var cts = new CancellationTokenSource();
        MvccGarbageCollectionCursor? last = null;
        storage.GarbageCollectionAfterApply = (chunk, _) =>
        {
            if (chunk == 1)
            {
                cts.Cancel();
            }
            return ValueTask.CompletedTask;
        };

        await Assert.That(async () => await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions
        {
            ChunkSize = 2,
            Progress = new SyncProgress<MvccGarbageCollectionProgress>(p => last = p.Resume),
        }, cts.Token)).Throws<OperationCanceledException>();
        storage.GarbageCollectionAfterApply = null;

        await Assert.That(last).IsNotNull();
        var rest = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { Resume = last });
        await Assert.That(rest.VersionsDeleted).IsEqualTo(4);
        await Assert.That(await storage.CollectGarbageAsync()).IsEqualTo(0);
        await Assert.That(await ReadAsync(storage, 1)).IsEqualTo((byte)8);
    }

    [Test]
    public async Task StaleCursorNeverDeletesRequiredVersions()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        for (byte version = 1; version <= 4; version++)
        {
            storage.Put([1], [version]);
        }
        var partial = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { MaxVersionsExamined = 2, ChunkSize = 1 });
        using var snapshot = storage.CreateSnapshot();
        storage.Put([1], [5]);

        await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { Resume = partial.Resume });
        await Assert.That(await ReadAsync(snapshot, 1)).IsEqualTo((byte)4);
        await Assert.That(await ReadAsync(storage, 1)).IsEqualTo((byte)5);
    }

    [Test]
    public async Task InvalidArgumentsAreRejected()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        await Assert.That(async () => await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { ChunkSize = 0 }))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(async () => await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { MaxDuration = TimeSpan.Zero }))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(async () => await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions
        {
            Resume = new MvccGarbageCollectionCursor(new byte[] { 7, 7, 7 }),
        })).Throws<ArgumentException>();
    }

    [Test]
    public async Task DisposalWaitsForInFlightChunkAndCollectionFails()
    {
        using var folder = TempFolder.Create();
        var storage = await MvccStorage.OpenAsync(folder, _options);
        for (byte version = 1; version <= 4; version++)
        {
            storage.Put([1], [version]);
        }

        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        storage.GarbageCollectionBeforeApply = async (_, _) =>
        {
            reached.SetResult();
            await release.Task;
        };

        var collection = storage.CollectGarbageAsync(new MvccGarbageCollectionOptions()).AsTask();
        await reached.Task;
        var disposal = storage.DisposeAsync().AsTask();
        await Task.Delay(50);
        await Assert.That(disposal.IsCompleted).IsFalse();
        release.SetResult();

        await Assert.That(async () => await collection).Throws<ObjectDisposedException>();
        await disposal;
    }

    [Test]
    public async Task CompactionOptionRunsOnlyWhenVersionsWereDeleted()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        storage.Put([1], [1]);
        var none = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { Compact = true });
        await Assert.That(none.Compacted).IsFalse();
        storage.Put([1], [2]);
        var some = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions { Compact = true });
        await Assert.That(some.Compacted).IsTrue();
        await Assert.That(some.VersionsDeleted).IsEqualTo(1);
    }

    private sealed class SyncProgress<T>(Action<T> handler) : IProgress<T>
    {
        public void Report(T value) => handler(value);
    }
}
