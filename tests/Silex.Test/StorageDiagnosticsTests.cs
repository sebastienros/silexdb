using System.Buffers.Binary;

namespace Silex.Test;

public class StorageDiagnosticsTests
{
    private static StorageOptions Options(bool wal = false, CompactionStrategy strategy = CompactionStrategy.None) => new()
    {
        UseWriteAheadLog = wal,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = strategy,
        MemTableSizeLimit = 4 * 1024,
        BlockSize = 512,
    };

    private static byte[] Key(int i)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, i);
        return key;
    }

    private static void Fill(LsmStorage storage, int from, int count)
    {
        var value = new byte[64];
        for (var i = from; i < from + count; i++)
        {
            storage.Put(Key(i), value);
        }
    }

    [Test]
    public async Task PublishedTableMetadataMatchesStatisticsAndOwnsKeyCopies()
    {
        using var folder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(folder, Options());
        await Assert.That((await storage.GetTablesAsync()).Length).IsEqualTo(0);
        Fill(storage, 0, 200);
        await storage.FlushAndCompactAsync();

        var tables = await storage.GetTablesAsync();
        var stats = await storage.GetStatisticsAsync();
        await Assert.That(tables.Length).IsEqualTo(stats.TotalTableCount);
        await Assert.That(tables.Sum(table => table.Bytes)).IsEqualTo(stats.TotalTableBytes);
        foreach (var level in stats.Levels)
        {
            await Assert.That(tables.Count(table => table.Level == level.Level)).IsEqualTo(level.TableCount);
            await Assert.That(tables.Where(table => table.Level == level.Level).Sum(table => table.Bytes)).IsEqualTo(level.Bytes);
        }

        var first = tables[0];
        var expected = first.FirstKey.ToArray();
        first.FirstKey[0] ^= 0xff;
        first.LastKey[0] ^= 0xff;
        var next = (await storage.GetTablesAsync()).Single(table => table.FileId == first.FileId);
        await Assert.That(next.FirstKey.SequenceEqual(expected)).IsTrue();
        var buffer = new byte[64];
        await Assert.That(await storage.GetRawAsync(expected, buffer)).IsEqualTo(64);
    }

    [Test]
    public async Task WriteAndFlushCountersAreReported()
    {
        using var folder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(folder, Options());
        var before = await storage.GetStatisticsAsync();
        await Assert.That(before.WriteOperations).IsEqualTo(0L);
        await Assert.That(before.WriteAmplification).IsNull();
        await Assert.That(before.Flush.Duration.Last).IsNull();
        await Assert.That(before.Wal.Enabled).IsFalse();
        await Assert.That(before.CompactionBacklogBytes).IsNull();

        Fill(storage, 0, 200);
        var pending = await storage.GetStatisticsAsync();
        await Assert.That(pending.WriteOperations).IsEqualTo(200L);
        await Assert.That(pending.UserBytesWritten).IsEqualTo(200L * (4 + 64));
        await Assert.That(pending.ImmutableMemTableCount).IsGreaterThan(0);
        await Assert.That(pending.ImmutableMemTableBytes).IsGreaterThan(0L);

        await storage.FlushAndCompactAsync();
        var after = await storage.GetStatisticsAsync();
        await Assert.That(after.ImmutableMemTableCount).IsEqualTo(0);
        await Assert.That(after.Flush.Duration.Count).IsGreaterThan(0L);
        await Assert.That(after.Flush.Duration.Last).IsNotNull();
        await Assert.That(after.Flush.OutputBytes).IsGreaterThan(0L);
        await Assert.That(after.Flush.IsRunning).IsFalse();
        await Assert.That(after.TotalTableCount).IsGreaterThan(0);
        await Assert.That(after.TotalTableBytes).IsEqualTo(after.Levels.Sum(l => l.Bytes));
        await Assert.That(after.WriteAmplification).IsNotNull();
        await Assert.That(after.WriteAmplification!.Value).IsGreaterThan(0d);
        await Assert.That(after.SpaceAmplificationEstimate!.Value).IsGreaterThanOrEqualTo(1d);
    }

    [Test]
    public async Task WalCountersDistinguishDisabledFromMeasured()
    {
        using var folder = TempFolder.Create();
        var options = Options(wal: true);
        options.SyncWriteAheadLogToDisk = true;
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        Fill(storage, 0, 10);
        await storage.SynchronizeAsync();
        var stats = await storage.GetStatisticsAsync();
        await Assert.That(stats.Wal.Enabled).IsTrue();
        await Assert.That(stats.Wal.SynchronizesEveryWrite).IsTrue();
        await Assert.That(stats.Wal.BytesWritten).IsGreaterThan(0L);
        await Assert.That(stats.Wal.Sync.Count).IsGreaterThanOrEqualTo(11L);
        await Assert.That(stats.Wal.Sync.Max).IsNotNull();
        await Assert.That(stats.Wal.Sync.Average).IsNotNull();
        await Assert.That(stats.Wal.Sync.Max!.Value).IsGreaterThanOrEqualTo(stats.Wal.Sync.Last!.Value);
    }

    [Test]
    public async Task WalWithoutPerWriteSyncReportsNoSyncSamples()
    {
        using var folder = TempFolder.Create();
        var options = Options(wal: true);
        options.SyncWriteAheadLogToDisk = false;
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        Fill(storage, 0, 5);
        var stats = await storage.GetStatisticsAsync();
        await Assert.That(stats.Wal.Enabled).IsTrue();
        await Assert.That(stats.Wal.BytesWritten).IsGreaterThan(0L);
        await Assert.That(stats.Wal.Sync.Count).IsEqualTo(0L);
        await Assert.That(stats.Wal.Sync.Max).IsNull();
        await Assert.That(stats.Wal.Sync.Average).IsNull();
    }

    [Test]
    public async Task CacheHitsAndMissesAreCounted()
    {
        using var folder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(folder, Options());
        Fill(storage, 0, 100);
        await storage.FlushAndCompactAsync();

        var buffer = new byte[64];
        await storage.GetRawAsync(Key(5), buffer);
        var first = await storage.GetStatisticsAsync();
        await Assert.That(first.Cache.Enabled).IsTrue();
        await Assert.That(first.Cache.Misses).IsGreaterThan(0L);
        await Assert.That(first.Cache.EntryCount).IsGreaterThan(0L);
        await Assert.That(first.Cache.SizeBytes).IsGreaterThan(0L);

        for (var i = 0; i < 5; i++)
        {
            await storage.GetRawAsync(Key(5), buffer);
        }

        var second = await storage.GetStatisticsAsync();
        await Assert.That(second.Cache.Misses).IsEqualTo(first.Cache.Misses);
        await Assert.That(second.Cache.Hits).IsGreaterThanOrEqualTo(first.Cache.Hits + 5);
        await Assert.That(second.Cache.HitRatio!.Value).IsGreaterThan(0d);
    }

    [Test]
    public async Task DisabledCacheNeverReportsHits()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.BlockCacheSizeLimit = 0;
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        Fill(storage, 0, 100);
        await storage.FlushAndCompactAsync();
        var buffer = new byte[64];
        await storage.GetRawAsync(Key(5), buffer);
        await storage.GetRawAsync(Key(5), buffer);
        var stats = await storage.GetStatisticsAsync();
        await Assert.That(stats.Cache.Enabled).IsFalse();
        await Assert.That(stats.Cache.Hits).IsEqualTo(0L);
        await Assert.That(stats.Cache.EntryCount).IsEqualTo(0L);
        await Assert.That(stats.Cache.Misses).IsGreaterThanOrEqualTo(2L);
    }

    [Test]
    public async Task ImmutableMemTableLimitRaisesPressureWithoutThrottling()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.MemTableMaxCount = 2;
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        await Assert.That((await storage.GetStatisticsAsync()).Pressure).IsEqualTo(StoragePressureCauses.None);

        Fill(storage, 0, 400);
        var stats = await storage.GetStatisticsAsync();
        await Assert.That(stats.ImmutableMemTableCount).IsGreaterThanOrEqualTo(2);
        await Assert.That(stats.Pressure.HasFlag(StoragePressureCauses.MemTableLimit)).IsTrue();
        await Assert.That(stats.WriterLockWait.Count).IsEqualTo(0L);
    }

    [Test]
    [Arguments(CompactionStrategy.Leveled)]
    [Arguments(CompactionStrategy.Tiered)]
    public async Task LevelZeroBacklogIsReportedForConfiguredStrategy(CompactionStrategy strategy)
    {
        using var folder = TempFolder.Create();
        var options = Options(strategy: strategy);
        options.Level0CompactionThreshold = 2;
        options.MaxCompactionTiers = 2;
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        for (var round = 0; round < 3; round++)
        {
            Fill(storage, round * 1000, 100);
            storage._inner.ForceFreezeMemTable();
            await storage._inner.ForceFlushNextImmutableMemTableAsync();
        }

        var stats = await storage.GetStatisticsAsync();
        await Assert.That(stats.Levels[0].TableCount).IsGreaterThanOrEqualTo(2);
        await Assert.That(stats.Pressure.HasFlag(StoragePressureCauses.Level0Backlog)).IsTrue();
        await Assert.That(stats.Pressure.HasFlag(StoragePressureCauses.CompactionBacklog)).IsTrue();
        await Assert.That(stats.CompactionBacklogBytes).IsNotNull();
        await Assert.That(stats.CompactionBacklogBytes!.Value).IsGreaterThan(0L);

        await storage.FlushAndCompactAsync();
        var drained = await storage.GetStatisticsAsync();
        await Assert.That(drained.Compaction.Duration.Count).IsGreaterThan(0L);
        await Assert.That(drained.Compaction.InputBytes).IsGreaterThan(0L);
        await Assert.That(drained.Compaction.OutputBytes).IsGreaterThan(0L);
        await Assert.That(drained.Pressure.HasFlag(StoragePressureCauses.Level0Backlog)).IsFalse();
    }

    [Test]
    public async Task RangeEstimateIsPhysicalAndBounded()
    {
        using var folder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(folder, Options());
        Fill(storage, 0, 1000);
        var unflushed = await storage.EstimateRangeAsync();
        await Assert.That(unflushed.ApproximateStoredBytes).IsGreaterThanOrEqualTo(0L);
        await Assert.That(unflushed.UnflushedMemTableBytesUpperBound).IsGreaterThan(0L);

        await storage.FlushAndCompactAsync();
        var stats = await storage.GetStatisticsAsync();
        var whole = await storage.EstimateRangeAsync();
        var sub = await storage.EstimateRangeAsync(new OrderedReadRange(Key(100), true, Key(200), false));
        var disjoint = await storage.EstimateRangeAsync(new OrderedReadRange(Key(5000), true, Key(6000), false));
        var empty = await storage.EstimateRangeAsync(new OrderedReadRange(Key(10), false, Key(10), false));

        await Assert.That(whole.ApproximateStoredBytes).IsGreaterThan(0L);
        await Assert.That(whole.ApproximateStoredBytes).IsLessThanOrEqualTo(stats.TotalTableBytes);
        await Assert.That(whole.OverlappingTables).IsEqualTo(stats.TotalTableCount);
        await Assert.That(sub.ApproximateStoredBytes).IsGreaterThan(0L);
        await Assert.That(sub.ApproximateStoredBytes).IsLessThan(whole.ApproximateStoredBytes);
        await Assert.That(sub.OverlappingBlocks).IsLessThan(whole.OverlappingBlocks);
        await Assert.That(sub.ApproximateDecodedBytes).IsGreaterThan(0L);
        await Assert.That(disjoint.ApproximateStoredBytes).IsEqualTo(0L);
        await Assert.That(disjoint.OverlappingTables).IsEqualTo(0);
        await Assert.That(empty.OverlappingBlocks).IsEqualTo(0);
        await Assert.That(whole.UnflushedMemTableBytesUpperBound).IsEqualTo(0L);
    }

    [Test]
    public async Task StatisticsRemainAvailableAfterFaultAndThrowAfterDispose()
    {
        using var folder = TempFolder.Create();
        var options = Options(wal: true);
        var cause = new IOException("injected");
        options.FaultInjector = point =>
        {
            if (point == StorageFaultPoint.WriteBeforePublication)
            {
                throw cause;
            }
        };
        var storage = await LsmStorage.OpenAsync(folder, options);
        await Assert.That(() => storage.Put(Key(1), new byte[8])).Throws<StorageOperationException>();

        var stats = await storage.GetStatisticsAsync();
        await Assert.That(stats.Health).IsEqualTo(StorageHealth.Faulted);
        await Assert.That(stats.Fault).IsNotNull();
        await Assert.That(stats.Pressure.HasFlag(StoragePressureCauses.Faulted)).IsTrue();
        await Assert.That(stats.WriteOperations).IsEqualTo(0L);

        await storage.DisposeAsync();
        await Assert.That(async () => await storage.GetStatisticsAsync()).Throws<ObjectDisposedException>();
        await Assert.That(async () => await storage.EstimateRangeAsync()).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task BackgroundMaintenanceFailureIsRecorded()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.FlushPeriod = TimeSpan.FromMilliseconds(10);
        options.MemTableMaxCount = 1;
        var cause = new IOException("flush failed");
        options.FaultInjector = point =>
        {
            if (point == StorageFaultPoint.FlushBeforeBuild)
            {
                throw cause;
            }
        };
        await using var storage = await LsmStorage.OpenAsync(folder, options);
        Fill(storage, 0, 200);

        LsmStorageStatistics stats;
        var deadline = DateTime.UtcNow.AddSeconds(30);
        while ((stats = await storage.GetStatisticsAsync()).BackgroundFailures.Count == 0)
        {
            await Assert.That(DateTime.UtcNow).IsLessThan(deadline);
            await Task.Delay(10);
        }

        await Assert.That(stats.BackgroundFailures.Count).IsEqualTo(1L);
        await Assert.That(ReferenceEquals(stats.BackgroundFailures.Last, cause)).IsTrue();
        await Assert.That(stats.BackgroundFailures.LastAt).IsNotNull();
        await Assert.That(stats.Health).IsEqualTo(StorageHealth.Faulted);
        await Assert.That(stats.Flush.Duration.Count).IsEqualTo(0L);
        await Assert.That(stats.Flush.IsRunning).IsFalse();
    }

    [Test]
    public async Task ReadViewsAreReportedUntilReleased()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        var idle = await storage.GetStatisticsAsync();
        await Assert.That(idle.ActiveReadViews).IsEqualTo(0);
        await Assert.That(idle.OldestRetainedSequence).IsNull();
        await Assert.That(idle.RetainedSequenceSpan).IsNull();
        await Assert.That(idle.OldestRetainedViewAge).IsNull();

        storage.Put([1], [1]);
        var old = storage.CreateSnapshot();
        var oldSequence = old.Sequence;
        storage.Put([1], [2]);
        storage.Put([1], [3]);
        var newer = storage.CreateSnapshot();
        await Task.Delay(20);

        var pinned = await storage.GetStatisticsAsync();
        await Assert.That(pinned.ActiveReadViews).IsEqualTo(2);
        await Assert.That(pinned.DistinctPinnedSequences).IsEqualTo(2);
        await Assert.That(pinned.OldestRetainedSequence).IsEqualTo(oldSequence);
        await Assert.That(pinned.RetainedSequenceSpan).IsEqualTo(pinned.PublishedSequence - oldSequence);
        await Assert.That(pinned.RetainedSequenceSpan!.Value).IsEqualTo(2L);
        await Assert.That(pinned.OldestRetainedViewAge!.Value).IsGreaterThan(TimeSpan.Zero);
        await Assert.That(pinned.CommitsPublished).IsEqualTo(3L);

        old.Dispose();
        var afterOld = await storage.GetStatisticsAsync();
        await Assert.That(afterOld.ActiveReadViews).IsEqualTo(1);
        await Assert.That(afterOld.OldestRetainedSequence).IsEqualTo(newer.Sequence);

        newer.Dispose();
        var released = await storage.GetStatisticsAsync();
        await Assert.That(released.ActiveReadViews).IsEqualTo(0);
        await Assert.That(released.OldestRetainedSequence).IsNull();
    }

    [Test]
    public async Task VersionRangeEstimateCoversRetainedHistory()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        for (var round = 0; round < 5; round++)
        {
            for (var i = 0; i < 100; i++)
            {
                storage.Put(Key(i), new byte[64]);
            }
        }

        await storage.CollectGarbageAsync(compact: false);
        await storage.FlushAndCompactAsync();
        var stats = await storage.GetStatisticsAsync();
        await Assert.That(stats.VersionStorage.ApproximateStoredBytes).IsGreaterThan(0L);
        var whole = await storage.EstimateRangeAsync();
        var sub = await storage.EstimateRangeAsync(new OrderedReadRange(Key(10), true, Key(20), true));
        var disjoint = await storage.EstimateRangeAsync(new OrderedReadRange(Key(5000), true, null));
        await Assert.That(sub.ApproximateStoredBytes).IsGreaterThan(0L);
        await Assert.That(sub.ApproximateStoredBytes).IsLessThan(whole.ApproximateStoredBytes);
        await Assert.That(disjoint.OverlappingBlocks).IsEqualTo(0);
    }

    [Test]
    public async Task GarbageCollectionCountersTrackCompletionAndFailure()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, Options());
        for (var i = 1; i <= 5; i++)
        {
            storage.Put([1], [(byte)i]);
        }

        var none = await storage.GetStatisticsAsync();
        await Assert.That(none.GarbageCollection.PassesCompleted).IsEqualTo(0L);
        await Assert.That(none.GarbageCollection.LastResult).IsNull();
        await Assert.That(none.GarbageCollection.Duration.Last).IsNull();

        var result = await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions());
        var done = (await storage.GetStatisticsAsync()).GarbageCollection;
        await Assert.That(done.PassesCompleted).IsEqualTo(1L);
        await Assert.That(done.VersionsDeleted).IsEqualTo(result.VersionsDeleted);
        await Assert.That(done.VersionsExamined).IsEqualTo(result.VersionsExamined);
        await Assert.That(done.LastResult!.Value.VersionsDeleted).IsEqualTo(4L);
        await Assert.That(done.IsRunning).IsFalse();

        var cause = new InvalidOperationException("injected");
        storage.GarbageCollectionBeforeApply = (_, _) => throw cause;
        await Assert.That(async () => await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions()))
            .Throws<InvalidOperationException>();
        storage.GarbageCollectionBeforeApply = null;

        using var cts = new CancellationTokenSource();
        storage.GarbageCollectionBeforeApply = (_, _) =>
        {
            cts.Cancel();
            cts.Token.ThrowIfCancellationRequested();
            return ValueTask.CompletedTask;
        };
        await Assert.That(async () => await storage.CollectGarbageAsync(new MvccGarbageCollectionOptions(), cts.Token))
            .Throws<OperationCanceledException>();
        storage.GarbageCollectionBeforeApply = null;

        var failed = (await storage.GetStatisticsAsync()).GarbageCollection;
        await Assert.That(failed.PassesFailed).IsEqualTo(1L);
        await Assert.That(ReferenceEquals(failed.LastFailure, cause)).IsTrue();
        await Assert.That(failed.LastFailureAt).IsNotNull();
        await Assert.That(failed.PassesCanceled).IsEqualTo(1L);
        await Assert.That(failed.PassesCompleted).IsEqualTo(1L);
        await Assert.That(failed.IsRunning).IsFalse();
    }

    [Test]
    public async Task CommitGateContentionIsMeasured()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        using var entered = new ManualResetEventSlim();
        using var release = new ManualResetEventSlim();
        var blocked = 0;
        options.FaultInjector = point =>
        {
            if (point == StorageFaultPoint.MvccBeforePublication && Interlocked.Exchange(ref blocked, 1) == 0)
            {
                entered.Set();
                release.Wait();
            }
        };
        await using var storage = await MvccStorage.OpenAsync(folder, options);
        var uncontended = await storage.GetStatisticsAsync();
        await Assert.That(uncontended.CommitGateWait.Count).IsEqualTo(0L);
        await Assert.That(uncontended.CommitGateWait.Max).IsNull();

        var first = Task.Run(() => storage.Put([1], [1]));
        entered.Wait();
        var second = Task.Run(() => storage.Put([2], [2]));
        while ((await storage.GetStatisticsAsync()).CommitGateWaiters == 0)
        {
            await Task.Delay(1);
        }

        release.Set();
        await Task.WhenAll(first, second);
        var stats = await storage.GetStatisticsAsync();
        await Assert.That(stats.CommitGateWaiters).IsEqualTo(0);
        await Assert.That(stats.CommitGateWait.Count).IsEqualTo(1L);
        await Assert.That(stats.CommitGateWait.Max!.Value).IsGreaterThan(TimeSpan.Zero);
        await Assert.That(stats.CommitGateAcquisitions).IsGreaterThanOrEqualTo(2L);
        await Assert.That(stats.Engine.WriterLockWait.Count).IsEqualTo(0L);
    }

    [Test]
    public async Task GroupCommitCountersAreReported()
    {
        using var folder = TempFolder.Create();
        var options = Options(wal: true);
        options.SyncWriteAheadLogToDisk = true;
        options.MvccCommitGroupSize = 8;
        await using var storage = await MvccStorage.OpenAsync(folder, options);
        var idle = await storage.GetStatisticsAsync();
        await Assert.That(idle.GroupCommitEnabled).IsTrue();
        await Assert.That(idle.PendingCommitRequests).IsEqualTo(0);

        using (var tx = storage.BeginTransaction())
        {
            tx.Put([1], [1]);
            await tx.CommitAsync();
        }

        var stats = await storage.GetStatisticsAsync();
        await Assert.That(stats.CommitGroups).IsEqualTo(1L);
        await Assert.That(stats.GroupedCommits).IsEqualTo(1L);
        await Assert.That(stats.Engine.Wal.Sync.Count).IsGreaterThanOrEqualTo(1L);
    }

    [Test]
    public async Task StatisticsAfterMvccDisposeThrow()
    {
        using var folder = TempFolder.Create();
        var storage = await MvccStorage.OpenAsync(folder, Options());
        await storage.DisposeAsync();
        await Assert.That(async () => await storage.GetStatisticsAsync()).Throws<ObjectDisposedException>();
        await Assert.That(async () => await storage.EstimateRangeAsync()).Throws<ObjectDisposedException>();
    }

    [Test]
    public async Task MeterObservesOpenStoresAndStopsAfterDispose()
    {
        using var folder = TempFolder.Create();
        var storage = await LsmStorage.OpenAsync(folder, Options());
        Fill(storage, 0, 10);
        var seen = new Dictionary<string, long>();
        using var listener = new System.Diagnostics.Metrics.MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == StorageMeter.MeterName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        string? path = null;
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if ((string?)tag.Value == folder.ToString())
                {
                    path = (string?)tag.Value;
                    seen[instrument.Name] = value;
                }
            }
        });
        listener.Start();
        listener.RecordObservableInstruments();
        await Assert.That(path).IsNotNull();
        await Assert.That(seen["silex.storage.write.operations"]).IsEqualTo(10L);

        await storage.DisposeAsync();
        seen.Clear();
        listener.RecordObservableInstruments();
        await Assert.That(seen.Count).IsEqualTo(0);
    }
}
