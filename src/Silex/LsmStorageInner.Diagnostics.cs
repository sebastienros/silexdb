using Silex.MemTables;
using Silex.Tables;

namespace Silex;

internal sealed partial class LsmStorageInner
{
    internal async Task<StorageTableStatistics[]> GetTablesAsync(CancellationToken cancellationToken)
    {
        using var operation = Lifetime.Enter();
        await _level0Lock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var result = new List<StorageTableStatistics>();
            Add(0, _state.LevelZeroTables);
            for (var level = 0; level < _state.LeveledSsTables.Count; level++)
                Add(level + 1, _state.LeveledSsTables[level]);
            return result.ToArray();

            void Add(int level, List<SsTable> tables)
            {
                foreach (var table in tables)
                    result.Add(new(table.FileId, level, table.Size, table.BlockMetadata.Length,
                        table.BlockMetadata.Length == 0 ? [] : table.FirstKey.Span.ToArray(),
                        table.BlockMetadata.Length == 0 ? [] : table.LastKey.Span.ToArray()));
            }
        }
        finally { _level0Lock.ExitReadLock(); }
    }

    internal async Task<LsmStorageStatistics> GetStatisticsAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var operation = Lifetime.Enter();

        long activeBytes;
        long immutableBytes;
        int immutableCount;
        var levels = new List<StorageLevelStatistics>();
        long totalBytes = 0;
        var totalTables = 0;
        long largestRun = 0;
        long? backlogBytes = null;
        var pressure = StoragePressureCauses.None;

        await _level0Lock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _immutableMemTablesLock.EnterReadLock();
            try
            {
                activeBytes = _state.CurrentMemTable.Size;
                immutableCount = _state.ImmutableMemTables.Length;
                immutableBytes = 0;
                foreach (var memTable in _state.ImmutableMemTables)
                {
                    immutableBytes += memTable.Size;
                }
            }
            finally
            {
                _immutableMemTablesLock.ExitReadLock();
            }

            var level0 = _state.LevelZeroTables;
            AddLevel(0, level0);
            for (var i = 0; i < _state.LeveledSsTables.Count; i++)
            {
                AddLevel(i + 1, _state.LeveledSsTables[i]);
            }

            if (_compactionStrategy == CompactionStrategy.Tiered)
            {
                var start = SelectTieredCompaction(level0);
                backlogBytes = 0;
                if (start >= 0)
                {
                    for (var i = start; i < level0.Count; i++)
                    {
                        backlogBytes += level0[i].Size;
                    }
                    pressure |= StoragePressureCauses.CompactionBacklog;
                }
                if (level0.Count >= _maxCompactionTiers)
                {
                    pressure |= StoragePressureCauses.Level0Backlog;
                }
            }
            else if (_compactionStrategy == CompactionStrategy.Leveled)
            {
                backlogBytes = 0;
                if (level0.Count >= _level0CompactionThreshold)
                {
                    pressure |= StoragePressureCauses.Level0Backlog | StoragePressureCauses.CompactionBacklog;
                    backlogBytes += levels[0].Bytes;
                }
                for (var i = 0; i < _state.LeveledSsTables.Count && i < _maxLevels - 1; i++)
                {
                    var excess = levels[i + 1].Bytes - LevelTargetBytes(i);
                    if (excess > 0)
                    {
                        pressure |= StoragePressureCauses.CompactionBacklog;
                        backlogBytes += excess;
                    }
                }
            }
        }
        finally
        {
            _level0Lock.ExitReadLock();
        }

        if (immutableCount >= _memTableMaxCount)
        {
            pressure |= StoragePressureCauses.MemTableLimit;
        }

        var health = Health;
        var fault = Fault;
        if (fault is not null)
        {
            pressure |= StoragePressureCauses.Faulted;
        }

        var user = Metrics.UserBytesWritten;
        var walBytes = Metrics.WalBytesWritten;
        var flush = Metrics.Flush.ToStatistics();
        var compaction = Metrics.Compaction.ToStatistics();
        var cache = _blockCache;
        var failure = Metrics.LastBackgroundFailure;
        var failureTicks = Metrics.LastBackgroundFailureUtcTicks;

        return new LsmStorageStatistics
        {
            Health = health,
            Fault = fault,
            Pressure = pressure,
            ActiveMemTableBytes = activeBytes,
            ImmutableMemTableCount = immutableCount,
            ImmutableMemTableBytes = immutableBytes,
            MemTableCountLimit = _memTableMaxCount,
            Levels = levels,
            TotalTableBytes = totalBytes,
            TotalTableCount = totalTables,
            CompactionStrategy = _compactionStrategy,
            CompactionBacklogBytes = backlogBytes,
            Flush = flush,
            Compaction = compaction,
            WriterLockWait = Metrics.WriterWait,
            WriteOperations = Metrics.WriteOperations,
            Wal = new StorageWalStatistics(
                _useWriteAheadLog, _useWriteAheadLog && _syncWriteAheadLogToDisk, walBytes, Metrics.WalSync),
            Cache = new StorageCacheStatistics(
                cache.SizeLimit > 0, cache.SizeLimit, cache.SizeBytes, cache.EntryCount,
                cache.Hits, cache.Misses, cache.Evictions),
            BackgroundFailures = new StorageBackgroundFailureStatistics(
                Metrics.BackgroundFailures, failure,
                failure is null ? null : new DateTimeOffset(failureTicks, TimeSpan.Zero)),
            UserBytesWritten = user,
            WriteAmplification = user == 0 ? null : (double)(walBytes + flush.OutputBytes + compaction.OutputBytes) / user,
            SpaceAmplificationEstimate = largestRun == 0 ? null : (double)totalBytes / largestRun,
        };

        void AddLevel(int level, List<SsTable> tables)
        {
            long bytes = 0;
            foreach (var table in tables)
            {
                var size = table.Size;
                bytes += size;
                if (_compactionStrategy != CompactionStrategy.Leveled && size > largestRun)
                {
                    largestRun = size;
                }
            }

            if (_compactionStrategy == CompactionStrategy.Leveled && bytes > largestRun)
            {
                largestRun = bytes;
            }

            totalBytes += bytes;
            totalTables += tables.Count;
            levels.Add(new StorageLevelStatistics(level, tables.Count, bytes));
        }
    }

    internal async Task<StorageRangeEstimate> EstimateRangeAsync(OrderedReadRange range, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var operation = Lifetime.Enter();

        long stored = 0;
        long decoded = 0;
        var tables = 0;
        var blocks = 0;
        long memTableBytes;
        var empty = range.IsEmpty;

        await _level0Lock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            _immutableMemTablesLock.EnterReadLock();
            try
            {
                memTableBytes = _state.CurrentMemTable.Size;
                foreach (var memTable in _state.ImmutableMemTables)
                {
                    memTableBytes += memTable.Size;
                }
            }
            finally
            {
                _immutableMemTablesLock.ExitReadLock();
            }

            if (!empty)
            {
                Accumulate(_state.LevelZeroTables);
                foreach (var level in _state.LeveledSsTables)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    Accumulate(level);
                }
            }
        }
        finally
        {
            _level0Lock.ExitReadLock();
        }

        return new StorageRangeEstimate(stored, decoded, tables, blocks, memTableBytes);

        void Accumulate(List<SsTable> list)
        {
            foreach (var table in list)
            {
                if (range.Before(table.LastKey.Span) || range.After(table.FirstKey.Span))
                {
                    continue;
                }

                var metadata = table.BlockMetadataArray;
                var count = metadata.Length;

                // First block whose last key is not before the range.
                int lo = 0, hi = count;
                while (lo < hi)
                {
                    var mid = lo + ((hi - lo) >> 1);
                    if (range.Before(metadata.GetLastKeySpan(mid)))
                    {
                        lo = mid + 1;
                    }
                    else
                    {
                        hi = mid;
                    }
                }
                var first = lo;

                // One past the last block whose first key is not after the range.
                lo = first;
                hi = count;
                while (lo < hi)
                {
                    var mid = lo + ((hi - lo) >> 1);
                    if (range.After(metadata.GetFirstKeySpan(mid)))
                    {
                        hi = mid;
                    }
                    else
                    {
                        lo = mid + 1;
                    }
                }

                if (lo <= first)
                {
                    continue;
                }

                tables++;
                for (var i = first; i < lo; i++)
                {
                    stored += table.GetBlockExtent(i).Length;
                    decoded += metadata[i].UncompressedLength;
                }
                blocks += lo - first;
            }
        }
    }
}
