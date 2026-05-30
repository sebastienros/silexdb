using Microsoft.Extensions.Caching.Memory;
using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Buffers;
using Silex.MemTables;
using Silex.Serialization;
using Silex.Tables;
using Silex.Wal;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Silex;

/// <summary>
/// The inner storage engine. It handles thread-safety for the <see cref="StorageState"/>.
/// </summary>
internal sealed class LsmStorageInner<TKey, TValue> : IDisposable where TKey : notnull
{
    private static readonly IBinaryEncoder<TValue> _valueSerializer = BinaryEncoderFactory<TValue>.BinarySerializer;
    private static readonly IBinaryEncoder<TKey> _keySerializer= BinaryEncoderFactory<TKey>.BinarySerializer;
    private static readonly IComparer<TKey> _keyComparer = BinaryEncoderFactory<TKey>.BinarySerializer.Comparer;

    // Use different locks for each type of manipulated data such that we can lock them individually.
    // For instance updating the MemTable should be synchronized, but not blocked by compaction.
    // Moreover, some locks are asynchronous (level0) while other are synchronous (mem tables).

    private readonly ReaderWriterLockSlim _currentMemTableLock = new();
    private readonly ReaderWriterLockSlim _immutableMemTablesLock = new();
    private readonly AsyncReaderWriterLock _level0Lock = new();

    // Serializes flush and compaction so they never interleave. The manifest-free recovery scheme
    // relies on a flush never appending a tier in the middle of a compaction (which would make older
    // compacted data look newer than a fresh flush), and on two compactions never sharing inputs.
    private readonly SemaphoreSlim _maintenanceLock = new(1, 1);

    internal StorageState<TKey, TValue> _state;
    private bool _disposed;
    private readonly IBlockEncoder<TKey, TValue> _blockEncoder;
    private readonly ISsTableEncoder<TKey, TValue> _ssTableEncoder;
    private readonly ISsTableBuilderFactory _ssTableBuilderFactory;
    private readonly IBloomFilterFactory _bloomFilterFactory;
    private readonly long _memTableSizeLimit;
    private readonly IMemoryCache _blockCache;
    private readonly MemoryCacheEntryOptions _cacheEntryOptions;
    private readonly bool _useWriteAheadLog;
    private readonly bool _syncWriteAheadLogToDisk;
    private readonly CompactionStrategy _compactionStrategy;
    private readonly int _maxCompactionTiers;
    private readonly int _maxSizeAmplificationPercent;
    private readonly int _sizeRatioPercent;
    private readonly int _minMergeWidth;
    private readonly int _level0CompactionThreshold;
    private readonly long _baseLevelTargetBytes;
    private readonly int _levelSizeMultiplier;
    private readonly int _maxLevels;
    private readonly long _targetSstSizeBytes;
    private readonly int _maxCompactionParallelism;
    private readonly int _maxReadParallelism;
    private readonly IBlockEncoderFactory _blockEncoderFactory;
    private readonly ISsTableEncoderFactory _ssTableEncoderFactory;

    // Below this many overlapping L0 SSTs a point lookup probes them sequentially newest-first (the
    // short-circuit is optimal when the key lives in a recent table). Past it, and when read parallelism is
    // enabled, the probes run concurrently and the newest matching table wins.
    private const int ParallelL0ProbeThreshold = 8;

    public string StoragePath { get; }

    /// <summary>
    /// Creates an empty new <see cref="LsmStorageInner"/> that will store its tables to the specified existing path.
    /// Any existing tables in the path are ignored, use <see cref="OpenAsync(string, StorageOptions, CancellationToken)"/> to load
    /// an existing folder instead.
    /// </summary>
    /// <param name="path"></param>
    /// <param name="options"></param>
    internal LsmStorageInner(string path, StorageOptions options)
    {
        StoragePath = path;
        _useWriteAheadLog = options.UseWriteAheadLog;
        _syncWriteAheadLogToDisk = options.SyncWriteAheadLogToDisk;
        _compactionStrategy = options.CompactionStrategy;
        _maxCompactionTiers = options.MaxCompactionTiers;
        _maxSizeAmplificationPercent = options.MaxSizeAmplificationPercent;
        _sizeRatioPercent = options.SizeRatioPercent;
        _minMergeWidth = options.MinMergeWidth;
        _level0CompactionThreshold = options.Level0CompactionThreshold;
        _baseLevelTargetBytes = options.BaseLevelTargetBytes;
        _levelSizeMultiplier = options.LevelSizeMultiplier;
        _maxLevels = options.MaxLevels;
        _targetSstSizeBytes = Math.Max(1, options.TargetSstSizeBytes);
        _maxCompactionParallelism = Math.Max(1, options.MaxCompactionParallelism);
        _maxReadParallelism = Math.Max(1, options.MaxReadParallelism);
        _blockEncoderFactory = options.BlockEncoderFactory;
        _ssTableEncoderFactory = options.SsTableEncoderFactory;
        _state = new StorageState<TKey, TValue>() { CurrentMemTable = CreateCurrentMemTable(IdGenerator.GetNextId()) };
        _blockEncoder = options.BlockEncoderFactory.Create<TKey, TValue>();
        _ssTableEncoder = options.SsTableEncoderFactory.Create<TKey, TValue>();
        _ssTableBuilderFactory = options.SsTableBuilderFactory;
        _bloomFilterFactory = options.BloomFilterFactory;
        _memTableSizeLimit = options.MemTableSizeLimit;
        _blockCache = new MemoryCache(new MemoryCacheOptions { SizeLimit = options.BlockCacheSizeLimit });
        _cacheEntryOptions = new MemoryCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = options.BlockCacheAbsoluteExpiration == TimeSpan.Zero ? null : options.BlockCacheAbsoluteExpiration,
            SlidingExpiration = options.BlockCacheSlidingExpiration == TimeSpan.Zero ? null : options.BlockCacheSlidingExpiration
        };
    }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns><see cref="TValue"/> if the key was not found.</returns>
    public async ValueTask<TValue> GetAsync(TKey key, CancellationToken cancellationToken = default)
    {
        // The immutable MemTables can be accessed without lock since they are frozen (read-only),
        // and the collection won't be changed FreezeMemTable substitute the collection when it's altered.

        // Access the current and immutable MemTables in a read-lock to ensure no
        // other transaction is creating a new MemTable while we are reading variables. This is only
        // to ensure the mutable MemTable and immutable ones are coherent.

        // Capture just the references the read needs instead of cloning the whole state: the L0 and
        // leveled lists are read live under _level0Lock below, so cloning them here would allocate two
        // throwaway lists on every read. CurrentMemTable must be captured under the lock; ImmutableMemTables
        // is an immutable reference captured here to keep a coherent view with CurrentMemTable.
        _currentMemTableLock.EnterReadLock();

        IMemTable<TKey, TValue> currentMemTable;
        ImmutableQueue<IMemTable<TKey, TValue>> immutableMemTables;

        try
        {
            currentMemTable = _state.CurrentMemTable;
            immutableMemTables = _state.ImmutableMemTables;

            // CurrentMemTable is the only thing that needs to be locked
            // since all other collections are immutable
            if (currentMemTable.TryGet(key, out var result))
            {
                return result;
            }
        }
        finally 
        { 
            _currentMemTableLock.ExitReadLock(); 
        }

        // If any new immutable MemTable(s) was created after this call then we just ignore it, as 
        // the newly created MemTable(s).

        if (!immutableMemTables.IsEmpty)
        {
            try
            {
                _immutableMemTablesLock.EnterReadLock();

                // Immutable MemTables are enqueued oldest-first, so iterate in reverse to let the most
                // recently frozen table win when the same key exists in several of them.
                foreach (var memTable in immutableMemTables.Reverse())
                {
                    if (memTable.TryGet(key, out var result))
                    {
                        return result;
                    }
                }
            }
            finally
            {
                _immutableMemTablesLock.ExitReadLock();
            }
        }

        // Process L0 tables in reverse order since the last one to be flush is at the end

        var keyLength = _keySerializer.GetLength(key);
        var bufferWriter = new PooledArrayBufferWriter<byte>(keyLength);
        var writer = new EncoderBinaryWriter(bufferWriter);
        _keySerializer.Encode(key, ref writer);
        // Commit the encoded bytes to the buffer so the bloom filter probes the actual key.
        writer.Flush();
        var keyMemory = bufferWriter.WrittenMemory;
        
        try
        {
            await _level0Lock.EnterReadLockAsync();

            // Read the live L0 list while holding the read lock rather than from a snapshot taken
            // earlier: compaction disposes and deletes replaced SSTs under the write lock, so a table
            // referenced from a stale pre-lock snapshot could already be disposed. While the read lock
            // is held no writer runs, so the list and its tables are stable for the duration.
            var l0 = _state.LevelZeroTables;

            // Probe L0 newest-first. When many overlapping L0 SSTs have accumulated and read parallelism is
            // enabled, probe them concurrently instead: every table is checked, then the newest (highest
            // index) one that holds the key wins, preserving recency/shadowing exactly as the sequential
            // short-circuit would.
            if (_maxReadParallelism > 1 && l0.Count >= ParallelL0ProbeThreshold)
            {
                var probes = new (bool found, TValue resolved)[l0.Count];

                await Parallel.ForEachAsync(
                    Enumerable.Range(0, l0.Count),
                    new ParallelOptions { MaxDegreeOfParallelism = _maxReadParallelism, CancellationToken = cancellationToken },
                    async (index, ct) =>
                    {
                        probes[index] = await TryReadFromTableAsync(l0[index], key, keyMemory, ct);
                    });

                for (var i = l0.Count - 1; i >= 0; i--)
                {
                    if (probes[i].found)
                    {
                        return probes[i].resolved;
                    }
                }
            }
            else
            {
                for (var i = l0.Count - 1; i >= 0; i--)
                {
                    var (found, resolved) = await TryReadFromTableAsync(l0[i], key, keyMemory, cancellationToken);

                    if (found)
                    {
                        return resolved;
                    }
                }
            }

            // Below L0 come the compaction levels (leveled strategy). Level 1 holds the newest data and each
            // deeper level is older, so scan them in order; the first level that contains the key wins. Each
            // level is a single sorted run with non-overlapping ranges, so at most one of its SSTs matches.
            var levels = _state.LeveledSsTables;

            for (var level = 0; level < levels.Count; level++)
            {
                var tables = levels[level];

                for (var i = 0; i < tables.Count; i++)
                {
                    var (found, resolved) = await TryReadFromTableAsync(tables[i], key, keyMemory, cancellationToken);

                    if (found)
                    {
                        return resolved;
                    }
                }
            }
        }
        finally
        {
            // Return the key buffer
            bufferWriter.Dispose();

            _level0Lock.ExitReadLock();
        }

        return default!;
    }

    /// <summary>
    /// Resolves a key against a single block. Returns <c>true</c> when the key is present (the resolved
    /// value is the stored value, or <c>default</c> when it is a tombstone), and <c>false</c> when the key
    /// is absent. Kept synchronous so the value span never crosses an <c>await</c> boundary.
    /// </summary>
    private static bool TryResolveBlockValue(Block<TKey, TValue> block, TKey key, out TValue resolved)
    {
        if (block.TryGetValue(key, out var value))
        {
            // An empty stored value is a deletion for empty-tombstone encoders (byte[]/Bytes); sentinel-based
            // encoders (e.g. int) store a fixed non-empty value, so decode and ask the serializer.
            if (value.IsEmpty)
            {
                resolved = default!;
                return true;
            }

            var decoded = _valueSerializer.Decode(value);
            resolved = _valueSerializer.IsTombstoneValue(decoded) ? default! : decoded;
            return true;
        }

        resolved = default!;
        return false;
    }

    /// <summary>
    /// Probes a single SST for <paramref name="key"/>. Returns <c>found = true</c> with the resolved value
    /// (a tombstone resolves to <c>default</c>) when the key is present in the table, and
    /// <c>found = false</c> when the table cannot contain it (range/bloom miss) or a bloom false-positive
    /// turns out absent, so the caller falls through to older tables.
    /// </summary>
    private async ValueTask<(bool found, TValue resolved)> TryReadFromTableAsync(SsTable<TKey, TValue> table, TKey key, ReadOnlyMemory<byte> keyMemory, CancellationToken cancellationToken)
    {
        // The key could be in this table, if not go to the next one.
        if (_keyComparer.Compare(key, table.FirstKey) < 0 || _keyComparer.Compare(key, table.LastKey) > 0)
        {
            return (false, default!);
        }

        // Check if the bloom filter tells us to skip this table.
        if (!table.BloomFilter.Probe(keyMemory.Span))
        {
            return (false, default!);
        }

        foreach (var metadata in table.BlockMetadata)
        {
            if (_keyComparer.Compare(key, metadata.FirstKey) < 0 || _keyComparer.Compare(key, metadata.LastKey) > 0)
            {
                continue;
            }

            var block = await table.ReadBlockCachedAsync(metadata.Index, _blockCache, _cacheEntryOptions, cancellationToken);

            if (block != null && TryResolveBlockValue(block, key, out var resolved))
            {
                // The key is present in this (newest matching) table, so it shadows any older one.
                // A found tombstone resolves to default.
                return (true, resolved);
            }

            // The only block whose range can cover the key has been read and the key was not in it
            // (a bloom-filter false positive): the key is absent from this table, so fall through to
            // older tables instead of masking them with a premature default.
            break;
        }

        return (false, default!);
    }

    /// <summary>
    /// Puts a value with the specified key in the current <see cref="IMemTable"/>. If one already exists it is replaced.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="value"></param>
    public void Put(TKey key, TValue value)
    {
        _currentMemTableLock.EnterWriteLock();

        try
        {
            _state.CurrentMemTable.Put(key, value);

            if (_state.CurrentMemTable.Size >= _memTableSizeLimit)
            {
                FreezeMemTable();
            }
        }
        finally
        {
            _currentMemTableLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Adds a delete operation for the specified key.
    /// </summary>
    /// <param name="key"></param>
    public void Delete(TKey key)
    {
        _currentMemTableLock.EnterWriteLock();

        try
        {
            _state.CurrentMemTable.Put(key, _valueSerializer.GetTombstoneValue());

            if (_state.CurrentMemTable.Size >= _memTableSizeLimit)
            {
                FreezeMemTable();
            }
        }
        finally
        {
            _currentMemTableLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Force freeze the current MemTable to an immutable MemTable.
    /// </summary>
    /// <remarks>
    /// Once a MemTable reaches the limit, we call ForceFreezeMemTable to freeze the MemTable and create a new one.
    /// </remarks>
    public void ForceFreezeMemTable()
    {
        _currentMemTableLock.EnterReadLock();

        try
        {
            if (_state.CurrentMemTable.Size == 0)
            {
                return;
            }
        }
        finally
        {
            _currentMemTableLock.ExitReadLock();
        }

        _currentMemTableLock.EnterWriteLock();

        try
        {
            FreezeMemTable();
        }
        finally
        {
            _currentMemTableLock.ExitWriteLock();
        }
    }

    /// <summary>
    /// Force flush the earliest created MemTable to disk.
    /// </summary>
    public async Task ForceFlushNextImmutableMemTableAsync(CancellationToken cancellationToken = default)
    {
        if (_state.ImmutableMemTables.IsEmpty)
        {
            return;
        }

        // Hold the maintenance lock for the whole flush so it never interleaves with a compaction.
        await _maintenanceLock.WaitAsync(cancellationToken);

        try
        {
            IMemTable<TKey, TValue> memTableToFlush;

            _immutableMemTablesLock.EnterReadLock();

            try
            {
                // double-check lock
                if (_state.ImmutableMemTables.IsEmpty)
                {
                    return;
                }

                // Peek the oldest immutable MemTable without removing it. It stays queued (and therefore
                // visible to concurrent scans) until its SST is published into L0 atomically below.
                memTableToFlush = _state.ImmutableMemTables.Peek();
            }
            finally
            {
                _immutableMemTablesLock.ExitReadLock();
            }

            var sstFilename = GetSstPath(memTableToFlush.Id);
            using var builder = _ssTableBuilderFactory.CreateSsTableBuilder(sstFilename, _ssTableEncoder, _blockEncoder, _bloomFilterFactory, memTableToFlush.Count);
            await memTableToFlush.FlushAsync(builder);

            var ssTable = await builder.BuildAsync(cancellationToken);

            Manifest? manifest = null;

            await _level0Lock.EnterWriteLockAsync();

            try
            {
                // Atomic, scan-visible transition: drop the MemTable from the immutable queue and publish its
                // SST into L0 under the same level0 write lock. A scan holding the level0 read lock therefore
                // always sees the data in exactly one place (the queued MemTable or the L0 SST), never neither.
                _immutableMemTablesLock.EnterWriteLock();

                try
                {
                    _state.ImmutableMemTables = _state.ImmutableMemTables.Dequeue(out var dequeued);
                    Debug.Assert(ReferenceEquals(dequeued, memTableToFlush));
                }
                finally
                {
                    _immutableMemTablesLock.ExitWriteLock();
                }

                _state.LevelZeroTables.Add(ssTable);

                // Snapshot the new structure while the lock is held so the persisted manifest exactly matches
                // the installed state. The (small) JSON is written to disk after releasing the lock.
                if (_compactionStrategy == CompactionStrategy.Leveled)
                {
                    manifest = BuildManifestSnapshot();
                }
            }
            finally
            {
                _level0Lock.ExitWriteLock();

                memTableToFlush.Dispose();
            }

            // For leveled compaction the manifest rewrite is the durable commit point and must happen before
            // the WAL is deleted: a crash before it leaves the WAL to be replayed on open (the not-yet-committed
            // SST is cleaned up as an orphan), and a crash after it leaves a committed SST whose WAL recovery
            // correctly drops. Tiered/None keep the manifest-free, reopen-by-id recovery and skip this.
            manifest?.Write(StoragePath);

            // The data is now durable in an SST and visible in L0, and the memtable (and its WAL handle)
            // is disposed: the write-ahead log is obsolete and can be removed. If a crash happens before
            // this point the WAL survives and is replayed on the next open; if it happens after the SST is
            // durable (and, for leveled, committed to the manifest) but before the delete, recovery sees the
            // matching SST and drops the stale WAL.
            DeleteWal(memTableToFlush.Id);
        }
        finally
        {
            _maintenanceLock.Release();
        }
    }

    public string GetSstPath(long id)
    {
        return Path.Combine(StoragePath, $"{id.ToString(CultureInfo.InvariantCulture)}.sst");
    }

    public string GetWalPath(long id)
    {
        return Path.Combine(StoragePath, $"{id.ToString(CultureInfo.InvariantCulture)}.wal");
    }

    /// <summary>
    /// Parses the numeric <em>filename</em> id of an SST from its path. This is the id under which the file
    /// is stored on disk (and recorded in the manifest), which is distinct from the transient runtime
    /// <see cref="SsTable{TKey, TValue}.Id"/> assigned when the in-memory table object is created.
    /// </summary>
    private static long GetSstFileId(SsTable<TKey, TValue> table)
    {
        return long.Parse(Path.GetFileNameWithoutExtension(table.Filename), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Captures the current L0 + leveled structure as a <see cref="Manifest"/> of filename ids. Must be
    /// called while holding the <c>_level0Lock</c> (read or write) so the snapshot is consistent.
    /// </summary>
    private Manifest BuildManifestSnapshot()
    {
        var manifest = new Manifest();

        foreach (var table in _state.LevelZeroTables)
        {
            manifest.L0.Add(GetSstFileId(table));
        }

        foreach (var level in _state.LeveledSsTables)
        {
            var ids = new List<long>(level.Count);

            foreach (var table in level)
            {
                ids.Add(GetSstFileId(table));
            }

            manifest.Levels.Add(ids);
        }

        return manifest;
    }

    /// <summary>
    /// Runs at most one tiered (universal) compaction task if the configured triggers fire. Returns
    /// <see langword="true"/> when a compaction was performed.
    /// </summary>
    /// <remarks>
    /// This is driven sequentially by the single background compacter loop, the only component that
    /// flushes to and compacts L0 at runtime, so it never runs concurrently with a flush. That
    /// invariant is what keeps the manifest-free, reopen-by-id recovery correct: compaction always
    /// merges a <em>newest suffix</em> of the tier list into a single output SST whose id is freshly
    /// allocated (and therefore greater than every input), appended as the new newest tier.
    /// </remarks>
    public async Task<bool> TryTieredCompactionAsync(CancellationToken cancellationToken = default)
    {
        if (_compactionStrategy != CompactionStrategy.Tiered)
        {
            return false;
        }

        // Hold the maintenance lock for the whole compaction so no flush appends a tier mid-merge and no
        // second compaction shares (and disposes) the same input handles.
        await _maintenanceLock.WaitAsync(cancellationToken);

        try
        {
            // Snapshot the tier list while briefly holding the read lock. The maintenance lock already
            // excludes flush/compaction, so the snapshot stays valid through the merge below.
            List<SsTable<TKey, TValue>> tiers;

            await _level0Lock.EnterReadLockAsync();

            try
            {
                tiers = _state.LevelZeroTables.ToList();
            }
            finally
            {
                _level0Lock.ExitReadLock();
            }

            var startIndex = SelectTieredCompaction(tiers);

            if (startIndex < 0)
            {
                return false;
            }

            var count = tiers.Count - startIndex;

            // The inputs are the suffix [startIndex, end). They are in ascending-id (oldest-first) order
            // because tiers are appended on flush/compaction.
            var inputs = new List<SsTable<TKey, TValue>>(count);

            for (var i = startIndex; i < tiers.Count; i++)
            {
                inputs.Add(tiers[i]);
            }

            // Tombstones may only be dropped when the bottom-most tier participates: otherwise an older
            // tier could still hold the deleted key and dropping the tombstone would resurrect it.
            var dropTombstones = startIndex == 0;

            var outputId = IdGenerator.GetNextId();
            var outputPath = GetSstPath(outputId);

            long totalInputBytes = 0;

            foreach (var input in inputs)
            {
                totalInputBytes += input.Size;
            }

            // The bloom filter is sized from an estimate; an imperfect count only affects its false-positive
            // rate, never correctness.
            var estimatedCount = (int)Math.Min(int.MaxValue, Math.Max(1, totalInputBytes / 24));

            SsTable<TKey, TValue>? output = null;
            var addedEntries = 0;

            using (var builder = _ssTableBuilderFactory.CreateSsTableBuilder(outputPath, _ssTableEncoder, _blockEncoder, _bloomFilterFactory, estimatedCount))
            {
                // Feed iterators newest-first so the MergeIterator keeps the most recent value per key.
                var iterators = new List<IStorageIterator<TKey, TValue>>(count);

                for (var i = tiers.Count - 1; i >= startIndex; i--)
                {
                    iterators.Add(new SsTableIterator<TKey, TValue>(tiers[i]));
                }

                var merge = new MergeIterator<TKey, TValue>(iterators);

                await foreach (var entry in merge.EnumerateAsync(cancellationToken))
                {
                    if (dropTombstones && _valueSerializer.IsTombstoneValue(entry.Value))
                    {
                        continue;
                    }

                    await builder.AddAsync(entry.Key, entry.Value);
                    addedEntries++;
                }

                if (addedEntries > 0)
                {
                    output = await builder.BuildAsync(cancellationToken);
                }
            }

            // Install the result: drop the input tiers and append the merged output as the new newest tier.
            // Mutate the live list in place (like a flush) rather than reassigning the field so a concurrent
            // FreezeMemTable that copies the list reference observes a consistent collection.
            var installed = false;

            await _level0Lock.EnterWriteLockAsync();

            try
            {
                var live = _state.LevelZeroTables;

                // The maintenance lock guarantees the selected suffix is still the live tail; verify it at
                // runtime anyway and bail safely (rather than corrupt the list) if that ever fails to hold.
                if (live.Count == tiers.Count && SuffixMatches(live, inputs, startIndex))
                {
                    live.RemoveRange(startIndex, count);

                    if (output != null)
                    {
                        live.Add(output);
                    }

                    installed = true;
                }
            }
            finally
            {
                _level0Lock.ExitWriteLock();
            }

            if (!installed)
            {
                // The list changed unexpectedly: discard the output we built and leave the inputs in place.
                output?.Dispose();
                TryDeleteSstFile(outputPath);
                return false;
            }

            // The inputs are no longer reachable by new readers and any in-flight reader finished before the
            // write lock was granted, so their handles can be closed.
            foreach (var input in inputs)
            {
                input.Dispose();
            }

            // Delete the files oldest-first and STOP at the first failure. This keeps the deleted set a
            // prefix of the oldest tiers, so a newer tombstone is never removed while an older value it
            // shadows still exists on disk: a crash (or a failed delete) can never resurrect dropped data.
            foreach (var input in inputs)
            {
                if (!TryDeleteSstFile(input.Filename))
                {
                    break;
                }
            }

            return true;
        }
        finally
        {
            _maintenanceLock.Release();
        }
    }

    private static bool SuffixMatches(List<SsTable<TKey, TValue>> live, List<SsTable<TKey, TValue>> inputs, int startIndex)
    {
        for (var i = 0; i < inputs.Count; i++)
        {
            if (!ReferenceEquals(live[startIndex + i], inputs[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Runs at most one leveled-compaction task if a trigger fires. Returns <see langword="true"/> when a
    /// compaction was performed.
    /// </summary>
    /// <remarks>
    /// One action per invocation, ordered by priority: (1) flush L0 down into L1 once L0 has accumulated
    /// <c>Level0CompactionThreshold</c> SSTs; (2) otherwise push the most over-sized level Li down into
    /// Li+1. Each level L1..Ln is kept as a single sorted run, so a whole-level merge produces exactly one
    /// output SST. The manifest rewrite under the level0 write lock is the durable commit point; replaced
    /// input files are only deleted after it succeeds.
    /// </remarks>
    public async Task<bool> TryLeveledCompactionAsync(CancellationToken cancellationToken = default)
    {
        if (_compactionStrategy != CompactionStrategy.Leveled)
        {
            return false;
        }

        // Hold the maintenance lock for the whole compaction so no flush appends to L0 mid-merge and no
        // second compaction shares (and disposes) the same input handles.
        await _maintenanceLock.WaitAsync(cancellationToken);

        try
        {
            // Snapshot the structure while briefly holding the read lock. The maintenance lock already
            // excludes flush/compaction, so the snapshot stays valid through the merge below.
            List<SsTable<TKey, TValue>> level0;
            List<List<SsTable<TKey, TValue>>> levels;

            await _level0Lock.EnterReadLockAsync();

            try
            {
                level0 = _state.LevelZeroTables.ToList();
                levels = _state.LeveledSsTables.Select(level => level.ToList()).ToList();
            }
            finally
            {
                _level0Lock.ExitReadLock();
            }

            // Trigger 1 - flush L0 into L1 once enough L0 SSTs have accumulated.
            if (level0.Count >= _level0CompactionThreshold)
            {
                return await CompactLevelAsync(sourceLevelIndex: -1, level0, levels, cancellationToken);
            }

            // Trigger 2 - push the most over-sized level down into the next one.
            var sourceLevelIndex = SelectLeveledSourceLevel(levels);

            if (sourceLevelIndex >= 0)
            {
                return await CompactLevelAsync(sourceLevelIndex, level0, levels, cancellationToken);
            }

            return false;
        }
        finally
        {
            _maintenanceLock.Release();
        }
    }

    /// <summary>
    /// Selects the leveled source level (0-based, where index 0 is L1) with the highest size/target ratio
    /// above 1.0 that still has room to push down, or <c>-1</c> when no level is over its target.
    /// </summary>
    private int SelectLeveledSourceLevel(List<List<SsTable<TKey, TValue>>> levels)
    {
        var best = -1;
        var bestRatio = 1.0;

        // A level may only be chosen as a source if there is room to push into the level below it
        // (index + 1 must be a valid level, i.e. within MaxLevels). The deepest level never compacts down.
        for (var s = 0; s < levels.Count && s < _maxLevels - 1; s++)
        {
            if (levels[s].Count == 0)
            {
                continue;
            }

            long size = 0;

            foreach (var table in levels[s])
            {
                size += table.Size;
            }

            var target = LevelTargetBytes(s);

            if (target <= 0)
            {
                continue;
            }

            var ratio = (double)size / target;

            if (ratio > bestRatio)
            {
                bestRatio = ratio;
                best = s;
            }
        }

        return best;
    }

    /// <summary>
    /// Target size in bytes for a leveled level (0-based, index 0 is L1): the base target grows by the
    /// configured multiplier for each deeper level.
    /// </summary>
    private long LevelTargetBytes(int levelIndex)
    {
        var target = _baseLevelTargetBytes;

        for (var i = 0; i < levelIndex; i++)
        {
            target *= _levelSizeMultiplier;

            if (target < 0)
            {
                return long.MaxValue;
            }
        }

        return target;
    }

    /// <summary>
    /// Compacts a source into the level below it under <see cref="CompactionStrategy.Leveled"/>: either all
    /// of L0 (when <paramref name="sourceLevelIndex"/> is <c>-1</c>) or a single picked SST from leveled
    /// level <paramref name="sourceLevelIndex"/>. Only the destination SSTs whose key ranges overlap the
    /// source are rewritten (partial selection); the merged data is split into size-bounded, non-overlapping
    /// output SSTs. The structure is installed under the level0 write lock, the manifest is persisted (the
    /// durable commit point), and finally the replaced input files are deleted.
    /// </summary>
    private async Task<bool> CompactLevelAsync(int sourceLevelIndex, List<SsTable<TKey, TValue>> level0, List<List<SsTable<TKey, TValue>>> levels, CancellationToken cancellationToken)
    {
        // Destination level index (0-based, 0 == L1). L0 flushes into L1; level s pushes into s+1.
        var targetLevelIndex = sourceLevelIndex < 0 ? 0 : sourceLevelIndex + 1;

        var targetTables = targetLevelIndex < levels.Count ? levels[targetLevelIndex] : new List<SsTable<TKey, TValue>>();

        // Determine the source SSTs (newest-first for the merge) and what stays behind in the source level.
        var sourceTables = new List<SsTable<TKey, TValue>>();
        List<SsTable<TKey, TValue>>? remainingSource = null;
        var clearLevel0 = sourceLevelIndex < 0;

        if (clearLevel0)
        {
            // L0 SSTs overlap each other arbitrarily, so the whole of L0 is always consumed together.
            for (var i = level0.Count - 1; i >= 0; i--)
            {
                sourceTables.Add(level0[i]);
            }
        }
        else
        {
            var source = levels[sourceLevelIndex];

            // Pick the smallest-key SST. Each compaction removes one SST from the source level, so the
            // level shrinks until it is back under its target (deterministic forward progress).
            sourceTables.Add(source[0]);
            remainingSource = source.GetRange(1, source.Count - 1);
        }

        if (sourceTables.Count == 0)
        {
            return false;
        }

        // Combined key range of the source SSTs.
        var rangeFirst = sourceTables[0].FirstKey;
        var rangeLast = sourceTables[0].LastKey;

        foreach (var table in sourceTables)
        {
            if (_keyComparer.Compare(table.FirstKey, rangeFirst) < 0)
            {
                rangeFirst = table.FirstKey;
            }

            if (_keyComparer.Compare(table.LastKey, rangeLast) > 0)
            {
                rangeLast = table.LastKey;
            }
        }

        // The destination level is sorted and non-overlapping, so the SSTs that overlap [rangeFirst,
        // rangeLast] form a contiguous run [lo, hi). Everything before lo / from hi on is kept untouched.
        var lo = 0;

        while (lo < targetTables.Count && _keyComparer.Compare(targetTables[lo].LastKey, rangeFirst) < 0)
        {
            lo++;
        }

        var hi = lo;
        var overlapTargets = new List<SsTable<TKey, TValue>>();

        while (hi < targetTables.Count && _keyComparer.Compare(targetTables[hi].FirstKey, rangeLast) <= 0)
        {
            overlapTargets.Add(targetTables[hi]);
            hi++;
        }

        // Tombstones may only be dropped when the destination is the last non-empty level: otherwise a
        // deeper level could still hold the deleted key and dropping the tombstone would resurrect it. Kept
        // (non-overlapping) SSTs in the destination level never hold a key in the source range, so they
        // cannot resurrect a dropped key.
        var dropTombstones = !HasDataBelow(levels, targetLevelIndex);

        // Merge the source (newest) over the overlapping destination SSTs (older) into size-bounded outputs.
        var mergeInputs = new List<SsTable<TKey, TValue>>(sourceTables.Count + overlapTargets.Count);
        mergeInputs.AddRange(sourceTables);
        mergeInputs.AddRange(overlapTargets);

        var outputs = await MergeIntoSplitSsTablesAsync(mergeInputs, dropTombstones, cancellationToken);

        // The new destination level: kept-before ++ freshly merged outputs ++ kept-after. This stays sorted
        // and non-overlapping because the consumed targets were a contiguous run and the outputs cover only
        // keys from the source and that run (see the level0 install assert below).
        var newTargetLevel = new List<SsTable<TKey, TValue>>(lo + outputs.Count + (targetTables.Count - hi));

        for (var i = 0; i < lo; i++)
        {
            newTargetLevel.Add(targetTables[i]);
        }

        newTargetLevel.AddRange(outputs);

        for (var i = hi; i < targetTables.Count; i++)
        {
            newTargetLevel.Add(targetTables[i]);
        }

        // Install the new structure. Mutate the live collections in place (like a flush/tiered compaction)
        // rather than reassigning the struct fields so a concurrent FreezeMemTable that copies the list
        // references observes consistent collections.
        Manifest manifest;

        await _level0Lock.EnterWriteLockAsync();

        try
        {
            if (clearLevel0)
            {
                // The maintenance lock guarantees L0 is unchanged since the snapshot; the whole of L0 was
                // merged, so clear the live list.
                _state.LevelZeroTables.Clear();
            }
            else
            {
                _state.LeveledSsTables[sourceLevelIndex] = remainingSource!;
            }

            // Grow the leveled structure with empty runs until the destination level exists, then publish it.
            while (_state.LeveledSsTables.Count <= targetLevelIndex)
            {
                _state.LeveledSsTables.Add(new List<SsTable<TKey, TValue>>());
            }

            _state.LeveledSsTables[targetLevelIndex] = newTargetLevel;

            Debug.Assert(IsSortedNonOverlapping(newTargetLevel), "leveled compaction produced an overlapping destination level");

            manifest = BuildManifestSnapshot();
        }
        finally
        {
            _level0Lock.ExitWriteLock();
        }

        // The manifest rewrite is the durable commit point and must happen before the replaced inputs are
        // deleted: a crash before it leaves the inputs intact (the not-yet-committed outputs are cleaned up
        // as orphans on recovery), and a crash after it leaves committed outputs whose now-unreferenced
        // inputs recovery deletes as orphans. Because the manifest is authoritative, a failed delete here is
        // self-healing (the leftover file is an orphan on the next open), so no ordering guard is needed.
        manifest.Write(StoragePath);

        var consumed = mergeInputs;

        foreach (var input in consumed)
        {
            input.Dispose();
            TryDeleteSstFile(input.Filename);
        }

        return true;
    }

    /// <summary>
    /// Merges the <paramref name="inputs"/> (listed newest-first so the most recent value per key wins) into
    /// a sequence of size-bounded, key-ascending, non-overlapping output SSTs. When
    /// <see cref="_maxCompactionParallelism"/> is greater than <c>1</c> and there is enough data, the
    /// combined key range is partitioned into disjoint sub-ranges that are merged concurrently
    /// (subcompactions); their outputs are concatenated in key order. Returns an empty list when every
    /// entry is a dropped tombstone. On failure all partially built outputs are disposed and deleted so no
    /// file handles or orphan files leak.
    /// </summary>
    private async Task<List<SsTable<TKey, TValue>>> MergeIntoSplitSsTablesAsync(List<SsTable<TKey, TValue>> inputs, bool dropTombstones, CancellationToken cancellationToken)
    {
        var dop = _maxCompactionParallelism;

        // Decide how many parallel sub-compactions to run. Each partition should still produce roughly
        // target-sized files, so cap the count by the total input size, and never ask for more partitions
        // than there are distinct split boundaries available.
        var boundaries = dop <= 1 ? new List<TKey>() : ComputeSubcompactionBoundaries(inputs);

        long totalBytes = 0;

        foreach (var input in inputs)
        {
            totalBytes += input.Size;
        }

        var maxBySize = (int)Math.Max(1, Math.Min(int.MaxValue, totalBytes / _targetSstSizeBytes));
        var partitionCount = Math.Min(dop, Math.Min(maxBySize, boundaries.Count + 1));

        if (partitionCount <= 1)
        {
            // Single partition: behaves exactly like the original sequential split merge.
            var blockEncoder = _blockEncoderFactory.Create<TKey, TValue>();
            var tableEncoder = _ssTableEncoderFactory.Create<TKey, TValue>();

            return await MergeRangeIntoSplitSsTablesAsync(inputs, dropTombstones, blockEncoder, tableEncoder, hasLower: false, default!, hasUpper: false, default!, cancellationToken);
        }

        // Pick partitionCount - 1 evenly-spaced split keys. Partition j covers [split[j-1], split[j]):
        // the boundary key is the inclusive lower bound of the upper partition and the exclusive upper
        // bound of the lower one, so every key lands in exactly one partition.
        var splits = new TKey[partitionCount - 1];

        for (var j = 1; j < partitionCount; j++)
        {
            var index = (int)((long)j * boundaries.Count / partitionCount);
            index = Math.Min(boundaries.Count - 1, Math.Max(j - 1, index));
            splits[j - 1] = boundaries[index];
        }

        var results = new List<SsTable<TKey, TValue>>?[partitionCount];

        try
        {
            await Parallel.ForEachAsync(
                Enumerable.Range(0, partitionCount),
                new ParallelOptions { MaxDegreeOfParallelism = dop, CancellationToken = cancellationToken },
                async (partition, ct) =>
                {
                    var hasLower = partition > 0;
                    var lower = hasLower ? splits[partition - 1] : default!;
                    var hasUpper = partition < partitionCount - 1;
                    var upper = hasUpper ? splits[partition] : default!;

                    // Fresh encoder instances per partition: the shared encoder instances on the engine are
                    // not contractually thread-safe, and parallel builders would otherwise call into the
                    // same instance concurrently.
                    var blockEncoder = _blockEncoderFactory.Create<TKey, TValue>();
                    var tableEncoder = _ssTableEncoderFactory.Create<TKey, TValue>();

                    // Publish only after the whole partition succeeds, so the outer cleanup never double-frees
                    // a partially built partition (that one cleans up after itself before throwing).
                    results[partition] = await MergeRangeIntoSplitSsTablesAsync(inputs, dropTombstones, blockEncoder, tableEncoder, hasLower, lower, hasUpper, upper, ct);
                });
        }
        catch
        {
            foreach (var result in results)
            {
                if (result == null)
                {
                    continue;
                }

                foreach (var output in result)
                {
                    output.Dispose();
                    TryDeleteSstFile(output.Filename);
                }
            }

            throw;
        }

        // Concatenate partition outputs in key order. Sub-ranges are disjoint and each partition's outputs
        // are internally non-overlapping, so the result is globally sorted and non-overlapping.
        var outputs = new List<SsTable<TKey, TValue>>();

        foreach (var result in results)
        {
            if (result != null)
            {
                outputs.AddRange(result);
            }
        }

        return outputs;
    }

    /// <summary>
    /// Merges the <paramref name="inputs"/> restricted to the half-open key range
    /// <c>[lower, upper)</c> (either bound optional) into size-bounded, key-ascending, non-overlapping
    /// output SSTs, rolling over to a new file once the current one reaches <see cref="_targetSstSizeBytes"/>.
    /// Uses the supplied <paramref name="blockEncoder"/>/<paramref name="tableEncoder"/> so each caller (in
    /// particular each parallel sub-compaction) owns its encoder instances. On failure every partially built
    /// output is disposed and deleted, then the exception is rethrown.
    /// </summary>
    private async Task<List<SsTable<TKey, TValue>>> MergeRangeIntoSplitSsTablesAsync(List<SsTable<TKey, TValue>> inputs, bool dropTombstones, IBlockEncoder<TKey, TValue> blockEncoder, ISsTableEncoder<TKey, TValue> tableEncoder, bool hasLower, TKey lower, bool hasUpper, TKey upper, CancellationToken cancellationToken)
    {
        var outputs = new List<SsTable<TKey, TValue>>();

        // Per-output bloom sizing from the target file size; an imperfect estimate only affects the
        // false-positive rate, never correctness.
        var estimatedCount = (int)Math.Min(int.MaxValue, Math.Max(1, _targetSstSizeBytes / 24));

        ISsTableBuilder<TKey, TValue>? builder = null;
        string? builderPath = null;

        try
        {
            var iterators = new List<IStorageIterator<TKey, TValue>>(inputs.Count);

            foreach (var input in inputs)
            {
                iterators.Add(new SsTableIterator<TKey, TValue>(input));
            }

            var merge = new MergeIterator<TKey, TValue>(iterators);

            // Seek every input to the lower bound (skips earlier blocks); the merge stays ascending so the
            // upper bound is enforced with a single break below.
            var entries = hasLower ? merge.EnumerateAsync(lower, cancellationToken) : merge.EnumerateAsync(cancellationToken);

            await foreach (var entry in entries)
            {
                if (hasUpper && _keyComparer.Compare(entry.Key, upper) >= 0)
                {
                    break;
                }

                if (dropTombstones && _valueSerializer.IsTombstoneValue(entry.Value))
                {
                    continue;
                }

                if (builder == null)
                {
                    builderPath = GetSstPath(IdGenerator.GetNextId());
                    builder = _ssTableBuilderFactory.CreateSsTableBuilder(builderPath, tableEncoder, blockEncoder, _bloomFilterFactory, estimatedCount);
                }

                await builder.AddAsync(entry.Key, entry.Value);

                // Roll over to a new file once the current one reaches the target size. The split happens at
                // a key boundary, so the outputs stay non-overlapping.
                if (builder.EstimatedSize >= _targetSstSizeBytes)
                {
                    outputs.Add(await builder.BuildAsync(cancellationToken));
                    builder.Dispose();
                    builder = null;
                    builderPath = null;
                }
            }

            if (builder != null)
            {
                outputs.Add(await builder.BuildAsync(cancellationToken));
                builder.Dispose();
                builder = null;
                builderPath = null;
            }

            return outputs;
        }
        catch
        {
            // Roll back: discard the in-progress builder and every already-built output so the failed merge
            // leaves no open handles or orphan files behind.
            if (builder != null)
            {
                builder.Dispose();

                if (builderPath != null)
                {
                    TryDeleteSstFile(builderPath);
                }
            }

            foreach (var output in outputs)
            {
                output.Dispose();
                TryDeleteSstFile(output.Filename);
            }

            throw;
        }
    }

    /// <summary>
    /// Computes candidate split keys for partitioning a leveled merge into parallel sub-compactions: every
    /// input block's first key, sorted ascending and de-duplicated, with the global minimum dropped (a split
    /// equal to the range start would make an empty first partition). The returned keys are strictly greater
    /// than the combined range start, so any prefix of them yields strictly increasing partition bounds.
    /// </summary>
    private List<TKey> ComputeSubcompactionBoundaries(List<SsTable<TKey, TValue>> inputs)
    {
        var candidates = new List<TKey>();

        foreach (var input in inputs)
        {
            foreach (var metadata in input.BlockMetadata)
            {
                candidates.Add(metadata.FirstKey);
            }
        }

        candidates.Sort(_keyComparer);

        var distinct = new List<TKey>(candidates.Count);

        foreach (var key in candidates)
        {
            if (distinct.Count == 0 || _keyComparer.Compare(distinct[distinct.Count - 1], key) != 0)
            {
                distinct.Add(key);
            }
        }

        // Drop the smallest distinct key (the combined range start) so all boundaries are usable splits.
        if (distinct.Count > 0)
        {
            distinct.RemoveAt(0);
        }

        return distinct;
    }

    /// <summary>
    /// Returns <see langword="true"/> when the SSTs are in ascending key order with non-overlapping ranges,
    /// the invariant every leveled level must satisfy. Used only by debug assertions.
    /// </summary>
    private static bool IsSortedNonOverlapping(List<SsTable<TKey, TValue>> tables)
    {
        for (var i = 1; i < tables.Count; i++)
        {
            if (_keyComparer.Compare(tables[i - 1].LastKey, tables[i].FirstKey) >= 0)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Returns <see langword="true"/> when any leveled level below <paramref name="targetLevelIndex"/>
    /// still holds data, meaning tombstones must be preserved when compacting into the target level.
    /// </summary>
    private static bool HasDataBelow(List<List<SsTable<TKey, TValue>>> levels, int targetLevelIndex)
    {
        for (var i = targetLevelIndex + 1; i < levels.Count; i++)
        {
            if (levels[i].Count > 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Evaluates the tiered-compaction triggers over the tier list (oldest-first) and returns the start
    /// index of the newest suffix to merge, or <c>-1</c> when no compaction is warranted.
    /// </summary>
    private int SelectTieredCompaction(List<SsTable<TKey, TValue>> tiers)
    {
        var n = tiers.Count;

        // Only act once enough sorted runs have accumulated, and never try to merge fewer than two.
        if (n < 2 || n < _maxCompactionTiers)
        {
            return -1;
        }

        // Trigger 1 - space amplification: when everything above the oldest tier together reaches
        // MaxSizeAmplificationPercent of the oldest tier, merge all tiers into one.
        var oldestSize = tiers[0].Size;
        long sizeExceptOldest = 0;

        for (var i = 1; i < n; i++)
        {
            sizeExceptOldest += tiers[i].Size;
        }

        if (oldestSize > 0 && sizeExceptOldest * 100 >= oldestSize * (long)_maxSizeAmplificationPercent)
        {
            return 0;
        }

        // Trigger 2 - size ratio: scanning from the newest tier, accumulate the size of all newer tiers.
        // At the first (older) tier that is larger than (100 + SizeRatioPercent)% of that running sum,
        // merge the newer tiers preceding it, provided there are at least MinMergeWidth of them.
        long sumNewer = 0;

        for (var k = 0; k < n; k++)
        {
            var thisSize = tiers[n - 1 - k].Size;

            if (k >= _minMergeWidth && sumNewer > 0 && thisSize * 100 > sumNewer * (long)(100 + _sizeRatioPercent))
            {
                return n - k;
            }

            sumNewer += thisSize;
        }

        // Trigger 3 - reduce sorted runs: nothing else fired, so merge enough of the newest tiers to
        // bring the count back below the limit (always at least two, so this makes real progress).
        var merge = n - _maxCompactionTiers + 2;

        if (merge < 2)
        {
            merge = 2;
        }

        if (merge > n)
        {
            merge = n;
        }

        return n - merge;
    }

    private bool TryDeleteSstFile(string filename)
    {
        try
        {
            File.Delete(filename);
            return true;
        }
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
    }

    /// <summary>
    /// Creates a new current (writable) MemTable, attaching a write-ahead log when durability is enabled.
    /// </summary>
    private MemTable<TKey, TValue> CreateCurrentMemTable(long id)
    {
        if (!_useWriteAheadLog)
        {
            return new MemTable<TKey, TValue>(id);
        }

        var wal = new WriteAheadLog<TKey, TValue>(GetWalPath(id), _syncWriteAheadLogToDisk);
        return new MemTable<TKey, TValue>(id, wal);
    }

    /// <summary>
    /// Deletes the write-ahead log file for the specified memtable id, if any. Never throws: a stale
    /// WAL whose SST already exists is cleaned up on the next open, so a failed delete is not fatal.
    /// </summary>
    private void DeleteWal(long id)
    {
        if (!_useWriteAheadLog)
        {
            return;
        }

        try
        {
            File.Delete(GetWalPath(id));
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    /// <summary>
    /// On a clean shutdown the current MemTable is empty (any pending data was frozen and flushed):
    /// remove its write-ahead log so that reopening the store finds nothing to replay.
    /// </summary>
    public void DeleteCurrentMemTableWal()
    {
        if (!_useWriteAheadLog)
        {
            return;
        }

        var id = _state.CurrentMemTable.Id;

        // Close the handle before deleting (required on Windows where an open handle blocks deletion).
        _state.CurrentMemTable.Dispose();
        DeleteWal(id);
    }

    public IStorageIterator<TKey, TValue> CreateIterator()
    {
        return new LsmStorageIterator(this);
    }

    /// <summary>
    /// Freezes the current MemTable to an immutable MemTable. This method is not synchronized and should
    /// only be called by other synchronized methods.
    /// </summary>
    private void FreezeMemTable()
    {
        Debug.Assert(_currentMemTableLock.IsWriteLockHeld);

        _immutableMemTablesLock.EnterWriteLock();

        try
        {
            var _previousMemTable = _state.CurrentMemTable;

            _state = new StorageState<TKey, TValue>
            {
                CurrentMemTable = CreateCurrentMemTable(IdGenerator.GetNextId()),
                ImmutableMemTables = _state.ImmutableMemTables.Enqueue(_previousMemTable),
                LevelZeroTables = _state.LevelZeroTables,
                LeveledSsTables = _state.LeveledSsTables
            };
        }
        finally
        {
            _immutableMemTablesLock.ExitWriteLock();
        }
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);
        DisposeInternal();

        _disposed = true;
    }

    public void DisposeInternal()
    {
        _state.CurrentMemTable.Dispose();

        foreach (var memTable in _state.ImmutableMemTables)
        {
            memTable.Dispose();
        }

        foreach (var table in _state.LevelZeroTables)
        {
            table.Dispose();
        }

        foreach (var table in _state.LeveledSsTables.SelectMany(x => x))
        {
            table.Dispose();
        }
    }

    ~LsmStorageInner()
    {
        DisposeInternal();
    }

    private sealed class LsmStorageIterator : IStorageIterator<TKey, TValue>
    {
        private readonly LsmStorageInner<TKey, TValue> _storage;

        public LsmStorageIterator(LsmStorageInner<TKey, TValue> storage)
        {
            _storage = storage;
        }

        public IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(CancellationToken cancellationToken = default)
        {
            return EnumerateAsync(hasFrom: false, default!, cancellationToken);
        }

        /// <summary>
        /// Returns all the values whose key is greater than or equal to <paramref name="from"/>, merging the
        /// current MemTable, the immutable MemTables and the on-disk L0 SSTables.
        /// </summary>
        public IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(TKey from, CancellationToken cancellationToken = default)
        {
            return EnumerateAsync(hasFrom: true, from, cancellationToken);
        }

        private async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(bool hasFrom, TKey from, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Hold the level0 read lock for the whole scan: it freezes the L0 tier list (no flush append or
            // compaction swap) and, because flush only disposes a flushed immutable MemTable after its own
            // level0 write-lock section, it also keeps the immutable MemTables we iterate alive throughout.
            await _storage._level0Lock.EnterReadLockAsync();

            try
            {
                var iterators = BuildIterators(hasFrom, from, cancellationToken);
                var merge = new MergeIterator<TKey, TValue>(iterators);

                var enumerable = hasFrom
                    ? merge.EnumerateAsync(from, cancellationToken)
                    : merge.EnumerateAsync(cancellationToken);

                await foreach (var entry in enumerable)
                {
                    if (!_valueSerializer.IsTombstoneValue(entry.Value))
                    {
                        yield return entry;
                    }
                }
            }
            finally
            {
                _storage._level0Lock.ExitReadLock();
            }
        }

        /// <summary>
        /// Builds the merge inputs in most-recent-first order (current MemTable, then immutable MemTables
        /// newest-first, then L0 SSTables newest-first) so the <see cref="MergeIterator{TKey, TValue}"/>
        /// keeps the latest value on duplicate keys. The current MemTable is materialized under its
        /// (thread-affine) lock so the rest of the scan can perform async SST I/O without holding it.
        /// </summary>
        private List<IStorageIterator<TKey, TValue>> BuildIterators(bool hasFrom, TKey from, CancellationToken cancellationToken)
        {
            List<KeyValuePair<TKey, TValue>> currentSnapshot;
            ImmutableQueue<IMemTable<TKey, TValue>> immutableMemTables;
            List<SsTable<TKey, TValue>> levelZeroTables;
            List<List<SsTable<TKey, TValue>>> leveledTables;

            _storage._currentMemTableLock.EnterReadLock();

            try
            {
                // Read a consistent state snapshot: FreezeMemTable swaps the whole state object under this
                // same lock, so current + immutable + L0 references are coherent here.
                var state = _storage._state;

                // Drain the current MemTable into a list while holding the lock. Its iterator is synchronous,
                // so this completes inline without a thread switch (required for the thread-affine lock).
                currentSnapshot = MaterializeCurrentMemTable(state.CurrentMemTable, cancellationToken);

                immutableMemTables = state.ImmutableMemTables;
                // The level0 read lock (held by the caller) keeps these stable; copy them defensively anyway.
                levelZeroTables = state.LevelZeroTables.ToList();
                leveledTables = state.LeveledSsTables.Select(level => level.ToList()).ToList();
            }
            finally
            {
                _storage._currentMemTableLock.ExitReadLock();
            }

            var iterators = new List<IStorageIterator<TKey, TValue>>
            {
                new ListStorageIterator(currentSnapshot)
            };

            // Immutable MemTables are enqueued oldest-first; add them newest-first to preserve precedence.
            foreach (var memTable in immutableMemTables.Reverse())
            {
                iterators.Add(memTable.CreateIterator());
            }

            // L0 SSTs are appended oldest-first; add them newest-first so a newer table wins on duplicates.
            for (var i = levelZeroTables.Count - 1; i >= 0; i--)
            {
                iterators.Add(new SsTableIterator<TKey, TValue>(levelZeroTables[i]));
            }

            // Then the compaction levels, newest-first (L1 before L2 ...). Within a level the SSTs are
            // non-overlapping, so their relative order does not affect correctness.
            foreach (var level in leveledTables)
            {
                foreach (var table in level)
                {
                    iterators.Add(new SsTableIterator<TKey, TValue>(table));
                }
            }

            return iterators;
        }

        private static List<KeyValuePair<TKey, TValue>> MaterializeCurrentMemTable(IMemTable<TKey, TValue> memTable, CancellationToken cancellationToken)
        {
            var snapshot = new List<KeyValuePair<TKey, TValue>>();

            // The MemTable iterator is synchronous, so this blocking drain stays on the calling thread.
            foreach (var entry in memTable.CreateIterator().EnumerateAsync(cancellationToken).ToBlockingEnumerable(cancellationToken))
            {
                snapshot.Add(entry);
            }

            return snapshot;
        }
    }

    /// <summary>
    /// An <see cref="IStorageIterator{TKey, TValue}"/> over an already-materialized, key-ascending list.
    /// Used to snapshot the current MemTable so the rest of a scan can run async I/O off its lock.
    /// </summary>
    private sealed class ListStorageIterator : IStorageIterator<TKey, TValue>
    {
        private readonly List<KeyValuePair<TKey, TValue>> _entries;

        public ListStorageIterator(List<KeyValuePair<TKey, TValue>> entries)
        {
            _entries = entries;
        }

        public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;

            foreach (var entry in _entries)
            {
                yield return entry;
            }
        }

        public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(TKey from, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;

            foreach (var entry in _entries)
            {
                if (_keyComparer.Compare(entry.Key, from) >= 0)
                {
                    yield return entry;
                }
            }
        }
    }
}
