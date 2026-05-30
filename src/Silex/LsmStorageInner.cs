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
    private readonly AsyncReaderWriterLock _leveledTablesLock = new();

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

        _currentMemTableLock.EnterReadLock();

        var snapshot = _state.Clone();

        try
        {
            // CurrentMemTable is the only thing that needs to be locked
            // since all other collections are immutable
            if (snapshot.CurrentMemTable.TryGet(key, out var result))
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

        try
        {
            _immutableMemTablesLock.EnterReadLock();

            // Immutable MemTables are enqueued oldest-first, so iterate in reverse to let the most
            // recently frozen table win when the same key exists in several of them.
            foreach (var memTable in snapshot.ImmutableMemTables.Reverse())
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
            
            // TODO: this can be parallelized, multiple table could return the value, but the one 
            // from the most recent table would be used.

            for (var i = l0.Count - 1; i >= 0; i--)
            {
                var table = l0[i];

                // The key could be in this table, if not will go to the next one
                if (_keyComparer.Compare(key, table.FirstKey) < 0 || _keyComparer.Compare(key, table.LastKey) > 0)
                {
                    continue;
                }

                // Check if the bloom filter tells us to skip this table
                if (!table.BloomFilter.Probe(keyMemory.Span))
                {
                    continue; 
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
                        // The key is present in this (newest matching) table, so it shadows any older tier.
                        // A found tombstone resolves to default.
                        return resolved;
                    }

                    // The only block whose range can cover the key has been read and the key was not in it
                    // (a bloom-filter false positive): the key is absent from this table, so fall through to
                    // older tables instead of masking them with a premature default.
                    break;
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
    /// Puts a value with the specified key in the current <see cref="IMemTable">. If one already exists it is replaced.
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
            }
            finally
            {
                _level0Lock.ExitWriteLock();

                memTableToFlush.Dispose();
            }

            // The data is now durable in an SST and visible in L0, and the memtable (and its WAL handle)
            // is disposed: the write-ahead log is obsolete and can be removed. If a crash happens before
            // this point the WAL survives and is replayed on the next open; if it happens after the SST is
            // durable but before the delete, recovery sees the matching SST and drops the stale WAL.
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
                // The level0 read lock (held by the caller) keeps this list stable; copy it defensively anyway.
                levelZeroTables = state.LevelZeroTables.ToList();
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
