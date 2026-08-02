using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Buffers;
using Silex.MemTables;
using Silex.Serialization;
using Silex.Tables;
using Silex.Wal;
using System.Buffers;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;

namespace Silex;

/// <summary>
/// The inner storage engine. It handles thread-safety for the <see cref="StorageState"/>.
/// </summary>
internal sealed class LsmStorageInner : IDisposable
{
    private static readonly IBinaryEncoder<ByteSlice> _valueSerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;
    private static readonly IBinaryEncoder<ByteSlice> _keySerializer= BinaryEncoderFactory<ByteSlice>.BinarySerializer;
    private static readonly IComparer<ByteSlice> _keyComparer = BinaryEncoderFactory<ByteSlice>.BinarySerializer.Comparer;

    // Use different locks for each type of manipulated data such that we can lock them individually.
    // For instance updating the MemTable should be synchronized, but not blocked by compaction.
    // Moreover, some locks are asynchronous (level0) while other are synchronous (mem tables).

    private readonly ReaderWriterLockSlim _currentMemTableLock = new();
    private readonly ReaderWriterLockSlim _immutableMemTablesLock = new();
    private readonly AsyncReaderWriterLock _level0Lock = new();

    // Serializes flush and compaction so they never interleave. This keeps each structural change's
    // in-memory install and manifest commit ordered with respect to every other structural change.
    private readonly SemaphoreSlim _maintenanceLock = new(1, 1);

    internal StorageState _state;
    private bool _disposed;
    private readonly IBlockEncoder _blockEncoder;
    private readonly ISsTableEncoder _ssTableEncoder;
    private readonly ISsTableBuilderFactory _ssTableBuilderFactory;
    private readonly IBloomFilterFactory _bloomFilterFactory;
    private readonly long _memTableSizeLimit;
    private readonly int _memTableArenaBlockSize;
    private readonly BlockCache _blockCache;
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
    private readonly SstCompression _compression;
    private readonly int _compressionLevel;
    private readonly double _minimumCompressionSavingsPercent;
    private readonly IBlockEncoderFactory _blockEncoderFactory;
    private readonly ISsTableEncoderFactory _ssTableEncoderFactory;
    private SsTable[]? _sortedSsTableRun;

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
        _compression = options.Compression;
        _compressionLevel = options.CompressionLevel;
        _minimumCompressionSavingsPercent = options.MinimumCompressionSavingsPercent;
        _blockEncoderFactory = options.BlockEncoderFactory;
        _ssTableEncoderFactory = options.SsTableEncoderFactory;
        _memTableArenaBlockSize = options.MemTableArenaBlockSize;
        _state = new StorageState() { CurrentMemTable = CreateCurrentMemTable(IdGenerator.GetNextId()) };
        _blockEncoder = options.BlockEncoderFactory.Create();
        _ssTableEncoder = options.SsTableEncoderFactory.Create();
        _ssTableBuilderFactory = options.SsTableBuilderFactory;
        _bloomFilterFactory = options.BloomFilterFactory;
        _memTableSizeLimit = options.MemTableSizeLimit;
        _blockCache = new BlockCache(options.BlockCacheSizeLimit);
    }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns><see cref="OwnedByteSlice"/> if the key was found; otherwise <see langword="null"/>.</returns>
    public async ValueTask<OwnedByteSlice?> GetAsync(ByteSlice key, CancellationToken cancellationToken = default)
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

        IMemTable currentMemTable;
        ImmutableQueue<IMemTable> immutableMemTables;

        try
        {
            currentMemTable = _state.CurrentMemTable;
            immutableMemTables = _state.ImmutableMemTables;

            // CurrentMemTable is the only thing that needs to be locked
            // since all other collections are immutable
            if (currentMemTable.TryGet(key, out var result))
            {
                return result.IsTombstone ? null : OwnedByteSlice.CopyFrom(result.Span);
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
                        return result.IsTombstone ? null : OwnedByteSlice.CopyFrom(result.Span);
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
            await _level0Lock.EnterReadLockAsync(cancellationToken);

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
                var probes = new (bool found, OwnedByteSlice? resolved)[l0.Count];

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
                        var selected = probes[i].resolved;
                        for (var j = 0; j < i; j++)
                        {
                            probes[j].resolved?.Dispose();
                        }

                        return selected;
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

        return null;
    }

    /// <summary>
    /// Resolves a key against a single block. Returns <c>true</c> when the key is present (the resolved
    /// value is the stored value, or <c>default</c> when it is a tombstone), and <c>false</c> when the key
    /// is absent. Kept synchronous so the value span never crosses an <c>await</c> boundary.
    /// </summary>
    private static bool TryResolveBlockValue(Block block, ReadOnlySpan<byte> encodedKey, out OwnedByteSlice? resolved)
    {
        if (block.TryGetValue(encodedKey, out var value, out var isTombstone))
        {
            if (isTombstone)
            {
                resolved = null;
                return true;
            }

            resolved = OwnedByteSlice.CopyFrom(value);
            return true;
        }

        resolved = null;
        return false;
    }

    /// <summary>
    /// Probes a single SST for <paramref name="key"/>. Returns <c>found = true</c> with the resolved value
    /// (a tombstone resolves to <c>default</c>) when the key is present in the table, and
    /// <c>found = false</c> when the table cannot contain it (range/bloom miss) or a bloom false-positive
    /// turns out absent, so the caller falls through to older tables.
    /// </summary>
    private async ValueTask<(bool found, OwnedByteSlice? resolved)> TryReadFromTableAsync(SsTable table, ByteSlice key, ReadOnlyMemory<byte> keyMemory, CancellationToken cancellationToken)
    {
        // The key could be in this table, if not go to the next one.
        if (_keyComparer.Compare(key, table.FirstKey) < 0 || _keyComparer.Compare(key, table.LastKey) > 0)
        {
            return (false, null);
        }

        // Check if the bloom filter tells us to skip this table.
        if (!table.BloomFilter.Probe(keyMemory.Span))
        {
            return (false, null);
        }

        var blockIndex = FindMatchingBlockIndex(table.BlockMetadataArray, key);
        if (blockIndex >= 0)
        {
            using var blockLease = await table.ReadBlockCachedAsync(blockIndex, _blockCache, cancellationToken);
            var block = blockLease.Block;

            if (block != null && TryResolveBlockValue(block, keyMemory.Span, out var resolved))
            {
                // The key is present in this (newest matching) table, so it shadows any older one.
                // A found tombstone resolves to default.
                return (true, resolved);
            }
        }

        return (false, null);
    }

    private static int FindMatchingBlockIndex(BlockMetadata[] blockMetadata, ByteSlice key)
    {
        var start = 0;
        var end = blockMetadata.Length - 1;

        while (start <= end)
        {
            var middle = start + (end - start) / 2;
            var metadata = blockMetadata[middle];

            if (_keyComparer.Compare(key, metadata.LastKey) > 0)
            {
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
        }

        if ((uint)start >= (uint)blockMetadata.Length)
        {
            return -1;
        }

        var candidate = blockMetadata[start];
        return _keyComparer.Compare(key, candidate.FirstKey) >= 0 ? candidate.Index : -1;
    }

    private static int FindMatchingBlockIndex(BlockMetadata[] blockMetadata, ReadOnlySpan<byte> key)
    {
        var start = 0;
        var end = blockMetadata.Length - 1;

        while (start <= end)
        {
            var middle = start + (end - start) / 2;
            var metadata = blockMetadata[middle];

            if (key.SequenceCompareTo(metadata.LastKey.Span) > 0)
            {
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
        }

        if ((uint)start >= (uint)blockMetadata.Length)
        {
            return -1;
        }

        var candidate = blockMetadata[start];
        return key.SequenceCompareTo(candidate.FirstKey.Span) >= 0 ? candidate.Index : -1;
    }

    // ---------------------------------------------------------------------------------------------
    // Raw (zero-copy) read path
    //
    // These overloads read an entry by its typed key but surface the value as raw bytes, avoiding the
    // per-read value allocation that GetAsync(ByteSlice) incurs (ByteArrayEncoder.Decode -> ToArray). They
    // share a single sequential traversal driven by a struct sink so each destination shape (buffer
    // writer, caller buffer, inspection callback) costs no extra allocation and no boxing.
    //
    // Tombstone semantics differ from GetAsync on purpose: a deleted key is reported as "not found"
    // (false / -1) rather than surfacing an empty value. An internal tri-state keeps "absent" and
    // "tombstone" distinct during traversal so a delete in a newer table correctly shadows older data
    // instead of letting an older live value resurface.
    // ---------------------------------------------------------------------------------------------

    private enum RawLookup
    {
        // The key is absent from this source; keep searching older sources.
        Miss,

        // The key is present as a deletion; it shadows older sources and resolves to "not found".
        Tombstone,

        // The key is present with a live value, already handed to the sink.
        Live,
    }

    /// <summary>
    /// Receives the raw value bytes of a live entry. Implemented by allocation-free structs and passed by
    /// value through the generic traversal so the JIT specialises each call site (no boxing, no closures).
    /// </summary>
    private interface IValueByteSink
    {
        void Accept(ReadOnlySpan<byte> valueBytes);
    }

    private readonly struct BufferWriterSink(IBufferWriter<byte> writer) : IValueByteSink
    {
        public void Accept(ReadOnlySpan<byte> valueBytes) => writer.Write(valueBytes);
    }

    private readonly struct MemoryCopySink(Memory<byte> destination) : IValueByteSink
    {
        // Copy only when the value fits; the public GetRawAsync still reports the full length so the
        // caller can detect the short buffer and retry without an exception.
        public void Accept(ReadOnlySpan<byte> valueBytes)
        {
            if (valueBytes.Length <= destination.Length)
            {
                valueBytes.CopyTo(destination.Span);
            }
        }
    }

    private readonly struct DelegateSink<TArg>(TArg arg, ReadValueAction<TArg> reader) : IValueByteSink
    {
        public void Accept(ReadOnlySpan<byte> valueBytes) => reader(arg, valueBytes);
    }

    /// <summary>
    /// Reads the raw value bytes for <paramref name="key"/> into <paramref name="destination"/>.
    /// </summary>
    /// <returns><c>true</c> when the key exists with a live value; <c>false</c> when it is missing or deleted.</returns>
    /// <remarks>
    /// The bytes are written to <paramref name="destination"/> synchronously while the entry's source is
    /// locked. The caller must keep <paramref name="destination"/> valid and must not reuse it concurrently
    /// until the returned task completes.
    /// </remarks>
    public ValueTask<bool> TryGetRawAsync(ByteSlice key, IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);

        return TryGetRawAsync(key.Memory, destination, cancellationToken);
    }

    public ValueTask<bool> TryGetRawAsync(ReadOnlyMemory<byte> key, IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(destination);

        return FoundAsync(TryReadRawCoreAsync(key, new BufferWriterSink(destination), cancellationToken));

        static async ValueTask<bool> FoundAsync(ValueTask<int> length) => await length >= 0;
    }

    /// <summary>
    /// Copies the raw value bytes for <paramref name="key"/> into <paramref name="destination"/>.
    /// </summary>
    /// <returns>
    /// The length of the value in bytes when the key exists with a live value, or <c>-1</c> when it is
    /// missing or deleted. When the returned length is greater than <paramref name="destination"/>'s length
    /// the buffer was too small and nothing was written; retry with a buffer of at least that size.
    /// </returns>
    public async ValueTask<int> GetRawAsync(ByteSlice key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        return await GetRawAsync(key.Memory, destination, cancellationToken);
    }

    public async ValueTask<int> GetRawAsync(ReadOnlyMemory<byte> key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        return await TryReadRawCoreAsync(key, new MemoryCopySink(destination), cancellationToken);
    }

    /// <summary>
    /// Invokes <paramref name="reader"/> with a read-only borrow of the raw value bytes for
    /// <paramref name="key"/>, without copying them.
    /// </summary>
    /// <returns><c>true</c> when the key exists with a live value (the reader ran); otherwise <c>false</c>.</returns>
    /// <remarks>
    /// <paramref name="reader"/> runs synchronously while the entry's source is locked. It must not await,
    /// block, store the span, or call back into this store; doing so risks deadlock or use of freed memory.
    /// <paramref name="arg"/> is passed through to avoid a closure allocation.
    /// </remarks>
    public ValueTask<bool> TryReadRawAsync<TArg>(ByteSlice key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);

        return TryReadRawAsync(key.Memory, arg, reader, cancellationToken);
    }

    public ValueTask<bool> TryReadRawAsync<TArg>(ReadOnlyMemory<byte> key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);

        return FoundAsync(TryReadRawCoreAsync(key, new DelegateSink<TArg>(arg, reader), cancellationToken));

        static async ValueTask<bool> FoundAsync(ValueTask<int> length) => await length >= 0;
    }

    /// <summary>
    /// Scans live entries in key order, invoking <paramref name="reader"/> with encoded key bytes and raw
    /// value bytes. The spans are borrowed and valid only during the synchronous callback.
    /// </summary>
    /// <remarks>
    /// The allocation-free SST fast path is used only when the on-disk tables form a single globally
    /// non-overlapping sorted run. Other layouts fall back to the regular iterator to preserve duplicate-key
    /// and tombstone semantics.
    /// </remarks>
    public async ValueTask<long> ScanRawAsync<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentOutOfRangeException.ThrowIfNegative(maxEntries);

        if (maxEntries == 0)
        {
            return 0;
        }

        await _level0Lock.EnterReadLockAsync(cancellationToken);

        try
        {
            if (TryGetGloballySortedSsTableRun(out var tables))
            {
                var state = new RawScanState<TArg>(arg, reader, maxEntries);

                foreach (var table in tables)
                {
                    var blockMetadata = table.BlockMetadataArray;
                    using var blockReader = table.CreateSequentialBlockReader();

                    for (var i = 0; i < blockMetadata.Length; i++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        using var block = blockReader.ReadNextBlock();

                        if (!block.ForEachRaw(state, static (s, key, value) => s.Accept(key, value), skipTombstones: true))
                        {
                            return state.Count;
                        }
                    }
                }

                return state.Count;
            }
        }
        finally
        {
            _level0Lock.ExitReadLock();
        }

        return await ScanRawFallbackAsync(arg, reader, maxEntries, cancellationToken);
    }

    private async ValueTask<long> ScanRawFallbackAsync<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries, CancellationToken cancellationToken)
    {
        long count = 0;

        await foreach (var entry in CreateIterator().EnumerateAsync(cancellationToken))
        {
            if (count >= maxEntries)
            {
                break;
            }

            count++;

            if (!InvokeRawEntryReader(arg, reader, entry.Key, entry.Value))
            {
                break;
            }
        }

        return count;
    }

    /// <summary>
    /// Raw, cached, lower-bound seek: invokes <paramref name="reader"/> for up to <paramref name="maxEntries"/>
    /// live entries whose key is greater than or equal to <paramref name="from"/>, in ascending key order.
    /// Mirrors <see cref="ScanRawAsync{TArg}"/> but starts at <paramref name="from"/> and reads blocks through
    /// the block cache, so a hot working set is reused across seeks instead of issuing an uncached read per call.
    /// Returns the number of live entries delivered.
    /// </summary>
    public async ValueTask<long> SeekRawAsync<TArg>(ByteSlice from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
    {
        return await SeekRawAsync(from.Memory, arg, reader, maxEntries, cancellationToken);
    }

    public async ValueTask<long> SeekRawAsync<TArg>(ReadOnlyMemory<byte> from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentOutOfRangeException.ThrowIfNegative(maxEntries);

        if (maxEntries == 0)
        {
            return 0;
        }

        await _level0Lock.EnterReadLockAsync(cancellationToken);

        try
        {
            if (TryGetGloballySortedSsTableRun(out var tables))
            {
                RawScanState<TArg>? state = maxEntries == 1 ? null : new RawScanState<TArg>(arg, reader, maxEntries);
                var startTableIndex = FindStartTableIndex(tables, from.Span);

                for (var t = startTableIndex; t < tables.Count; t++)
                {
                    var table = tables[t];
                    var blockMetadata = table.BlockMetadataArray;

                    var blockStart = 0;
                    var seekInFirstBlock = false;

                    if (t == startTableIndex)
                    {
                        blockStart = FindStartBlockIndex(blockMetadata, from.Span);

                        // The stepped-back block ends before 'from' (this happens when 'from' falls exactly on
                        // a later block's FirstKey, or in a gap between blocks): the first key >= from lives in
                        // a later block, so advance to it instead of giving up.
                        if (blockMetadata[blockStart].LastKey.Span.SequenceCompareTo(from.Span) < 0)
                        {
                            blockStart++;
                        }
                        else
                        {
                            seekInFirstBlock = true;
                        }
                    }

                    for (var b = blockStart; b < blockMetadata.Length; b++)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        using var lease = await table.ReadBlockCachedAsync(blockMetadata[b].Index, _blockCache, cancellationToken);
                        var block = lease.Block;

                        if (block == null)
                        {
                            continue;
                        }

                        if (maxEntries == 1)
                        {
                            var encodedFrom = seekInFirstBlock && b == blockStart
                                ? from.Span
                                : ReadOnlySpan<byte>.Empty;

                            if (block.TryGetFirstRawFrom(encodedFrom, out var key, out var value, skipTombstones: true))
                            {
                                reader(arg, key, value);
                                return 1;
                            }

                            continue;
                        }

                        bool keepGoing;
                        if (seekInFirstBlock && b == blockStart)
                        {
                            keepGoing = block.ForEachRawFrom(from.Span, state!, static (s, key, value) => s.Accept(key, value), skipTombstones: true);
                        }
                        else
                        {
                            keepGoing = block.ForEachRaw(state!, static (s, key, value) => s.Accept(key, value), skipTombstones: true);
                        }

                        if (!keepGoing)
                        {
                            return state?.Count ?? 0;
                        }
                    }
                }

                return state!.Count;
            }
        }
        finally
        {
            _level0Lock.ExitReadLock();
        }

        using var ownedFrom = OwnedByteSlice.CopyFrom(from.Span);
        return await SeekRawFallbackAsync(ownedFrom.Slice, arg, reader, maxEntries, cancellationToken);
    }

    private async ValueTask<long> SeekRawFallbackAsync<TArg>(ByteSlice from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries, CancellationToken cancellationToken)
    {
        long count = 0;

        await foreach (var entry in CreateIterator().EnumerateAsync(from, cancellationToken))
        {
            if (count >= maxEntries)
            {
                break;
            }

            count++;

            if (!InvokeRawEntryReader(arg, reader, entry.Key, entry.Value))
            {
                break;
            }
        }

        return count;
    }

    /// <summary>
    /// Returns the index of the first table whose <see cref="SsTable{ByteSlice, ByteSlice}.LastKey"/> is greater than or
    /// equal to <paramref name="from"/>, or <paramref name="tables"/>.Count when every table ends before it.
    /// </summary>
    private static int FindStartTableIndex(IReadOnlyList<SsTable> tables, ReadOnlySpan<byte> from)
    {
        var start = 0;
        var end = tables.Count - 1;

        while (start <= end)
        {
            var m = start + (end - start) / 2;

            if (tables[m].LastKey.Span.SequenceCompareTo(from) < 0)
            {
                start = m + 1;
            }
            else
            {
                end = m - 1;
            }
        }

        return start;
    }

    /// <summary>
    /// Returns the index of the block that may contain <paramref name="from"/>: the last block whose FirstKey is
    /// less than or equal to <paramref name="from"/>, clamped to the first block. Mirrors the seek used by
    /// <see cref="SsTableIterator{ByteSlice, ByteSlice}"/>; callers must apply the stepped-back-block correction.
    /// </summary>
    private static int FindStartBlockIndex(BlockMetadata[] blockMetadata, ReadOnlySpan<byte> from)
    {
        var start = 0;
        var end = blockMetadata.Length - 1;

        while (start <= end)
        {
            var m = start + (end - start) / 2;
            var compare = blockMetadata[m].FirstKey.Span.SequenceCompareTo(from);

            if (compare == 0)
            {
                return Math.Max(0, m - 1);
            }

            if (compare < 0)
            {
                start = m + 1;
            }
            else
            {
                end = m - 1;
            }
        }

        return Math.Max(0, start - 1);
    }

    private static bool InvokeRawEntryReader<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, ByteSlice key, ByteSlice value)
    {
        if (_keySerializer.TryGetRawBytes(key, out var keyBytes) && _valueSerializer.TryGetRawBytes(value, out var valueBytes))
        {
            return reader(arg, keyBytes, valueBytes);
        }

        using var keyBuffer = new PooledArrayBufferWriter<byte>(Math.Max(1, _keySerializer.GetLength(key)));
        using var valueBuffer = new PooledArrayBufferWriter<byte>(Math.Max(1, _valueSerializer.GetLength(value)));

        if (!_keySerializer.TryGetRawBytes(key, out keyBytes))
        {
            var keyWriter = new EncoderBinaryWriter(keyBuffer);
            _keySerializer.Encode(key, ref keyWriter);
            keyWriter.Flush();
            keyBytes = keyBuffer.WrittenMemory.Span;
        }

        if (!_valueSerializer.TryGetRawBytes(value, out valueBytes))
        {
            var valueWriter = new EncoderBinaryWriter(valueBuffer);
            _valueSerializer.Encode(value, ref valueWriter);
            valueWriter.Flush();
            valueBytes = valueBuffer.WrittenMemory.Span;
        }

        return reader(arg, keyBytes, valueBytes);
    }

    private bool TryGetGloballySortedSsTableRun(out IReadOnlyList<SsTable> tables)
    {
        var cached = Volatile.Read(ref _sortedSsTableRun);
        if (cached != null)
        {
            tables = cached;
            return true;
        }

        _currentMemTableLock.EnterReadLock();

        try
        {
            var state = _state;

            if (state.CurrentMemTable.Count != 0 || !state.ImmutableMemTables.IsEmpty)
            {
                tables = Array.Empty<SsTable>();
                return false;
            }

            var count = state.LevelZeroTables.Count;

            foreach (var level in state.LeveledSsTables)
            {
                count += level.Count;
            }

            var sorted = new SsTable[count];
            var index = 0;

            foreach (var table in state.LevelZeroTables)
            {
                sorted[index++] = table;
            }

            foreach (var level in state.LeveledSsTables)
            {
                foreach (var table in level)
                {
                    sorted[index++] = table;
                }
            }

            tables = sorted;
            if (sorted.Length > 1)
            {
                Array.Sort(sorted, static (left, right) => _keyComparer.Compare(left.FirstKey, right.FirstKey));

                for (var i = 1; i < sorted.Length; i++)
                {
                    if (_keyComparer.Compare(sorted[i - 1].LastKey, sorted[i].FirstKey) >= 0)
                    {
                        return false;
                    }
                }
            }

            // Publish before releasing the memtable read lock so a writer cannot invalidate the cache
            // and then have this older snapshot republished after its mutation.
            Volatile.Write(ref _sortedSsTableRun, sorted);
            return true;
        }
        finally
        {
            _currentMemTableLock.ExitReadLock();
        }
    }

    private void InvalidateSortedSsTableRun() => Volatile.Write(ref _sortedSsTableRun, null);

    private sealed class RawScanState<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries)
    {
        public long Count { get; private set; }

        public bool Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (Count >= maxEntries)
            {
                return false;
            }

            Count++;
            return reader(arg, key, value) && Count < maxEntries;
        }
    }

    /// <summary>
    /// Shared sequential traversal for the raw read overloads. Hands the live value bytes to
    /// <paramref name="sink"/> and returns the value length, or <c>-1</c> when the key is missing or deleted.
    /// </summary>
    private async ValueTask<int> TryReadRawCoreAsync<TSink>(ReadOnlyMemory<byte> keyMemory, TSink sink, CancellationToken cancellationToken)
        where TSink : struct, IValueByteSink
    {
        // Current and immutable memtables: see GetAsync for the locking rationale.
        _currentMemTableLock.EnterReadLock();

        IMemTable currentMemTable;
        ImmutableQueue<IMemTable> immutableMemTables;
        ByteSlice? key = null;

        try
        {
            currentMemTable = _state.CurrentMemTable;
            immutableMemTables = _state.ImmutableMemTables;

            if (currentMemTable.Count != 0)
            {
                key = ByteSlice.FromMemory(keyMemory);
                var memResult = TryResolveMemTableRaw(currentMemTable, key, sink, out var length);
                if (memResult == RawLookup.Live)
                {
                    return length;
                }

                if (memResult == RawLookup.Tombstone)
                {
                    return -1;
                }
            }
        }
        finally
        {
            _currentMemTableLock.ExitReadLock();
        }

        if (!immutableMemTables.IsEmpty)
        {
            try
            {
                _immutableMemTablesLock.EnterReadLock();
                key ??= ByteSlice.FromMemory(keyMemory);

                foreach (var memTable in immutableMemTables.Reverse())
                {
                    var memResult = TryResolveMemTableRaw(memTable, key, sink, out var length);
                    if (memResult == RawLookup.Live)
                    {
                        return length;
                    }

                    if (memResult == RawLookup.Tombstone)
                    {
                        return -1;
                    }
                }
            }
            finally
            {
                _immutableMemTablesLock.ExitReadLock();
            }
        }

        await _level0Lock.EnterReadLockAsync(cancellationToken);
        try
        {
            var l0 = _state.LevelZeroTables;

            // Probe L0 newest-first sequentially. Unlike GetAsync this path does not fan out across L0 with
            // Parallel.ForEachAsync: the struct sink writes into a single shared destination, so concurrent
            // probes could race on it. Point reads touch at most one block per table, so the sequential
            // cost is bounded.
            for (var i = l0.Count - 1; i >= 0; i--)
            {
                var (kind, length) = await TryReadRawFromTableAsync(l0[i], keyMemory, sink, cancellationToken);
                if (kind == RawLookup.Live)
                {
                    return length;
                }

                if (kind == RawLookup.Tombstone)
                {
                    return -1;
                }
            }

            var levels = _state.LeveledSsTables;

            for (var level = 0; level < levels.Count; level++)
            {
                var tables = levels[level];

                for (var i = 0; i < tables.Count; i++)
                {
                    var (kind, length) = await TryReadRawFromTableAsync(tables[i], keyMemory, sink, cancellationToken);
                    if (kind == RawLookup.Live)
                    {
                        return length;
                    }

                    if (kind == RawLookup.Tombstone)
                    {
                        return -1;
                    }
                }
            }
        }
        finally
        {
            _level0Lock.ExitReadLock();
        }

        return -1;
    }

    /// <summary>
    /// Resolves <paramref name="key"/> against a memtable, handing live value bytes to <paramref name="sink"/>.
    /// Kept synchronous so the borrowed value span never crosses an <c>await</c>.
    /// </summary>
    private static RawLookup TryResolveMemTableRaw<TSink>(IMemTable memTable, ByteSlice key, TSink sink, out int length)
        where TSink : struct, IValueByteSink
    {
        length = 0;

        if (!memTable.TryGet(key, out var value))
        {
            return RawLookup.Miss;
        }

        // A present key shadows every older source, whether live or a tombstone.
        if (value.IsTombstone)
        {
            return RawLookup.Tombstone;
        }

        if (_valueSerializer.TryGetRawBytes(value, out var bytes))
        {
            sink.Accept(bytes);
            length = bytes.Length;
            return RawLookup.Live;
        }

        // The value cannot expose its bytes directly (non-identity encoder); encode it into a pooled buffer.
        var bufferWriter = new PooledArrayBufferWriter<byte>(Math.Max(1, _valueSerializer.GetLength(value)));

        try
        {
            var writer = new EncoderBinaryWriter(bufferWriter);
            _valueSerializer.Encode(value, ref writer);
            writer.Flush();
            var encoded = bufferWriter.WrittenMemory.Span;

            sink.Accept(encoded);
            length = encoded.Length;
            return RawLookup.Live;
        }
        finally
        {
            bufferWriter.Dispose();
        }
    }

    /// <summary>
    /// Probes a single SST for <paramref name="key"/> on the raw path, returning a tri-state result and the
    /// live value length. Mirrors <see cref="TryReadFromTableAsync"/> but hands value bytes to the sink.
    /// </summary>
    private async ValueTask<(RawLookup kind, int length)> TryReadRawFromTableAsync<TSink>(SsTable table, ReadOnlyMemory<byte> keyMemory, TSink sink, CancellationToken cancellationToken)
        where TSink : struct, IValueByteSink
    {
        if (keyMemory.Span.SequenceCompareTo(table.FirstKey.Span) < 0 || keyMemory.Span.SequenceCompareTo(table.LastKey.Span) > 0)
        {
            return (RawLookup.Miss, 0);
        }

        if (!table.BloomFilter.Probe(keyMemory.Span))
        {
            return (RawLookup.Miss, 0);
        }

        var blockIndex = FindMatchingBlockIndex(table.BlockMetadataArray, keyMemory.Span);
        if (blockIndex >= 0)
        {
            using var blockLease = await table.ReadBlockCachedAsync(blockIndex, _blockCache, cancellationToken);
            var block = blockLease.Block;

            if (block != null)
            {
                var kind = TryResolveRawBlockValue(block, keyMemory.Span, sink, out var length);
                if (kind != RawLookup.Miss)
                {
                    return (kind, length);
                }
            }

        }

        return (RawLookup.Miss, 0);
    }

    /// <summary>
    /// Resolves <paramref name="key"/> against a single block on the raw path. Kept synchronous so the
    /// borrowed value span never crosses an <c>await</c>.
    /// </summary>
    private static RawLookup TryResolveRawBlockValue<TSink>(Block block, ReadOnlySpan<byte> encodedKey, TSink sink, out int length)
        where TSink : struct, IValueByteSink
    {
        length = 0;

        if (!block.TryGetValue(encodedKey, out var value, out var isTombstone))
        {
            return RawLookup.Miss;
        }

        if (isTombstone)
        {
            return RawLookup.Tombstone;
        }

        sink.Accept(value);
        length = value.Length;
        return RawLookup.Live;
    }

    public void Put(ByteSlice key, ByteSlice value)
    {
        _currentMemTableLock.EnterWriteLock();

        try
        {
            _state.CurrentMemTable.Put(key, value);
            InvalidateSortedSsTableRun();

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

    public void PutRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        if (typeof(ByteSlice) != typeof(ByteSlice) || typeof(ByteSlice) != typeof(ByteSlice))
        {
            throw new InvalidOperationException("Raw byte inserts are only supported by byte-oriented stores.");
        }

        _currentMemTableLock.EnterWriteLock();

        try
        {
            ((IRawBytesMemTable)_state.CurrentMemTable).PutRaw(key, value);
            InvalidateSortedSsTableRun();

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
    public void Delete(ByteSlice key)
    {
        _currentMemTableLock.EnterWriteLock();

        try
        {
            _state.CurrentMemTable.Put(key, ByteSlice.Tombstone);
            InvalidateSortedSsTableRun();

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

    public void DeleteRaw(ReadOnlySpan<byte> key)
    {
        _currentMemTableLock.EnterWriteLock();

        try
        {
            ((IRawBytesMemTable)_state.CurrentMemTable).DeleteRaw(key);
            InvalidateSortedSsTableRun();

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

    public void WriteBatchRaw(ReadOnlySpan<WriteBatchEntry> entries)
    {
        if (entries.IsEmpty)
        {
            return;
        }

        _currentMemTableLock.EnterWriteLock();

        try
        {
            ((MemTable)_state.CurrentMemTable).WriteBatchRaw(entries);
            InvalidateSortedSsTableRun();

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
            IMemTable memTableToFlush;

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
            using var builder = _ssTableBuilderFactory.CreateSsTableBuilder(
                sstFilename,
                _ssTableEncoder,
                _blockEncoder,
                _bloomFilterFactory,
                memTableToFlush.Count,
                _compression,
                _compressionLevel,
                _minimumCompressionSavingsPercent);
            await memTableToFlush.FlushAsync(builder, cancellationToken);

            var ssTable = await builder.BuildAsync(cancellationToken);

            Manifest manifest;

            await _level0Lock.EnterWriteLockAsync(cancellationToken);

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
                InvalidateSortedSsTableRun();

                // Snapshot the new structure while the lock is held so the persisted manifest exactly matches
                // the installed state. The (small) JSON is written to disk after releasing the lock.
                manifest = BuildManifestSnapshot();
            }
            finally
            {
                _level0Lock.ExitWriteLock();

                memTableToFlush.Dispose();
            }

            // The manifest rewrite is the durable commit point and must happen before the WAL is deleted: a
            // crash before it leaves the WAL to be replayed on open (the not-yet-committed SST is cleaned up
            // as an orphan), and a crash after it leaves a committed SST whose WAL recovery correctly drops.
            manifest.Write(StoragePath);

            // The data is now durable in an SST and visible in L0, and the memtable (and its WAL handle)
            // is disposed: the write-ahead log is obsolete and can be removed. If a crash happens before
            // this point the WAL survives and is replayed on the next open; if it happens after the SST is
            // durable and committed to the manifest but before the delete, recovery sees the
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
    /// <see cref="SsTable{ByteSlice, ByteSlice}.Id"/> assigned when the in-memory table object is created.
    /// </summary>
    private static long GetSstFileId(SsTable table)
    {
        return long.Parse(Path.GetFileNameWithoutExtension(table.Filename), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Captures the current L0 + leveled structure as a <see cref="Manifest"/> of filename ids. Must be
    /// called while holding the <c>_level0Lock</c> (read or write) so the snapshot is consistent.
    /// </summary>
    internal Manifest BuildManifestSnapshot()
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
    /// flushes to and compacts L0 at runtime, so it never runs concurrently with a flush. Each successful
    /// compaction commits the updated live-file layout to the manifest before removing input SSTs.
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
            List<SsTable> tiers;
            bool hasLeveledData;

            await _level0Lock.EnterReadLockAsync(cancellationToken);

            try
            {
                tiers = _state.LevelZeroTables.ToList();
                hasLeveledData = _state.LeveledSsTables.Any(level => level.Count > 0);
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
            var inputs = new List<SsTable>(count);

            for (var i = startIndex; i < tiers.Count; i++)
            {
                inputs.Add(tiers[i]);
            }

            // Tombstones may only be dropped when no older source remains below the compacted L0 tiers.
            // After a leveled store is reopened as tiered, existing leveled levels are still older sources.
            var dropTombstones = startIndex == 0 && !hasLeveledData;

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

            SsTable? output = null;
            var addedEntries = 0;

            using (var builder = _ssTableBuilderFactory.CreateSsTableBuilder(
                outputPath,
                _ssTableEncoder,
                _blockEncoder,
                _bloomFilterFactory,
                estimatedCount,
                _compression,
                _compressionLevel,
                _minimumCompressionSavingsPercent))
            {
                // Feed iterators newest-first so the MergeIterator keeps the most recent value per key.
                var iterators = new List<IStorageIterator>(count);

                for (var i = tiers.Count - 1; i >= startIndex; i--)
                {
                    iterators.Add(new SsTableIterator(tiers[i]));
                }

                var merge = new MergeIterator(iterators);

                await foreach (var entry in merge.EnumerateAsync(cancellationToken))
                {
                    if (dropTombstones && entry.Value.IsTombstone)
                    {
                        continue;
                    }

                    await builder.AddAsync(entry.Key, entry.Value, cancellationToken);
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
            Manifest? manifest = null;

            await _level0Lock.EnterWriteLockAsync(cancellationToken);

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

                    InvalidateSortedSsTableRun();
                    installed = true;
                    manifest = BuildManifestSnapshot();
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

            // The manifest is the durable commit point for the new tier list. Replaced inputs are only
            // disposed/deleted after it succeeds, so a crash before this write keeps the old manifest intact
            // and treats the uncommitted output as an orphan on recovery.
            manifest!.Write(StoragePath);

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

    private static bool SuffixMatches(List<SsTable> live, List<SsTable> inputs, int startIndex)
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
            List<SsTable> level0;
            List<List<SsTable>> levels;

            await _level0Lock.EnterReadLockAsync(cancellationToken);

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
    private int SelectLeveledSourceLevel(List<List<SsTable>> levels)
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
    private async Task<bool> CompactLevelAsync(int sourceLevelIndex, List<SsTable> level0, List<List<SsTable>> levels, CancellationToken cancellationToken)
    {
        // Destination level index (0-based, 0 == L1). L0 flushes into L1; level s pushes into s+1.
        var targetLevelIndex = sourceLevelIndex < 0 ? 0 : sourceLevelIndex + 1;

        var targetTables = targetLevelIndex < levels.Count ? levels[targetLevelIndex] : new List<SsTable>();

        // Determine the source SSTs (newest-first for the merge) and what stays behind in the source level.
        var sourceTables = new List<SsTable>();
        List<SsTable>? remainingSource = null;
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
        var overlapTargets = new List<SsTable>();

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
        var mergeInputs = new List<SsTable>(sourceTables.Count + overlapTargets.Count);
        mergeInputs.AddRange(sourceTables);
        mergeInputs.AddRange(overlapTargets);

        var outputs = await MergeIntoSplitSsTablesAsync(mergeInputs, dropTombstones, cancellationToken);

        // The new destination level: kept-before ++ freshly merged outputs ++ kept-after. This stays sorted
        // and non-overlapping because the consumed targets were a contiguous run and the outputs cover only
        // keys from the source and that run (see the level0 install assert below).
        var newTargetLevel = new List<SsTable>(lo + outputs.Count + (targetTables.Count - hi));

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

        await _level0Lock.EnterWriteLockAsync(cancellationToken);

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
                _state.LeveledSsTables.Add(new List<SsTable>());
            }

            _state.LeveledSsTables[targetLevelIndex] = newTargetLevel;
            InvalidateSortedSsTableRun();

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
    private async Task<List<SsTable>> MergeIntoSplitSsTablesAsync(List<SsTable> inputs, bool dropTombstones, CancellationToken cancellationToken)
    {
        var dop = _maxCompactionParallelism;

        // Decide how many parallel sub-compactions to run. Each partition should still produce roughly
        // target-sized files, so cap the count by the total input size, and never ask for more partitions
        // than there are distinct split boundaries available.
        var boundaries = dop <= 1 ? new List<ByteSlice>() : ComputeSubcompactionBoundaries(inputs);

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
            var blockEncoder = _blockEncoderFactory.Create();
            var tableEncoder = _ssTableEncoderFactory.Create();

            return await MergeRangeIntoSplitSsTablesAsync(inputs, dropTombstones, blockEncoder, tableEncoder, hasLower: false, default!, hasUpper: false, default!, cancellationToken);
        }

        // Pick partitionCount - 1 evenly-spaced split keys. Partition j covers [split[j-1], split[j]):
        // the boundary key is the inclusive lower bound of the upper partition and the exclusive upper
        // bound of the lower one, so every key lands in exactly one partition.
        var splits = new ByteSlice[partitionCount - 1];

        for (var j = 1; j < partitionCount; j++)
        {
            var index = (int)((long)j * boundaries.Count / partitionCount);
            index = Math.Min(boundaries.Count - 1, Math.Max(j - 1, index));
            splits[j - 1] = boundaries[index];
        }

        var results = new List<SsTable>?[partitionCount];

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
                    var blockEncoder = _blockEncoderFactory.Create();
                    var tableEncoder = _ssTableEncoderFactory.Create();

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
        var outputs = new List<SsTable>();

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
    private async Task<List<SsTable>> MergeRangeIntoSplitSsTablesAsync(List<SsTable> inputs, bool dropTombstones, IBlockEncoder blockEncoder, ISsTableEncoder tableEncoder, bool hasLower, ByteSlice lower, bool hasUpper, ByteSlice upper, CancellationToken cancellationToken)
    {
        var outputs = new List<SsTable>();

        // Per-output bloom sizing from the target file size; an imperfect estimate only affects the
        // false-positive rate, never correctness.
        var estimatedCount = (int)Math.Min(int.MaxValue, Math.Max(1, _targetSstSizeBytes / 24));

        ISsTableBuilder? builder = null;
        string? builderPath = null;

        try
        {
            var iterators = new List<IStorageIterator>(inputs.Count);

            foreach (var input in inputs)
            {
                iterators.Add(new SsTableIterator(input));
            }

            var merge = new MergeIterator(iterators);

            // Seek every input to the lower bound (skips earlier blocks); the merge stays ascending so the
            // upper bound is enforced with a single break below.
            var entries = hasLower ? merge.EnumerateAsync(lower, cancellationToken) : merge.EnumerateAsync(cancellationToken);

            await foreach (var entry in entries)
            {
                if (hasUpper && _keyComparer.Compare(entry.Key, upper) >= 0)
                {
                    break;
                }

                if (dropTombstones && entry.Value.IsTombstone)
                {
                    continue;
                }

                if (builder == null)
                {
                    builderPath = GetSstPath(IdGenerator.GetNextId());
                    builder = _ssTableBuilderFactory.CreateSsTableBuilder(
                        builderPath,
                        tableEncoder,
                        blockEncoder,
                        _bloomFilterFactory,
                        estimatedCount,
                        _compression,
                        _compressionLevel,
                        _minimumCompressionSavingsPercent);
                }

                await builder.AddAsync(entry.Key, entry.Value, cancellationToken);

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
    private List<ByteSlice> ComputeSubcompactionBoundaries(List<SsTable> inputs)
    {
        var candidates = new List<ByteSlice>();

        foreach (var input in inputs)
        {
            foreach (var metadata in input.BlockMetadata)
            {
                candidates.Add(metadata.FirstKey);
            }
        }

        candidates.Sort(_keyComparer);

        var distinct = new List<ByteSlice>(candidates.Count);

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
    private static bool IsSortedNonOverlapping(List<SsTable> tables)
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
    private static bool HasDataBelow(List<List<SsTable>> levels, int targetLevelIndex)
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
    private int SelectTieredCompaction(List<SsTable> tiers)
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
    private MemTable CreateCurrentMemTable(long id)
    {
        if (!_useWriteAheadLog)
        {
            return new MemTable(id, arenaBlockSize: _memTableArenaBlockSize);
        }

        var wal = new WriteAheadLog(GetWalPath(id), _syncWriteAheadLogToDisk);
        return new MemTable(id, wal, _memTableArenaBlockSize);
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

    public IStorageIterator CreateIterator()
    {
        return new LsmStorageIterator(this);
    }

    public async Task FlushAndCompactAsync(CancellationToken cancellationToken = default)
    {
        ForceFreezeMemTable();

        while (!_state.ImmutableMemTables.IsEmpty)
        {
            await ForceFlushNextImmutableMemTableAsync(cancellationToken);
        }

        while (true)
        {
            var compacted = await TryTieredCompactionAsync(cancellationToken);
            compacted |= await TryLeveledCompactionAsync(cancellationToken);

            if (!compacted)
            {
                break;
            }
        }
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

            _state = new StorageState
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
        _state.CurrentMemTable?.Dispose();

        if (_state.ImmutableMemTables is not null)
        {
            foreach (var memTable in _state.ImmutableMemTables)
            {
                memTable.Dispose();
            }
        }

        if (_state.LevelZeroTables is not null)
        {
            foreach (var table in _state.LevelZeroTables)
            {
                table.Dispose();
            }
        }

        if (_state.LeveledSsTables is not null)
        {
            foreach (var table in _state.LeveledSsTables.SelectMany(x => x))
            {
                table.Dispose();
            }
        }

        _blockCache?.Dispose();
    }

    ~LsmStorageInner()
    {
        DisposeInternal();
    }

    private sealed class LsmStorageIterator : IStorageIterator
    {
        private readonly LsmStorageInner _storage;

        public LsmStorageIterator(LsmStorageInner storage)
        {
            _storage = storage;
        }

        public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(CancellationToken cancellationToken = default)
        {
            return EnumerateAsync(hasFrom: false, default!, backwards: false, cancellationToken);
        }

        /// <summary>
        /// Returns all the values whose key is greater than or equal to <paramref name="from"/>, merging the
        /// current MemTable, the immutable MemTables and the on-disk L0 SSTables.
        /// </summary>
        public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(ByteSlice from, CancellationToken cancellationToken = default)
        {
            return EnumerateAsync(hasFrom: true, from, backwards: false, cancellationToken);
        }

        public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(CancellationToken cancellationToken = default)
        {
            return EnumerateAsync(hasFrom: false, default!, backwards: true, cancellationToken);
        }

        /// <summary>
        /// Returns all values whose key is less than or equal to <paramref name="from"/> in descending key order.
        /// </summary>
        public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(ByteSlice from, CancellationToken cancellationToken = default)
        {
            return EnumerateAsync(hasFrom: true, from, backwards: true, cancellationToken);
        }

        private async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(bool hasFrom, ByteSlice from, bool backwards, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Hold the level0 read lock for the whole scan: it freezes the L0 tier list (no flush append or
            // compaction swap) and, because flush only disposes a flushed immutable MemTable after its own
            // level0 write-lock section, it also keeps the immutable MemTables we iterate alive throughout.
            await _storage._level0Lock.EnterReadLockAsync(cancellationToken);

            try
            {
                // Fast path: when the memtables are empty and the on-disk SSTables form a single globally
                // sorted, non-overlapping run, the data is already in final key order with exactly one version
                // per key. We can stream directly from the relevant tables instead of building one iterator per
                // table and priming them all through the merge heap. For a seek that reads a single entry this
                // turns ~N block reads (one per table) into one binary search + one block read.
                if (_storage.TryGetGloballySortedSsTableRun(out var sortedRun))
                {
                    await foreach (var entry in EnumerateSortedRunAsync(sortedRun, hasFrom, from, backwards, cancellationToken))
                    {
                        yield return entry;
                    }

                    yield break;
                }

                var iterators = BuildIterators(hasFrom, from, cancellationToken);
                var merge = new MergeIterator(iterators);

                var enumerable = backwards
                    ? hasFrom
                        ? merge.EnumerateBackwardsAsync(from, cancellationToken)
                        : merge.EnumerateBackwardsAsync(cancellationToken)
                    : hasFrom
                        ? merge.EnumerateAsync(from, cancellationToken)
                        : merge.EnumerateAsync(cancellationToken);

                await foreach (var entry in enumerable)
                {
                    if (!entry.Value.IsTombstone)
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
        /// Streams a single globally sorted, non-overlapping SSTable run in key order. The run already holds
        /// exactly one version per key, so no merge or duplicate resolution is needed — only tombstone
        /// filtering. When <paramref name="hasFrom"/> is set, a binary search skips straight to the first table
        /// whose range can contain <paramref name="from"/>.
        /// </summary>
        private async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateSortedRunAsync(
            IReadOnlyList<SsTable> tables,
            bool hasFrom,
            ByteSlice from,
            bool backwards,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            if (backwards)
            {
                var endIndex = tables.Count - 1;

                if (hasFrom)
                {
                    var lo = 0;
                    var hi = tables.Count;

                    while (lo < hi)
                    {
                        var mid = (lo + hi) >> 1;

                        if (_keyComparer.Compare(tables[mid].FirstKey, from) <= 0)
                        {
                            lo = mid + 1;
                        }
                        else
                        {
                            hi = mid;
                        }
                    }

                    endIndex = lo - 1;
                }

                for (var i = endIndex; i >= 0; i--)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var iterator = new SsTableIterator(tables[i]);
                    var enumerable = hasFrom && i == endIndex
                        ? iterator.EnumerateBackwardsAsync(from, cancellationToken)
                        : iterator.EnumerateBackwardsAsync(cancellationToken);

                    await foreach (var entry in enumerable)
                    {
                        if (!entry.Value.IsTombstone)
                        {
                            yield return entry;
                        }
                    }
                }

                yield break;
            }

            var startIndex = 0;

            if (hasFrom)
            {
                // First table whose LastKey >= from. Tables are FirstKey-sorted and non-overlapping, so LastKey
                // is monotonically increasing and binary search is valid. If from falls past every table's
                // LastKey, lo lands at tables.Count and nothing is yielded.
                var lo = 0;
                var hi = tables.Count;

                while (lo < hi)
                {
                    var mid = (lo + hi) >> 1;

                    if (_keyComparer.Compare(tables[mid].LastKey, from) < 0)
                    {
                        lo = mid + 1;
                    }
                    else
                    {
                        hi = mid;
                    }
                }

                startIndex = lo;
            }

            for (var i = startIndex; i < tables.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var iterator = new SsTableIterator(tables[i]);

                // Only the first table needs the seek; every later table starts at its FirstKey, which is
                // already >= from because the run is non-overlapping.
                var enumerable = hasFrom && i == startIndex
                    ? iterator.EnumerateAsync(from, cancellationToken)
                    : iterator.EnumerateAsync(cancellationToken);

                await foreach (var entry in enumerable)
                {
                    if (!entry.Value.IsTombstone)
                    {
                        yield return entry;
                    }
                }
            }
        }

        /// <summary>
        /// Builds the merge inputs in most-recent-first order (current MemTable, then immutable MemTables
        /// newest-first, then L0 SSTables newest-first) so the <see cref="MergeIterator{ByteSlice, ByteSlice}"/>
        /// keeps the latest value on duplicate keys. The current MemTable is materialized under its
        /// (thread-affine) lock so the rest of the scan can perform async SST I/O without holding it.
        /// </summary>
        private List<IStorageIterator> BuildIterators(bool hasFrom, ByteSlice from, CancellationToken cancellationToken)
        {
            List<KeyValuePair<ByteSlice, ByteSlice>> currentSnapshot;
            ImmutableQueue<IMemTable> immutableMemTables;
            List<SsTable> levelZeroTables;
            List<List<SsTable>> leveledTables;

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

            var iterators = new List<IStorageIterator>
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
                iterators.Add(new SsTableIterator(levelZeroTables[i]));
            }

            // Then the compaction levels, newest-first (L1 before L2 ...). Within a level the SSTs are
            // non-overlapping, so their relative order does not affect correctness.
            foreach (var level in leveledTables)
            {
                foreach (var table in level)
                {
                    iterators.Add(new SsTableIterator(table));
                }
            }

            return iterators;
        }

        private static List<KeyValuePair<ByteSlice, ByteSlice>> MaterializeCurrentMemTable(IMemTable memTable, CancellationToken cancellationToken)
        {
            var snapshot = new List<KeyValuePair<ByteSlice, ByteSlice>>();

            // The MemTable iterator is synchronous, so this blocking drain stays on the calling thread.
            foreach (var entry in memTable.CreateIterator().EnumerateAsync(cancellationToken).ToBlockingEnumerable(cancellationToken))
            {
                snapshot.Add(entry);
            }

            return snapshot;
        }
    }

    /// <summary>
    /// An <see cref="IStorageIterator{ByteSlice, ByteSlice}"/> over an already-materialized, key-ascending list.
    /// Used to snapshot the current MemTable so the rest of a scan can run async I/O off its lock.
    /// </summary>
    private sealed class ListStorageIterator : IStorageIterator
    {
        private readonly List<KeyValuePair<ByteSlice, ByteSlice>> _entries;

        public ListStorageIterator(List<KeyValuePair<ByteSlice, ByteSlice>> entries)
        {
            _entries = entries;
        }

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;

            foreach (var entry in _entries)
            {
                yield return entry;
            }
        }

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(ByteSlice from, [EnumeratorCancellation] CancellationToken cancellationToken = default)
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

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;

            for (var i = _entries.Count - 1; i >= 0; i--)
            {
                yield return _entries[i];
            }
        }

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(ByteSlice from, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            await Task.CompletedTask;

            var lo = 0;
            var hi = _entries.Count;

            while (lo < hi)
            {
                var mid = (lo + hi) >> 1;

                if (_keyComparer.Compare(_entries[mid].Key, from) <= 0)
                {
                    lo = mid + 1;
                }
                else
                {
                    hi = mid;
                }
            }

            for (var i = lo - 1; i >= 0; i--)
            {
                yield return _entries[i];
            }
        }
    }
}
