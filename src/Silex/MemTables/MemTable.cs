using Silex.Tables;
using Silex.Wal;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.X86;

namespace Silex.MemTables;

/// <summary>
/// Stores keys and values in arena slabs and keeps compact record descriptors in contiguous arrays.
/// Point lookups use open addressing; ordered reads use a lazily built B+-tree of record positions
/// that inserts keep current once it exists.
/// </summary>
/// <remarks>
/// Writes are synchronized by <see cref="LsmStorageInner"/>. Frozen tables support concurrent readers.
/// A MemTable only owns writes and tombstones; it is not a read cache.
/// <para>
/// The active table also supports optimistic readers that run alongside the single writer without a lock
/// (<see cref="TryGetSpanOptimistic"/>, <see cref="TryGetNextVersionOptimistic"/>, <see cref="TryScanRawOptimistic"/>).
/// They read without blocking, then validate versions and retry: <c>_layoutVersion</c> is odd while the writer
/// replaces an array or drops an index, <c>_batchVersion</c> is odd while an atomic batch of several entries
/// applies (so no reader sees part of it), each record's flags carry a sequence that is odd while its value is
/// rewritten, and each ordered index node has its own version. Inserts publish a fully written record with a
/// release store, and arena bytes never change while the table is alive, so a validated slice reads stable bytes.
/// Optimistic reads bound every index and loop, because a reader can see torn state before it validates.
/// The caller keeps the table from being disposed while it reads.
/// </para>
/// </remarks>
internal sealed partial class MemTable : IMemTable, IRawBytesMemTable
{
    private const int InitialRecordCapacity = 256;
    private const int InitialBucketCapacity = 512;
    private const int MinInsertsBeforeOrderedIndexDrop = 1024;
    /// <summary>The slots ahead of a leaf scan whose record is prefetched; 8 measured best of 4, 8 and 16 on x64.</summary>
    private const int RecordPrefetchDistance = 8;
    /// <summary>Added to a record's flags around a value rewrite; the flag bits below it are left intact.</summary>
    private const uint RecordWriteStep = 4;
    private const int MaxOptimisticAttempts = 16;

    private readonly long _id;
    private readonly WriteAheadLog? _wal;
    private readonly MemTableArena _arena;
    private readonly RecordIndexComparer _recordComparer;

    private MemTableRecord[] _records;
    private int[] _buckets;
    private int _bucketCapacity;
    private int[]? _prefixBuckets;
    private int _prefixBucketCapacity;
    private OrderedIndex? _orderedIndex;
    private int _count;
    private long _size;
    private volatile bool _sortDirty;
    private int _insertsSinceOrderedRead;
    private int _layoutVersion;
    private int _batchVersion;
    private bool _disposed;
    private readonly Action<StorageFaultPoint>? _faultInjector;
    internal StorageCommitOutcome WriteOutcome { get; private set; }

    public MemTable(long id, WriteAheadLog? wal = null, int arenaBlockSize = 32 * 1024, Action<StorageFaultPoint>? faultInjector = null)
    {
        _id = id;
        _wal = wal;
        _faultInjector = faultInjector;
        _arena = new MemTableArena(arenaBlockSize);
        _records = ArrayPool<MemTableRecord>.Shared.Rent(InitialRecordCapacity);
        _buckets = ArrayPool<int>.Shared.Rent(InitialBucketCapacity);
        _bucketCapacity = InitialBucketCapacity;
        _buckets.AsSpan(0, _bucketCapacity).Clear();
        _recordComparer = new RecordIndexComparer(this);
    }

    public long Id => _id;

    internal WriteAheadLog? Wal => _wal;

    public long Size => _size;

    public int Count => _count;

    /// <summary>The ordered index when it is current, or null when the next ordered read rebuilds it. For invariant tests.</summary>
    internal OrderedIndex? CurrentOrderedIndex => _sortDirty ? null : _orderedIndex;

    /// <summary>The version-prefix hash buckets (record position + 1 of each chain head), or empty when not built. For invariant tests.</summary>
    internal ReadOnlySpan<int> VersionPrefixBuckets => _prefixBuckets is { } buckets ? buckets.AsSpan(0, _prefixBucketCapacity) : default;

    internal ReadOnlySpan<byte> GetRecordKey(int record) => _arena.GetSpan(_records[record].Key);

    internal int GetNextVersionRecord(int record) => _records[record].NextVersionRecord;

    internal void EnsureSortedIndex()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_sortDirty && _orderedIndex is not null)
        {
            // Reset only when an insert happened, so concurrent readers of an unchanged table never write the field.
            if (_insertsSinceOrderedRead != 0)
            {
                _insertsSinceOrderedRead = 0;
            }
            return;
        }

        lock (_recordComparer)
        {
            if (!_sortDirty && _orderedIndex is not null)
            {
                return;
            }

            var sorted = ArrayPool<int>.Shared.Rent(Math.Max(1, _count));
            try
            {
                var isAlreadySorted = true;
                for (var i = 0; i < _count; i++)
                {
                    sorted[i] = i;
                    if (isAlreadySorted && i > 0 && _recordComparer.Compare(i - 1, i) > 0)
                    {
                        isAlreadySorted = false;
                    }
                }
                if (!isAlreadySorted)
                {
                    Array.Sort(sorted, 0, _count, _recordComparer);
                }
                var rebuilt = new OrderedIndex(this, sorted.AsSpan(0, _count));
                BeginLayoutChange();
                Volatile.Write(ref _orderedIndex, rebuilt);
                _insertsSinceOrderedRead = 0;
                _sortDirty = false;
                EndLayoutChange();
            }
            finally
            {
                ArrayPool<int>.Shared.Return(sorted);
            }
        }
    }

    public bool TryGet(ByteSlice key, [MaybeNullWhen(false)] out ByteSlice result)
    {
        if (!TryFindRecord(key.Span, out var record))
        {
            result = default;
            return false;
        }

        result = record.IsTombstone ? ByteSlice.Tombstone : _arena.GetByteSlice(record.Value);
        return true;
    }

    public bool TryGetRaw(ReadOnlySpan<byte> key, out ReadOnlyMemory<byte> value, out bool isTombstone)
    {
        if (!TryFindRecord(key, out var record))
        {
            value = default;
            isTombstone = false;
            return false;
        }

        isTombstone = record.IsTombstone;
        value = isTombstone ? default : _arena.GetMemory(record.Value);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool TryGetSpan(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value, out bool isTombstone)
    {
        FindBucket(key, Hash(key), ReadKeyPrefixWord(key), out var recordIndex);
        if (recordIndex < 0)
        {
            value = default;
            isTombstone = false;
            return false;
        }

        ref var record = ref _records[recordIndex];
        isTombstone = record.IsTombstone;
        value = isTombstone ? default : _arena.GetSpan(record.Value);
        return true;
    }

    /// <summary>
    /// Point lookup that runs alongside the writer without a lock. Returns false when it could not get a
    /// consistent read after a few attempts; the caller then takes the lock and uses <see cref="TryGetSpan"/>.
    /// </summary>
    internal bool TryGetSpanOptimistic(ReadOnlySpan<byte> key, out bool found, out ReadOnlySpan<byte> value, out bool isTombstone)
    {
        var hash = Hash(key);
        var keyPrefixWord = ReadKeyPrefixWord(key);
        var spinner = new SpinWait();
        for (var attempt = 0; attempt < MaxOptimisticAttempts; attempt++)
        {
            if (TryReadVersions(out var layout, out var batch) &&
                TryFindOptimistic(key, hash, keyPrefixWord, out found, out value, out isTombstone) &&
                ValidateVersions(layout, batch))
            {
                return true;
            }
            spinner.SpinOnce(sleep1Threshold: -1);
        }
        found = isTombstone = false;
        value = default;
        return false;
    }

    private bool TryFindOptimistic(ReadOnlySpan<byte> key, uint hash, ulong keyPrefixWord,
        out bool found, out ReadOnlySpan<byte> value, out bool isTombstone)
    {
        found = isTombstone = false;
        value = default;
        var buckets = Volatile.Read(ref _buckets);
        var mask = _bucketCapacity - 1;
        var records = Volatile.Read(ref _records);
        if ((uint)mask >= (uint)buckets.Length)
        {
            return false;
        }
        var bucket = (int)hash & mask;
        for (var probes = 0; probes <= mask; probes++)
        {
            var recordIndex = Volatile.Read(ref buckets[bucket]) - 1;
            if (recordIndex < 0)
            {
                return true;
            }
            if ((uint)recordIndex >= (uint)records.Length)
            {
                return false;
            }
            ref var record = ref records[recordIndex];
            if (record.Hash == hash && record.KeyPrefixWord == keyPrefixWord && record.Key.Length == key.Length)
            {
                if (key.Length > 8)
                {
                    if (!_arena.TryGetSpanOptimistic(record.Key, out var stored))
                    {
                        return false;
                    }
                    if (!key[8..].SequenceEqual(stored[8..]))
                    {
                        bucket = (bucket + 1) & mask;
                        continue;
                    }
                }
                if (!TryReadRecordValue(ref record, out var slice, out isTombstone) ||
                    (!isTombstone && !_arena.TryGetSpanOptimistic(slice, out value)))
                {
                    return false;
                }
                found = true;
                return true;
            }
            bucket = (bucket + 1) & mask;
        }
        return false;
    }

    /// <summary>
    /// Lock-free counterpart of <see cref="TryGetNextVersionRaw"/> over the version-prefix index. Returns false,
    /// without touching the outputs' meaning, when the index is not built or no consistent read succeeded.
    /// </summary>
    internal bool TryGetNextVersionOptimistic(ReadOnlySpan<byte> from, ReadOnlySpan<byte> prefix, bool inclusive,
        out bool found, out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value, out bool tombstone)
    {
        var prefixHash = Hash(prefix);
        var keyPrefixWord = ReadKeyPrefixWord(prefix);
        var spinner = new SpinWait();
        for (var attempt = 0; attempt < MaxOptimisticAttempts; attempt++)
        {
            if (TryReadVersions(out var layout, out var batch))
            {
                if (Volatile.Read(ref _prefixBuckets) is null)
                {
                    break;
                }
                if (TryFindNextVersionOptimistic(from, prefix, prefixHash, keyPrefixWord, inclusive,
                        out found, out key, out value, out tombstone) &&
                    ValidateVersions(layout, batch))
                {
                    return true;
                }
            }
            spinner.SpinOnce(sleep1Threshold: -1);
        }
        found = tombstone = false;
        key = value = default;
        return false;
    }

    private bool TryFindNextVersionOptimistic(ReadOnlySpan<byte> from, ReadOnlySpan<byte> prefix, uint prefixHash,
        ulong keyPrefixWord, bool inclusive, out bool found, out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value,
        out bool tombstone)
    {
        found = tombstone = false;
        key = value = default;
        var buckets = Volatile.Read(ref _prefixBuckets);
        var mask = _prefixBucketCapacity - 1;
        var records = Volatile.Read(ref _records);
        if (buckets is null || (uint)mask >= (uint)buckets.Length)
        {
            return false;
        }
        var expectedKeyLength = prefix.Length + sizeof(long);
        var bucket = (int)prefixHash & mask;
        var curr = -1;
        for (var probes = 0; ; probes++)
        {
            if (probes > mask)
            {
                return false;
            }
            var headIndex = Volatile.Read(ref buckets[bucket]) - 1;
            if (headIndex < 0)
            {
                return true;
            }
            if ((uint)headIndex >= (uint)records.Length)
            {
                return false;
            }
            ref var head = ref records[headIndex];
            if (head.PrefixHash == prefixHash && head.Key.Length == expectedKeyLength)
            {
                if (!_arena.TryGetSpanOptimistic(head.Key, out var headKey))
                {
                    return false;
                }
                if (prefix.Length >= 8
                        ? head.KeyPrefixWord == keyPrefixWord && headKey.Slice(8, prefix.Length - 8).SequenceEqual(prefix[8..])
                        : headKey[..prefix.Length].SequenceEqual(prefix))
                {
                    curr = headIndex;
                    break;
                }
            }
            bucket = (bucket + 1) & mask;
        }

        var compareTrailers = from.Length == expectedKeyLength;
        var fromTrailer = compareTrailers ? System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(from[^sizeof(long)..]) : 0;
        for (var steps = 0; curr >= 0; steps++)
        {
            if (steps >= records.Length || (uint)curr >= (uint)records.Length)
            {
                return false;
            }
            ref var rec = ref records[curr];
            if (!_arena.TryGetSpanOptimistic(rec.Key, out var candidateKey) || candidateKey.Length != expectedKeyLength)
            {
                return false;
            }
            bool match;
            if (compareTrailers)
            {
                var candidateTrailer = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(candidateKey[^sizeof(long)..]);
                match = candidateTrailer > fromTrailer || (inclusive && candidateTrailer == fromTrailer);
            }
            else
            {
                var cmp = candidateKey.SequenceCompareTo(from);
                match = cmp > 0 || (inclusive && cmp == 0);
            }
            if (match)
            {
                if (!TryReadRecordValue(ref rec, out var slice, out tombstone) ||
                    (!tombstone && !_arena.TryGetSpanOptimistic(slice, out value)))
                {
                    return false;
                }
                key = candidateKey;
                found = true;
                return true;
            }
            curr = Volatile.Read(ref rec.NextVersionRecord);
        }
        return true;
    }

    /// <summary>
    /// Lock-free counterpart of <see cref="ScanRaw"/> with one entry: finds the first live entry at or after
    /// <paramref name="from"/>. Returns false when no consistent read succeeded; the caller then takes the lock.
    /// A multi-entry scan keeps using the lock so it still reads one snapshot of the table.
    /// </summary>
    internal bool TrySeekFirstRawOptimistic(ReadOnlySpan<byte> from, out bool found, out ReadOnlySpan<byte> key,
        out ReadOnlySpan<byte> value)
    {
        var visitor = new FirstLiveVisitor();
        return TrySeekFirstOptimistic(from, ref visitor, out found, out key, out value);
    }

    /// <summary>
    /// Lock-free counterpart of <see cref="ScanMvccRaw"/> with one entry: finds the first user key at or after
    /// <paramref name="from"/> whose newest version visible at <paramref name="snapshotSequence"/> is live, and
    /// returns its encoded user key and value without the kind marker. <paramref name="isCleanKey"/> is false
    /// when the user key still needs <see cref="MvccStorage.InvokeUserReader"/> to decode it.
    /// </summary>
    internal bool TrySeekFirstMvccOptimistic(long snapshotSequence, ReadOnlySpan<byte> from, out bool found,
        out ReadOnlySpan<byte> encodedUserKey, out ReadOnlySpan<byte> value, out bool isCleanKey)
    {
        var visitor = new FirstVisibleMvccVisitor(snapshotSequence);
        var result = TrySeekFirstOptimistic(from, ref visitor, out found, out var key, out value);
        isCleanKey = visitor.IsCleanKey;
        encodedUserKey = found ? key.Slice(1, key.Length - 11) : default;
        if (found)
        {
            value = value[1..];
        }
        return result;
    }

    /// <summary>
    /// Walks the ordered index from <paramref name="from"/> without a lock until <paramref name="visitor"/>
    /// accepts a record. Every leaf the walk crosses is validated again at the end, so the result is a consistent
    /// read even though the writer keeps running. Returns false when the ordered index is not current, the walk
    /// crosses too many leaves, the visitor reports torn state too often, or no consistent read succeeded.
    /// </summary>
    private bool TrySeekFirstOptimistic<TVisitor>(ReadOnlySpan<byte> from, scoped ref TVisitor visitor, out bool found,
        out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
        where TVisitor : struct, IOptimisticSeekVisitor
    {
        const int MaxLeaves = 4;
        var visited = new VisitedLeaves();
        var spinner = new SpinWait();
        for (var attempt = 0; attempt < MaxOptimisticAttempts; attempt++, spinner.SpinOnce(sleep1Threshold: -1))
        {
            if (!TryReadVersions(out var layout, out var batch))
            {
                continue;
            }
            if (_sortDirty || Volatile.Read(ref _orderedIndex) is not { } index)
            {
                break;
            }
            if (_insertsSinceOrderedRead != 0)
            {
                _insertsSinceOrderedRead = 0;
            }
            var records = Volatile.Read(ref _records);
            if (!index.TrySeekOptimistic(from, records, out var node, out var slot, out var version))
            {
                continue;
            }

            visitor.Reset();
            var leaves = 0;
            var result = SeekStep.Retry;
            var acceptedKey = default(MemTableArena.ArenaSlice);
            var acceptedValue = default(MemTableArena.ArenaSlice);
            var accepted = false;
            while (true)
            {
                visited[leaves++] = (node, version);
                var keys = node.Keys;
                var nodeCount = node.Count;
                if ((uint)nodeCount > (uint)keys.Length)
                {
                    break;
                }
                var step = SeekStep.Continue;
                for (; slot < nodeCount; slot++)
                {
                    var recordIndex = keys[slot];
                    if ((uint)recordIndex >= (uint)records.Length)
                    {
                        step = SeekStep.Retry;
                        break;
                    }
                    ref var record = ref records[recordIndex];
                    if (!TryReadRecordValue(ref record, out var valueSlice, out var isTombstone))
                    {
                        step = SeekStep.Retry;
                        break;
                    }
                    step = visitor.Visit(_arena, ref record, valueSlice, isTombstone);
                    if (step == SeekStep.Done)
                    {
                        acceptedKey = record.Key;
                        acceptedValue = valueSlice;
                        accepted = true;
                        break;
                    }
                    if (step != SeekStep.Continue)
                    {
                        break;
                    }
                }
                if (step != SeekStep.Continue)
                {
                    result = step;
                    break;
                }

                var next = Volatile.Read(ref node.Next);
                if (next is null)
                {
                    result = SeekStep.Done;
                    break;
                }
                if (leaves == MaxLeaves)
                {
                    result = SeekStep.GiveUp;
                    break;
                }
                var nextVersion = Volatile.Read(ref next.Version);
                if ((nextVersion & 1) != 0)
                {
                    break;
                }
                node = next;
                version = nextVersion;
                slot = 0;
            }

            if (result == SeekStep.GiveUp)
            {
                break;
            }
            if (result == SeekStep.Retry)
            {
                continue;
            }
            var valid = ValidateVersions(layout, batch);
            for (var i = 0; valid && i < leaves; i++)
            {
                valid = Volatile.Read(ref visited[i].Node.Version) == visited[i].Version;
            }
            if (!valid)
            {
                continue;
            }
            found = accepted;
            key = value = default;
            // The validated slices point at bytes that never change while the table is alive.
            if (!accepted ||
                (_arena.TryGetSpanOptimistic(acceptedKey, out key) && _arena.TryGetSpanOptimistic(acceptedValue, out value)))
            {
                return true;
            }
        }
        found = false;
        key = value = default;
        return false;
    }

    private enum SeekStep : byte
    {
        Continue,
        Retry,
        Done,
        GiveUp,
    }

    /// <summary>Decides, record by record, where an optimistic seek stops; it may see torn state.</summary>
    private interface IOptimisticSeekVisitor
    {
        void Reset();

        /// <returns>
        /// <see cref="SeekStep.Done"/> to accept the record, <see cref="SeekStep.Continue"/> to skip it, or
        /// <see cref="SeekStep.Retry"/> when it cannot be interpreted, which may only mean the read was torn.
        /// </returns>
        SeekStep Visit(MemTableArena arena, ref MemTableRecord record, MemTableArena.ArenaSlice value, bool isTombstone);
    }

    private struct FirstLiveVisitor : IOptimisticSeekVisitor
    {
        public void Reset()
        {
        }

        public readonly SeekStep Visit(MemTableArena arena, ref MemTableRecord record, MemTableArena.ArenaSlice value, bool isTombstone) =>
            isTombstone ? SeekStep.Continue : SeekStep.Done;
    }

    /// <summary>The version-group logic of <see cref="ScanMvccRaw"/>, for one entry.</summary>
    private struct FirstVisibleMvccVisitor(long snapshotSequence) : IOptimisticSeekVisitor
    {
        private MemTableArena.ArenaSlice _lastKeySlice;
        private ulong _lastWord;
        private uint _lastPrefixHash;
        private bool _hasGroup;
        private bool _resolved;
        public bool IsCleanKey;

        public void Reset()
        {
            _lastKeySlice = default;
            _lastWord = 0;
            _lastPrefixHash = 0;
            _hasGroup = _resolved = IsCleanKey = false;
        }

        public SeekStep Visit(MemTableArena arena, ref MemTableRecord record, MemTableArena.ArenaSlice valueSlice, bool isTombstone)
        {
            if (isTombstone)
            {
                return SeekStep.Continue;
            }
            if (!arena.TryGetSpanOptimistic(record.Key, out var key))
            {
                return SeekStep.Retry;
            }
            if (key.Length < 11 || key[0] != 1)
            {
                return SeekStep.Continue;
            }
            var terminator = key.Length - 10;
            var decodedSequence = ~System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(key[^sizeof(long)..]);
            if (key[terminator] != 0 || key[terminator + 1] != 0 || decodedSequence > long.MaxValue)
            {
                // Invalid data: let the locked scan report it.
                return SeekStep.Retry;
            }
            var encodedUserKey = key.Slice(1, terminator - 1);
            var sameGroup = false;
            if (_hasGroup && record.Key.Length == _lastKeySlice.Length && record.KeyPrefixWord == _lastWord &&
                (record.PrefixHash == 0 || record.PrefixHash == _lastPrefixHash))
            {
                if (encodedUserKey.Length <= 7)
                {
                    sameGroup = true;
                }
                else if (!arena.TryGetSpanOptimistic(_lastKeySlice, out var lastKey))
                {
                    return SeekStep.Retry;
                }
                else
                {
                    sameGroup = encodedUserKey.SequenceEqual(lastKey.Slice(1, terminator - 1));
                }
            }
            if (sameGroup)
            {
                if (_resolved)
                {
                    return SeekStep.Continue;
                }
            }
            else
            {
                _hasGroup = true;
                _resolved = false;
                _lastKeySlice = record.Key;
                _lastWord = record.KeyPrefixWord;
                _lastPrefixHash = record.PrefixHash;
            }
            if ((long)decodedSequence > snapshotSequence)
            {
                return SeekStep.Continue;
            }
            if (!arena.TryGetSpanOptimistic(valueSlice, out var value))
            {
                return SeekStep.Retry;
            }
            if (value.IsEmpty || value[0] > 1)
            {
                return SeekStep.Retry;
            }
            _resolved = true;
            if (value[0] == 0)
            {
                return SeekStep.Continue;
            }
            IsCleanKey = record.IsCleanMvccKey;
            return SeekStep.Done;
        }
    }

    [InlineArray(4)]
    private struct VisitedLeaves
    {
        private (OrderedIndex.Leaf Node, int Version) _element;
    }

    public void Put(ByteSlice key, ByteSlice value)
    {
        if (value.IsTombstone)
        {
            DeleteRaw(key.Span);
        }
        else
        {
            PutRaw(key.Span, value.Span);
        }
    }

    public void PutRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value) =>
        PutRawCore(key, value, isTombstone: false);

    public void DeleteRaw(ReadOnlySpan<byte> key) =>
        PutRawCore(key, default, isTombstone: true);

    /// <param name="atomic">
    /// False when the batch only coalesces independent writes, so optimistic readers may see its entries one by one.
    /// </param>
    internal void WriteBatch(LsmWriteBatch batch, bool atomic = true)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (batch.Count == 0)
        {
            return;
        }

        JournalBatch(batch);
        ApplyBatchSlice(batch, 0, batch.Count, atomic);
    }

    /// <summary>Writes <paramref name="batch"/> to the WAL as one frame; the caller then applies its entries.</summary>
    internal void JournalBatch(LsmWriteBatch batch)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        WriteOutcome = StorageCommitOutcome.NotCommitted;
        try
        {
            _wal?.AppendBatch(batch);
        }
        catch
        {
            WriteOutcome = _wal!.AppendOutcome;
            throw;
        }
    }

    internal void ApplyBatch(LsmWriteBatch batch) => ApplyBatchSlice(batch, 0, batch.Count);

    /// <summary>
    /// Applies entries of a batch. An atomic batch of several entries keeps <c>_batchVersion</c> odd while it
    /// applies, so optimistic readers wait for it rather than see part of it; a batch that fails midway leaves
    /// it odd, sending readers to the lock, where they find the store faulted.
    /// </summary>
    internal void ApplyBatchSlice(LsmWriteBatch batch, int startInclusive, int endExclusive, bool atomic = true)
    {
        WriteOutcome = StorageCommitOutcome.Indeterminate;
        _faultInjector?.Invoke(StorageFaultPoint.BatchBeforeApply);
        var bracket = atomic && endExclusive - startInclusive > 1;
        if (bracket)
        {
            Interlocked.Increment(ref _batchVersion);
        }
        for (var i = startInclusive; i < endExclusive; i++)
        {
            batch.GetEntry(i, out var key, out var value, out var isTombstone);
            ApplyRaw(key, value, isTombstone);
            _faultInjector?.Invoke(StorageFaultPoint.BatchAfterApplyEntry);
        }
        if (bracket)
        {
            Volatile.Write(ref _batchVersion, _batchVersion + 1);
        }
        WriteOutcome = StorageCommitOutcome.Committed;
    }

    private void PutRawCore(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        WriteOutcome = StorageCommitOutcome.NotCommitted;
        try
        {
            if (isTombstone)
            {
                _wal?.AppendDeleteRaw(key);
            }
            else
            {
                _wal?.AppendRaw(key, value);
            }
        }
        catch
        {
            WriteOutcome = _wal!.AppendOutcome;
            throw;
        }

        WriteOutcome = StorageCommitOutcome.Indeterminate;
        _faultInjector?.Invoke(StorageFaultPoint.BatchBeforeApply);
        ApplyRaw(key, value, isTombstone);
        _faultInjector?.Invoke(StorageFaultPoint.BatchAfterApplyEntry);
        WriteOutcome = StorageCommitOutcome.Committed;
    }

    private void ApplyRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone)
    {
        var hash = Hash(key);
        var keyPrefixWord = ReadKeyPrefixWord(key);
        var bucket = FindBucket(key, hash, keyPrefixWord, out var recordIndex);
        var storedValue = isTombstone ? MemTableArena.ArenaSlice.Empty : _arena.Copy(value);

        if (recordIndex >= 0)
        {
            ref var record = ref _records[recordIndex];
            var flags = record.Flags;
            Interlocked.Exchange(ref record.Flags, flags + RecordWriteStep);
            record.Value = storedValue;
            Volatile.Write(ref record.Flags, ((flags + (2 * RecordWriteStep)) & ~1u) | (isTombstone ? 1u : 0u));
        }
        else
        {
            if (EnsureInsertCapacity())
            {
                bucket = FindEmptyBucket(hash);
            }
            EnsureRecordCapacity();

            var newIndex = _count;
            var isCleanMvcc = key.Length >= 11 && key[0] == 1 && key[^10] == 0 && key[^9] == 0 &&
                key.Slice(1, key.Length - 11).IndexOf((byte)0) < 0;
            _records[newIndex] = new MemTableRecord
            {
                Key = _arena.Copy(key),
                Value = storedValue,
                Hash = hash,
                Flags = (isTombstone ? 1u : 0u) | (isCleanMvcc ? 2u : 0u),
                NextVersionRecord = -1,
                KeyPrefixWord = keyPrefixWord,
            };
            Volatile.Write(ref _buckets[bucket], newIndex + 1);
            _singleVersionCleanMvccState = 0;
            _count++;

            // Counted first, so an index that fails to take the record rebuilds with it instead of losing it.
            IndexNewRecord(newIndex, key);
        }

        _size += key.Length + value.Length + sizeof(int);
    }

    /// <summary>
    /// Adds a new record to the ordered and version-prefix indexes once a read has built them, so a read
    /// after a write does not rebuild either index from every record. Before that, inserts only mark the
    /// ordered index dirty and a write-only table pays for the hash index alone. Writers are exclusive, so
    /// readers never observe a partial update.
    /// </summary>
    /// <remarks>
    /// An insert into the ordered index costs several times its share of a rebuild, so once inserts run far
    /// ahead of ordered reads the index is left dirty and the next ordered read rebuilds it. This bounds what
    /// a bulk write after a single scan pays, while interleaved reads and writes keep the index current.
    /// </remarks>
    private void IndexNewRecord(int newIndex, ReadOnlySpan<byte> key)
    {
        if (_orderedIndex is null || _sortDirty ||
            ++_insertsSinceOrderedRead > Math.Max(MinInsertsBeforeOrderedIndexDrop, _count / 8))
        {
            MarkSortDirty();
        }
        else
        {
            try
            {
                _orderedIndex.Insert(newIndex);
            }
            catch
            {
                // The version-prefix insert below is skipped, so that index would miss the record too.
                MarkSortDirty();
                DropVersionPrefixIndex();
                throw;
            }
        }

        if (_prefixBuckets is not null &&
            (_records[newIndex].IsCleanMvccKey || VersionPrefixBloom.IsVersionKey(key)))
        {
            try
            {
                InsertVersionPrefixLive(newIndex, key[..^sizeof(long)]);
            }
            catch
            {
                DropVersionPrefixIndex();
                throw;
            }
        }
    }

    private void DropVersionPrefixIndex()
    {
        if (_prefixBuckets is { } prefixBuckets)
        {
            BeginLayoutChange();
            _prefixBuckets = null;
            ArrayPool<int>.Shared.Return(prefixBuckets);
            EndLayoutChange();
        }
    }

    /// <summary>Stops maintaining the ordered index; an optimistic reader of the old one fails validation.</summary>
    private void MarkSortDirty()
    {
        if (!_sortDirty)
        {
            BeginLayoutChange();
            _sortDirty = true;
            EndLayoutChange();
        }
    }

    /// <summary>
    /// Makes <c>_layoutVersion</c> odd before the writer replaces an array, returns one to the pool or swaps an
    /// index. The interlocked increment is a full fence, so no store that follows is visible before it.
    /// </summary>
    private void BeginLayoutChange() => Interlocked.Increment(ref _layoutVersion);

    private void EndLayoutChange() => Volatile.Write(ref _layoutVersion, _layoutVersion + 1);

    /// <summary>Reads the table-wide versions an optimistic read starts from; false while either is odd.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryReadVersions(out int layout, out int batch)
    {
        layout = Volatile.Read(ref _layoutVersion);
        batch = Volatile.Read(ref _batchVersion);
        return ((layout | batch) & 1) == 0;
    }

    /// <summary>Checks that no array was replaced and no atomic batch applied since <see cref="TryReadVersions"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ValidateVersions(int layout, int batch)
    {
        ReadBarrier();
        return Volatile.Read(ref _layoutVersion) == layout && Volatile.Read(ref _batchVersion) == batch;
    }

    /// <summary>Orders the loads before it ahead of the version load after it, for seqlock-style validation.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ReadBarrier()
    {
#if NET10_0_OR_GREATER
        Volatile.ReadBarrier();
#else
        Interlocked.MemoryBarrier();
#endif
    }

    /// <summary>
    /// Reads a consistent value and tombstone flag from a record whose value the writer may be rewriting.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryReadRecordValue(ref MemTableRecord record, out MemTableArena.ArenaSlice value, out bool isTombstone)
    {
        var flags = Volatile.Read(ref record.Flags);
        value = record.Value;
        ReadBarrier();
        isTombstone = (flags & 1) != 0;
        return (flags & RecordWriteStep) == 0 && Volatile.Read(ref record.Flags) == flags;
    }

    /// <summary>
    /// Asks the CPU to load the record that a leaf scan reaches a few slots later. Leaves hold record positions in
    /// key order, so the records a scan reads are scattered and the hardware prefetcher cannot predict them.
    /// </summary>
    /// <remarks>
    /// The address is taken without pinning: a collection that moves the array in between only makes the hint stale.
    /// Only x86 exposes a prefetch instruction; elsewhere the JIT removes the call.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private unsafe void PrefetchRecord(Span<int> keys, int slot, int count)
    {
        if (Sse.IsSupported && (uint)slot < (uint)count)
        {
            Sse.Prefetch0(Unsafe.AsPointer(ref _records[keys[slot]]));
        }
    }

    public IStorageIterator CreateIterator() => new MemTableIterator(this);

    public async Task FlushAsync(ISsTableBuilder builder, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureSortedIndex();

        if (builder is BufferedSsTableBuilder bufferedBuilder)
        {
            var node = _orderedIndex!.Begin(false).CurrentNode;
            while (node is not null)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var nodeCount = node.Count;
                for (var slot = 0; slot < nodeCount; slot++)
                {
                    PrefetchRecord(node.Keys, slot + RecordPrefetchDistance, nodeCount);
                    var recIdx = node.Keys[slot];
                    var isTomb = _records[recIdx].IsTombstone;
                    if (!bufferedBuilder.TryAddRawFast(
                        _arena.GetSpan(_records[recIdx].Key),
                        isTomb ? default : _arena.GetSpan(_records[recIdx].Value),
                        isTomb))
                    {
                        await bufferedBuilder.RolloverBlockForRawAsync(cancellationToken).ConfigureAwait(false);
                        if (!bufferedBuilder.TryAddRawFast(
                            _arena.GetSpan(_records[recIdx].Key),
                            isTomb ? default : _arena.GetSpan(_records[recIdx].Value),
                            isTomb))
                        {
                            throw new InvalidOperationException("The data was not successfully added to a block.");
                        }
                    }
                }
                node = node.Next;
            }
            return;
        }

        for (var cursor = _orderedIndex!.Begin(false); cursor.IsValid; cursor.Move(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var record = _records[cursor.Record];
            var key = _arena.GetByteSlice(record.Key);
            var value = record.IsTombstone ? ByteSlice.Tombstone : _arena.GetByteSlice(record.Value);
            await builder.AddAsync(key, value, cancellationToken).ConfigureAwait(false);
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
        _wal?.Dispose();
        _disposed = true;
    }

    private bool TryFindRecord(ReadOnlySpan<byte> key, out MemTableRecord record)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        FindBucket(key, Hash(key), ReadKeyPrefixWord(key), out var recordIndex);
        if (recordIndex < 0)
        {
            record = default;
            return false;
        }

        record = _records[recordIndex];
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FindBucket(ReadOnlySpan<byte> key, uint hash, ulong keyPrefixWord, out int recordIndex)
    {
        var mask = _bucketCapacity - 1;
        var bucket = (int)hash & mask;

        while (true)
        {
            recordIndex = _buckets[bucket] - 1;
            if (recordIndex < 0)
            {
                return bucket;
            }

            ref var record = ref _records[recordIndex];
            if (record.Hash == hash &&
                record.KeyPrefixWord == keyPrefixWord &&
                record.Key.Length == key.Length &&
                (key.Length <= 8 || key[8..].SequenceEqual(_arena.GetSpan(record.Key)[8..])))
            {
                return bucket;
            }

            bucket = (bucket + 1) & mask;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FindEmptyBucket(uint hash)
    {
        var mask = _bucketCapacity - 1;
        var bucket = (int)hash & mask;
        while (_buckets[bucket] != 0)
        {
            bucket = (bucket + 1) & mask;
        }
        return bucket;
    }

    private bool EnsureInsertCapacity()
    {
        if ((_count + 1) * 10 < _bucketCapacity * 7)
        {
            return false;
        }

        var oldBuckets = _buckets;
        var capacity = _bucketCapacity * 2;
        var buckets = ArrayPool<int>.Shared.Rent(capacity);
        buckets.AsSpan(0, capacity).Clear();
        var mask = capacity - 1;

        for (var i = 0; i < _count; i++)
        {
            var bucket = (int)_records[i].Hash & mask;
            while (buckets[bucket] != 0)
            {
                bucket = (bucket + 1) & mask;
            }
            buckets[bucket] = i + 1;
        }

        BeginLayoutChange();
        Volatile.Write(ref _buckets, buckets);
        _bucketCapacity = capacity;
        ArrayPool<int>.Shared.Return(oldBuckets);
        EndLayoutChange();
        return true;
    }

    private void EnsureRecordCapacity()
    {
        if (_count < _records.Length)
        {
            return;
        }

        var oldRecords = _records;
        var records = ArrayPool<MemTableRecord>.Shared.Rent(oldRecords.Length * 2);
        oldRecords.AsSpan(0, _count).CopyTo(records);
        BeginLayoutChange();
        Volatile.Write(ref _records, records);
        ArrayPool<MemTableRecord>.Shared.Return(oldRecords);
        EndLayoutChange();
    }

    private int FindPrefixBucket(ReadOnlySpan<byte> prefix, uint prefixHash, ulong keyPrefixWord, out int headIndex)
    {
        var mask = _prefixBucketCapacity - 1;
        var bucket = (int)prefixHash & mask;
        var expectedKeyLength = prefix.Length + sizeof(long);
        while (true)
        {
            headIndex = _prefixBuckets![bucket] - 1;
            if (headIndex < 0)
            {
                return bucket;
            }
            ref var record = ref _records[headIndex];
            if (record.PrefixHash == prefixHash &&
                record.Key.Length == expectedKeyLength &&
                (prefix.Length >= 8
                    ? record.KeyPrefixWord == keyPrefixWord && _arena.GetSpan(record.Key).Slice(8, prefix.Length - 8).SequenceEqual(prefix[8..])
                    : _arena.GetSpan(record.Key)[..prefix.Length].SequenceEqual(prefix)))
            {
                return bucket;
            }
            bucket = (bucket + 1) & mask;
        }
    }

    private void InsertVersionPrefixLive(int newIndex, ReadOnlySpan<byte> prefix)
    {
        if (_count * 10 >= _prefixBucketCapacity * 7)
        {
            _faultInjector?.Invoke(StorageFaultPoint.MemTableBeforeVersionPrefixGrow);
            var oldPrefixBuckets = _prefixBuckets!;
            var oldCap = _prefixBucketCapacity;
            var newCap = oldCap * 2;
            var newBuckets = ArrayPool<int>.Shared.Rent(newCap);
            newBuckets.AsSpan(0, newCap).Clear();
            var newMask = newCap - 1;
            for (var b = 0; b < oldCap; b++)
            {
                var head = oldPrefixBuckets[b] - 1;
                if (head >= 0)
                {
                    var slot = (int)_records[head].PrefixHash & newMask;
                    while (newBuckets[slot] != 0)
                    {
                        slot = (slot + 1) & newMask;
                    }
                    newBuckets[slot] = head + 1;
                }
            }
            BeginLayoutChange();
            Volatile.Write(ref _prefixBuckets, newBuckets);
            _prefixBucketCapacity = newCap;
            ArrayPool<int>.Shared.Return(oldPrefixBuckets);
            EndLayoutChange();
        }

        IndexVersionPrefixInto(_prefixBuckets!, _prefixBucketCapacity, newIndex, prefix);
    }

    private void EnsurePrefixBuckets()
    {
        if (_prefixBuckets is not null)
        {
            return;
        }
        lock (_recordComparer)
        {
            if (_prefixBuckets is not null)
            {
                return;
            }
            var targetCapacity = InitialBucketCapacity;
            while (_count * 10 >= targetCapacity * 7)
            {
                targetCapacity *= 2;
            }
            _prefixBucketCapacity = Math.Max(targetCapacity, _bucketCapacity);
            var buckets = ArrayPool<int>.Shared.Rent(_prefixBucketCapacity);
            buckets.AsSpan(0, _prefixBucketCapacity).Clear();
            for (var i = 0; i < _count; i++)
            {
                ref var rec = ref _records[i];
                var key = _arena.GetSpan(rec.Key);
                if (rec.IsCleanMvccKey || Tables.VersionPrefixBloom.IsVersionKey(key))
                {
                    IndexVersionPrefixInto(buckets, _prefixBucketCapacity, i, key[..^sizeof(long)]);
                }
            }
            Volatile.Write(ref _prefixBuckets, buckets);
        }
    }

    private void IndexVersionPrefixInto(int[] buckets, int capacity, int newIndex, ReadOnlySpan<byte> prefix)
    {
        var prefixHash = Hash(prefix);
        ref var newRec = ref _records[newIndex];
        newRec.PrefixHash = prefixHash;
        newRec.NextVersionRecord = -1;
        var keyPrefixWord = newRec.KeyPrefixWord;
        var expectedKeyLength = prefix.Length + sizeof(long);
        var mask = System.Numerics.BitOperations.RoundUpToPowerOf2((uint)capacity) - 1;
        var bucket = (int)(prefixHash & mask);
        int headIndex;
        for (var probes = 0; probes <= mask; probes++)
        {
            headIndex = buckets[bucket] - 1;
            if (headIndex < 0)
            {
                Volatile.Write(ref buckets[bucket], newIndex + 1);
                return;
            }
            ref var record = ref _records[headIndex];
            if (record.PrefixHash == prefixHash &&
                record.Key.Length == expectedKeyLength &&
                (prefix.Length >= 8
                    ? record.KeyPrefixWord == keyPrefixWord && _arena.GetSpan(record.Key).Slice(8, prefix.Length - 8).SequenceEqual(prefix[8..])
                    : _arena.GetSpan(record.Key)[..prefix.Length].SequenceEqual(prefix)))
            {
                goto FoundChain;
            }
            bucket = (int)((bucket + 1) & mask);
        }
        return;

    FoundChain:

        // Fast path: in normal MVCC execution, newer versions have a smaller ~sequence trailer and belong at the head.
        var newTrailer = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(_arena.GetSpan(newRec.Key)[^sizeof(long)..]);
        var headTrailer = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(_arena.GetSpan(_records[headIndex].Key)[^sizeof(long)..]);
        if (newTrailer <= headTrailer)
        {
            newRec.NextVersionRecord = headIndex;
            Volatile.Write(ref buckets[bucket], newIndex + 1);
            return;
        }

        var prev = headIndex;
        var curr = _records[headIndex].NextVersionRecord;
        while (curr >= 0 &&
               System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(_arena.GetSpan(_records[curr].Key)[^sizeof(long)..]) < newTrailer)
        {
            prev = curr;
            curr = _records[curr].NextVersionRecord;
        }
        newRec.NextVersionRecord = curr;
        Volatile.Write(ref _records[prev].NextVersionRecord, newIndex);
    }

    private int _unindexedPrefixReads;

    internal bool TryGetNextVersionRaw(ReadOnlySpan<byte> from, ReadOnlySpan<byte> prefix, bool inclusive,
        out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value, out bool tombstone)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_prefixBuckets is null && _count != 0)
        {
            if (!_sortDirty && _orderedIndex is not null && Interlocked.Increment(ref _unindexedPrefixReads) <= 8)
            {
                if (!TryGetNextRaw(from, true, inclusive, false, out key, out value, out tombstone) ||
                    !key.StartsWith(prefix))
                {
                    key = value = default;
                    tombstone = false;
                    return false;
                }
                return true;
            }
            EnsurePrefixBuckets();
        }
        if (_prefixBuckets is not null)
        {
            FindPrefixBucket(prefix, Hash(prefix), ReadKeyPrefixWord(prefix), out var curr);
            if (curr >= 0 && from.Length == prefix.Length + sizeof(long))
            {
                var fromTrailer = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(from[^sizeof(long)..]);
                while (curr >= 0)
                {
                    ref var rec = ref _records[curr];
                    var candidateKey = _arena.GetSpan(rec.Key);
                    var candidateTrailer = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(candidateKey[^sizeof(long)..]);
                    if (candidateTrailer > fromTrailer || (inclusive && candidateTrailer == fromTrailer))
                    {
                        key = candidateKey;
                        tombstone = rec.IsTombstone;
                        value = tombstone ? default : _arena.GetSpan(rec.Value);
                        return true;
                    }
                    curr = rec.NextVersionRecord;
                }
                key = value = default;
                tombstone = false;
                return false;
            }
            while (curr >= 0)
            {
                ref var rec = ref _records[curr];
                var candidateKey = _arena.GetSpan(rec.Key);
                var cmp = candidateKey.SequenceCompareTo(from);
                if (cmp > 0 || (inclusive && cmp == 0))
                {
                    key = candidateKey;
                    tombstone = rec.IsTombstone;
                    value = tombstone ? default : _arena.GetSpan(rec.Value);
                    return true;
                }
                curr = rec.NextVersionRecord;
            }
            key = value = default;
            tombstone = false;
            return false;
        }

        if (!TryGetNextRaw(from, true, inclusive, false, out key, out value, out tombstone) ||
            !key.StartsWith(prefix))
        {
            key = value = default;
            tombstone = false;
            return false;
        }
        return true;
    }

    internal long ScanReverseRaw<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries)
    {
        EnsureSortedIndex();
        long count = 0;
        var cursor = _orderedIndex!.Begin(true);
        var node = cursor.CurrentNode;
        var slot = cursor.CurrentSlot;

        while (node is not null && (uint)slot < (uint)node.Count)
        {
            var keys = node.Keys;
            for (; slot >= 0; slot--)
            {
                PrefetchRecord(keys, slot - RecordPrefetchDistance, node.Count);
                ref var record = ref _records[keys[slot]];
                if (record.IsTombstone)
                {
                    continue;
                }
                count++;
                if (!reader(arg, _arena.GetSpan(record.Key), _arena.GetSpan(record.Value)) || count >= maxEntries)
                {
                    return count;
                }
            }
            node = node.Previous;
            slot = node is null ? -1 : node.Count - 1;
        }
        return count;
    }

    internal long ScanRaw<TArg>(bool hasFrom, ReadOnlySpan<byte> from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries)
    {
        EnsureSortedIndex();
        long count = 0;
        var cursor = hasFrom ? _orderedIndex!.Seek(from, true, false) : _orderedIndex!.Begin(false);
        var node = cursor.CurrentNode;
        var slot = cursor.CurrentSlot;

        while (node is not null && (uint)slot < (uint)node.Count)
        {
            var keys = node.Keys;
            var nodeCount = node.Count;
            for (; slot < nodeCount; slot++)
            {
                PrefetchRecord(keys, slot + RecordPrefetchDistance, nodeCount);
                ref var record = ref _records[keys[slot]];
                if (record.IsTombstone)
                {
                    continue;
                }
                count++;
                if (!reader(arg, _arena.GetSpan(record.Key), _arena.GetSpan(record.Value)) || count >= maxEntries)
                {
                    return count;
                }
            }
            node = node.Next;
            slot = 0;
        }
        return count;
    }

    internal long ScanMvccReverseRaw<TArg>(long snapshotSequence, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries)
    {
        EnsureSortedIndex();
        long count = 0;
        var cursor = _orderedIndex!.Begin(true);
        if (_singleVersionCleanMvccState == 1)
        {
            return ScanMvccReverseSingleVersionFast(snapshotSequence, cursor, arg, reader, maxEntries);
        }

        var node = cursor.CurrentNode;
        var slot = cursor.CurrentSlot;
        var lastPrefixHash = 0u;
        var lastKeySlice = default(MemTableArena.ArenaSlice);
        var lastWord = 0UL;
        var hasCandidate = false;
        var candidateRecordIndex = -1;
        var sawDuplicateOrUnclean = false;

        while (node is not null && (uint)slot < (uint)node.Count)
        {
            var keys = node.Keys;
            for (; slot >= 0; slot--)
            {
                PrefetchRecord(keys, slot - RecordPrefetchDistance, node.Count);
                var recIdx = keys[slot];
                ref var record = ref _records[recIdx];
                if (record.IsTombstone)
                {
                    continue;
                }
                var key = _arena.GetSpan(record.Key);
                if (key.Length < 11 || key[0] != 1)
                {
                    continue;
                }
                var terminator = key.Length - 10;
                if (key[terminator] != 0 || key[terminator + 1] != 0)
                {
                    throw new InvalidDataException("An MVCC data key has an invalid terminator.");
                }
                var encodedUserKey = key.Slice(1, terminator - 1);
                var encodedSequence = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(key[^sizeof(long)..]);
                var decodedSequence = ~encodedSequence;
                if (decodedSequence > long.MaxValue)
                {
                    throw new InvalidDataException("An MVCC data key has an invalid sequence.");
                }
                var sequence = (long)decodedSequence;

                var sameGroup = lastKeySlice.Length != 0 &&
                    record.Key.Length == lastKeySlice.Length &&
                    record.KeyPrefixWord == lastWord &&
                    (record.PrefixHash == 0 || record.PrefixHash == lastPrefixHash) &&
                    (encodedUserKey.Length <= 7 || encodedUserKey.SequenceEqual(_arena.GetSpan(lastKeySlice).Slice(1, terminator - 1)));

                if (!sameGroup)
                {
                    if (hasCandidate && !EmitCandidate(candidateRecordIndex, arg, reader, maxEntries, ref count))
                    {
                        return count;
                    }
                    hasCandidate = false;
                    candidateRecordIndex = -1;
                    lastKeySlice = record.Key;
                    lastWord = record.KeyPrefixWord;
                    lastPrefixHash = record.PrefixHash;
                }
                else
                {
                    sawDuplicateOrUnclean = true;
                }

                if (!record.IsCleanMvccKey)
                {
                    sawDuplicateOrUnclean = true;
                }

                if (sequence > snapshotSequence)
                {
                    continue;
                }

                var value = _arena.GetSpan(record.Value);
                if (value.IsEmpty || value[0] > 1)
                {
                    throw new InvalidDataException("An MVCC value has an invalid kind marker.");
                }
                hasCandidate = true;
                candidateRecordIndex = value[0] == 1 ? recIdx : -1;
            }
            node = node.Previous;
            slot = node is null ? -1 : node.Count - 1;
        }

        if (hasCandidate)
        {
            EmitCandidate(candidateRecordIndex, arg, reader, maxEntries, ref count);
        }
        if (_singleVersionCleanMvccState == 0)
        {
            _singleVersionCleanMvccState = sawDuplicateOrUnclean ? 2 : 1;
        }
        return count;
    }

    private long ScanMvccReverseSingleVersionFast<TArg>(long snapshotSequence, OrderedIndex.Cursor cursor, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries)
    {
        long count = 0;
        var node = cursor.CurrentNode;
        var slot = cursor.CurrentSlot;

        while (node is not null && (uint)slot < (uint)node.Count)
        {
            var keys = node.Keys;
            for (; slot >= 0; slot--)
            {
                PrefetchRecord(keys, slot - RecordPrefetchDistance, node.Count);
                ref var record = ref _records[keys[slot]];
                if (record.IsTombstone) continue;
                var key = _arena.GetSpan(record.Key);
                if (key.Length < 11 || key[0] != 1) continue;
                var sequence = (long)~System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(key[^sizeof(long)..]);
                if (sequence > snapshotSequence) continue;
                var value = _arena.GetSpan(record.Value);
                if (value.IsEmpty || value[0] == 0) continue;
                count++;
                if (!reader(arg, key.Slice(1, key.Length - 11), value[1..]) || count >= maxEntries)
                {
                    return count;
                }
            }
            node = node.Previous;
            slot = node is null ? -1 : node.Count - 1;
        }
        return count;
    }

    private int _singleVersionCleanMvccState; // 0 = unknown, 1 = clean & distinct single-version keys, 2 = general

    private bool EmitCandidate<TArg>(int recIdx, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries, ref long count)
    {
        if (recIdx < 0)
        {
            return true;
        }
        ref var rec = ref _records[recIdx];
        var key = _arena.GetSpan(rec.Key);
        var encodedUserKey = key.Slice(1, key.Length - 11);
        var value = _arena.GetSpan(rec.Value)[1..];
        count++;
        var keepGoing = rec.IsCleanMvccKey
            ? reader(arg, encodedUserKey, value)
            : MvccStorage.InvokeUserReader(arg, reader, encodedUserKey, value);
        return keepGoing && count < maxEntries;
    }

    internal long ScanMvccRaw<TArg>(long snapshotSequence, bool hasFrom, ReadOnlySpan<byte> from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries)
    {
        EnsureSortedIndex();
        long count = 0;
        var cursor = hasFrom ? _orderedIndex!.Seek(from, true, false) : _orderedIndex!.Begin(false);
        if (_singleVersionCleanMvccState == 1 &&
            (!hasFrom || (from.Length >= 8 && System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(from[^sizeof(long)..]) == 0)))
        {
            return ScanMvccSingleVersionFast(snapshotSequence, cursor, arg, reader, maxEntries);
        }

        var node = cursor.CurrentNode;
        var slot = cursor.CurrentSlot;
        var lastPrefixHash = 0u;
        var lastKeySlice = default(MemTableArena.ArenaSlice);
        var lastWord = 0UL;
        var hasGroup = false;
        var resolved = false;
        var sawDuplicateOrUnclean = false;

        while (node is not null && (uint)slot < (uint)node.Count)
        {
            var keys = node.Keys;
            var nodeCount = node.Count;
            for (; slot < nodeCount; slot++)
            {
                PrefetchRecord(keys, slot + RecordPrefetchDistance, node.Count);
                ref var record = ref _records[keys[slot]];
                if (record.IsTombstone)
                {
                    continue;
                }
                var key = _arena.GetSpan(record.Key);
                if (key.Length < 11 || key[0] != 1)
                {
                    continue;
                }
                var terminator = key.Length - 10;
                if (key[terminator] != 0 || key[terminator + 1] != 0)
                {
                    throw new InvalidDataException("An MVCC data key has an invalid terminator.");
                }
                var encodedUserKey = key.Slice(1, terminator - 1);
                var encodedSequence = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(key[^sizeof(long)..]);
                var decodedSequence = ~encodedSequence;
                if (decodedSequence > long.MaxValue)
                {
                    throw new InvalidDataException("An MVCC data key has an invalid sequence.");
                }
                var sequence = (long)decodedSequence;

                var sameGroup = hasGroup &&
                    record.Key.Length == lastKeySlice.Length &&
                    record.KeyPrefixWord == lastWord &&
                    (record.PrefixHash == 0 || record.PrefixHash == lastPrefixHash) &&
                    (encodedUserKey.Length <= 7 || encodedUserKey.SequenceEqual(_arena.GetSpan(lastKeySlice).Slice(1, terminator - 1)));

                if (sameGroup)
                {
                    sawDuplicateOrUnclean = true;
                    if (resolved)
                    {
                        continue;
                    }
                }
                else
                {
                    hasGroup = true;
                    resolved = false;
                    lastKeySlice = record.Key;
                    lastWord = record.KeyPrefixWord;
                    lastPrefixHash = record.PrefixHash;
                }

                if (!record.IsCleanMvccKey)
                {
                    sawDuplicateOrUnclean = true;
                }

                if (sequence > snapshotSequence)
                {
                    continue;
                }

                var value = _arena.GetSpan(record.Value);
                if (value.IsEmpty || value[0] > 1)
                {
                    throw new InvalidDataException("An MVCC value has an invalid kind marker.");
                }
                resolved = true;
                if (value[0] == 0)
                {
                    continue;
                }

                count++;
                var keepGoing = record.IsCleanMvccKey
                    ? reader(arg, encodedUserKey, value[1..])
                    : MvccStorage.InvokeUserReader(arg, reader, encodedUserKey, value[1..]);
                if (!keepGoing || count >= maxEntries)
                {
                    return count;
                }
            }
            node = node.Next;
            slot = 0;
        }
        if (!hasFrom && _singleVersionCleanMvccState == 0)
        {
            _singleVersionCleanMvccState = sawDuplicateOrUnclean ? 2 : 1;
        }
        return count;
    }

    private long ScanMvccSingleVersionFast<TArg>(long snapshotSequence, OrderedIndex.Cursor cursor, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries)
    {
        long count = 0;
        var node = cursor.CurrentNode;
        var slot = cursor.CurrentSlot;

        while (node is not null && (uint)slot < (uint)node.Count)
        {
            var keys = node.Keys;
            var nodeCount = node.Count;
            for (; slot < nodeCount; slot++)
            {
                PrefetchRecord(keys, slot + RecordPrefetchDistance, node.Count);
                ref var record = ref _records[keys[slot]];
                if (record.IsTombstone) continue;
                var key = _arena.GetSpan(record.Key);
                if (key.Length < 11 || key[0] != 1) continue;
                var sequence = (long)~System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(key[^sizeof(long)..]);
                if (sequence > snapshotSequence) continue;
                var value = _arena.GetSpan(record.Value);
                if (value.IsEmpty || value[0] == 0) continue;
                count++;
                if (!reader(arg, key.Slice(1, key.Length - 11), value[1..]) || count >= maxEntries)
                {
                    return count;
                }
            }
            node = node.Next;
            slot = 0;
        }
        return count;
    }

    internal bool TryGetNextRaw(ReadOnlySpan<byte> from, bool hasFrom, bool inclusive, bool backwards,
        out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value, out bool tombstone)
    {
        EnsureSortedIndex();
        var cursor = hasFrom ? _orderedIndex!.Seek(from, inclusive, backwards) : _orderedIndex!.Begin(backwards);
        if (!cursor.IsValid)
        {
            key = value = default;
            tombstone = false;
            return false;
        }
        var record = _records[cursor.Record];
        key = _arena.GetSpan(record.Key);
        tombstone = record.IsTombstone;
        value = tombstone ? default : _arena.GetSpan(record.Value);
        return true;
    }

    internal bool TryAdvanceRaw(ref OrderedIndex.Cursor cursor, ref bool hasCursor,
        ReadOnlySpan<byte> from, bool hasFrom, bool inclusive, bool backwards,
        out ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value, out bool tombstone)
    {
        EnsureSortedIndex();
        var index = _orderedIndex!;
        if (hasCursor && cursor.IsCurrent(index))
        {
            cursor.Move(backwards);
        }
        else
        {
            cursor = hasFrom ? index.Seek(from, inclusive, backwards) : index.Begin(backwards);
            hasCursor = true;
        }
        if (!cursor.IsValid)
        {
            key = value = default;
            tombstone = false;
            return false;
        }
        var record = _records[cursor.Record];
        key = _arena.GetSpan(record.Key);
        tombstone = record.IsTombstone;
        value = tombstone ? default : _arena.GetSpan(record.Value);
        return true;
    }

    private KeyValuePair<ByteSlice, ByteSlice> Materialize(int recordIndex)
    {
        var record = _records[recordIndex];
        return new KeyValuePair<ByteSlice, ByteSlice>(
            _arena.GetByteSlice(record.Key),
            record.IsTombstone ? ByteSlice.Tombstone : _arena.GetByteSlice(record.Value));
    }

    private void DisposeInternal()
    {
        _arena?.Dispose();

        var records = _records;
        _records = [];
        if (records is not null)
        {
            ArrayPool<MemTableRecord>.Shared.Return(records);
        }

        var buckets = _buckets;
        _buckets = [];
        if (buckets is not null)
        {
            ArrayPool<int>.Shared.Return(buckets);
        }

        var prefixBuckets = _prefixBuckets;
        _prefixBuckets = null;
        if (prefixBuckets is not null)
        {
            ArrayPool<int>.Shared.Return(prefixBuckets);
        }

        _orderedIndex = null;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static uint Hash(ReadOnlySpan<byte> key) =>
        (uint)System.IO.Hashing.XxHash3.HashToUInt64(key);

    ~MemTable()
    {
        if (!_disposed)
        {
            DisposeInternal();
        }
    }

    internal struct MemTableRecord
    {
        public MemTableArena.ArenaSlice Key;
        public MemTableArena.ArenaSlice Value;
        public uint Hash;
        public uint Flags; // bit 0: IsTombstone, bit 1: IsCleanMvccKey (no zero-escaping in user key)
        public int NextVersionRecord;
        public uint PrefixHash;
        public ulong KeyPrefixWord;

        public bool IsTombstone
        {
            readonly get => (Flags & 1) != 0;
            set => Flags = value ? (Flags | 1u) : (Flags & ~1u);
        }

        public bool IsCleanMvccKey
        {
            readonly get => (Flags & 2) != 0;
            set => Flags = value ? (Flags | 2u) : (Flags & ~2u);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadKeyPrefixWord(ReadOnlySpan<byte> key)
    {
        if (key.Length >= 8)
        {
            return System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(key);
        }
        ulong word = 0;
        for (var i = 0; i < key.Length; i++)
        {
            word |= (ulong)key[i] << (56 - (i * 8));
        }
        return word;
    }

    private sealed class RecordIndexComparer(MemTable table) : IComparer<int>
    {
        public int Compare(int x, int y)
        {
            ref var rx = ref table._records[x];
            ref var ry = ref table._records[y];
            var wx = rx.KeyPrefixWord;
            var wy = ry.KeyPrefixWord;
            if (wx != wy)
            {
                return wx < wy ? -1 : 1;
            }
            return table._arena.GetSpan(rx.Key).SequenceCompareTo(table._arena.GetSpan(ry.Key));
        }
    }

    private sealed class MemTableIterator(MemTable table) : IStorageIterator
    {
#pragma warning disable CS1998
        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            table.EnsureSortedIndex();
            for (var cursor = table._orderedIndex!.Begin(false); cursor.IsValid; cursor.Move(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return table.Materialize(cursor.Record);
            }
        }

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(
            ByteSlice from,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            table.EnsureSortedIndex();
            for (var cursor = table._orderedIndex!.Seek(from.Span, true, false); cursor.IsValid; cursor.Move(false))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return table.Materialize(cursor.Record);
            }
        }

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            table.EnsureSortedIndex();
            for (var cursor = table._orderedIndex!.Begin(true); cursor.IsValid; cursor.Move(true))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return table.Materialize(cursor.Record);
            }
        }

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(
            ByteSlice from,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            table.EnsureSortedIndex();
            for (var cursor = table._orderedIndex!.Seek(from.Span, true, true); cursor.IsValid; cursor.Move(true))
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return table.Materialize(cursor.Record);
            }
        }
#pragma warning restore CS1998
    }
}
