using Silex.Tables;
using Silex.Wal;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace Silex.MemTables;

/// <summary>
/// Stores keys and values in arena slabs and keeps compact record descriptors in contiguous arrays.
/// Point lookups use open addressing; sorted iteration uses a lazily rebuilt index of record positions.
/// </summary>
/// <remarks>
/// Writes are synchronized by <see cref="LsmStorageInner"/>. Frozen tables support concurrent readers.
/// A MemTable only owns writes and tombstones; it is not a read cache.
/// </remarks>
internal sealed class MemTable : IMemTable, IRawBytesMemTable
{
    private const int InitialRecordCapacity = 16;
    private const int InitialBucketCapacity = 32;

    private readonly long _id;
    private readonly WriteAheadLog? _wal;
    private readonly MemTableArena _arena;
    private readonly RecordIndexComparer _recordComparer;

    private MemTableRecord[] _records;
    private int[] _buckets;
    private int _bucketCapacity;
    private int[]? _sortedIndices;
    private int _count;
    private long _size;
    private volatile bool _sortDirty;
    private bool _disposed;

    public MemTable(long id, WriteAheadLog? wal = null, int arenaBlockSize = 32 * 1024)
    {
        _id = id;
        _wal = wal;
        _arena = new MemTableArena(arenaBlockSize);
        _records = ArrayPool<MemTableRecord>.Shared.Rent(InitialRecordCapacity);
        _buckets = ArrayPool<int>.Shared.Rent(InitialBucketCapacity);
        _bucketCapacity = InitialBucketCapacity;
        _buckets.AsSpan(0, _bucketCapacity).Clear();
        _recordComparer = new RecordIndexComparer(this);
    }

    public long Id => _id;

    public long Size => _size;

    public int Count => _count;

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

    internal void WriteBatch(List<LsmWriteBatchEntry> entries)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (entries.Count == 0)
        {
            return;
        }

        _wal?.AppendBatch(entries);
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            ApplyRaw(entry.Key, entry.Value ?? default, entry.IsTombstone);
        }
    }

    private void PutRawCore(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (isTombstone)
        {
            _wal?.AppendDeleteRaw(key);
        }
        else
        {
            _wal?.AppendRaw(key, value);
        }

        ApplyRaw(key, value, isTombstone);
    }

    private void ApplyRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone)
    {
        var hash = Hash(key);
        var bucket = FindBucket(key, hash, out var recordIndex);
        var storedValue = isTombstone ? MemTableArena.ArenaSlice.Empty : _arena.Copy(value);

        if (recordIndex >= 0)
        {
            _records[recordIndex].Value = storedValue;
            _records[recordIndex].IsTombstone = isTombstone;
        }
        else
        {
            EnsureInsertCapacity();
            bucket = FindBucket(key, hash, out _);
            EnsureRecordCapacity();

            var newIndex = _count;
            _records[newIndex] = new MemTableRecord
            {
                Key = _arena.Copy(key),
                Value = storedValue,
                Hash = hash,
                IsTombstone = isTombstone,
            };
            _buckets[bucket] = newIndex + 1;

            if (_sortedIndices is not null && !_sortDirty)
            {
                InsertSortedRecord(newIndex);
            }
            else
            {
                _sortDirty = true;
            }

            _count++;
        }

        _size += key.Length + value.Length + sizeof(int);
    }

    public IStorageIterator CreateIterator() => new MemTableIterator(this);

    public async Task FlushAsync(ISsTableBuilder builder, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        EnsureSortedIndex();

        for (var i = 0; i < _count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var record = _records[_sortedIndices![i]];
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
        FindBucket(key, Hash(key), out var recordIndex);
        if (recordIndex < 0)
        {
            record = default;
            return false;
        }

        record = _records[recordIndex];
        return true;
    }

    private int FindBucket(ReadOnlySpan<byte> key, uint hash, out int recordIndex)
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
            if (record.Hash == hash && key.SequenceEqual(_arena.GetSpan(record.Key)))
            {
                return bucket;
            }

            bucket = (bucket + 1) & mask;
        }
    }

    private void EnsureInsertCapacity()
    {
        if ((_count + 1) * 10 < _bucketCapacity * 7)
        {
            return;
        }

        var oldBuckets = _buckets;
        _bucketCapacity *= 2;
        _buckets = ArrayPool<int>.Shared.Rent(_bucketCapacity);
        _buckets.AsSpan(0, _bucketCapacity).Clear();

        for (var i = 0; i < _count; i++)
        {
            ref var record = ref _records[i];
            var bucket = FindBucket(_arena.GetSpan(record.Key), record.Hash, out _);
            _buckets[bucket] = i + 1;
        }

        ArrayPool<int>.Shared.Return(oldBuckets);
    }

    private void EnsureRecordCapacity()
    {
        if (_count < _records.Length)
        {
            return;
        }

        var oldRecords = _records;
        _records = ArrayPool<MemTableRecord>.Shared.Rent(oldRecords.Length * 2);
        oldRecords.AsSpan(0, _count).CopyTo(_records);
        ArrayPool<MemTableRecord>.Shared.Return(oldRecords);
    }

    private void EnsureSortedIndex()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (!_sortDirty && _sortedIndices is not null)
        {
            return;
        }

        lock (_recordComparer)
        {
            if (!_sortDirty && _sortedIndices is not null)
            {
                return;
            }

            EnsureSortedCapacity(_count);

            for (var i = 0; i < _count; i++)
            {
                _sortedIndices[i] = i;
            }

            Array.Sort(_sortedIndices, 0, _count, _recordComparer);
            _sortDirty = false;
        }
    }

    private void InsertSortedRecord(int recordIndex)
    {
        EnsureSortedCapacity(_count + 1);

        var key = _arena.GetSpan(_records[recordIndex].Key);
        var low = 0;
        var high = _count;
        while (low < high)
        {
            var middle = low + ((high - low) >> 1);
            var middleKey = _arena.GetSpan(_records[_sortedIndices![middle]].Key);
            if (middleKey.SequenceCompareTo(key) < 0)
            {
                low = middle + 1;
            }
            else
            {
                high = middle;
            }
        }

        _sortedIndices!.AsSpan(low, _count - low).CopyTo(_sortedIndices.AsSpan(low + 1));
        _sortedIndices[low] = recordIndex;
    }

    [MemberNotNull(nameof(_sortedIndices))]
    private void EnsureSortedCapacity(int required)
    {
        if (_sortedIndices is not null && _sortedIndices.Length >= required)
        {
            return;
        }

        var replacement = ArrayPool<int>.Shared.Rent(Math.Max(1, required));
        if (_sortedIndices is not null)
        {
            _sortedIndices.AsSpan(0, Math.Min(_count, required)).CopyTo(replacement);
            ArrayPool<int>.Shared.Return(_sortedIndices);
        }

        _sortedIndices = replacement;
    }

    private int FindFirstAtOrAfter(ReadOnlySpan<byte> key)
    {
        var low = 0;
        var high = _count;

        while (low < high)
        {
            var middle = low + ((high - low) >> 1);
            var record = _records[_sortedIndices![middle]];
            if (_arena.GetSpan(record.Key).SequenceCompareTo(key) < 0)
            {
                low = middle + 1;
            }
            else
            {
                high = middle;
            }
        }

        return low;
    }

    private KeyValuePair<ByteSlice, ByteSlice> Materialize(int sortedIndex)
    {
        var record = _records[_sortedIndices![sortedIndex]];
        return new KeyValuePair<ByteSlice, ByteSlice>(
            _arena.GetByteSlice(record.Key),
            record.IsTombstone ? ByteSlice.Tombstone : _arena.GetByteSlice(record.Value));
    }

    private void DisposeInternal()
    {
        _arena.Dispose();

        var records = _records;
        _records = [];
        ArrayPool<MemTableRecord>.Shared.Return(records);

        var buckets = _buckets;
        _buckets = [];
        ArrayPool<int>.Shared.Return(buckets);

        if (_sortedIndices is not null)
        {
            ArrayPool<int>.Shared.Return(_sortedIndices);
            _sortedIndices = null;
        }
    }

    private static uint Hash(ReadOnlySpan<byte> key)
    {
        var hash = 2166136261u;
        for (var i = 0; i < key.Length; i++)
        {
            hash = (hash ^ key[i]) * 16777619u;
        }

        hash ^= hash >> 16;
        return hash;
    }

    ~MemTable()
    {
        if (!_disposed)
        {
            DisposeInternal();
        }
    }

    private struct MemTableRecord
    {
        public MemTableArena.ArenaSlice Key;
        public MemTableArena.ArenaSlice Value;
        public uint Hash;
        public bool IsTombstone;
    }

    private sealed class RecordIndexComparer(MemTable table) : IComparer<int>
    {
        public int Compare(int x, int y) =>
            table._arena.GetSpan(table._records[x].Key).SequenceCompareTo(
                table._arena.GetSpan(table._records[y].Key));
    }

    private sealed class MemTableIterator(MemTable table) : IStorageIterator
    {
#pragma warning disable CS1998
        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            table.EnsureSortedIndex();
            for (var i = 0; i < table._count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return table.Materialize(i);
            }
        }

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(
            ByteSlice from,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            table.EnsureSortedIndex();
            var start = table.FindFirstAtOrAfter(from.Span);
            for (var i = start; i < table._count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return table.Materialize(i);
            }
        }

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            table.EnsureSortedIndex();
            for (var i = table._count - 1; i >= 0; i--)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return table.Materialize(i);
            }
        }

        public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(
            ByteSlice from,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            table.EnsureSortedIndex();
            var start = table.FindFirstAtOrAfter(from.Span);
            if (start == table._count ||
                !table._arena.GetSpan(table._records[table._sortedIndices![start]].Key).SequenceEqual(from.Span))
            {
                start--;
            }

            for (var i = start; i >= 0; i--)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return table.Materialize(i);
            }
        }
#pragma warning restore CS1998
    }
}
