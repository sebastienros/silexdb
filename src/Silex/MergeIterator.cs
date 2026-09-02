using Silex.Blocks;
using Silex.Tables;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace Silex;

/// <summary>
/// Merges sorted sources in newest-first order using synchronous list/block cursors and an index heap.
/// </summary>
internal sealed class MergeIterator : IStorageIterator
{
    private readonly IReadOnlyList<Input> _inputs;
    private readonly BlockCache? _blockCache;

    public MergeIterator(IReadOnlyList<SsTable> tables, BlockCache? blockCache = null)
    {
        var inputs = new Input[tables.Count];
        for (var i = 0; i < tables.Count; i++)
        {
            inputs[i] = Input.FromTable(tables[i]);
        }

        _inputs = inputs;
        _blockCache = blockCache;
    }

    public MergeIterator(IReadOnlyList<Input> inputs, BlockCache? blockCache = null)
    {
        _inputs = inputs;
        _blockCache = blockCache;
    }

    public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(CancellationToken cancellationToken = default)
    {
        return EnumerateCore(hasFrom: false, default, backwards: false, cancellationToken);
    }

    public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(ByteSlice from, CancellationToken cancellationToken = default)
    {
        return EnumerateCore(hasFrom: true, from.Memory, backwards: false, cancellationToken);
    }

    public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(CancellationToken cancellationToken = default)
    {
        return EnumerateCore(hasFrom: false, default, backwards: true, cancellationToken);
    }

    public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(ByteSlice from, CancellationToken cancellationToken = default)
    {
        return EnumerateCore(hasFrom: true, from.Memory, backwards: true, cancellationToken);
    }

    internal async ValueTask<long> ScanRawAsync<TArg>(
        bool hasFrom,
        ReadOnlyMemory<byte> from,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        CancellationToken cancellationToken)
    {
        var sourceCount = _inputs.Count;
        if (sourceCount == 0 || maxEntries == 0)
        {
            return 0;
        }

        var sources = ArrayPool<Source>.Shared.Rent(sourceCount);
        var heap = ArrayPool<int>.Shared.Rent(sourceCount);
        var heapCount = 0;
        const bool backwards = false;
        long count = 0;

        try
        {
            for (var i = 0; i < sourceCount; i++)
            {
                sources[i] = Source.Create(_inputs[i], hasFrom, from, backwards, cancellationToken);
                if (await MoveNextSourceAsync(sources, i, cancellationToken))
                {
                    Push(sources, heap, ref heapCount, i, backwards);
                }
            }

            while (heapCount != 0 && count < maxEntries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var winner = Pop(sources, heap, ref heapCount, backwards);
                while (heapCount != 0 && KeysEqual(sources[winner], sources[heap[0]]))
                {
                    var duplicate = Pop(sources, heap, ref heapCount, backwards);
                    if (await MoveNextSourceAsync(sources, duplicate, cancellationToken))
                    {
                        Push(sources, heap, ref heapCount, duplicate, backwards);
                    }
                }

                var keepGoing = true;
                if (!sources[winner].IsTombstone)
                {
                    count++;
                    keepGoing =                     reader(arg, sources[winner].KeySpan, sources[winner].ValueSpan);
                }

                if (!keepGoing || count >= maxEntries)
                {
                    break;
                }

                if (await MoveNextSourceAsync(sources, winner, cancellationToken))
                {
                    Push(sources, heap, ref heapCount, winner, backwards);
                }
            }

            return count;
        }
        finally
        {
            for (var i = 0; i < sourceCount; i++)
            {
                await sources[i].DisposeAsync();
                sources[i] = default;
            }

            ArrayPool<Source>.Shared.Return(sources);
            ArrayPool<int>.Shared.Return(heap);
        }
    }

    private IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateCore(
        bool hasFrom,
        ReadOnlyMemory<byte> from,
        bool backwards,
        CancellationToken cancellationToken)
    {
        return MergeInputsAsync(hasFrom, from, backwards, cancellationToken);
    }

    private async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> MergeInputsAsync(
        bool hasFrom,
        ReadOnlyMemory<byte> from,
        bool backwards,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var sourceCount = _inputs.Count;
        if (sourceCount == 0)
        {
            yield break;
        }

        var sources = ArrayPool<Source>.Shared.Rent(sourceCount);
        var heap = ArrayPool<int>.Shared.Rent(sourceCount);
        var heapCount = 0;

        try
        {
            for (var i = 0; i < sourceCount; i++)
            {
                sources[i] = Source.Create(_inputs[i], hasFrom, from, backwards, cancellationToken);
                if (await MoveNextSourceAsync(sources, i, cancellationToken))
                {
                    Push(sources, heap, ref heapCount, i, backwards);
                }
            }

            while (heapCount != 0)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var winner = Pop(sources, heap, ref heapCount, backwards);
                while (heapCount != 0 && KeysEqual(sources[winner], sources[heap[0]]))
                {
                    var duplicate = Pop(sources, heap, ref heapCount, backwards);
                    if (await MoveNextSourceAsync(sources, duplicate, cancellationToken))
                    {
                        Push(sources, heap, ref heapCount, duplicate, backwards);
                    }
                }

                yield return sources[winner].Current;

                if (await MoveNextSourceAsync(sources, winner, cancellationToken))
                {
                    Push(sources, heap, ref heapCount, winner, backwards);
                }
            }
        }
        finally
        {
            for (var i = 0; i < sourceCount; i++)
            {
                await sources[i].DisposeAsync();
                sources[i] = default;
            }

            ArrayPool<Source>.Shared.Return(sources);
            ArrayPool<int>.Shared.Return(heap);
        }
    }

    private async ValueTask<bool> MoveNextSourceAsync(Source[] sources, int sourceIndex, CancellationToken cancellationToken)
    {
        if (sources[sourceIndex].Entries != null)
        {
            return sources[sourceIndex].MoveNextList();
        }

        var enumerator = sources[sourceIndex].Enumerator;
        if (enumerator != null)
        {
            return await enumerator.MoveNextAsync();
        }

        if (sources[sourceIndex].HasBlock && sources[sourceIndex].Cursor.MoveNext())
        {
            return true;
        }

        if (sources[sourceIndex].HasBlock)
        {
            sources[sourceIndex].DisposeBlock();
            sources[sourceIndex].BlockIndex += sources[sourceIndex].Step;
        }

        var table = sources[sourceIndex].Table!;
        while ((uint)sources[sourceIndex].BlockIndex < (uint)table.BlockMetadataArray.Length)
        {
            cancellationToken.ThrowIfCancellationRequested();

            Block? block;
            if (_blockCache == null)
            {
                block = await table.ReadBlockAsync(sources[sourceIndex].BlockIndex, cancellationToken);
                sources[sourceIndex].OwnsBlock = block != null;
            }
            else
            {
                sources[sourceIndex].Lease = await table.ReadBlockCachedAsync(
                    sources[sourceIndex].BlockIndex,
                    _blockCache,
                    cancellationToken);
                block = sources[sourceIndex].Lease.Block;
            }

            if (block == null)
            {
                sources[sourceIndex].BlockIndex += sources[sourceIndex].Step;
                continue;
            }

            var seek = sources[sourceIndex].SeekPending ? sources[sourceIndex].From.Span : default;
            sources[sourceIndex].SeekPending = false;
            sources[sourceIndex].Block = block;
            sources[sourceIndex].HasBlock = true;
            sources[sourceIndex].Cursor = new BlockIterator.Cursor(block, sources[sourceIndex].Step < 0, seek);

            if (sources[sourceIndex].Cursor.MoveNext())
            {
                return true;
            }

            sources[sourceIndex].DisposeBlock();
            sources[sourceIndex].BlockIndex += sources[sourceIndex].Step;
        }

        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool KeysEqual(in Source left, in Source right)
    {
        return left.KeySpan.SequenceEqual(right.KeySpan);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ComesBefore(Source[] sources, int left, int right, bool backwards)
    {
        var comparison = sources[left].KeySpan.SequenceCompareTo(sources[right].KeySpan);
        if (comparison != 0)
        {
            return backwards ? comparison > 0 : comparison < 0;
        }

        return left < right;
    }

    private static void Push(Source[] sources, int[] heap, ref int count, int source, bool backwards)
    {
        var child = count++;
        while (child != 0)
        {
            var parent = (child - 1) >> 1;
            var parentSource = heap[parent];
            if (!ComesBefore(sources, source, parentSource, backwards))
            {
                break;
            }

            heap[child] = parentSource;
            child = parent;
        }

        heap[child] = source;
    }

    private static int Pop(Source[] sources, int[] heap, ref int count, bool backwards)
    {
        var result = heap[0];
        var replacement = heap[--count];
        if (count == 0)
        {
            return result;
        }

        var parent = 0;
        while (true)
        {
            var left = (parent << 1) + 1;
            if (left >= count)
            {
                break;
            }

            var right = left + 1;
            var child = right < count && ComesBefore(sources, heap[right], heap[left], backwards) ? right : left;
            if (!ComesBefore(sources, heap[child], replacement, backwards))
            {
                break;
            }

            heap[parent] = heap[child];
            parent = child;
        }

        heap[parent] = replacement;
        return result;
    }

    private struct Source
    {
        public SsTable? Table;
        public List<KeyValuePair<ByteSlice, ByteSlice>>? Entries;
        public IAsyncEnumerator<KeyValuePair<ByteSlice, ByteSlice>>? Enumerator;
        public int EntryIndex;
        public int BlockIndex;
        public int Step;
        public bool SeekPending;
        public ReadOnlyMemory<byte> From;
        public BlockIterator.Cursor Cursor;
        public Block? Block;
        public BlockLease Lease;
        public bool OwnsBlock;
        public bool HasBlock;

        public readonly ReadOnlySpan<byte> KeySpan =>
            Entries != null
                ? Entries[EntryIndex].Key.Span
                : Enumerator != null
                    ? Enumerator.Current.Key.Span
                    : Cursor.KeySpan;

        public readonly ReadOnlySpan<byte> ValueSpan =>
            Entries != null
                ? Entries[EntryIndex].Value.Span
                : Enumerator != null
                    ? Enumerator.Current.Value.Span
                    : Cursor.ValueSpan;

        public readonly bool IsTombstone =>
            Entries != null
                ? Entries[EntryIndex].Value.IsTombstone
                : Enumerator != null
                    ? Enumerator.Current.Value.IsTombstone
                    : Cursor.IsTombstone;

        public readonly KeyValuePair<ByteSlice, ByteSlice> Current =>
            Entries != null
                ? Entries[EntryIndex]
                : Enumerator != null
                    ? Enumerator.Current
                    : Cursor.Current;

        public static Source Create(
            Input input,
            bool hasFrom,
            ReadOnlyMemory<byte> from,
            bool backwards,
            CancellationToken cancellationToken)
        {
            if (input.Entries != null)
            {
                var entries = input.Entries;
                var index = backwards ? entries.Count : -1;

                if (hasFrom)
                {
                    var start = 0;
                    var end = entries.Count;
                    while (start < end)
                    {
                        var middle = start + ((end - start) >> 1);
                        var comparison = entries[middle].Key.Span.SequenceCompareTo(from.Span);
                        if (backwards ? comparison <= 0 : comparison < 0)
                        {
                            start = middle + 1;
                        }
                        else
                        {
                            end = middle;
                        }
                    }

                    index = backwards ? start : start - 1;
                }

                return new Source
                {
                    Entries = entries,
                    EntryIndex = index,
                    Step = backwards ? -1 : 1,
                };
            }

            if (input.Iterator != null)
            {
                var fromSlice = hasFrom ? ByteSlice.FromMemory(from) : null;
                var enumerable = backwards
                    ? hasFrom
                        ? input.Iterator.EnumerateBackwardsAsync(fromSlice!, cancellationToken)
                        : input.Iterator.EnumerateBackwardsAsync(cancellationToken)
                    : hasFrom
                        ? input.Iterator.EnumerateAsync(fromSlice!, cancellationToken)
                        : input.Iterator.EnumerateAsync(cancellationToken);

                return new Source
                {
                    Enumerator = enumerable.GetAsyncEnumerator(cancellationToken),
                    Step = backwards ? -1 : 1,
                };
            }

            var table = input.Table!;
            var metadata = table.BlockMetadataArray;
            var blockIndex = backwards ? metadata.Length - 1 : 0;

            if (hasFrom)
            {
                if (backwards)
                {
                    var start = 0;
                    var end = metadata.Length;
                    while (start < end)
                    {
                        var middle = start + ((end - start) >> 1);
                        if (metadata.GetFirstKeySpan(middle).SequenceCompareTo(from.Span) <= 0)
                        {
                            start = middle + 1;
                        }
                        else
                        {
                            end = middle;
                        }
                    }

                    blockIndex = start - 1;
                }
                else
                {
                    var start = 0;
                    var end = metadata.Length;
                    while (start < end)
                    {
                        var middle = start + ((end - start) >> 1);
                        if (metadata.GetFirstKeySpan(middle).SequenceCompareTo(from.Span) <= 0)
                        {
                            start = middle + 1;
                        }
                        else
                        {
                            end = middle;
                        }
                    }

                    blockIndex = Math.Max(0, start - 1);
                    if (blockIndex < metadata.Length
                        && metadata.GetLastKeySpan(blockIndex).SequenceCompareTo(from.Span) < 0)
                    {
                        blockIndex++;
                    }
                }
            }

            return new Source
            {
                Table = table,
                BlockIndex = blockIndex,
                Step = backwards ? -1 : 1,
                SeekPending = hasFrom,
                From = from,
            };
        }

        public bool MoveNextList()
        {
            EntryIndex += Step;
            return (uint)EntryIndex < (uint)Entries!.Count;
        }

        public void DisposeBlock()
        {
            if (OwnsBlock)
            {
                Block?.Dispose();
            }
            else
            {
                Lease.Dispose();
            }

            Block = null;
            Lease = default;
            OwnsBlock = false;
            HasBlock = false;
            Cursor = default;
        }

        public async ValueTask DisposeAsync()
        {
            DisposeBlock();
            if (Enumerator != null)
            {
                await Enumerator.DisposeAsync();
                Enumerator = null;
            }
        }
    }

    internal readonly struct Input
    {
        private Input(
            SsTable? table,
            List<KeyValuePair<ByteSlice, ByteSlice>>? entries,
            IStorageIterator? iterator)
        {
            Table = table;
            Entries = entries;
            Iterator = iterator;
        }

        public SsTable? Table { get; }

        public List<KeyValuePair<ByteSlice, ByteSlice>>? Entries { get; }

        public IStorageIterator? Iterator { get; }

        public static Input FromTable(SsTable table) => new(table, null, null);

        public static Input FromEntries(List<KeyValuePair<ByteSlice, ByteSlice>> entries) => new(null, entries, null);

        public static Input FromIterator(IStorageIterator iterator) => new(null, null, iterator);
    }
}
