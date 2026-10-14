using Silex.Blocks;
using Silex.Tables;
using Silex.MemTables;
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

    internal async ValueTask<bool> ScanBoundedRawAsync<TArg>(OrderedReadRange range, bool backwards,
        TArg arg, ReadRawEntryAction<TArg> reader, CancellationToken cancellationToken, int valuePrefixLength = int.MaxValue)
    {
        if (range.IsEmpty)
        {
            return true;
        }
        var start = backwards ? range.Upper : range.Lower;
        var sourceCount = _inputs.Count;
        var sources = ArrayPool<Source>.Shared.Rent(Math.Max(1, sourceCount));
        var heap = ArrayPool<int>.Shared.Rent(Math.Max(1, sourceCount));
        var initialized = 0;
        var heapCount = 0;
        try
        {
            for (var i = 0; i < sourceCount; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                sources[i] = Source.Create(_inputs[i], start.HasValue, start.GetValueOrDefault(), backwards, cancellationToken);
                sources[i].Range = range;
                sources[i].ValuePrefixLength = valuePrefixLength;
                initialized++;
                if (await MoveNextSourceAsync(sources, i, cancellationToken).ConfigureAwait(false))
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
                    if (await MoveNextSourceAsync(sources, duplicate, cancellationToken).ConfigureAwait(false))
                    {
                        Push(sources, heap, ref heapCount, duplicate, backwards);
                    }
                }
                if (!sources[winner].IsTombstone &&
                    !reader(arg, sources[winner].KeySpan, sources[winner].ValueSpan))
                {
                    return false;
                }
                if (await MoveNextSourceAsync(sources, winner, cancellationToken).ConfigureAwait(false))
                {
                    Push(sources, heap, ref heapCount, winner, backwards);
                }
            }
            return true;
        }
        finally
        {
            for (var i = 0; i < initialized; i++)
            {
                await sources[i].DisposeAsync().ConfigureAwait(false);
                sources[i] = default;
            }
            ArrayPool<Source>.Shared.Return(sources);
            ArrayPool<int>.Shared.Return(heap);
        }
    }

    internal async ValueTask<int> CompactToBuilderAsync(
        BufferedSsTableBuilder builder,
        bool dropTombstones,
        CancellationToken cancellationToken)
    {
        var sourceCount = _inputs.Count;
        if (sourceCount == 0)
        {
            return 0;
        }

        var sources = ArrayPool<Source>.Shared.Rent(sourceCount);
        var heap = ArrayPool<int>.Shared.Rent(sourceCount);
        var heapCount = 0;
        const bool backwards = false;
        var added = 0;

        try
        {
            for (var i = 0; i < sourceCount; i++)
            {
                sources[i] = Source.Create(_inputs[i], hasFrom: false, default, backwards, cancellationToken);
                if (await MoveNextSourceAsync(sources, i, cancellationToken).ConfigureAwait(false))
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
                    if (await MoveNextSourceAsync(sources, duplicate, cancellationToken).ConfigureAwait(false))
                    {
                        Push(sources, heap, ref heapCount, duplicate, backwards);
                    }
                }

                var isTombstone = sources[winner].IsTombstone;
                if (!dropTombstones || !isTombstone)
                {
                    if (!builder.TryAddRawFast(sources[winner].KeySpan, isTombstone ? default : sources[winner].ValueSpan, isTombstone))
                    {
                        await builder.RolloverBlockForRawAsync(cancellationToken).ConfigureAwait(false);
                        if (!builder.TryAddRawFast(sources[winner].KeySpan, isTombstone ? default : sources[winner].ValueSpan, isTombstone))
                        {
                            throw new InvalidOperationException("The data was not successfully added to a block.");
                        }
                    }
                    added++;
                }

                if (await MoveNextSourceAsync(sources, winner, cancellationToken).ConfigureAwait(false))
                {
                    Push(sources, heap, ref heapCount, winner, backwards);
                }
            }

            return added;
        }
        finally
        {
            for (var i = 0; i < sourceCount; i++)
            {
                await sources[i].DisposeAsync().ConfigureAwait(false);
                sources[i] = default;
            }

            ArrayPool<Source>.Shared.Return(sources);
            ArrayPool<int>.Shared.Return(heap);
        }
    }

    internal static bool TryScanMvccSourcesSync<TArg>(
        Source[]? sources,
        int[]? heap,
        int sourceCount,
        BlockCache? blockCache,
        long snapshotSequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        CancellationToken cancellationToken,
        out long count)
    {
        count = 0;
        if (sources == null || heap == null || sourceCount == 0 || maxEntries == 0)
        {
            return true;
        }

        var heapCount = 0;
        const bool backwards = false;
        ulong lastWord = 0;
        var lastLen = -1;
        var resolved = false;
        byte[]? groupBuf = null;

        try
        {
            if (sourceCount == 0 || maxEntries == 0)
            {
                return true;
            }

            for (var i = 0; i < sourceCount; i++)
            {
                if (!TryMoveNextSourceSync(sources, i, blockCache, cancellationToken, out var moved))
                {
                    moved = MoveNextSourceCoreStaticAsync(sources, i, blockCache, cancellationToken).AsTask().GetAwaiter().GetResult();
                }
                if (moved)
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
                    if (!TryMoveNextSourceSync(sources, duplicate, blockCache, cancellationToken, out var dupMoved))
                    {
                        dupMoved = MoveNextSourceCoreStaticAsync(sources, duplicate, blockCache, cancellationToken).AsTask().GetAwaiter().GetResult();
                    }
                    if (dupMoved)
                    {
                        Push(sources, heap, ref heapCount, duplicate, backwards);
                    }
                }

                var keepGoing = true;
                if (!sources[winner].IsTombstone)
                {
                    var key = sources[winner].KeySpan;
                    var value = sources[winner].ValueSpan;
                    if (key.Length >= 3 + sizeof(long) && key[0] == 1 && !value.IsEmpty)
                    {
                        var terminator = key.Length - sizeof(long) - 2;
                        if (key[terminator] == 0 && key[terminator + 1] == 0)
                        {
                            var encodedUserKey = key.Slice(1, terminator - 1);
                            var word = encodedUserKey.Length >= 8 ? System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(encodedUserKey) : 0UL;
                            var sameKey = encodedUserKey.Length == lastLen && word == lastWord &&
                                (encodedUserKey.Length <= 8
                                    ? encodedUserKey.SequenceEqual(groupBuf.AsSpan(0, lastLen))
                                    : encodedUserKey[8..].SequenceEqual(groupBuf.AsSpan(8, lastLen - 8)));

                            if (!sameKey)
                            {
                                lastWord = word;
                                lastLen = encodedUserKey.Length;
                                resolved = false;
                                if (encodedUserKey.Length < 8 || encodedUserKey.Length > 8)
                                {
                                    if (groupBuf is null || groupBuf.Length < encodedUserKey.Length)
                                    {
                                        if (groupBuf is not null) ArrayPool<byte>.Shared.Return(groupBuf);
                                        groupBuf = ArrayPool<byte>.Shared.Rent(encodedUserKey.Length);
                                    }
                                    encodedUserKey.CopyTo(groupBuf);
                                }
                            }

                            if (!resolved)
                            {
                                var encoded = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(key[(terminator + 2)..]);
                                var seq = (long)~encoded;
                                if (seq >= 0 && seq <= snapshotSequence)
                                {
                                    resolved = true;
                                    if (value[0] == 1)
                                    {
                                        count++;
                                        keepGoing = MvccStorage.InvokeUserReader(arg, reader, encodedUserKey, value[1..]);
                                    }
                                }
                            }
                        }
                    }
                }

                if (!keepGoing || count >= maxEntries)
                {
                    break;
                }

                if (!TryMoveNextSourceSync(sources, winner, blockCache, cancellationToken, out var winMoved))
                {
                    // Could not advance to next block synchronously after already delivering entries;
                    // load the missing block synchronously via ReadBlockCachedAsync.AsTask().GetAwaiter().GetResult()
                    // so we don't duplicate already-delivered entries on fallback.
                    winMoved = MoveNextSourceCoreStaticAsync(sources, winner, blockCache, cancellationToken).AsTask().GetAwaiter().GetResult();
                }
                if (winMoved)
                {
                    Push(sources, heap, ref heapCount, winner, backwards);
                }
            }

            return true;
        }
        finally
        {
            if (groupBuf is not null)
            {
                ArrayPool<byte>.Shared.Return(groupBuf);
            }
        }
    }

    internal static ValueTask<long> ScanRawSourcesAsync<TArg>(
        Source[]? sources,
        int[]? heap,
        int sourceCount,
        BlockCache? blockCache,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        CancellationToken cancellationToken)
    {
        if (sources == null || heap == null)
        {
            return ValueTask.FromResult(0L);
        }

        var heapCount = 0;
        const bool backwards = false;
        var count = 0;

        try
        {
            if (sourceCount == 0 || maxEntries == 0)
            {
                for (var i = 0; i < sourceCount; i++)
                {
                    sources[i].DisposeSyncOrAsync();
                    sources[i] = default;
                }
                ArrayPool<Source>.Shared.Return(sources);
                ArrayPool<int>.Shared.Return(heap);
                return ValueTask.FromResult(0L);
            }

            for (var i = 0; i < sourceCount; i++)
            {
                if (!TryMoveNextSourceSync(sources, i, blockCache, cancellationToken, out var moved))
                {
                    return ScanRawSourcesSlowAsync(sources, heap, sourceCount, i, heapCount, count, blockCache, arg, reader, maxEntries, cancellationToken);
                }
                if (moved)
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
                    if (!TryMoveNextSourceSync(sources, duplicate, blockCache, cancellationToken, out var dupMoved))
                    {
                        return ScanRawSourcesSlowFromDuplicateAsync(sources, heap, sourceCount, winner, duplicate, heapCount, count, blockCache, arg, reader, maxEntries, cancellationToken);
                    }
                    if (dupMoved)
                    {
                        Push(sources, heap, ref heapCount, duplicate, backwards);
                    }
                }

                var keepGoing = true;
                if (!sources[winner].IsTombstone)
                {
                    count++;
                    keepGoing = reader(arg, sources[winner].KeySpan, sources[winner].ValueSpan);
                }

                if (!keepGoing || count >= maxEntries)
                {
                    break;
                }

                if (!TryMoveNextSourceSync(sources, winner, blockCache, cancellationToken, out var winMoved))
                {
                    return ScanRawSourcesSlowFromWinnerAsync(sources, heap, sourceCount, winner, heapCount, count, blockCache, arg, reader, maxEntries, cancellationToken);
                }
                if (winMoved)
                {
                    Push(sources, heap, ref heapCount, winner, backwards);
                }
            }

            for (var i = 0; i < sourceCount; i++)
            {
                sources[i].DisposeSyncOrAsync();
                sources[i] = default;
            }
            ArrayPool<Source>.Shared.Return(sources);
            ArrayPool<int>.Shared.Return(heap);
            return ValueTask.FromResult((long)count);
        }
        catch
        {
            for (var i = 0; i < sourceCount; i++)
            {
                sources[i].DisposeSyncOrAsync();
                sources[i] = default;
            }
            ArrayPool<Source>.Shared.Return(sources);
            ArrayPool<int>.Shared.Return(heap);
            throw;
        }
    }

    private static ValueTask<long> ScanRawSourcesSlowFromDuplicateAsync<TArg>(
        Source[] sources, int[] heap, int sourceCount, int winner, int duplicate, int heapCount, long count,
        BlockCache? blockCache, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries, CancellationToken cancellationToken)
        => ScanRawSourcesContinueAsync(sources, heap, sourceCount, initIndex: sourceCount, winner, duplicate, state: 1, heapCount, count, blockCache, arg, reader, maxEntries, cancellationToken);

    private static ValueTask<long> ScanRawSourcesSlowFromWinnerAsync<TArg>(
        Source[] sources, int[] heap, int sourceCount, int winner, int heapCount, long count,
        BlockCache? blockCache, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries, CancellationToken cancellationToken)
        => ScanRawSourcesContinueAsync(sources, heap, sourceCount, initIndex: sourceCount, winner, duplicate: -1, state: 2, heapCount, count, blockCache, arg, reader, maxEntries, cancellationToken);

    private static ValueTask<long> ScanRawSourcesSlowAsync<TArg>(
        Source[] sources, int[] heap, int sourceCount, int initIndex, int heapCount, long count,
        BlockCache? blockCache, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries, CancellationToken cancellationToken)
        => ScanRawSourcesContinueAsync(sources, heap, sourceCount, initIndex, winner: -1, duplicate: -1, state: 0, heapCount, count, blockCache, arg, reader, maxEntries, cancellationToken);

    private static async ValueTask<long> ScanRawSourcesContinueAsync<TArg>(
        Source[] sources, int[] heap, int sourceCount, int initIndex, int winner, int duplicate, int state, int heapCount, long count,
        BlockCache? blockCache, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries, CancellationToken cancellationToken)
    {
        const bool backwards = false;
        try
        {
            if (state == 0)
            {
                for (var i = initIndex; i < sourceCount; i++)
                {
                    if ((i > initIndex && TryMoveNextSourceSync(sources, i, blockCache, cancellationToken, out var moved))
                        ? moved
                        : await MoveNextSourceCoreStaticAsync(sources, i, blockCache, cancellationToken).ConfigureAwait(false))
                    {
                        Push(sources, heap, ref heapCount, i, backwards);
                    }
                }
            }
            else if (state == 1)
            {
                if (await MoveNextSourceCoreStaticAsync(sources, duplicate, blockCache, cancellationToken).ConfigureAwait(false))
                {
                    Push(sources, heap, ref heapCount, duplicate, backwards);
                }
                while (heapCount != 0 && KeysEqual(sources[winner], sources[heap[0]]))
                {
                    var dup = Pop(sources, heap, ref heapCount, backwards);
                    if (TryMoveNextSourceSync(sources, dup, blockCache, cancellationToken, out var dupMoved)
                        ? dupMoved
                        : await MoveNextSourceCoreStaticAsync(sources, dup, blockCache, cancellationToken).ConfigureAwait(false))
                    {
                        Push(sources, heap, ref heapCount, dup, backwards);
                    }
                }
                var keepGoing = true;
                if (!sources[winner].IsTombstone)
                {
                    count++;
                    keepGoing = reader(arg, sources[winner].KeySpan, sources[winner].ValueSpan);
                }
                if (!keepGoing || count >= maxEntries)
                {
                    return count;
                }
                if (TryMoveNextSourceSync(sources, winner, blockCache, cancellationToken, out var winMoved)
                    ? winMoved
                    : await MoveNextSourceCoreStaticAsync(sources, winner, blockCache, cancellationToken).ConfigureAwait(false))
                {
                    Push(sources, heap, ref heapCount, winner, backwards);
                }
            }
            else if (state == 2)
            {
                if (await MoveNextSourceCoreStaticAsync(sources, winner, blockCache, cancellationToken).ConfigureAwait(false))
                {
                    Push(sources, heap, ref heapCount, winner, backwards);
                }
            }

            while (heapCount != 0 && count < maxEntries)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var w = Pop(sources, heap, ref heapCount, backwards);
                while (heapCount != 0 && KeysEqual(sources[w], sources[heap[0]]))
                {
                    var dup = Pop(sources, heap, ref heapCount, backwards);
                    if (TryMoveNextSourceSync(sources, dup, blockCache, cancellationToken, out var dupMoved)
                        ? dupMoved
                        : await MoveNextSourceCoreStaticAsync(sources, dup, blockCache, cancellationToken).ConfigureAwait(false))
                    {
                        Push(sources, heap, ref heapCount, dup, backwards);
                    }
                }

                var keepGoing = true;
                if (!sources[w].IsTombstone)
                {
                    count++;
                    keepGoing = reader(arg, sources[w].KeySpan, sources[w].ValueSpan);
                }

                if (!keepGoing || count >= maxEntries)
                {
                    break;
                }

                if (TryMoveNextSourceSync(sources, w, blockCache, cancellationToken, out var winMoved)
                    ? winMoved
                    : await MoveNextSourceCoreStaticAsync(sources, w, blockCache, cancellationToken).ConfigureAwait(false))
                {
                    Push(sources, heap, ref heapCount, w, backwards);
                }
            }

            return count;
        }
        finally
        {
            for (var i = 0; i < sourceCount; i++)
            {
                sources[i].DisposeSyncOrAsync();
                sources[i] = default;
            }

            ArrayPool<Source>.Shared.Return(sources);
            ArrayPool<int>.Shared.Return(heap);
        }
    }

    private static bool TryMoveNextSourceSync(Source[] sources, int sourceIndex, BlockCache? blockCache, CancellationToken cancellationToken, out bool moved)
    {
        ref var source = ref sources[sourceIndex];
        if (source.MemTable is not null)
        {
            moved = source.MoveNextMemTable();
            return true;
        }
        if (source.Entries != null)
        {
            moved = source.MoveNextList();
            return true;
        }
        if (source.Enumerator != null)
        {
            moved = false;
            return false;
        }
        if (source.HasBlock && source.Cursor.MoveNext())
        {
            moved = true;
            return true;
        }
        if (source.HasBlock)
        {
            source.DisposeBlock();
            source.BlockIndex += source.Step;
        }

        var table = source.Table!;
        while ((uint)source.BlockIndex < (uint)table.BlockMetadataArray.Length)
        {
            if (blockCache == null)
            {
                moved = false;
                return false;
            }
            if (!table.TryAcquireCachedBlock(source.BlockIndex, blockCache, cancellationToken, out source.Lease))
            {
                var leaseTask = table.ReadBlockCachedAsync(source.BlockIndex, blockCache, cancellationToken);
                if (!leaseTask.IsCompletedSuccessfully)
                {
                    moved = false;
                    return false;
                }
                source.Lease = leaseTask.GetAwaiter().GetResult();
            }

            var block = source.Lease.Block;
            if (block == null)
            {
                source.BlockIndex += source.Step;
                continue;
            }

            var hasSeek = source.SeekPending;
            var seek = hasSeek ? source.From.Span : default;
            source.SeekPending = false;
            source.Block = block;
            source.HasBlock = true;
            source.Cursor = new BlockIterator.Cursor(block, source.Step < 0, seek, hasSeek);

            if (source.Cursor.MoveNext())
            {
                moved = true;
                return true;
            }

            source.DisposeBlock();
            source.BlockIndex += source.Step;
        }

        moved = false;
        return true;
    }

    private static async ValueTask<bool> MoveNextSourceCoreStaticAsync(Source[] sources, int sourceIndex, BlockCache? blockCache, CancellationToken cancellationToken)
    {
        var enumerator = sources[sourceIndex].Enumerator;
        if (enumerator != null)
        {
            return await enumerator.MoveNextAsync().ConfigureAwait(false);
        }

        var table = sources[sourceIndex].Table!;
        while ((uint)sources[sourceIndex].BlockIndex < (uint)table.BlockMetadataArray.Length)
        {
            cancellationToken.ThrowIfCancellationRequested();
            Block? block;
            if (blockCache == null)
            {
                block = await table.ReadBlockAsync(sources[sourceIndex].BlockIndex, cancellationToken).ConfigureAwait(false);
                sources[sourceIndex].OwnsBlock = block != null;
            }
            else
            {
                sources[sourceIndex].Lease = await table.ReadBlockCachedAsync(
                    sources[sourceIndex].BlockIndex,
                    blockCache,
                    cancellationToken).ConfigureAwait(false);
                block = sources[sourceIndex].Lease.Block;
            }

            if (block == null)
            {
                sources[sourceIndex].BlockIndex += sources[sourceIndex].Step;
                continue;
            }

            var hasSeek = sources[sourceIndex].SeekPending;
            var seek = hasSeek ? sources[sourceIndex].From.Span : default;
            sources[sourceIndex].SeekPending = false;
            sources[sourceIndex].Block = block;
            sources[sourceIndex].HasBlock = true;
            sources[sourceIndex].Cursor = new BlockIterator.Cursor(block, sources[sourceIndex].Step < 0, seek, hasSeek);

            if (sources[sourceIndex].Cursor.MoveNext())
            {
                return true;
            }

            sources[sourceIndex].DisposeBlock();
            sources[sourceIndex].BlockIndex += sources[sourceIndex].Step;
        }

        return false;
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

    private ValueTask<bool> MoveNextSourceAsync(Source[] sources, int sourceIndex, CancellationToken cancellationToken)
    {
        return sources[sourceIndex].Range is null
            ? MoveNextSourceCoreAsync(sources, sourceIndex, cancellationToken)
            : MoveNextBoundedSourceAsync(sources, sourceIndex, cancellationToken);
    }

    private async ValueTask<bool> MoveNextBoundedSourceAsync(Source[] sources, int sourceIndex, CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!await MoveNextSourceCoreAsync(sources, sourceIndex, cancellationToken).ConfigureAwait(false))
            {
                return false;
            }
            var range = sources[sourceIndex].Range;
            if (range is null)
            {
                return true;
            }
            var before = range.Before(sources[sourceIndex].KeySpan);
            var after = range.After(sources[sourceIndex].KeySpan);
            if (sources[sourceIndex].Step < 0 ? before : after)
            {
                return false;
            }
            if (!before && !after)
            {
                return true;
            }
        }
    }

    private async ValueTask<bool> MoveNextSourceCoreAsync(Source[] sources, int sourceIndex, CancellationToken cancellationToken)
    {
        if (sources[sourceIndex].MemTable is not null)
        {
            return sources[sourceIndex].MoveNextMemTable();
        }
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
            var range = sources[sourceIndex].Range;
            var metadata = table.BlockMetadataArray;
            var blockIndex = sources[sourceIndex].BlockIndex;
            if (range is not null && (sources[sourceIndex].Step < 0
                ? range.Before(metadata.GetLastKeySpan(blockIndex))
                : range.After(metadata.GetFirstKeySpan(blockIndex))))
            {
                return false;
            }

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

            var hasSeek = sources[sourceIndex].SeekPending;
            var seek = hasSeek ? sources[sourceIndex].From.Span : default;
            sources[sourceIndex].SeekPending = false;
            sources[sourceIndex].Block = block;
            sources[sourceIndex].HasBlock = true;
            sources[sourceIndex].Cursor = new BlockIterator.Cursor(block, sources[sourceIndex].Step < 0, seek, hasSeek);

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

    internal struct Source
    {
        public OrderedReadRange? Range;
        public MemTable? MemTable;
        public StripedReaderWriterLock? MemTableLock;
        public LsmStorageInner? MemTableStorage;
        public byte[]? MemKey;
        public byte[]? MemValue;
        public int MemKeyLength;
        public int MemValueLength;
        public bool MemStarted;
        public bool MemTombstone;
        public int ValuePrefixLength;
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

        public MemTable.OrderedIndex.Cursor MemCursor;
        public bool MemHasCursor;

        public readonly ReadOnlySpan<byte> KeySpan =>
            MemTable != null ? MemKey.AsSpan(0, MemKeyLength) : Entries != null
                ? Entries[EntryIndex].Key.Span
                : Enumerator != null
                    ? Enumerator.Current.Key.Span
                    : Cursor.KeySpan;

        public readonly ReadOnlySpan<byte> ValueSpan =>
            MemTable != null ? MemValue.AsSpan(0, MemValueLength) : Entries != null
                ? Entries[EntryIndex].Value.Span
                : Enumerator != null
                    ? Enumerator.Current.Value.Span
                    : Cursor.ValueSpan;

        public readonly bool IsTombstone =>
            MemTable != null ? MemTombstone : Entries != null
                ? Entries[EntryIndex].Value.IsTombstone
                : Enumerator != null
                    ? Enumerator.Current.Value.IsTombstone
                    : Cursor.IsTombstone;

        public readonly KeyValuePair<ByteSlice, ByteSlice> Current =>
            MemTable != null
                ? new(ByteSlice.FromMemory(MemKey.AsMemory(0, MemKeyLength)),
                    MemTombstone ? ByteSlice.Tombstone : ByteSlice.FromMemory(MemValue.AsMemory(0, MemValueLength)))
                : Entries != null
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
            if (input.MemTable is not null)
            {
                return new Source
                {
                    MemTable = input.MemTable,
                    MemTableLock = input.MemTableLock,
                    MemTableStorage = input.MemTableStorage,
                    From = from,
                    SeekPending = hasFrom,
                    Step = backwards ? -1 : 1,
                    ValuePrefixLength = int.MaxValue,
                };
            }
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

        public bool MoveNextMemTable()
        {
            MemTableStorage?.FaultInjector?.Invoke(StorageFaultPoint.OrderedReadBeforeMemTableRead);
            MemTableLock!.EnterReadLock();
            try
            {
                MemTableStorage?.CheckHealthy();
                var from = MemStarted ? MemKey.AsSpan(0, MemKeyLength) : From.Span;
                var inclusive = !MemStarted && (Range is null || (Step < 0 ? Range.UpperInclusive : Range.LowerInclusive));
                if (!MemTable!.TryAdvanceRaw(ref MemCursor, ref MemHasCursor, from, MemStarted || SeekPending, inclusive, Step < 0,
                    out var key, out var value, out var tombstone))
                {
                    return false;
                }
                if (Range is not null && (Step < 0 ? Range.Before(key) : Range.After(key)))
                {
                    return false;
                }
                SeekPending = false;
                Copy(ref MemKey, key);
                var retainedValue = value[..Math.Min(value.Length, ValuePrefixLength)];
                Copy(ref MemValue, retainedValue);
                MemKeyLength = key.Length;
                MemValueLength = retainedValue.Length;
                MemTombstone = tombstone;
                MemStarted = true;
                return true;
            }
            finally
            {
                MemTableLock.ExitReadLock();
            }
        }

        private static void Copy(ref byte[]? buffer, ReadOnlySpan<byte> bytes)
        {
            if (buffer is null || buffer.Length < bytes.Length)
            {
                var replacement = ArrayPool<byte>.Shared.Rent(Math.Max(1, bytes.Length));
                if (buffer is not null)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                }
                buffer = replacement;
            }
            bytes.CopyTo(buffer);
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

        public void DisposeSyncOrAsync()
        {
            if (MemKey is not null)
            {
                ArrayPool<byte>.Shared.Return(MemKey);
                MemKey = null;
            }
            if (MemValue is not null)
            {
                ArrayPool<byte>.Shared.Return(MemValue);
                MemValue = null;
            }
            DisposeBlock();
            if (Enumerator != null)
            {
                Enumerator.DisposeAsync().AsTask().GetAwaiter().GetResult();
                Enumerator = null;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (MemKey is not null)
            {
                ArrayPool<byte>.Shared.Return(MemKey);
                MemKey = null;
            }
            if (MemValue is not null)
            {
                ArrayPool<byte>.Shared.Return(MemValue);
                MemValue = null;
            }
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
        private Input(MemTable memTable, StripedReaderWriterLock memTableLock, LsmStorageInner? storage)
        {
            MemTable = memTable;
            MemTableLock = memTableLock;
            MemTableStorage = storage;
        }
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
        public MemTable? MemTable { get; }
        public StripedReaderWriterLock? MemTableLock { get; }
        public LsmStorageInner? MemTableStorage { get; }

        public List<KeyValuePair<ByteSlice, ByteSlice>>? Entries { get; }

        public IStorageIterator? Iterator { get; }

        public static Input FromTable(SsTable table) => new(table, null, null);
        public static Input FromMemTable(MemTable table, StripedReaderWriterLock tableLock, LsmStorageInner? storage = null) =>
            new(table, tableLock, storage);

        public static Input FromEntries(List<KeyValuePair<ByteSlice, ByteSlice>> entries) => new(null, entries, null);

        public static Input FromIterator(IStorageIterator iterator) => new(null, null, iterator);
    }
}
