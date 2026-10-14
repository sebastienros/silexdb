using Silex.MemTables;
using Silex.Tables;
using System.Buffers;

namespace Silex;

internal interface IRawEntrySink
{
    void Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value);
}
internal sealed partial class LsmStorageInner
{
    internal bool TryReadFirstRawSync<TSink>(
        ReadOnlySpan<byte> from, ReadOnlySpan<byte> prefix, ref TSink sink,
        CancellationToken cancellationToken)
        where TSink : struct, IRawEntrySink
    {
        CheckHealthy();
        cancellationToken.ThrowIfCancellationRequested();

        using var operation = Lifetime.Enter();

        // Fast path 1: when all user data is in a globally sorted SST run (and no writes are in MemTable),
        // avoid _currentMemTableLock completely and binary-search the cached SST run under a striped read lock.
        var cachedTables = Volatile.Read(ref _sortedSsTableRun) ?? Volatile.Read(ref _sortedMvccSsTableRun);
        if (cachedTables is not null && _level0Lock.TryEnterReadLock(out var stripeSlot))
        {
            try
            {
                CheckHealthy();
                cachedTables = Volatile.Read(ref _sortedSsTableRun) ?? Volatile.Read(ref _sortedMvccSsTableRun);
                if (cachedTables is not null &&
                    TryReadFirstSortedSync(cachedTables, from, prefix, ref sink, cancellationToken))
                {
                    return true;
                }
            }
            finally
            {
                _level0Lock.ExitReadLock(stripeSlot);
            }
        }

        // Fast path 2: check active and immutable memtables before acquiring _level0Lock.
        _currentMemTableLock.EnterReadLock();
        try
        {
            CheckHealthy();
            if (TryReadLiveMemTableRaw(from, prefix, ref sink, out var hitTombstone) && !hitTombstone)
            {
                return true;
            }
        }
        finally
        {
            _currentMemTableLock.ExitReadLock();
        }

        if (_level0Lock.TryEnterReadLock(out var stripeSlot2))
        {
            try
            {
                CheckHealthy();
                _currentMemTableLock.EnterReadLock();
                try
                {
                    CheckHealthy();
                    var hasTables = _state.LevelZeroTables.Count != 0;
                    if (!hasTables)
                    {
                        foreach (var level in _state.LeveledSsTables)
                        {
                            if (level.Count != 0)
                            {
                                hasTables = true;
                                break;
                            }
                        }
                    }
                    if (!hasTables)
                    {
                        sink = ReadFirstMemTableRaw(from, prefix, sink, cancellationToken);
                        return true;
                    }
                }
                finally
                {
                    _currentMemTableLock.ExitReadLock();
                }

                cachedTables = Volatile.Read(ref _sortedSsTableRun) ?? Volatile.Read(ref _sortedMvccSsTableRun);
                if (cachedTables is not null &&
                    TryReadFirstSortedSync(cachedTables, from, prefix, ref sink, cancellationToken))
                {
                    return true;
                }
                if (cachedTables is null &&
                    TryGetGloballySortedMvccSsTableRun(out var tables) &&
                    TryReadFirstSortedSync(tables, from, prefix, ref sink, cancellationToken))
                {
                    return true;
                }
            }
            finally
            {
                _level0Lock.ExitReadLock(stripeSlot2);
            }
        }

        return false;
    }

    // Version keys have a fixed-width suffix after their escaped, terminated user-key prefix.
    internal ValueTask<TSink> ReadFirstRawAsync<TSink>(
        ReadOnlyMemory<byte> from, ReadOnlyMemory<byte> prefix, TSink sink,
        CancellationToken cancellationToken, int valuePrefixLength)
        where TSink : struct, IRawEntrySink
    {
        if (TryReadFirstRawSync(from.Span, prefix.Span, ref sink, cancellationToken))
        {
            return ValueTask.FromResult(sink);
        }

        return ReadFirstRawSlowAsync(from, prefix, sink, cancellationToken, valuePrefixLength);
    }

    private bool TryReadLiveMemTableRaw<TSink>(
        ReadOnlySpan<byte> from, ReadOnlySpan<byte> prefix, ref TSink sink, out bool hitTombstone)
        where TSink : struct, IRawEntrySink
    {
        ReadOnlySpan<byte> bestKey = default;
        ReadOnlySpan<byte> bestValue = default;
        var found = false;
        var tombstone = false;

        Consider((MemTable)_state.CurrentMemTable, from, prefix, true,
            ref bestKey, ref bestValue, ref found, ref tombstone);
        if (!_state.ImmutableMemTables.IsEmpty)
        {
            _immutableMemTablesLock.EnterReadLock();
            try
            {
                for (var i = _state.ImmutableMemTables.Length - 1; i >= 0; i--)
                {
                    Consider((MemTable)_state.ImmutableMemTables[i], from, prefix, true,
                        ref bestKey, ref bestValue, ref found, ref tombstone);
                }
            }
            finally
            {
                _immutableMemTablesLock.ExitReadLock();
            }
        }

        hitTombstone = tombstone;
        if (!found)
        {
            return false;
        }
        if (!tombstone)
        {
            sink.Accept(bestKey, bestValue);
        }
        return true;

        static void Consider(MemTable table, ReadOnlySpan<byte> from, ReadOnlySpan<byte> prefix, bool inclusive,
            ref ReadOnlySpan<byte> bestKey, ref ReadOnlySpan<byte> bestValue, ref bool found, ref bool tombstone)
        {
            if (table.Count != 0 &&
                table.TryGetNextVersionRaw(from, prefix, inclusive, out var key, out var value, out var deleted) &&
                (!found || key.SequenceCompareTo(bestKey) < 0))
            {
                bestKey = key;
                bestValue = value;
                found = true;
                tombstone = deleted;
            }
        }
    }

    private bool TryReadFirstSortedSync<TSink>(
        IReadOnlyList<SsTable> tables, ReadOnlySpan<byte> from, ReadOnlySpan<byte> prefix,
        ref TSink sink, CancellationToken cancellationToken)
        where TSink : struct, IRawEntrySink
    {
        if (_blockCache is null)
        {
            return false;
        }
        var startTable = FindStartTableIndex(tables, from);
        for (var t = startTable; t < tables.Count; t++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var table = tables[t];
            var metadata = table.BlockMetadataArray;
            int startBlock;
            if (t == startTable)
            {
                startBlock = metadata.FindStartBlockIndex(from);
                if (metadata.GetLastKeySpan(startBlock).SequenceCompareTo(from) < 0)
                {
                    startBlock++;
                }
            }
            else
            {
                if (table.FirstKey.Span.SequenceCompareTo(from) > 0 &&
                    !table.FirstKey.Span.StartsWith(prefix))
                {
                    return true;
                }
                startBlock = 0;
            }

            var checkedPrefixBloom = false;
            for (var b = startBlock; b < metadata.Length; b++)
            {
                if (!table.TryAcquireCachedBlock(b, _blockCache, cancellationToken, out var lease))
                {
                    if (!checkedPrefixBloom)
                    {
                        if (!table.MayContainVersionPrefix(prefix))
                        {
                            break;
                        }
                        checkedPrefixBloom = true;
                    }
                    var leaseTask = table.ReadBlockCachedAsync(b, _blockCache, cancellationToken);
                    if (!leaseTask.IsCompletedSuccessfully)
                    {
                        return false;
                    }
                    lease = leaseTask.GetAwaiter().GetResult();
                }

                using (lease)
                {
                    var block = lease.Block;
                    if (block is null)
                    {
                        continue;
                    }
                    if (!block.TryReadFirstMatchingPrefix(from, prefix, ref sink))
                    {
                        return true;
                    }
                }
            }
        }
        return true;
    }

    private async ValueTask<TSink> ReadFirstRawSlowAsync<TSink>(
        ReadOnlyMemory<byte> from, ReadOnlyMemory<byte> prefix, TSink sink,
        CancellationToken cancellationToken, int valuePrefixLength)
        where TSink : struct, IRawEntrySink
    {
        using var operation = Lifetime.Enter();
        CheckHealthy();
        cancellationToken.ThrowIfCancellationRequested();
        await _level0Lock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            CheckHealthy();
            var cachedTables = Volatile.Read(ref _sortedSsTableRun) ?? Volatile.Read(ref _sortedMvccSsTableRun);
            if (cachedTables is not null)
            {
                return await ReadFirstSortedRawAsync(cachedTables, from, prefix, sink, cancellationToken)
                    .ConfigureAwait(false);
            }

            _currentMemTableLock.EnterReadLock();
            try
            {
                CheckHealthy();
                var hasTables = _state.LevelZeroTables.Count != 0;
                foreach (var level in _state.LeveledSsTables)
                {
                    hasTables |= level.Count != 0;
                }
                if (!hasTables)
                {
                    return ReadFirstMemTableRaw(from.Span, prefix.Span, sink, cancellationToken);
                }
            }
            finally
            {
                _currentMemTableLock.ExitReadLock();
            }

            if (TryGetGloballySortedMvccSsTableRun(out var tables))
            {
                return await ReadFirstSortedRawAsync(tables, from, prefix, sink, cancellationToken)
                    .ConfigureAwait(false);
            }
            return await ReadFirstMixedRawAsync(from, prefix, sink, cancellationToken, valuePrefixLength)
                .ConfigureAwait(false);
        }
        finally
        {
            _level0Lock.ExitReadLock();
        }
    }

    // Both locks are held: mutable bytes are stable and frozen tables cannot be retired during lookup.
    private TSink ReadFirstMemTableRaw<TSink>(
        ReadOnlySpan<byte> from, ReadOnlySpan<byte> prefix, TSink sink, CancellationToken cancellationToken)
        where TSink : struct, IRawEntrySink
    {
        _immutableMemTablesLock.EnterReadLock();
        try
        {
            var inclusive = true;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                ReadOnlySpan<byte> bestKey = default;
                ReadOnlySpan<byte> bestValue = default;
                var found = false;
                var tombstone = false;
                Consider((MemTable)_state.CurrentMemTable, from, prefix, inclusive,
                    ref bestKey, ref bestValue, ref found, ref tombstone);
                for (var i = _state.ImmutableMemTables.Length - 1; i >= 0; i--)
                {
                    Consider((MemTable)_state.ImmutableMemTables[i], from, prefix, inclusive,
                        ref bestKey, ref bestValue, ref found, ref tombstone);
                }

                if (!found)
                {
                    return sink;
                }
                if (!tombstone)
                {
                    sink.Accept(bestKey, bestValue);
                    return sink;
                }

                // A physical deletion shadows the same version in older sources, not the next version.
                from = bestKey;
                inclusive = false;
            }
        }
        finally
        {
            _immutableMemTablesLock.ExitReadLock();
        }

        static void Consider(MemTable table, ReadOnlySpan<byte> from, ReadOnlySpan<byte> prefix, bool inclusive,
            ref ReadOnlySpan<byte> bestKey, ref ReadOnlySpan<byte> bestValue, ref bool found, ref bool tombstone)
        {
            if (table.Count != 0 &&
                table.TryGetNextVersionRaw(from, prefix, inclusive, out var key, out var value, out var deleted) &&
                (!found || key.SequenceCompareTo(bestKey) < 0))
            {
                bestKey = key;
                bestValue = value;
                found = true;
                tombstone = deleted;
            }
        }
    }

    private async ValueTask<TSink> ReadFirstSortedRawAsync<TSink>(
        IReadOnlyList<SsTable> tables, ReadOnlyMemory<byte> from, ReadOnlyMemory<byte> prefix,
        TSink sink, CancellationToken cancellationToken)
        where TSink : struct, IRawEntrySink
    {
        var startTable = FindStartTableIndex(tables, from.Span);
        for (var t = startTable; t < tables.Count; t++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var table = tables[t];
            if (table.FirstKey.Span.SequenceCompareTo(from.Span) > 0 &&
                !table.FirstKey.Span.StartsWith(prefix.Span))
            {
                return sink;
            }
            if (!table.MayContainVersionPrefix(prefix.Span))
            {
                continue;
            }
            var metadata = table.BlockMetadataArray;
            var startBlock = t == startTable ? metadata.FindStartBlockIndex(from.Span) : 0;
            if (metadata.GetLastKeySpan(startBlock).SequenceCompareTo(from.Span) < 0)
            {
                startBlock++;
            }
            for (var b = startBlock; b < metadata.Length; b++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                using var lease = await table.ReadBlockCachedAsync(b, _blockCache, cancellationToken)
                    .ConfigureAwait(false);
                var block = lease.Block;
                if (block is null)
                {
                    continue;
                }
                var start = block.LowerBound(from.Span);
                for (var i = start; i < block.Offsets.Count; i++)
                {
                    block.GetRawEntry(i, out var key, out var value, out var tombstone);
                    if (!key.Span.StartsWith(prefix.Span))
                    {
                        return sink;
                    }
                    if (!tombstone)
                    {
                        sink.Accept(key.Span, value.Span);
                        return sink;
                    }
                }
            }
        }
        return sink;
    }

    private async ValueTask<TSink> ReadFirstMixedRawAsync<TSink>(
        ReadOnlyMemory<byte> from, ReadOnlyMemory<byte> prefix, TSink sink,
        CancellationToken cancellationToken, int valuePrefixLength)
        where TSink : struct, IRawEntrySink
    {
        var lower = ArrayPool<byte>.Shared.Rent(from.Length);
        var candidate = default(PointCandidate);
        try
        {
            candidate = new PointCandidate(from.Length, valuePrefixLength);
            from.Span.CopyTo(lower);
            var bound = lower.AsMemory(0, from.Length);
            var inclusive = true;
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                candidate.Found = false;
                _currentMemTableLock.EnterReadLock();
                try
                {
                    CheckHealthy();
                    Consider((MemTable)_state.CurrentMemTable, bound.Span, inclusive, prefix.Span, ref candidate);
                    _immutableMemTablesLock.EnterReadLock();
                    try
                    {
                        for (var i = _state.ImmutableMemTables.Length - 1; i >= 0; i--)
                            Consider((MemTable)_state.ImmutableMemTables[i], bound.Span, inclusive, prefix.Span, ref candidate);
                    }
                    finally
                    {
                        _immutableMemTablesLock.ExitReadLock();
                    }
                }
                finally
                {
                    _currentMemTableLock.ExitReadLock();
                }
                for (var i = _state.LevelZeroTables.Count - 1; i >= 0; i--)
                    candidate = await ReadTableCandidateAsync(_state.LevelZeroTables[i], bound, prefix, inclusive,
                        candidate, cancellationToken).ConfigureAwait(false);
                foreach (var level in _state.LeveledSsTables)
                {
                    foreach (var table in level)
                        candidate = await ReadTableCandidateAsync(table, bound, prefix, inclusive,
                            candidate, cancellationToken).ConfigureAwait(false);
                }
                if (!candidate.Found) return sink;
                if (!candidate.Tombstone)
                {
                    sink.Accept(candidate.Key, candidate.Value);
                    return sink;
                }
                // Skip this exact physical key in every source, retaining older visible versions.
                candidate.Key.CopyTo(lower);
                inclusive = false;
            }
        }
        finally
        {
            candidate.Dispose();
            ArrayPool<byte>.Shared.Return(lower);
        }

        static void Consider(MemTable table, ReadOnlySpan<byte> bound, bool inclusive,
            ReadOnlySpan<byte> prefix, ref PointCandidate candidate)
        {
            if (table.Count != 0 && table.TryGetNextVersionRaw(bound, prefix, inclusive,
                out var key, out var value, out var tombstone))
                candidate.Consider(key, value, tombstone);
        }
    }

    private async ValueTask<PointCandidate> ReadTableCandidateAsync(
        SsTable table, ReadOnlyMemory<byte> from, ReadOnlyMemory<byte> prefix, bool inclusive,
        PointCandidate candidate, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (table.LastKey.Span.SequenceCompareTo(from.Span) < 0 ||
            table.FirstKey.Span.SequenceCompareTo(from.Span) > 0 && !table.FirstKey.Span.StartsWith(prefix.Span) ||
            candidate.Found && table.FirstKey.Span.SequenceCompareTo(candidate.Key) >= 0 ||
            !table.MayContainVersionPrefix(prefix.Span))
            return candidate;
        var metadata = table.BlockMetadataArray;
        var startBlock = metadata.FindStartBlockIndex(from.Span);
        if (metadata.GetLastKeySpan(startBlock).SequenceCompareTo(from.Span) < 0) startBlock++;
        for (var b = startBlock; b < metadata.Length; b++)
        {
            using var lease = await table.ReadBlockCachedAsync(b, _blockCache, cancellationToken).ConfigureAwait(false);
            var block = lease.Block;
            if (block is null) continue;
            for (var i = block.LowerBound(from.Span); i < block.Offsets.Count; i++)
            {
                block.GetRawEntry(i, out var key, out var value, out var tombstone);
                if (!inclusive && key.Span.SequenceEqual(from.Span)) continue;
                if (key.Span.StartsWith(prefix.Span)) candidate.Consider(key.Span, value.Span, tombstone);
                return candidate;
            }
        }
        return candidate;
    }

    private struct PointCandidate : IDisposable
    {
        private readonly byte[]? _key;
        private byte[]? _value;
        private int _keyLength;
        private int _valueLength;
        private readonly int _valuePrefixLength;
        public bool Found;
        public bool Tombstone;
        public readonly ReadOnlySpan<byte> Key => _key.AsSpan(0, _keyLength);
        public readonly ReadOnlySpan<byte> Value => _value.AsSpan(0, _valueLength);

        public PointCandidate(int keyLength, int valuePrefixLength)
        {
            _key = ArrayPool<byte>.Shared.Rent(keyLength);
            _valuePrefixLength = valuePrefixLength;
        }

        public void Consider(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool tombstone)
        {
            if (Found && key.SequenceCompareTo(Key) >= 0) return;
            key.CopyTo(_key);
            _keyLength = key.Length;
            _valueLength = tombstone ? 0 : Math.Min(value.Length, _valuePrefixLength);
            if (_valueLength != 0)
            {
                if (_value is null || _value.Length < _valueLength)
                {
                    var replacement = ArrayPool<byte>.Shared.Rent(_valueLength);
                    if (_value is not null) ArrayPool<byte>.Shared.Return(_value);
                    _value = replacement;
                }
                value[.._valueLength].CopyTo(_value);
            }
            Found = true;
            Tombstone = tombstone;
        }

        public void Dispose()
        {
            if (_key is not null) ArrayPool<byte>.Shared.Return(_key);
            if (_value is not null) ArrayPool<byte>.Shared.Return(_value);
        }
    }
}
