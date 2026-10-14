using System.Buffers;
using System.Diagnostics;

namespace Silex;

public sealed partial class MvccStorage
{
    private readonly object _garbageCollectionCursorOrigin = new();

    // Test seams: invoked after a chunk's obsolete keys are chosen and before they are deleted, and after the delete.
    internal Func<int, CancellationToken, ValueTask>? GarbageCollectionBeforeApply;
    internal Func<int, CancellationToken, ValueTask>? GarbageCollectionAfterApply;

    /// <summary>
    /// Deletes every physical version that is not visible to the published state or any active snapshot or
    /// transaction, running to completion in bounded chunks.
    /// </summary>
    /// <param name="compact">
    /// When <c>true</c>, flushes and compacts afterward so the version tombstones' storage can be reclaimed.
    /// </param>
    /// <returns>The number of obsolete physical versions deleted.</returns>
    /// <remarks>
    /// Equivalent to <see cref="CollectGarbageAsync(MvccGarbageCollectionOptions, CancellationToken)"/> with
    /// default limits. Commits and snapshot creation proceed between chunks.
    /// </remarks>
    public async ValueTask<long> CollectGarbageAsync(bool compact = false, CancellationToken cancellationToken = default)
    {
        var result = await CollectGarbageAsync(new MvccGarbageCollectionOptions { Compact = compact }, cancellationToken)
            .ConfigureAwait(false);
        return result.VersionsDeleted;
    }

    /// <summary>
    /// Deletes obsolete physical versions incrementally, within the limits of <paramref name="options"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Safety.</b> The set of required sequences is the active snapshots and transactions plus the published
    /// sequence <c>P</c>, read under the snapshot registry lock. A version is obsolete only if it is not the newest
    /// version at or below any required sequence. Commits only add versions above <c>P</c>, and every snapshot
    /// created later reads a sequence of at least <c>P</c>, so no later commit or snapshot can make a version that
    /// was obsolete at that point needed again. Versions above <c>P</c> (commits that began after the horizon was
    /// read) are never examined as obsolete, and the newest version or tombstone of every key is never deleted.
    /// The horizon is therefore valid without holding the commit gate while scanning; a stale horizon is merely
    /// conservative. Each chunk is scanned without the commit gate against the engine's ordinary ordered-read
    /// view, then its deletions are applied as one atomic batch while holding the gate only for that write.
    /// </para>
    /// <para>
    /// <b>Bounds.</b> Per chunk, at most <see cref="MvccGarbageCollectionOptions.ChunkSize"/> versions are
    /// examined and <see cref="MvccGarbageCollectionOptions.MaxChunkBytes"/> bytes of keys are buffered. Per call,
    /// examined/deleted counts and <see cref="MvccGarbageCollectionOptions.MaxDuration"/> are honored at chunk
    /// granularity (the time budget is also checked every 64 versions). Every call examines at least one version.
    /// </para>
    /// <para>
    /// <b>Cancellation, progress, resumption.</b> Cancellation throws <see cref="OperationCanceledException"/>
    /// at the next scan step or commit-gate wait; chunks already applied remain committed and valid, and the last
    /// chunk is either applied entirely or not at all. <see cref="MvccGarbageCollectionOptions.Progress"/> reports
    /// the resume cursor after each applied chunk, and a result's <c>Resume</c> continues where the call stopped.
    /// Resuming a cursor returned by this open instance uses verified scan history. Reconstructed or foreign
    /// cursors retain the remainder of the starting key group rather than inferring history from position bytes.
    /// Concurrent calls are serialized. Disposal waits for the current chunk and the call then fails with
    /// <see cref="ObjectDisposedException"/>.
    /// </para>
    /// <para>
    /// Optional <see cref="MvccGarbageCollectionOptions.Compact"/> runs without the commit gate and is not bounded.
    /// Pruning during compaction is deliberately not implemented: it would need the engine's compaction to
    /// consult the snapshot registry, which plain LSM users must not pay for.
    /// </para>
    /// </remarks>
    public async ValueTask<MvccGarbageCollectionResult> CollectGarbageAsync(
        MvccGarbageCollectionOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        CheckDisposed();
        await _garbageCollectionGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            CheckDisposed();
            return await TrackGarbageCollectionAsync(options, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _garbageCollectionGate.Release();
        }
    }

    private async ValueTask<MvccGarbageCollectionResult> RunGarbageCollectionAsync(
        MvccGarbageCollectionOptions options, CancellationToken cancellationToken)
    {
        var start = Stopwatch.GetTimestamp();
        var deadline = 0L;
        if (options.MaxDuration is { } budget)
        {
            var ticks = budget.TotalSeconds * Stopwatch.Frequency;
            deadline = ticks >= long.MaxValue - start ? 0 : start + (long)ticks;
        }

        using var state = new GarbageCollectionChunkState(options, deadline);
        var cursor = options.Resume;
        long examined = 0, deleted = 0;
        var reason = MvccGarbageCollectionStopReason.Completed;
        var chunk = 0;

        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            CheckDisposed();
            if (examined > 0 && deadline != 0 && Stopwatch.GetTimestamp() >= deadline)
            {
                reason = MvccGarbageCollectionStopReason.TimeBudget;
                break;
            }

            RefreshRequiredSequences(state);
            state.BeginChunk(cursor, _garbageCollectionCursorOrigin, examined, deleted);

            var lower = cursor is null ? new byte[] { DataPrefix } : cursor.PositionArray;
            var range = new OrderedReadRange(lower, cursor is null, new byte[] { DataPrefix + 1 }, false);
            await _storage.ScanOrderedAsync(range, OrderedReadDirection.Forward, state,
                static (s, key, _) => s.Accept(key), cancellationToken).ConfigureAwait(false);

            if (GarbageCollectionBeforeApply is { } before)
            {
                await before(chunk, cancellationToken).ConfigureAwait(false);
            }

            if (state.Batch.Count != 0)
            {
                await EnterCommitGateAsync(cancellationToken).ConfigureAwait(false);
                try
                {
                    CheckDisposed();
                    _storage.Write(state.Batch);
                }
                finally
                {
                    _commitGate.Release();
                }
            }

            examined += state.ChunkExamined;
            deleted += state.Batch.Count;
            RecordGarbageCollectionChunk(state.ChunkExamined, state.Batch.Count);
            var exhausted = state.Stop == GarbageCollectionStop.None;
            if (state.ChunkExamined != 0)
            {
                cursor = new MvccGarbageCollectionCursor(state.CopyLastKey(), _garbageCollectionCursorOrigin,
                    state.LastKeyPreserveGroup);
            }

            options.Progress?.Report(new MvccGarbageCollectionProgress(examined, deleted, exhausted ? null : cursor));
            if (GarbageCollectionAfterApply is { } after)
            {
                await after(chunk, cancellationToken).ConfigureAwait(false);
            }
            chunk++;

            if (exhausted)
            {
                cursor = null;
                break;
            }
            if (state.Stop == GarbageCollectionStop.Deleted)
            {
                reason = MvccGarbageCollectionStopReason.DeletedLimit;
                break;
            }
            if (state.Stop == GarbageCollectionStop.Time)
            {
                reason = MvccGarbageCollectionStopReason.TimeBudget;
                break;
            }
            if (examined >= options.MaxVersionsExamined)
            {
                reason = MvccGarbageCollectionStopReason.ExaminedLimit;
                break;
            }
        }

        var elapsed = Stopwatch.GetElapsedTime(start);
        var compacted = false;
        if (options.Compact && deleted != 0)
        {
            await _storage.FlushAndCompactAsync(cancellationToken).ConfigureAwait(false);
            compacted = true;
        }

        return new MvccGarbageCollectionResult(examined, deleted, reason, reason == MvccGarbageCollectionStopReason.Completed ? null : cursor, elapsed, compacted);
    }

    private void RefreshRequiredSequences(GarbageCollectionChunkState state)
    {
        lock (_snapshotLock)
        {
            state.SetRequired(_activeSnapshots.Keys, _activeSnapshots.Count, _transientPinSlots, Volatile.Read(ref _publishedSequence));
        }
    }

    private enum GarbageCollectionStop
    {
        None,
        Chunk,
        Deleted,
        Time,
    }

    private sealed class GarbageCollectionChunkState(MvccGarbageCollectionOptions options, long deadline) : IDisposable
    {
        private const int TimeCheckMask = 63;

        private readonly KeyGroup _group = new();
        private long[] _required = new long[8];
        private int _requiredCount;
        private int _nextRequired;
        private byte[] _lastKey = ArrayPool<byte>.Shared.Rent(64);
        private int _lastKeyLength;
        private long _passExamined;
        private long _passDeleted;
        private bool _preserveGroup;

        public LsmWriteBatch Batch { get; } = new();
        public int ChunkExamined { get; private set; }
        public GarbageCollectionStop Stop { get; private set; }
        public bool LastKeyPreserveGroup { get; private set; }

        // Sorted descending and distinct, always including the published sequence; element 0 is the largest.
        public void SetRequired(Dictionary<long, SnapshotEntry>.KeyCollection active, int activeCount, TransientPinSlot[] transientSlots, long published)
        {
            var maxPossible = activeCount + transientSlots.Length + 1;
            if (_required.Length < maxPossible)
            {
                _required = new long[Math.Max(maxPossible, _required.Length * 2)];
            }

            var n = 0;
            _required[n++] = published;
            foreach (var sequence in active)
            {
                _required[n++] = sequence;
            }
            for (var i = 0; i < transientSlots.Length; i++)
            {
                var seq = Volatile.Read(ref transientSlots[i].Sequence);
                if (seq >= 0)
                {
                    _required[n++] = seq;
                }
            }

            Array.Sort(_required, 0, n);
            Array.Reverse(_required, 0, n);
            var distinct = 1;
            for (var i = 1; i < n; i++)
            {
                if (_required[i] != _required[distinct - 1])
                {
                    _required[distinct++] = _required[i];
                }
            }

            _requiredCount = distinct;
        }

        public void BeginChunk(MvccGarbageCollectionCursor? cursor, object origin, long examined, long deleted)
        {
            _passExamined = examined;
            _passDeleted = deleted;
            ChunkExamined = 0;
            Stop = GarbageCollectionStop.None;
            Batch.Clear();
            _nextRequired = 0;
            _preserveGroup = cursor is not null &&
                (!ReferenceEquals(cursor.Origin, origin) || cursor.PreserveGroup);
            if (cursor is null)
            {
                _lastKeyLength = 0;
                return;
            }

            var cursorKey = cursor.PositionArray;
            long sequence;
            try
            {
                if (!TryParseVersionKey(cursorKey, out var encodedUserKey, out sequence))
                {
                    throw new ArgumentException("The cursor is not an MVCC version position.", nameof(options));
                }
                _group.Set(encodedUserKey);
            }
            catch (InvalidDataException exception)
            {
                throw new ArgumentException("The cursor is not an MVCC version position.", nameof(options), exception);
            }

            // Only a locally issued, immutable cursor proves that this store's preceding versions were scanned.
            // Unverified positions may precede the newest version; keep that entire group across resumptions.
            while (!_preserveGroup && _nextRequired < _requiredCount && _required[_nextRequired] >= sequence)
            {
                _nextRequired++;
            }
            CopyLastKey(cursorKey);
        }

        public bool Accept(ReadOnlySpan<byte> key)
        {
            if (!TryParseVersionKey(key, out var encodedUserKey, out var sequence))
            {
                return true;
            }

            var done = _passExamined + ChunkExamined;
            if (ChunkExamined >= options.ChunkSize || done >= options.MaxVersionsExamined)
            {
                Stop = GarbageCollectionStop.Chunk;
                return false;
            }
            if (deadline != 0 && done != 0 && (done & TimeCheckMask) == 0 && Stopwatch.GetTimestamp() >= deadline)
            {
                Stop = GarbageCollectionStop.Time;
                return false;
            }

            if (_group.Set(encodedUserKey))
            {
                _nextRequired = 0;
                _preserveGroup = false;
            }

            var keep = _preserveGroup;
            while (_nextRequired < _requiredCount && sequence <= _required[_nextRequired])
            {
                keep = true;
                _nextRequired++;
            }

            // Versions above the horizon were committed after the required set was read; never reclaim them.
            if (!keep && sequence > _required[0])
            {
                keep = true;
            }

            if (!keep)
            {
                if (_passDeleted + Batch.Count >= options.MaxVersionsDeleted)
                {
                    Stop = GarbageCollectionStop.Deleted;
                    return false;
                }
                if (Batch.Count != 0 && (long)Batch.ByteCount + key.Length > options.MaxChunkBytes)
                {
                    Stop = GarbageCollectionStop.Chunk;
                    return false;
                }
                Batch.Delete(key);
            }

            ChunkExamined++;
            CopyLastKey(key);
            return true;
        }

        public byte[] CopyLastKey() => _lastKey.AsSpan(0, _lastKeyLength).ToArray();

        private void CopyLastKey(ReadOnlySpan<byte> key)
        {
            if (_lastKey.Length < key.Length)
            {
                ArrayPool<byte>.Shared.Return(_lastKey);
                _lastKey = ArrayPool<byte>.Shared.Rent(key.Length);
            }

            key.CopyTo(_lastKey);
            _lastKeyLength = key.Length;
            LastKeyPreserveGroup = _preserveGroup;
        }

        public void Dispose()
        {
            _group.Dispose();
            ArrayPool<byte>.Shared.Return(_lastKey);
        }
    }
}
