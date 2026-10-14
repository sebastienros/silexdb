using System.Threading.Tasks.Sources;

namespace Silex;

public sealed partial class MvccStorage
{
    private sealed class CommitCoordinator
    {
        private const int HistoryRingSize = 256;
        private const int HistoryRingMask = HistoryRingSize - 1;
        private const int MaxInlineHistoryHashes = 8;

        private readonly MvccStorage _owner;
        private readonly bool _syncWal;
        private readonly object _lock = new();
        private readonly SemaphoreSlim _slots;
        private readonly CommitRequest[] _ring;
        private readonly Stack<CommitRequest> _freePool;
        private readonly CommitRequest?[] _requests;
        private readonly CommitRequest?[] _accepted;
        private readonly LsmWriteBatch[] _batches;
        private readonly StorageCommitOutcome[] _outcomes;
        private readonly CommitHistoryEntry[] _history = new CommitHistoryEntry[HistoryRingSize];
        private long _minHistorySequence = long.MaxValue;
        private long _maxHistorySequence;
        private int _head;
        private int _tail;
        private int _count;
        private Task? _worker;
        private bool _stopped;

        public CommitCoordinator(MvccStorage owner, int groupSize, bool syncWal)
        {
            _owner = owner;
            _syncWal = syncWal;
            var capacity = groupSize * 2;
            _ring = new CommitRequest[capacity];
            _freePool = new Stack<CommitRequest>(capacity);
            for (var i = 0; i < capacity; i++)
            {
                _freePool.Push(new CommitRequest(this));
            }
            _slots = new SemaphoreSlim(capacity, capacity);
            _requests = new CommitRequest[groupSize];
            _accepted = new CommitRequest[groupSize];
            _batches = new LsmWriteBatch[groupSize];
            _outcomes = new StorageCommitOutcome[groupSize];
            for (var i = 0; i < groupSize; i++)
            {
                _batches[i] = new LsmWriteBatch();
            }
        }

        public ValueTask<MvccConflict?> CommitAsync(long sequence, MvccWriteBuffer writes, CancellationToken cancellationToken)
        {
            if (!_syncWal && Volatile.Read(ref _count) == 0 && _owner._commitGate.Wait(0))
            {
                bool canRunInline;
                lock (_lock)
                {
                    canRunInline = !_stopped && _count == 0 && _worker is null;
                }
                if (canRunInline)
                {
                    return CommitInlineLeaderAsync(sequence, writes, cancellationToken);
                }
                _owner._commitGate.Release();
            }

            return CommitQueuedAsync(sequence, writes, cancellationToken);
        }

        private ValueTask<MvccConflict?> CommitInlineLeaderAsync(long sequence, MvccWriteBuffer writes, CancellationToken cancellationToken)
        {
            var releaseInFinally = true;
            try
            {
                _owner.CheckDisposed();
                cancellationToken.ThrowIfCancellationRequested();
                var nextSeq = _owner._allocatedSequence;
                if (writes.HasVersionstamps)
                {
                    if (nextSeq == long.MaxValue)
                    {
                        throw new InvalidOperationException("The MVCC sequence space is exhausted.");
                    }
                    writes.ResolveVersionstamps(nextSeq + 1);
                }
                if (!CanSkipStorageValidation(sequence, writes))
                {
                    releaseInFinally = false;
                    return CommitInlineLeaderSlowAsync(sequence, writes, cancellationToken);
                }
                if (writes.MutationCount == 0)
                {
                    return ValueTask.FromResult<MvccConflict?>(null);
                }
                if (nextSeq == long.MaxValue)
                {
                    throw new InvalidOperationException("The MVCC sequence space is exhausted.");
                }
                var commitSeq = nextSeq + 1;
                var batch = _owner._commitBatch;
                PrepareBatch(writes, batch, commitSeq);
                _owner._storage.Write(batch);
                _owner._storage.BeforeMvccPublication();
                RecordCommittedHistory(commitSeq, writes);
                _owner._allocatedSequence = commitSeq;
                Volatile.Write(ref _owner._publishedSequence, commitSeq);
                Interlocked.Increment(ref _owner._commitGroups);
                Interlocked.Increment(ref _owner._groupedCommits);
                writes.CommitSequence = commitSeq;
                return ValueTask.FromResult<MvccConflict?>(null);
            }
            finally
            {
                if (releaseInFinally)
                {
                    ReleaseInlineLeader();
                }
            }
        }

        private async ValueTask<MvccConflict?> CommitInlineLeaderSlowAsync(long sequence, MvccWriteBuffer writes, CancellationToken cancellationToken)
        {
            try
            {
                var conflict = await _owner.ValidateAsync(sequence, writes, cancellationToken).ConfigureAwait(false);
                if (conflict is not null || writes.MutationCount == 0)
                {
                    return conflict;
                }
                var nextSeq = _owner._allocatedSequence;
                if (nextSeq == long.MaxValue)
                {
                    throw new InvalidOperationException("The MVCC sequence space is exhausted.");
                }
                var commitSeq = nextSeq + 1;
                var batch = _owner._commitBatch;
                PrepareBatch(writes, batch, commitSeq);
                _owner._storage.Write(batch);
                _owner._storage.BeforeMvccPublication();
                RecordCommittedHistory(commitSeq, writes);
                _owner._allocatedSequence = commitSeq;
                Volatile.Write(ref _owner._publishedSequence, commitSeq);
                Interlocked.Increment(ref _owner._commitGroups);
                Interlocked.Increment(ref _owner._groupedCommits);
                writes.CommitSequence = commitSeq;
                return null;
            }
            finally
            {
                ReleaseInlineLeader();
            }
        }

        private void ReleaseInlineLeader()
        {
            lock (_lock)
            {
                if (_count > 0 && !_stopped)
                {
                    _worker ??= Task.Run(RunAsync);
                }
            }
            _owner._commitGate.Release();
        }

        private async ValueTask<MvccConflict?> CommitQueuedAsync(long sequence, MvccWriteBuffer writes, CancellationToken cancellationToken)
        {
            await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                CommitRequest request;
                short version;
                lock (_lock)
                {
                    _owner.CheckDisposed();
                    ObjectDisposedException.ThrowIf(_stopped, _owner);
                    cancellationToken.ThrowIfCancellationRequested();
                    request = _freePool.Pop();
                    version = request.Initialize(sequence, writes, cancellationToken);
                    _ring[_tail] = request;
                    _tail = (_tail + 1) % _ring.Length;
                    _count++;
                    _worker ??= Task.Run(RunAsync);
                }
                return await request.WaitAsync(version).ConfigureAwait(false);
            }
            finally
            {
                _slots.Release();
            }
        }

        private void ReturnRequest(CommitRequest request)
        {
            lock (_lock)
            {
                _freePool.Push(request);
            }
        }

        public int PendingCount
        {
            get
            {
                lock (_lock)
                {
                    return _count;
                }
            }
        }

        public Task StopAsync()
        {
            lock (_lock)
            {
                _stopped = true;
                return _worker ?? Task.CompletedTask;
            }
        }

        private async Task RunAsync()
        {
            while (true)
            {
                await _owner.EnterCommitGateAsync(CancellationToken.None).ConfigureAwait(false);
                try
                {
                    int count;
                    lock (_lock)
                    {
                        count = Math.Min(_count, _requests.Length);
                        if (count == 0)
                        {
                            _worker = null;
                            return;
                        }
                        for (var i = 0; i < count; i++)
                        {
                            _requests[i] = _ring[_head];
                            _ring[_head] = null!;
                            _head = (_head + 1) % _ring.Length;
                        }
                        _count -= count;
                    }
                    try
                    {
                        await CommitGroupAsync(count).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        for (var i = 0; i < count; i++)
                        {
                            _requests[i]!.TrySetException(exception);
                        }
                    }
                    finally
                    {
                        Array.Clear(_requests, 0, count);
                        Array.Clear(_accepted, 0, count);
                    }
                }
                finally
                {
                    _owner._commitGate.Release();
                }
            }
        }

        private async Task CommitGroupAsync(int count)
        {
            var acceptedCount = 0;
            var sequence = _owner._allocatedSequence;
            for (var i = 0; i < count; i++)
            {
                var request = _requests[i]!;
                try
                {
                    _owner.CheckDisposed();
                    request.CancellationToken.ThrowIfCancellationRequested();
                    if (request.Writes.HasVersionstamps)
                    {
                        if (sequence == long.MaxValue)
                        {
                            throw new InvalidOperationException("The MVCC sequence space is exhausted.");
                        }
                        request.Writes.ResolveVersionstamps(sequence + 1);
                    }
                    var conflict = ConflictsWithAccepted(request, acceptedCount) ??
                        (CanSkipStorageValidation(request)
                            ? null
                            : await _owner.ValidateAsync(request.Sequence, request.Writes, request.CancellationToken).ConfigureAwait(false));
                    if (conflict is not null)
                    {
                        request.SetResult(conflict);
                        continue;
                    }
                    if (request.Writes.MutationCount == 0)
                    {
                        request.CancellationToken.ThrowIfCancellationRequested();
                        request.SetResult(null);
                        continue;
                    }
                    if (sequence == long.MaxValue)
                    {
                        throw new InvalidOperationException("The MVCC sequence space is exhausted.");
                    }
                    PrepareBatch(request.Writes, _batches[acceptedCount], sequence + 1);
                    request.CancellationToken.ThrowIfCancellationRequested();
                    // Acceptance ends cancellation: all accepted frames must finish as one durability group.
                    request.CommitSequence = ++sequence;
                    _accepted[acceptedCount] = request;
                    _outcomes[acceptedCount++] = StorageCommitOutcome.NotCommitted;
                }
                catch (OperationCanceledException exception)
                {
                    request.SetException(exception);
                }
                catch (Exception exception)
                {
                    request.SetException(exception);
                }
            }

            if (acceptedCount == 0)
            {
                return;
            }
            try
            {
                _owner._storage.WriteGroup(_batches, _outcomes, acceptedCount);
                for (var i = 0; i < acceptedCount; i++)
                {
                    _owner._storage.BeforeMvccPublication();
                    var commitSeq = _accepted[i]!.CommitSequence;
                    RecordCommittedHistory(commitSeq, _accepted[i]!.Writes);
                    _owner._allocatedSequence = commitSeq;
                    Volatile.Write(ref _owner._publishedSequence, commitSeq);
                }
                Interlocked.Increment(ref _owner._commitGroups);
                Interlocked.Add(ref _owner._groupedCommits, acceptedCount);
                for (var i = 0; i < acceptedCount; i++)
                {
                    _accepted[i]!.Writes.CommitSequence = _accepted[i]!.CommitSequence;
                    _accepted[i]!.SetResult(null);
                }
            }
            catch (Exception exception)
            {
                var cause = exception is StorageOperationException failure ? failure.InnerException! : exception;
                for (var i = 0; i < acceptedCount; i++)
                {
                    _accepted[i]!.SetException(new StorageOperationException(_outcomes[i], cause));
                }
            }
        }

        private MvccConflict? ConflictsWithAccepted(CommitRequest request, int acceptedCount)
        {
            for (var i = 0; i < acceptedCount; i++)
            {
                var accepted = _accepted[i]!;
                var writes = accepted.Writes;
                for (var j = 0; j < writes.MutationCount; j++)
                {
                    var key = writes.GetMutationKey(j).Span;
                    if (request.Writes.FindMutation(key) >= 0)
                    {
                        return new MvccConflict(MvccConflictKind.WrittenKey, key, request.Sequence, accepted.CommitSequence);
                    }
                    if (request.Writes.FindTrackedRead(key) >= 0)
                    {
                        return new MvccConflict(MvccConflictKind.TrackedPointRead, key, request.Sequence, accepted.CommitSequence);
                    }
                    var rangeIndex = request.Writes.FindReadRange(key);
                    if (rangeIndex >= 0)
                    {
                        return new MvccConflict(MvccConflictKind.TrackedRange, key, request.Sequence,
                            accepted.CommitSequence, request.Writes.GetReadRange(rangeIndex));
                    }
                }
            }
            return null;
        }

        internal void RecordSingleCommit(long sequence, ReadOnlySpan<byte> key)
        {
            var hash = HashKey(key);
            var bloom = BloomMask(hash);
            ref var slot = ref _history[sequence & HistoryRingMask];
            slot.Sequence = sequence;
            slot.Bloom = bloom;
            slot.Count = 1;
            slot.Hash0 = hash;
            if (_maxHistorySequence == 0)
            {
                _minHistorySequence = sequence;
            }
            else if (sequence != _maxHistorySequence + 1)
            {
                _minHistorySequence = sequence;
            }
            else if (sequence - _minHistorySequence >= HistoryRingSize)
            {
                _minHistorySequence = sequence - HistoryRingSize + 1;
            }
            _maxHistorySequence = sequence;
        }

        private void RecordCommittedHistory(long sequence, MvccWriteBuffer writes)
        {
            var count = writes.MutationCount;
            ulong bloom = 0;
            ref var slot = ref _history[sequence & HistoryRingMask];
            slot.Sequence = sequence;
            slot.Count = count;
            for (var i = 0; i < count; i++)
            {
                var hash = HashKey(writes.GetMutationKey(i).Span);
                bloom |= BloomMask(hash);
                if (i < MaxInlineHistoryHashes)
                {
                    slot.SetHash(i, hash);
                }
            }
            slot.Bloom = bloom;
            if (_maxHistorySequence == 0)
            {
                _minHistorySequence = sequence;
            }
            else if (sequence != _maxHistorySequence + 1)
            {
                _minHistorySequence = sequence;
            }
            else if (sequence - _minHistorySequence >= HistoryRingSize)
            {
                _minHistorySequence = sequence - HistoryRingSize + 1;
            }
            _maxHistorySequence = sequence;
        }

        private bool CanSkipStorageValidation(CommitRequest request)
        {
            return CanSkipStorageValidation(request.Sequence, request.Writes);
        }

        private bool CanSkipStorageValidation(long snapshot, MvccWriteBuffer writes)
        {
            if (writes.ReadRangeCount != 0)
            {
                return false;
            }
            var published = Volatile.Read(ref _owner._publishedSequence);
            if (published <= snapshot)
            {
                return true;
            }
            if (_maxHistorySequence != published || snapshot + 1 < _minHistorySequence)
            {
                return false;
            }

            // Build bloom filter of this transaction's written and tracked keys.
            ulong txBloom = 0;
            var mutationCount = writes.MutationCount;
            var trackedReadCount = writes.TrackedReadCount;
            for (var i = 0; i < mutationCount; i++)
            {
                txBloom |= BloomMask(HashKey(writes.GetMutationKey(i).Span));
            }
            for (var i = 0; i < trackedReadCount; i++)
            {
                txBloom |= BloomMask(HashKey(writes.GetTrackedRead(i).Span));
            }

            for (var seq = snapshot + 1; seq <= published; seq++)
            {
                ref readonly var entry = ref _history[seq & HistoryRingMask];
                if (entry.Sequence != seq)
                {
                    return false;
                }
                if ((entry.Bloom & txBloom) == 0)
                {
                    continue;
                }
                if (entry.Count > MaxInlineHistoryHashes)
                {
                    return false;
                }
                for (var i = 0; i < mutationCount; i++)
                {
                    if (entry.ContainsHash(HashKey(writes.GetMutationKey(i).Span)))
                    {
                        return false;
                    }
                }
                for (var i = 0; i < trackedReadCount; i++)
                {
                    if (entry.ContainsHash(HashKey(writes.GetTrackedRead(i).Span)))
                    {
                        return false;
                    }
                }
            }

            return true;
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private static uint HashKey(ReadOnlySpan<byte> key) => MemTables.MemTable.Hash(key);

        private static ulong BloomMask(uint hash) =>
            (1UL << (int)(hash & 63)) | (1UL << (int)((hash >> 6) & 63));

        private struct CommitHistoryEntry
        {
            public long Sequence;
            public ulong Bloom;
            public int Count;
            public uint Hash0, Hash1, Hash2, Hash3, Hash4, Hash5, Hash6, Hash7;

            public void SetHash(int index, uint hash)
            {
                switch (index)
                {
                    case 0: Hash0 = hash; break;
                    case 1: Hash1 = hash; break;
                    case 2: Hash2 = hash; break;
                    case 3: Hash3 = hash; break;
                    case 4: Hash4 = hash; break;
                    case 5: Hash5 = hash; break;
                    case 6: Hash6 = hash; break;
                    case 7: Hash7 = hash; break;
                }
            }

            public readonly bool ContainsHash(uint hash)
            {
                if (Count > 0 && Hash0 == hash) return true;
                if (Count > 1 && Hash1 == hash) return true;
                if (Count > 2 && Hash2 == hash) return true;
                if (Count > 3 && Hash3 == hash) return true;
                if (Count > 4 && Hash4 == hash) return true;
                if (Count > 5 && Hash5 == hash) return true;
                if (Count > 6 && Hash6 == hash) return true;
                if (Count > 7 && Hash7 == hash) return true;
                return false;
            }
        }

        private sealed class CommitRequest(CommitCoordinator coordinator) : IValueTaskSource<MvccConflict?>
        {
            private ManualResetValueTaskSourceCore<MvccConflict?> _core = new() { RunContinuationsAsynchronously = true };
            private bool _completed;

            public long Sequence { get; private set; }
            public MvccWriteBuffer Writes { get; private set; } = null!;
            public CancellationToken CancellationToken { get; private set; }
            public long CommitSequence { get; set; }

            public short Initialize(long sequence, MvccWriteBuffer writes, CancellationToken cancellationToken)
            {
                _core.Reset();
                _completed = false;
                Sequence = sequence;
                Writes = writes;
                CancellationToken = cancellationToken;
                CommitSequence = 0;
                return _core.Version;
            }

            public ValueTask<MvccConflict?> WaitAsync(short version) => new(this, version);

            public void SetResult(MvccConflict? result)
            {
                _completed = true;
                _core.SetResult(result);
            }

            public void SetException(Exception exception)
            {
                _completed = true;
                _core.SetException(exception);
            }

            public void TrySetException(Exception exception)
            {
                if (!_completed)
                {
                    _completed = true;
                    _core.SetException(exception);
                }
            }

            public MvccConflict? GetResult(short token)
            {
                try
                {
                    return _core.GetResult(token);
                }
                finally
                {
                    Writes = null!;
                    CancellationToken = default;
                    coordinator.ReturnRequest(this);
                }
            }

            public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

            public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
                => _core.OnCompleted(continuation, state, token, flags);
        }
    }
}
