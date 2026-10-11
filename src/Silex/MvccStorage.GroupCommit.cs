namespace Silex;

public sealed partial class MvccStorage
{
    private sealed class CommitCoordinator
    {
        private readonly MvccStorage _owner;
        private readonly object _lock = new();
        private readonly Queue<CommitRequest> _pending;
        private readonly SemaphoreSlim _slots;
        private readonly CommitRequest?[] _requests;
        private readonly CommitRequest?[] _accepted;
        private readonly LsmWriteBatch[] _batches;
        private readonly StorageCommitOutcome[] _outcomes;
        private Task? _worker;
        private bool _stopped;

        public CommitCoordinator(MvccStorage owner, int groupSize)
        {
            _owner = owner;
            _pending = new Queue<CommitRequest>(groupSize * 2);
            _slots = new SemaphoreSlim(groupSize * 2, groupSize * 2);
            _requests = new CommitRequest[groupSize];
            _accepted = new CommitRequest[groupSize];
            _batches = new LsmWriteBatch[groupSize];
            _outcomes = new StorageCommitOutcome[groupSize];
            for (var i = 0; i < groupSize; i++)
            {
                _batches[i] = new LsmWriteBatch();
            }
        }

        public async ValueTask<MvccConflict?> CommitAsync(long sequence, MvccWriteBuffer writes, CancellationToken cancellationToken)
        {
            await _slots.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                CommitRequest request;
                lock (_lock)
                {
                    _owner.CheckDisposed();
                    ObjectDisposedException.ThrowIf(_stopped, _owner);
                    cancellationToken.ThrowIfCancellationRequested();
                    request = new CommitRequest(sequence, writes, cancellationToken);
                    _pending.Enqueue(request);
                    _worker ??= Task.Run(RunAsync);
                }
                return await request.Completion.Task.ConfigureAwait(false);
            }
            finally
            {
                _slots.Release();
            }
        }

        public int PendingCount
        {
            get
            {
                lock (_lock)
                {
                    return _pending.Count;
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
                        count = Math.Min(_pending.Count, _requests.Length);
                        if (count == 0)
                        {
                            _worker = null;
                            return;
                        }
                        for (var i = 0; i < count; i++)
                        {
                            _requests[i] = _pending.Dequeue();
                        }
                    }
                    try
                    {
                        await CommitGroupAsync(count).ConfigureAwait(false);
                    }
                    catch (Exception exception)
                    {
                        for (var i = 0; i < count; i++)
                        {
                            _requests[i]!.Completion.TrySetException(exception);
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
                    var conflict = ConflictsWithAccepted(request, acceptedCount) ??
                        await _owner.ValidateAsync(request.Sequence, request.Writes, request.CancellationToken).ConfigureAwait(false);
                    if (conflict is not null)
                    {
                        request.Completion.SetResult(conflict);
                        continue;
                    }
                    if (request.Writes.MutationCount == 0)
                    {
                        request.CancellationToken.ThrowIfCancellationRequested();
                        request.Completion.SetResult(null);
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
                    request.Completion.SetCanceled(exception.CancellationToken);
                }
                catch (Exception exception)
                {
                    request.Completion.SetException(exception);
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
                    _owner._allocatedSequence = _accepted[i]!.CommitSequence;
                    Volatile.Write(ref _owner._publishedSequence, _accepted[i]!.CommitSequence);
                }
                Interlocked.Increment(ref _owner._commitGroups);
                Interlocked.Add(ref _owner._groupedCommits, acceptedCount);
                for (var i = 0; i < acceptedCount; i++)
                {
                    _accepted[i]!.Completion.SetResult(null);
                }
            }
            catch (Exception exception)
            {
                var cause = exception is StorageOperationException failure ? failure.InnerException! : exception;
                for (var i = 0; i < acceptedCount; i++)
                {
                    _accepted[i]!.Completion.SetException(new StorageOperationException(_outcomes[i], cause));
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

        private sealed class CommitRequest(long sequence, MvccWriteBuffer writes, CancellationToken cancellationToken)
        {
            public long Sequence { get; } = sequence;
            public MvccWriteBuffer Writes { get; } = writes;
            public CancellationToken CancellationToken { get; } = cancellationToken;
            public long CommitSequence { get; set; }
            public TaskCompletionSource<MvccConflict?> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}
