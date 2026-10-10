using Silex.MemTables;

namespace Silex;

internal sealed partial class LsmStorageInner
{
    internal async ValueTask<bool> ReadOrderedAsync<TArg>(OrderedReadRange range, OrderedReadDirection direction,
        byte[]? resume, TArg arg, ReadRawEntryAction<TArg> reader, bool exhausted,
        CancellationToken cancellationToken)
    {
        using var operation = Lifetime.Enter();
        CheckHealthy();
        cancellationToken.ThrowIfCancellationRequested();
        if (exhausted || range.IsEmpty)
        {
            return true;
        }
        var backwards = direction == OrderedReadDirection.Reverse;
        var effective = resume is null ? range : backwards
            ? new OrderedReadRange(range.Lower, range.LowerInclusive, resume, false)
            : new OrderedReadRange(resume, false, range.Upper, range.UpperInclusive);
        await _level0Lock.EnterReadLockAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var inputs = new List<MergeIterator.Input>();
            _currentMemTableLock.EnterReadLock();
            try
            {
                CheckHealthy();
                // The structural read lock retains this table if a writer rotates it during the batch.
                if (_state.CurrentMemTable.Count != 0)
                {
                    inputs.Add(MergeIterator.Input.FromMemTable((MemTable)_state.CurrentMemTable, _currentMemTableLock, this));
                }
                for (var i = _state.ImmutableMemTables.Length - 1; i >= 0; i--)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    inputs.Add(MergeIterator.Input.FromMemTable((MemTable)_state.ImmutableMemTables[i], _currentMemTableLock));
                }
                for (var i = _state.LevelZeroTables.Count - 1; i >= 0; i--)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var table = _state.LevelZeroTables[i];
                    if (!effective.Before(table.LastKey.Span) && !effective.After(table.FirstKey.Span))
                    {
                        inputs.Add(MergeIterator.Input.FromTable(table));
                    }
                }
                foreach (var level in _state.LeveledSsTables)
                {
                    foreach (var table in level)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        if (!effective.Before(table.LastKey.Span) && !effective.After(table.FirstKey.Span))
                        {
                            inputs.Add(MergeIterator.Input.FromTable(table));
                        }
                    }
                }
            }
            finally
            {
                _currentMemTableLock.ExitReadLock();
            }
            return await new MergeIterator(inputs, _blockCache).ScanBoundedRawAsync(
                effective, backwards, arg, reader, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _level0Lock.ExitReadLock();
        }
    }
}
