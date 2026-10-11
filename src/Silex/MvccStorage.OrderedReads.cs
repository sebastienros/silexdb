using Silex.Buffers;

namespace Silex;

public sealed partial class MvccStorage
{
    /// <summary>Creates a cursor at the current published sequence, retained until cursor disposal.</summary>
    public OrderedReadCursor CreateOrderedReadCursor(OrderedReadRange? range = null,
        OrderedReadDirection direction = OrderedReadDirection.Forward)
    {
        ValidateDirection(direction);
        range ??= new OrderedReadRange();
        var sequence = RegisterCurrentRead();
        try
        {
            return new OrderedReadCursor(null, this, sequence, range, direction);
        }
        catch
        {
            ReleaseSnapshot(sequence);
            throw;
        }
    }

    internal OrderedReadCursor CreateOrderedReadCursorAt(long sequence, OrderedReadRange? range,
        OrderedReadDirection direction, OrderedWriteSnapshot? writes = null)
    {
        ValidateDirection(direction);
        range ??= new OrderedReadRange();
        lock (_snapshotLock)
        {
            CheckDisposed();
            AddSnapshot(sequence);
            try
            {
                return new OrderedReadCursor(null, this, sequence, range, direction, writes);
            }
            catch
            {
                ReleaseSnapshot(sequence);
                throw;
            }
        }
    }

    private static void ValidateDirection(OrderedReadDirection direction)
    {
        if (direction is not (OrderedReadDirection.Forward or OrderedReadDirection.Reverse))
        {
            throw new ArgumentOutOfRangeException(nameof(direction));
        }
    }

    internal async ValueTask ReadOrderedBatchAsync(OrderedReadRange range, OrderedReadDirection direction,
        byte[]? resume, long sequence, OrderedReadBatch batch, bool exhausted, CancellationToken cancellationToken,
        OrderedWriteSnapshot? writes = null)
    {
        CheckDisposed();
        var backwards = direction == OrderedReadDirection.Reverse;
        var effective = resume is null ? range : backwards
            ? new OrderedReadRange(range.Lower, range.LowerInclusive, resume, false)
            : new OrderedReadRange(resume, false, range.Upper, range.UpperInclusive);
        using var lower = effective.Lower is { } low
            ? EncodeVersionKey(low.Span, effective.LowerInclusive ? long.MaxValue : 0) : null;
        using var upper = effective.Upper is { } high
            ? EncodeVersionKey(high.Span, effective.UpperInclusive ? 0 : long.MaxValue) : null;
        var physical = exhausted || effective.IsEmpty
            ? new OrderedReadRange(new byte[] { 2 }, true, new byte[] { 1 })
            : new OrderedReadRange(
                lower is null ? new byte[] { DataPrefix } : lower.Slice.Memory, effective.LowerInclusive,
                upper is null ? new byte[] { 2 } : upper.Slice.Memory,
                upper is not null && effective.UpperInclusive);
        using var state = new OrderedScanState(sequence, backwards, batch, exhausted ? null : writes,
            resume, cancellationToken);
        await _storage.ScanOrderedAsync(physical, direction, state,
            static (s, key, value) => s.Accept(key, value), cancellationToken).ConfigureAwait(false);
        if (backwards && !batch.Full)
        {
            state.Flush();
        }
        if (!batch.Full)
        {
            state.FlushWrites();
        }
        batch.IsExhausted = !batch.Full;
    }

    private sealed class OrderedScanState(long sequence, bool backwards, OrderedReadBatch batch,
        OrderedWriteSnapshot? writes, byte[]? resume, CancellationToken cancellationToken) : IDisposable
    {
        private readonly PooledArrayBufferWriter<byte> _key = new();
        private readonly PooledArrayBufferWriter<byte> _value = new();
        private bool _hasGroup;
        private bool _resolved;
        private bool _live;
        private int _writeIndex = writes?.Seek(resume, backwards) ?? 0;

        public bool Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (!TryParseVersionKey(key, out var userKey, out var version))
            {
                return true;
            }
            if (!_hasGroup || !userKey.SequenceEqual(_key.WrittenSpan))
            {
                if (backwards && !Flush())
                {
                    return false;
                }
                _key.Reset();
                userKey.CopyTo(_key.GetSpan(userKey.Length));
                _key.Advance(userKey.Length);
                _hasGroup = true;
                _resolved = false;
                _live = false;
            }
            if (version > sequence || (!backwards && _resolved))
            {
                return true;
            }
            ValidateStoredValue(value);
            _resolved = true;
            _live = value[0] == LiveValue;
            if (!backwards)
            {
                return !_live || InvokeUserReader(this,
                    static (s, k, v) => s.Emit(k, v), userKey, value[1..]);
            }
            // Physical reverse order visits oldest versions first; retain the newest eligible candidate.
            _value.Reset();
            if (_live)
            {
                value[1..].CopyTo(_value.GetSpan(value.Length - 1));
                _value.Advance(value.Length - 1);
            }
            return true;
        }

        public bool Flush() => !_hasGroup || !_live || InvokeUserReader(this,
            static (s, k, v) => s.Emit(k, v), _key.WrittenSpan, _value.WrittenSpan);

        private bool HasWrite => writes is not null && _writeIndex >= 0 && _writeIndex < writes.Count;

        private bool Emit(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            while (HasWrite)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var comparison = writes!.Key(_writeIndex).SequenceCompareTo(key);
                if (backwards ? comparison < 0 : comparison > 0)
                {
                    break;
                }
                if (!EmitWrite())
                {
                    return false;
                }
                if (comparison == 0)
                {
                    return true;
                }
            }
            return batch.Add(key, value);
        }

        private bool EmitWrite()
        {
            var index = _writeIndex;
            _writeIndex += backwards ? -1 : 1;
            return writes!.IsTombstone(index) || batch.Add(writes.Key(index), writes.Value(index));
        }

        public void FlushWrites()
        {
            while (HasWrite)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (!EmitWrite())
                {
                    break;
                }
            }
        }

        public void Dispose()
        {
            _key.Dispose();
            _value.Dispose();
        }
    }
}
