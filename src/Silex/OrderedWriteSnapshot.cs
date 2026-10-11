using System.Buffers;

namespace Silex;

internal sealed class OrderedWriteSnapshot : IDisposable, IComparer<int>
{
    private readonly MvccWriteBuffer _writes = new();
    private int[] _order = [];

    private OrderedWriteSnapshot() { }

    public int Count => _writes.MutationCount;

    public static OrderedWriteSnapshot? Create(MvccWriteBuffer writes, OrderedReadRange range)
    {
        OrderedWriteSnapshot? snapshot = null;
        try
        {
            if (range.IsEmpty)
            {
                return null;
            }
            for (var i = 0; i < writes.MutationCount; i++)
            {
                var mutation = writes.GetMutation(i);
                var key = writes.GetKey(mutation).Span;
                if (range.Before(key) || range.After(key))
                {
                    continue;
                }
                snapshot ??= new OrderedWriteSnapshot();
                snapshot._writes.AddMutation(key, writes.GetValue(mutation).Span, mutation.IsTombstone);
            }
            if (snapshot is not null)
            {
                snapshot._order = ArrayPool<int>.Shared.Rent(snapshot.Count);
                for (var i = 0; i < snapshot.Count; i++)
                {
                    snapshot._order[i] = i;
                }
                Array.Sort(snapshot._order, 0, snapshot.Count, snapshot);
            }
            return snapshot;
        }
        catch
        {
            snapshot?.Dispose();
            throw;
        }
    }

    public ReadOnlySpan<byte> Key(int index) => _writes.GetMutationKey(_order[index]).Span;
    public bool IsTombstone(int index) => _writes.GetMutation(_order[index]).IsTombstone;
    public ReadOnlySpan<byte> Value(int index) => _writes.GetValue(_writes.GetMutation(_order[index])).Span;

    public int Seek(byte[]? resume, bool backwards)
    {
        if (resume is null)
        {
            return backwards ? Count - 1 : 0;
        }
        var low = 0;
        var high = Count;
        while (low < high)
        {
            var mid = low + (high - low) / 2;
            var comparison = Key(mid).SequenceCompareTo(resume);
            if (comparison < 0 || (!backwards && comparison == 0))
            {
                low = mid + 1;
            }
            else
            {
                high = mid;
            }
        }
        return backwards ? low - 1 : low;
    }

    public int Compare(int x, int y) =>
        _writes.GetMutationKey(x).Span.SequenceCompareTo(_writes.GetMutationKey(y).Span);

    public void Dispose()
    {
        _writes.Dispose();
        if (_order.Length != 0)
        {
            ArrayPool<int>.Shared.Return(_order);
            _order = [];
        }
    }
}
