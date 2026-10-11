namespace Silex;

internal sealed class OrderedWriteSnapshot : IDisposable
{
    private readonly MvccWriteBuffer _writes = new(indexThreshold: int.MaxValue);

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
            var order = writes.OrderedMutations();
            var low = 0;
            var high = order.Length;
            while (low < high)
            {
                var mid = low + (high - low) / 2;
                if (range.Before(writes.GetMutationKey(order[mid]).Span)) low = mid + 1;
                else high = mid;
            }
            for (var i = low; i < order.Length; i++)
            {
                var mutation = writes.GetMutation(order[i]);
                var key = writes.GetKey(mutation).Span;
                if (range.After(key)) break;
                snapshot ??= new OrderedWriteSnapshot();
                snapshot._writes.AddMutation(key, writes.GetValue(mutation).Span, mutation.IsTombstone);
            }
            return snapshot;
        }
        catch
        {
            snapshot?.Dispose();
            throw;
        }
    }

    public ReadOnlySpan<byte> Key(int index) => _writes.GetMutationKey(index).Span;
    public bool IsTombstone(int index) => _writes.GetMutation(index).IsTombstone;
    public ReadOnlySpan<byte> Value(int index) => _writes.GetValue(_writes.GetMutation(index)).Span;

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

    public void Dispose() => _writes.Dispose();
}
