namespace Silex;

public sealed partial class LsmStorage
{
    /// <summary>Creates a live, bounded cursor; no historical snapshot is implied.</summary>
    public OrderedReadCursor CreateOrderedReadCursor(OrderedReadRange? range = null,
        OrderedReadDirection direction = OrderedReadDirection.Forward)
    {
        CheckDisposed();
        _inner.CheckHealthy();
        if (direction is not (OrderedReadDirection.Forward or OrderedReadDirection.Reverse))
        {
            throw new ArgumentOutOfRangeException(nameof(direction));
        }
        return new OrderedReadCursor(this, null, null, range ?? new OrderedReadRange(), direction);
    }

    internal async ValueTask ReadOrderedBatchAsync(OrderedReadRange range, OrderedReadDirection direction,
        byte[]? resume, OrderedReadBatch batch, bool exhausted, CancellationToken cancellationToken)
    {
        batch.IsExhausted = await _inner.ReadOrderedAsync(range, direction, resume, batch,
            static (b, key, value) => b.Add(key, value), exhausted, cancellationToken).ConfigureAwait(false);
    }

    internal ValueTask<bool> ScanOrderedAsync<TArg>(OrderedReadRange range, OrderedReadDirection direction,
        TArg arg, ReadRawEntryAction<TArg> reader, CancellationToken cancellationToken) =>
        _inner.ReadOrderedAsync(range, direction, null, arg, reader, false, cancellationToken);
}
