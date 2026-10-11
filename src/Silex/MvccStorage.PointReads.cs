namespace Silex;

public sealed partial class MvccStorage
{
    /// <summary>
    /// Reads many keys from one consistent view of the latest published state.
    /// </summary>
    /// <param name="keys">Keys to resolve; results correspond by position. Keys must stay unchanged until completion.</param>
    /// <param name="maxBytes">Maximum value bytes the returned batch may own.</param>
    /// <returns>
    /// A batch owning copies of the found values. When the next value would exceed <paramref name="maxBytes"/>
    /// the batch stops early (<see cref="PointReadBatch.IsComplete"/> is <c>false</c>); read the remaining keys
    /// with a follow-up call. A first value larger than <paramref name="maxBytes"/> throws.
    /// </returns>
    public async ValueTask<PointReadBatch> ReadManyRawAsync(
        ReadOnlyMemory<ReadOnlyMemory<byte>> keys,
        int maxBytes,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBytes);
        CheckDisposed();
        var sequence = RegisterCurrentRead();
        try
        {
            return await ReadManyAtAsync(keys, sequence, maxBytes, null, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseSnapshot(sequence);
        }
    }

    internal async ValueTask<PointReadBatch> ReadManyAtAsync(
        ReadOnlyMemory<ReadOnlyMemory<byte>> keys,
        long sequence,
        int maxBytes,
        MvccWriteBuffer? writes,
        CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxBytes);
        CheckDisposed();
        var batch = new PointReadBatch(keys.Length, maxBytes);
        try
        {
            for (var i = 0; i < keys.Length; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var key = keys.Span[i].Span;
                var mutationIndex = writes?.FindMutation(key) ?? -1;
                if (mutationIndex >= 0)
                {
                    var mutation = writes!.GetMutation(mutationIndex);
                    if (mutation.IsTombstone)
                    {
                        batch.AddMissing();
                    }
                    else
                    {
                        batch.AddValue(writes.GetValue(mutation).Span);
                    }
                }
                else
                {
                    var length = await ReadAtAsync(key, sequence, new BatchSink(batch), cancellationToken).ConfigureAwait(false);
                    if (length < 0)
                    {
                        batch.AddMissing();
                    }
                }

                if (batch.Overflowed)
                {
                    if (batch.Count == 0)
                    {
                        throw new ArgumentException($"Value exceeds maxBytes ({maxBytes}).", nameof(maxBytes));
                    }

                    break;
                }
            }

            return batch;
        }
        catch
        {
            batch.Dispose();
            throw;
        }
    }

    private readonly struct BatchSink(PointReadBatch batch) : IValueSink
    {
        public void Accept(ReadOnlySpan<byte> value) => batch.AddValue(value);
    }
}
