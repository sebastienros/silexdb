namespace Silex;

public interface IStorageIterator<TKey, TValue>
{
    IAsyncEnumerable<RecordLocation<TKey>> EnumerateAsync(CancellationToken cancellationToken = default);

    IAsyncEnumerable<RecordLocation<TKey>> EnumerateAsync(TKey afterKey, CancellationToken cancellationToken = default);
}
