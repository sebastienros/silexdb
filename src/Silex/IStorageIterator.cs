namespace Silex;

public interface IStorageIterator<TKey, TValue>
{
    IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(CancellationToken cancellationToken = default);

    IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(TKey from, CancellationToken cancellationToken = default);
}
