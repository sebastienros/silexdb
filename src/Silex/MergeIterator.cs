using Silex.Serialization;
using System.Runtime.CompilerServices;

namespace Silex;

/// <summary>
/// Merges multiple <see cref="IStorageIterator{TKey, TValue}"/> into a single ascending stream of
/// key/value pairs. When the same key is present in several iterators, the value from the iterator
/// listed first wins (iterators should be provided in most-recent-first order). Tombstones are not
/// filtered out; callers that need to hide deleted entries should filter the merged stream.
/// </summary>
internal sealed class MergeIterator<TKey> : IStorageIterator<TKey, ValueBuffer>
{
    private static readonly IComparer<TKey> _keyComparer = KeyEncoderFactory<TKey>.Encoder.Comparer;

    private readonly IEnumerable<IStorageIterator<TKey, ValueBuffer>> _iterators;

    public MergeIterator(IEnumerable<IStorageIterator<TKey, ValueBuffer>> iterators)
    {
        _iterators = iterators;
    }

    public IAsyncEnumerable<KeyValuePair<TKey, ValueBuffer>> EnumerateAsync(CancellationToken cancellationToken = default)
    {
        return MergeAsync(iterator => iterator.EnumerateAsync(cancellationToken), cancellationToken);
    }

    public IAsyncEnumerable<KeyValuePair<TKey, ValueBuffer>> EnumerateAsync(TKey from, CancellationToken cancellationToken = default)
    {
        return MergeAsync(iterator => iterator.EnumerateAsync(from, cancellationToken), cancellationToken);
    }

    private async IAsyncEnumerable<KeyValuePair<TKey, ValueBuffer>> MergeAsync(
        Func<IStorageIterator<TKey, ValueBuffer>, IAsyncEnumerable<KeyValuePair<TKey, ValueBuffer>>> selector,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var enumerators = new List<IAsyncEnumerator<KeyValuePair<TKey, ValueBuffer>>>();

        try
        {
            // Prime each enumerator and only keep the ones that contain at least one element.
            foreach (var iterator in _iterators)
            {
                var enumerator = selector(iterator).GetAsyncEnumerator(cancellationToken);

                if (await enumerator.MoveNextAsync())
                {
                    enumerators.Add(enumerator);
                }
                else
                {
                    await enumerator.DisposeAsync();
                }
            }

            if (enumerators.Count == 1)
            {
                var enumerator = enumerators[0];

                do
                {
                    yield return enumerator.Current;
                }
                while (await enumerator.MoveNextAsync());

                yield break;
            }

            while (enumerators.Count > 0)
            {
                // Assume the smallest is the element from the first iterator
                var smallest = enumerators[0].Current;
                var smallestIndex = 0;

                for (var i = 1; i < enumerators.Count; i++)
                {
                    var enumerator = enumerators[i];
                    var current = enumerator.Current;

                    switch (_keyComparer.Compare(smallest.Key, current.Key))
                    {
                        // Discard the entry since there is the same key from a more recent iterator
                        case 0:
                            if (!await enumerator.MoveNextAsync())
                            {
                                await enumerator.DisposeAsync();
                                enumerators.RemoveAt(i);
                                i--;
                            }
                            break;

                        case > 0:
                            smallestIndex = i;
                            smallest = current;
                            break;

                        default:
                            break;
                    }
                }

                // Consume the smallest element
                if (!await enumerators[smallestIndex].MoveNextAsync())
                {
                    await enumerators[smallestIndex].DisposeAsync();
                    enumerators.RemoveAt(smallestIndex);
                }

                yield return smallest;
            }
        }
        finally
        {
            foreach (var enumerator in enumerators)
            {
                await enumerator.DisposeAsync();
            }
        }
    }
}
