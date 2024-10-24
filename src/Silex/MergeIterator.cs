using Silex.Serialization;
using System.Runtime.CompilerServices;

namespace Silex;

internal class MergeIterator<TKey, TValue> : IStorageIterator<TKey, TValue>
{
    private static readonly IComparer<TKey> _keyComparer = BinaryEncoderFactory<TKey>.BinarySerializer.Comparer;

    private readonly IEnumerable<IStorageIterator<TKey, TValue>> _iterators;

    public MergeIterator(IEnumerable<IStorageIterator<TKey, TValue>> iterators)
    {
        _iterators = iterators;
    }

    public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<IAsyncEnumerator<KeyValuePair<TKey, TValue>>> enumerators = [];

        foreach (var iterator in _iterators)
        {
            enumerators.Add(iterator.EnumerateAsync(cancellationToken).GetAsyncEnumerator(cancellationToken));
        }

        await foreach (var r in MergeIterator<TKey, TValue>.EnumerateAsync(enumerators, cancellationToken))
        {
            yield return r;
        }
    }

    public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(TKey from, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<IAsyncEnumerator<KeyValuePair<TKey, TValue>>> enumerators = [];

        foreach (var iterator in _iterators)
        {
            enumerators.Add(iterator.EnumerateAsync(from, cancellationToken).GetAsyncEnumerator(cancellationToken));
        }

        await foreach (var r in MergeIterator<TKey, TValue>.EnumerateAsync(enumerators, cancellationToken))
        {
            yield return r;
        }
    }

    private static async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(List<IAsyncEnumerator<KeyValuePair<TKey, TValue>>> enumerators, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        while (enumerators.Count > 0)
        {
            // Assume the smallest is the element from the first iterator
            var smallest = enumerators[0].Current;

            var smallestIndex = 0;

            for (var i = 1; i < enumerators.Count; i++)
            {
                var iterator = enumerators[i];

                var current = iterator.Current;

                switch (_keyComparer.Compare(smallest.Key, current.Key))
                {
                    // Discard the entry since there is the same key from a more recent table
                    case 0:
                        if (!await iterator.MoveNextAsync())
                        {
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
                enumerators.RemoveAt(smallestIndex);
            }

            yield return smallest;
        }
    }
}
