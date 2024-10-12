namespace Silex;

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

internal class MergeIterator : IStorageIterator
{
    private readonly IEnumerable<IStorageIterator> _iterators;

    public MergeIterator(IEnumerable<IStorageIterator> iterators)
    {
        _iterators = iterators;
    }

    public IAsyncEnumerable<RecordLocation> EnumerateAsync(CancellationToken cancellationToken = default)
    {
        return EnumerateAsync(Bytes.Empty, cancellationToken);
    }

    public async IAsyncEnumerable<RecordLocation> EnumerateAsync(Bytes minValue, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        List<IAsyncEnumerator<RecordLocation>> enumerators = [];

        foreach (var iterator in _iterators)
        {
            enumerators.Add(iterator.EnumerateAsync(minValue, cancellationToken).GetAsyncEnumerator(cancellationToken));
        }

        while (enumerators.Count > 0)
        {
            // Assume the smallest is the element from the first iterator
            var smallest = enumerators[0].Current;

            var smallestIndex = 0;

            for (var i = 1; i < enumerators.Count; i++)
            {
                var iterator = enumerators[i];

                var current = iterator.Current;

                switch (smallest.Key.CompareTo(current.Key))
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

            if (smallest.Length != 0)
            {
                yield return smallest;
            }
        }
    }
}
