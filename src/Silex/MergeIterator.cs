using Silex.Serialization;
using System.Runtime.CompilerServices;

namespace Silex;

/// <summary>
/// Merges multiple <see cref="IStorageIterator{ByteSlice, ByteSlice}"/> into a single ascending stream of
/// key/value pairs. When the same key is present in several iterators, the value from the iterator
/// listed first wins (iterators should be provided in most-recent-first order). Tombstones are not
/// filtered out; callers that need to hide deleted entries should filter the merged stream.
/// </summary>
internal sealed class MergeIterator : IStorageIterator
{
    private static readonly IComparer<ByteSlice> _keyComparer = BinaryEncoderFactory<ByteSlice>.BinarySerializer.Comparer;

    private readonly IEnumerable<IStorageIterator> _iterators;

    public MergeIterator(IEnumerable<IStorageIterator> iterators)
    {
        _iterators = iterators;
    }

    public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(CancellationToken cancellationToken = default)
    {
        return MergeAsync(iterator => iterator.EnumerateAsync(cancellationToken), backwards: false, cancellationToken);
    }

    public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(ByteSlice from, CancellationToken cancellationToken = default)
    {
        return MergeAsync(iterator => iterator.EnumerateAsync(from, cancellationToken), backwards: false, cancellationToken);
    }

    public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(CancellationToken cancellationToken = default)
    {
        return MergeAsync(iterator => iterator.EnumerateBackwardsAsync(cancellationToken), backwards: true, cancellationToken);
    }

    public IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(ByteSlice from, CancellationToken cancellationToken = default)
    {
        return MergeAsync(iterator => iterator.EnumerateBackwardsAsync(from, cancellationToken), backwards: true, cancellationToken);
    }

    private async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> MergeAsync(
        Func<IStorageIterator, IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>>> selector,
        bool backwards,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var enumerators = new List<IAsyncEnumerator<KeyValuePair<ByteSlice, ByteSlice>>>();

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
                var selected = enumerators[0].Current;
                var selectedIndex = 0;

                for (var i = 1; i < enumerators.Count; i++)
                {
                    var enumerator = enumerators[i];
                    var current = enumerator.Current;

                    var comparison = _keyComparer.Compare(selected.Key, current.Key);
                    if (backwards ? comparison < 0 : comparison > 0)
                    {
                        selectedIndex = i;
                        selected = current;
                    }
                }

                for (var i = enumerators.Count - 1; i >= 0; i--)
                {
                    if (i == selectedIndex || _keyComparer.Compare(selected.Key, enumerators[i].Current.Key) != 0)
                    {
                        continue;
                    }

                    // Discard older duplicates before yielding the winner. The winner itself is advanced
                    // only after the caller has consumed it, so borrowed block slices stay valid.
                    if (!await enumerators[i].MoveNextAsync())
                    {
                        await enumerators[i].DisposeAsync();
                        enumerators.RemoveAt(i);

                        if (i < selectedIndex)
                        {
                            selectedIndex--;
                        }
                    }
                }

                yield return selected;

                if (!await enumerators[selectedIndex].MoveNextAsync())
                {
                    await enumerators[selectedIndex].DisposeAsync();
                    enumerators.RemoveAt(selectedIndex);
                }
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
