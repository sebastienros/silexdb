namespace Silex.Blocks;

using Silex;
using System.Runtime.CompilerServices;

internal sealed class BlockIterator<TKey, TValue> : IStorageIterator<TKey, TValue>
{
    private readonly Block<TKey, TValue> _block;

    private readonly RecordLocation<TKey>[] _entries;

    public BlockIterator(Block<TKey, TValue> block)
    {
        _block = block;
        _entries = _block.Offsets.Select(x => _block.GetEntry(x)).ToArray();
    }

    public async IAsyncEnumerable<RecordLocation<TKey>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();

        foreach (var entry in _entries)
        {
            yield return entry;
        }
    }

#pragma warning disable 1998 // async function without await
    public async IAsyncEnumerable<RecordLocation<TKey>> EnumerateAsync(TKey afterKey, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore 1998
    {
        var startIndex = 0;

        var compare = Array.BinarySearch(_entries, new RecordLocation<TKey> { Key = afterKey });

        startIndex = compare >= 0 ? compare : ~compare;

        if (startIndex > _entries.Length + 1)
        {
            yield break;
        }

        for (var i = startIndex; i < _entries.Length; i++)
        {
            yield return _entries[i];
        }
    }
}
