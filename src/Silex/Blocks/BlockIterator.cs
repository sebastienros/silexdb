using Silex.Serialization;
using System.Runtime.CompilerServices;

namespace Silex.Blocks;

internal sealed class BlockIterator<TKey> : IStorageIterator<TKey, ValueBuffer>
{
    private readonly Block<TKey> _block;

    private readonly RecordLocation<TKey>[] _entries;

    public BlockIterator(Block<TKey> block)
    {
        _block = block;

        var offsets = block.Offsets;
        var entries = new RecordLocation<TKey>[offsets.Count];
        for (var i = 0; i < entries.Length; i++)
        {
            entries[i] = block.GetEntry(offsets[i]);
        }

        _entries = entries;
    }

    public async IAsyncEnumerable<KeyValuePair<TKey, ValueBuffer>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();

        foreach (var entry in _entries)
        {
            yield return new KeyValuePair<TKey, ValueBuffer>(entry.Key, new ValueBuffer(_block.GetValue(entry).ToArray()));
        }
    }

#pragma warning disable 1998 // async function without await
    public async IAsyncEnumerable<KeyValuePair<TKey, ValueBuffer>> EnumerateAsync(TKey from, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore 1998
    {
        var startIndex = 0;

        var compare = Array.BinarySearch(_entries, new RecordLocation<TKey> { Key = from });

        startIndex = compare >= 0 ? compare : ~compare;

        if (startIndex > _entries.Length + 1)
        {
            yield break;
        }

        for (var i = startIndex; i < _entries.Length; i++)
        {
            var entry = _entries[i];
            yield return new KeyValuePair<TKey, ValueBuffer>(_entries[i].Key, new ValueBuffer(_block.GetValue(entry).ToArray()));
        }
    }
}
