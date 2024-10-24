using Silex.Serialization;
using System.Runtime.CompilerServices;

namespace Silex.Blocks;

internal sealed class BlockIterator<TKey, TValue> : IStorageIterator<TKey, TValue>
{
    private static readonly IBinaryEncoder<TValue> _valueSerializer = BinaryEncoderFactory<TValue>.BinarySerializer;

    private readonly Block<TKey, TValue> _block;

    private readonly RecordLocation<TKey>[] _entries;

    public BlockIterator(Block<TKey, TValue> block)
    {
        _block = block;
        _entries = _block.Offsets.Select(x => _block.GetEntry(x)).ToArray();
    }

    public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();

        foreach (var entry in _entries)
        {
            yield return new KeyValuePair<TKey, TValue>(entry.Key, _valueSerializer.Decode(_block.GetValue(entry)));
        }
    }

#pragma warning disable 1998 // async function without await
    public async IAsyncEnumerable<KeyValuePair<TKey, TValue>> EnumerateAsync(TKey from, [EnumeratorCancellation] CancellationToken _ = default)
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
            yield return new KeyValuePair<TKey, TValue>(_entries[i].Key, _valueSerializer.Decode(_block.GetValue(entry)));
        }
    }
}
