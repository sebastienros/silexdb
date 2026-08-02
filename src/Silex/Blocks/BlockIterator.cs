using System.Runtime.CompilerServices;

namespace Silex.Blocks;

internal sealed class BlockIterator : IStorageIterator
{
    private readonly Block _block;

    private readonly RecordLocation[] _entries;

    public BlockIterator(Block block)
    {
        _block = block;

        var offsets = block.Offsets;
        var entries = new RecordLocation[offsets.Count];
        for (var i = 0; i < entries.Length; i++)
        {
            entries[i] = block.GetEntry(offsets[i]);
        }

        _entries = entries;
    }

    public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();

        foreach (var entry in _entries)
        {
            var value = entry.IsTombstone ? ByteSlice.Tombstone : ByteSlice.FromMemory(_block.GetValueMemory(entry));
            yield return new KeyValuePair<ByteSlice, ByteSlice>(entry.Key, value);
        }
    }

#pragma warning disable 1998 // async function without await
    public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(ByteSlice from, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore 1998
    {
        var startIndex = 0;

        var compare = Array.BinarySearch(_entries, new RecordLocation { Key = from });

        startIndex = compare >= 0 ? compare : ~compare;

        if (startIndex > _entries.Length + 1)
        {
            yield break;
        }

        for (var i = startIndex; i < _entries.Length; i++)
        {
            var entry = _entries[i];
            var value = entry.IsTombstone ? ByteSlice.Tombstone : ByteSlice.FromMemory(_block.GetValueMemory(entry));
            yield return new KeyValuePair<ByteSlice, ByteSlice>(entry.Key, value);
        }
    }

    public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await Task.Yield();

        for (var i = _entries.Length - 1; i >= 0; i--)
        {
            var entry = _entries[i];
            var value = entry.IsTombstone ? ByteSlice.Tombstone : ByteSlice.FromMemory(_block.GetValueMemory(entry));
            yield return new KeyValuePair<ByteSlice, ByteSlice>(entry.Key, value);
        }
    }

#pragma warning disable 1998 // async function without await
    public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(ByteSlice from, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore 1998
    {
        var compare = Array.BinarySearch(_entries, new RecordLocation { Key = from });
        var startIndex = compare >= 0 ? compare : ~compare - 1;

        for (var i = startIndex; i >= 0; i--)
        {
            var entry = _entries[i];
            var value = entry.IsTombstone ? ByteSlice.Tombstone : ByteSlice.FromMemory(_block.GetValueMemory(entry));
            yield return new KeyValuePair<ByteSlice, ByteSlice>(entry.Key, value);
        }
    }
}
