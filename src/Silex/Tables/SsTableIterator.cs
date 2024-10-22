using Silex.Blocks;
using System.Runtime.CompilerServices;

namespace Silex.Tables;

internal sealed class SsTableIterator<TKey, TValue> : IStorageIterator<TKey, TValue>
{
    private readonly SsTable<TKey, TValue> _table;
    private TKey[]? _firstKeys;

    public SsTableIterator(SsTable<TKey, TValue> table)
    {
        _table = table;
    }

    public async IAsyncEnumerable<RecordLocation<TKey>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var i = 0; i < _table.BlockMetadata.Count; i++)
        {
            var blockMetadata = _table.BlockMetadata[i];

            using var block = await _table.ReadBlockAsync(blockMetadata.Index, cancellationToken);

            if (block != null)
            {
                var blockIterator = new BlockIterator<TKey, TValue>(block);
                await foreach (var entry in blockIterator.EnumerateAsync(cancellationToken))
                {
                    yield return new RecordLocation<TKey>
                    {
                        SsTableFilename = _table.Filename,
                        Length = entry.Length,
                        Key = entry.Key,
                        BlockOffset = entry.BlockOffset
                    };
                }
            }
        }
    }

    public async IAsyncEnumerable<RecordLocation<TKey>> EnumerateAsync(TKey afterKey, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var startBlockIndex = 0;

        // Create a reusable index for binary search
        _firstKeys ??= _table.BlockMetadata.Select(x => x.FirstKey).ToArray();

        var compare = Array.BinarySearch(_firstKeys, afterKey);

        startBlockIndex = (compare >= 0 ? compare : ~compare) - 1;

        // If the key doesn't exist, exit
        if (startBlockIndex > _table.BlockMetadata.Count - 1)
        {
            yield break;
        }

        // Iterate this block only, after the specified key
        var blockMetadata = _table.BlockMetadata[startBlockIndex];

        using var block = await _table.ReadBlockAsync(blockMetadata.Index, cancellationToken);

        if (block != null)
        {
            var blockIterator = new BlockIterator<TKey, TValue>(block);
            await foreach (var entry in blockIterator.EnumerateAsync(afterKey, cancellationToken))
            {
                yield return entry;
            }
        }

        startBlockIndex++;

        for (var i = startBlockIndex; i < _table.BlockMetadata.Count; i++)
        {
            blockMetadata = _table.BlockMetadata[i];

            using var block2 = await _table.ReadBlockAsync(blockMetadata.Index, cancellationToken);

            if (block2 != null)
            {
                var blockIterator = new BlockIterator<TKey, TValue>(block2);
                await foreach (var entry in blockIterator.EnumerateAsync(cancellationToken))
                {
                    yield return new RecordLocation<TKey>
                    {
                        SsTableFilename = _table.Filename,
                        Length = entry.Length,
                        Key = entry.Key,
                        BlockOffset = entry.BlockOffset
                    };
                }
            }
        }

        yield break;
    }
}
