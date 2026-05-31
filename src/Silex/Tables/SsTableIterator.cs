using Silex.Blocks;
using Silex.Serialization;
using System.Runtime.CompilerServices;

namespace Silex.Tables;

internal sealed class SsTableIterator<TKey> : IStorageIterator<TKey, ValueBuffer>
{
    private static readonly IComparer<TKey> _keyComparer = KeyEncoderFactory<TKey>.Encoder.Comparer;

    private readonly SsTable<TKey> _table;

    public SsTableIterator(SsTable<TKey> table)
    {
        _table = table;
    }

    public async IAsyncEnumerable<KeyValuePair<TKey, ValueBuffer>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        for (var i = 0; i < _table.BlockMetadata.Count; i++)
        {
            var blockMetadata = _table.BlockMetadata[i];

            using var block = await _table.ReadBlockAsync(blockMetadata.Index, cancellationToken);

            if (block != null)
            {
                var blockIterator = new BlockIterator<TKey>(block);
                await foreach (var entry in blockIterator.EnumerateAsync(cancellationToken))
                {
                    yield return entry;
                }
            }
        }
    }

    public async IAsyncEnumerable<KeyValuePair<TKey, ValueBuffer>> EnumerateAsync(TKey from, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var startBlockIndex = FindStartBlockIndex(from);

        // If the key doesn't exist, exit
        if (startBlockIndex > _table.BlockMetadata.Count - 1)
        {
            yield break;
        }

        // The stepped-back block ends before 'from' (this happens when 'from' falls exactly on a later
        // block's FirstKey, or in a gap between blocks): the first key >= from lives in a later block, so
        // advance to it instead of giving up. Breaking here would silently drop every key from 'from' on.
        if (_keyComparer.Compare(_table.BlockMetadata[startBlockIndex].LastKey, from) < 0)
        {
            startBlockIndex++;

            if (startBlockIndex > _table.BlockMetadata.Count - 1)
            {
                yield break;
            }
        }

        // Iterate this block only, after the specified key
        var blockMetadata = _table.BlockMetadata[startBlockIndex];

        using var block = await _table.ReadBlockAsync(blockMetadata.Index, cancellationToken);

        if (block != null)
        {
            var blockIterator = new BlockIterator<TKey>(block);
            await foreach (var entry in blockIterator.EnumerateAsync(from, cancellationToken))
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
                var blockIterator = new BlockIterator<TKey>(block2);
                await foreach (var entry in blockIterator.EnumerateAsync(cancellationToken))
                {
                    yield return entry;
                }
            }
        }

        yield break;
    }

    private int FindStartBlockIndex(TKey from)
    {
        var start = 0;
        var end = _table.BlockMetadata.Count - 1;

        while (start <= end)
        {
            var m = start + (end - start) / 2;
            var compare = _keyComparer.Compare(_table.BlockMetadata[m].FirstKey, from);

            if (compare == 0)
            {
                // The entry may live in the block whose FirstKey precedes 'from'.
                return Math.Max(0, m - 1);
            }

            if (compare < 0)
            {
                start = m + 1;
            }
            else
            {
                end = m - 1;
            }
        }

        // 'start' is the insertion index. Step back one block and clamp to the first block.
        return Math.Max(0, start - 1);
    }
}
