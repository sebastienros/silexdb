using Silex.Blocks;
using System.Runtime.CompilerServices;

namespace Silex.Tables;

internal sealed class SsTableIterator : IStorageIterator
{
    private readonly SsTable _table;
    private ReadOnlyMemory<byte>[]? _firstKeys;

    public SsTableIterator(SsTable table)
    {
        _table = table;
    }

    public IAsyncEnumerable<RecordLocation> EnumerateAsync(CancellationToken cancellationToken = default)
    {
        return EnumerateAsync(ReadOnlyMemory<byte>.Empty, cancellationToken);
    }

    public async IAsyncEnumerable<RecordLocation> EnumerateAsync(ReadOnlyMemory<byte> afterKey, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var startBlockIndex = 0;

        if (!afterKey.IsEmpty)
        {
            // Create a reusable index for binary search
            _firstKeys ??= _table.BlockMetadata.Select(x => x.FirstKey).ToArray();

            var compare = Array.BinarySearch(_firstKeys, afterKey, ByteArrayComparer.Instance);

            startBlockIndex = (compare >= 0 ? compare : ~compare) - 1;

            // If the key doesn't exist, exit
            if (startBlockIndex > _table.BlockMetadata.Count - 1)
            {
                yield break;
            }

            // Iterate this block only, after the specified key
            var blockMetadata = _table.BlockMetadata[startBlockIndex];

            using var block = await _table.LoadBlockAsync(blockMetadata.Index, cancellationToken);

            if (block != null)
            {
                var blockIterator = new BlockIterator(block);
                await foreach (var entry in blockIterator.EnumerateAsync(afterKey, cancellationToken))
                {
                    yield return entry;
                }
            }

            startBlockIndex++;
        }

        for (var i = startBlockIndex; i < _table.BlockMetadata.Count; i++)
        {
            var blockMetadata = _table.BlockMetadata[i];

            using var block = await _table.LoadBlockAsync(blockMetadata.Index, cancellationToken);

            if (block != null)
            {
                var blockIterator = new BlockIterator(block);
                await foreach (var entry in blockIterator.EnumerateAsync(cancellationToken))
                {
                    yield return new RecordLocation
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
