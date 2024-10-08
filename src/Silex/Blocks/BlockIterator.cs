namespace Silex.Blocks;

using Silex;
using System.Runtime.CompilerServices;

internal sealed class BlockIterator : IStorageIterator
{
    private readonly Block _block;

    private readonly RecordLocation[] _entries;

    public BlockIterator(Block block)
    {
        _block = block;
        _entries = _block.Offsets.Select(x => _block.GetEntry(x)).ToArray();
    }

    public IAsyncEnumerable<RecordLocation> EnumerateAsync(CancellationToken cancellationToken = default)
    {
        return EnumerateAsync(Bytes.Empty, cancellationToken);
    }

#pragma warning disable 1998 // async function without await
    public async IAsyncEnumerable<RecordLocation> EnumerateAsync(Bytes afterKey, [EnumeratorCancellation] CancellationToken _ = default)
#pragma warning restore 1998
    {
        var startIndex = 0;

        if (!afterKey.IsEmpty)
        {
            var compare = Array.BinarySearch(_entries, new RecordLocation { Key = afterKey });

            startIndex = compare >= 0 ? compare : ~compare;

            if (startIndex > _entries.Length + 1)
            {
                yield break;
            }
        }

        for (var i = startIndex; i < _entries.Length; i++)
        {
            yield return _entries[i];
        }
    }
}
