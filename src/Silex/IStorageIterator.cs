using Silex.Blocks;

namespace Silex;

public interface IStorageIterator
{
    IAsyncEnumerable<BlockEntry> EnumerateAsync(CancellationToken cancellationToken = default);

    IAsyncEnumerable<BlockEntry> EnumerateAsync(ReadOnlyMemory<byte> afterKey, CancellationToken cancellationToken = default);
}
