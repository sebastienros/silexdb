namespace Silex;

public interface IStorageIterator
{
    IAsyncEnumerable<RecordLocation> EnumerateAsync(CancellationToken cancellationToken = default);

    IAsyncEnumerable<RecordLocation> EnumerateAsync(ReadOnlyMemory<byte> afterKey, CancellationToken cancellationToken = default);
}
