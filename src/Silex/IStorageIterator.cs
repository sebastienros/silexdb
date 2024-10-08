namespace Silex;

public interface IStorageIterator
{
    IAsyncEnumerable<RecordLocation> EnumerateAsync(CancellationToken cancellationToken = default);

    IAsyncEnumerable<RecordLocation> EnumerateAsync(Bytes afterKey, CancellationToken cancellationToken = default);
}
