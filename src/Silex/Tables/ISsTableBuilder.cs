namespace Silex.Tables;
public interface ISsTableBuilder<TKey> : IDisposable
{
    long EstimatedSize { get; }
    Task AddAsync(TKey key, ValueBuffer value, CancellationToken cancellationToken = default);
    Task<SsTable<TKey>> BuildAsync(CancellationToken cancellationToken = default);
}
