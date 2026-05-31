namespace Silex.Tables;
public interface ISsTableBuilder<TKey, TValue> : IDisposable
{
    long EstimatedSize { get; }
    Task AddAsync(TKey key, TValue value, CancellationToken cancellationToken = default);
    Task<SsTable<TKey, TValue>> BuildAsync(CancellationToken cancellationToken = default);
}
