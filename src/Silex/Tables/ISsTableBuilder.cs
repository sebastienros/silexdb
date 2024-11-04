namespace Silex.Tables;
public interface ISsTableBuilder<TKey, TValue> : IDisposable
{
    long EstimatedSize { get; }
    Task AddAsync(TKey key, TValue value);
    Task<SsTable<TKey, TValue>> BuildAsync(CancellationToken cancellationToken = default);
}
