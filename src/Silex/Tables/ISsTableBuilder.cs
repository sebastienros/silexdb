namespace Silex.Tables;
internal interface ISsTableBuilder : IDisposable
{
    long EstimatedSize { get; }
    Task AddAsync(ByteSlice key, ByteSlice value, CancellationToken cancellationToken = default);
    Task<SsTable> BuildAsync(CancellationToken cancellationToken = default);
}
