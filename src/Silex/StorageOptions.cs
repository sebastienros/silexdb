namespace Silex;

using Silex.Blocks;
using Silex.MemTables;
using Silex.Tables;

public class StorageOptions
{
    /// <value>64MiB</value>
    private static readonly long _defaultTableSizeLimit = 64.MiB();

    /// <value>4KiB</value>
    private static readonly ushort _defaultBlockSize = (ushort)4.KiB();

    /// <value>50</value>
    private static readonly ushort _defaultMemTableMaxCount = 50;

    /// <value><see cref="DefaultBlockEncoder"></value>
    private static readonly IBlockEncoder _defaultBlockEncoder = new DefaultBlockEncoder();

    /// <value><see cref="DefaultSsTableEncoder"></value>
    private static readonly ISsTableEncoder _defaultSsTableEncoder = new DefaultSsTableEncoder();

    /// <value>1MiB</value>
    private static readonly long _defaultBlockCacheSizeLimit = 1.MiB();

    /// <value>5 minutes</value>
    private static readonly TimeSpan _defaultBlockCacheSlidingExpiration = 5.Minute();

    /// <value>1 day</value>
    private static readonly TimeSpan _defaultBlockCacheAbsoluteExpiration = 1.Day();

    /// <value>50ms</value>
    private static readonly TimeSpan _defaultFlushPeriod = TimeSpan.FromMilliseconds(50);

    /// <summary>
    /// Gets or sets the maximum size of a <see cref="MemTable">. When the size is reached it is made immutable and 
    /// can be stored as a level-0 SST.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultTableSizeLimit"/>.
    /// </value>
    public long MemTableSizeLimit { get; set; } = _defaultTableSizeLimit;

    /// <summary>
    /// Gets or sets the maximum number of <see cref="MemTable"> which can be kept in memory before being flushed
    /// and stored as a level-0 SST.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultTableSizeLimit"/>.
    /// </value>
    public ushort MemTableMaxCount { get; set; } = _defaultMemTableMaxCount;

    /// <summary>
    /// Gets or sets the size of a block in bytes.
    /// </summary>
    /// <remarks>A block is the unit of data that is stored on the disk at once.</remarks>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockSize"/>.
    /// </value>
    public ushort BlockSize { get; set; } = _defaultBlockSize;

    /// <summary>
    /// Gets or set the <see cref="IBlockEncoder"> to use.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockEncoder"/>.
    /// </value>
    public IBlockEncoder BlockEncoder { get; set; } = _defaultBlockEncoder;

    /// <summary>
    /// Gets or set the <see cref="ISsTableEncoder"> to use.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultSsTableEncoder"/>.
    /// </value>
    public ISsTableEncoder SsTableEncoder { get; set; } = _defaultSsTableEncoder;

    /// <summary>
    /// Gets or set the delays between mem table flushes. Setting a value of <c>TimeSpan.Zero</c> disables the flush thread.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultFlushPeriod"/>.
    /// </value>
    public TimeSpan FlushPeriod { get; set; } = _defaultFlushPeriod;

    /// <summary>
    /// Gets or sets the size of the block cache.
    /// </summary>
    /// <remarks>A block is the unit of data that is stored on the disk at once.</remarks>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockCacheSizeLimit"/>.
    /// </value>
    public long BlockCacheSizeLimit { get; set; } =_defaultBlockCacheSizeLimit;

    /// <summary>
    /// Gets or sets how long a cache entry can be inactive (e.g. not accessed) before it will be removed. This will not extend the entry lifetime beyond the absolute expiration (if set).
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockCacheSlidingExpiration"/>. Use <c>TimeSpan.Zero</c> (<c>"00:00:00"</c>) to disable sliding expiration.
    /// </value>
    public TimeSpan BlockCacheSlidingExpiration { get; set; } = _defaultBlockCacheSlidingExpiration;

    /// <summary>
    /// Gets or sets an absolute expiration time, relative to when the cache is added, even if it is effectively accessed.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockCacheAbsoluteExpiration"/>. Use <c>TimeSpan.Zero</c> (<c>"00:00:00"</c>) to disable absolute expiration.
    /// </value>
    public TimeSpan BlockCacheAbsoluteExpiration { get; set; } = _defaultBlockCacheAbsoluteExpiration;
}
