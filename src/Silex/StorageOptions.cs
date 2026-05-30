using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Tables;

namespace Silex;

public class StorageOptions
{
    /// <value>64MiB</value>
    private static readonly long _defaultTableSizeLimit = 64.MiB();

    /// <value>4KiB</value>
    private static readonly ushort _defaultBlockSize = (ushort)4.KiB();

    /// <value>50</value>
    private static readonly ushort _defaultMemTableMaxCount = 50;

    /// <value><see cref="DefaultBlockEncoderFactory"></value>
    private static readonly IBlockEncoderFactory _defaultBlockEncoderFactory = new DefaultBlockEncoderFactory();

    /// <value><see cref="DefaultSsTableEncoderFactory"></value>
    private static readonly ISsTableEncoderFactory _defaultSsTableEncoderFactory = new DefaultSsTableEncoderFactory();

    /// <value><see cref="BufferedSsTableBuilderFactory"></value>
    private static readonly ISsTableBuilderFactory _defaultSsTableBuilderFactory = new BufferedSsTableBuilderFactory();

    /// <value>1MiB</value>
    private static readonly long _defaultBlockCacheSizeLimit = 1.MiB();

    /// <value>5 minutes</value>
    private static readonly TimeSpan _defaultBlockCacheSlidingExpiration = 5.Minute();

    /// <value>1 day</value>
    private static readonly TimeSpan _defaultBlockCacheAbsoluteExpiration = 1.Day();

    /// <value>50ms</value>
    private static readonly TimeSpan _defaultFlushPeriod = TimeSpan.FromMilliseconds(50);

    /// <value><see cref="BloomFilterFactory"></value>
    private static readonly IBloomFilterFactory _defaultBloomFilterFactory = new DefaultBloomFilterFactory();

    /// <summary>
    /// Gets or sets the maximum size of a <see cref="MemTable">. When the size is reached it is made immutable and 
    /// can be stored as a level-0 SST.
    /// </summary>
    /// <remarks>
    /// This value and <see cref="MemTableMaxCount"/> will define the maximum size of allocated memory for keys and values.
    /// </remarks>/// <value>
    /// The default value is <inheritdoc cref="_defaultTableSizeLimit"/>.
    /// </value>
    public long MemTableSizeLimit { get; set; } = _defaultTableSizeLimit;

    /// <summary>
    /// Gets or sets the maximum number of <see cref="MemTable"> which can be kept in memory before being flushed
    /// and stored as a level-0 SST.
    /// </summary>
    /// <remarks>
    /// This value and <see cref="MemTableSizeLimit"/> will define the maximum size of allocated memory for keys and values.
    /// </remarks>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultMemTableMaxCount"/>.
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
    /// Gets or set the <see cref="IBlockEncoderFactory"> to use.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockEncoderFactory"/>.
    /// </value>
    public IBlockEncoderFactory BlockEncoderFactory { get; set; } = _defaultBlockEncoderFactory;

    /// <summary>
    /// Gets or set the <see cref="ISsTableEncoderFactory"> to use.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultSsTableEncoderFactory"/>.
    /// </value>
    public ISsTableEncoderFactory SsTableEncoderFactory { get; set; } = _defaultSsTableEncoderFactory;

    /// <summary>
    /// Gets or set the <see cref="ISsTableBuilderFactory"> to use.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultSsTableBuilderFactory"/>.
    /// </value>
    public ISsTableBuilderFactory SsTableBuilderFactory { get; set; } = _defaultSsTableBuilderFactory;

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
    public long BlockCacheSizeLimit { get; set; } = _defaultBlockCacheSizeLimit;

    /// <summary>
    /// Gets or sets how long a block cache entry can be inactive (e.g. not accessed) before it will be removed. This will not extend the entry lifetime beyond the absolute expiration (if set).
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockCacheSlidingExpiration"/>. Use <c>TimeSpan.Zero</c> (<c>"00:00:00"</c>) to disable sliding expiration.
    /// </value>
    public TimeSpan BlockCacheSlidingExpiration { get; set; } = _defaultBlockCacheSlidingExpiration;

    /// <summary>
    /// Gets or sets an absolute expiration time, relative to when a block cache is added, even if it is effectively accessed.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockCacheAbsoluteExpiration"/>. Use <c>TimeSpan.Zero</c> (<c>"00:00:00"</c>) to disable absolute expiration.
    /// </value>
    public TimeSpan BlockCacheAbsoluteExpiration { get; set; } = _defaultBlockCacheAbsoluteExpiration;

    /// <summary>
    /// Gets or sets the <see cref="IBloomFilterFactory"/> used to create bloom filters.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBloomFilterFactory"/>.
    /// </value>
    public IBloomFilterFactory BloomFilterFactory { get; set; } = _defaultBloomFilterFactory;

    /// <summary>
    /// Gets or sets whether a write-ahead log is maintained for each <see cref="MemTable"/> so that
    /// unflushed data can be recovered after a process crash when the store is reopened.
    /// </summary>
    /// <value>
    /// The default value is <c>true</c>.
    /// </value>
    public bool UseWriteAheadLog { get; set; } = true;

    /// <summary>
    /// Gets or sets whether each write-ahead log append is flushed all the way to disk (<c>fsync</c>).
    /// When <c>false</c> the append is only flushed to the operating system, which still survives a
    /// process crash but not a power loss. Enabling this is slower but survives power loss.
    /// </summary>
    /// <remarks>This has no effect when <see cref="UseWriteAheadLog"/> is <c>false</c>.</remarks>
    /// <value>
    /// The default value is <c>false</c>.
    /// </value>
    public bool SyncWriteAheadLogToDisk { get; set; } = false;
}
