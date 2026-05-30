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

    /// <summary>
    /// Gets or sets the compaction strategy used to merge on-disk SSTs in the background.
    /// </summary>
    /// <value>
    /// The default value is <see cref="Silex.CompactionStrategy.Tiered"/>.
    /// </value>
    public CompactionStrategy CompactionStrategy { get; set; } = CompactionStrategy.Tiered;

    /// <summary>
    /// Gets or sets the number of sorted runs (tiers) tolerated before tiered compaction starts merging
    /// them. This is the primary knob bounding read amplification under
    /// <see cref="Silex.CompactionStrategy.Tiered"/>.
    /// </summary>
    /// <value>The default value is <c>8</c>.</value>
    public int MaxCompactionTiers { get; set; } = 8;

    /// <summary>
    /// Gets or sets the space-amplification trigger for tiered compaction, as a percentage. When the
    /// combined size of every tier except the oldest divided by the oldest tier's size reaches this
    /// percentage, all tiers are merged into one. A value of <c>200</c> means a full compaction happens
    /// when the upper tiers together reach twice the size of the bottom tier.
    /// </summary>
    /// <value>The default value is <c>200</c>.</value>
    public int MaxSizeAmplificationPercent { get; set; } = 200;

    /// <summary>
    /// Gets or sets the size-ratio trigger for tiered compaction, as a percentage above 100%. Scanning
    /// from the newest tier, the newest run of tiers is merged once the next (older) tier is larger than
    /// <c>(100 + SizeRatioPercent)%</c> of the combined size of all newer tiers.
    /// </summary>
    /// <value>The default value is <c>1</c>.</value>
    public int SizeRatioPercent { get; set; } = 1;

    /// <summary>
    /// Gets or sets the minimum number of tiers that must participate before the size-ratio trigger of
    /// tiered compaction fires. Prevents merging a single tier.
    /// </summary>
    /// <value>The default value is <c>2</c>.</value>
    public int MinMergeWidth { get; set; } = 2;

    /// <summary>
    /// Gets or sets the number of L0 SSTs that triggers an L0-to-L1 compaction under
    /// <see cref="Silex.CompactionStrategy.Leveled"/>. L0 SSTs have overlapping key ranges, so keeping
    /// their count low bounds read amplification at the top of the tree.
    /// </summary>
    /// <value>The default value is <c>4</c>.</value>
    public int Level0CompactionThreshold { get; set; } = 4;

    /// <summary>
    /// Gets or sets the target total size, in bytes, of the base level (L1) under
    /// <see cref="Silex.CompactionStrategy.Leveled"/>. Each deeper level targets
    /// <see cref="LevelSizeMultiplier"/> times the size of the level above it. A level is compacted down
    /// once its total size exceeds its target.
    /// </summary>
    /// <value>The default value is <c>256 KiB</c> (<c>262144</c>).</value>
    public long BaseLevelTargetBytes { get; set; } = 256 * 1024;

    /// <summary>
    /// Gets or sets the size multiplier between adjacent levels under
    /// <see cref="Silex.CompactionStrategy.Leveled"/>. Level <c>k</c> targets
    /// <see cref="BaseLevelTargetBytes"/> × <c>multiplier^(k-1)</c> bytes.
    /// </summary>
    /// <value>The default value is <c>10</c>.</value>
    public int LevelSizeMultiplier { get; set; } = 10;

    /// <summary>
    /// Gets or sets the maximum number of levels below L0 under
    /// <see cref="Silex.CompactionStrategy.Leveled"/>. Once data reaches the deepest level it is only
    /// ever merged within that level.
    /// </summary>
    /// <value>The default value is <c>7</c>.</value>
    public int MaxLevels { get; set; } = 7;

    /// <summary>
    /// Gets or sets the approximate target size, in bytes, of a single SST produced by a leveled
    /// compaction. When a merge output reaches this size it is rolled over into a new SST, so a level is
    /// a sequence of size-bounded, non-overlapping runs rather than one giant file. This is a soft target:
    /// the estimate excludes the in-progress block and the metadata/bloom-filter trailer, so an output can
    /// exceed it by roughly one block plus that overhead.
    /// </summary>
    /// <value>The default value is <c>2 MiB</c>.</value>
    public long TargetSstSizeBytes { get; set; } = 2 * 1024 * 1024;

    /// <summary>
    /// Gets or sets the maximum degree of parallelism used when a single leveled compaction merges its
    /// inputs. When greater than <c>1</c> and there is enough data to warrant it, the merge is split into
    /// independent key-range sub-compactions that run concurrently (RocksDB calls these "subcompactions"),
    /// each producing its own size-bounded, non-overlapping output SSTs. A value of <c>1</c> runs the merge
    /// single-threaded. Only the <see cref="Silex.CompactionStrategy.Leveled"/> strategy uses this; tiered
    /// compaction always merges single-threaded.
    /// </summary>
    /// <value>The default value is <see cref="System.Environment.ProcessorCount"/>.</value>
    public int MaxCompactionParallelism { get; set; } = Environment.ProcessorCount;

    /// <summary>
    /// Gets or sets the maximum degree of parallelism used on the read/load path: SSTs are loaded
    /// concurrently when a store is opened, and once a store accumulates many overlapping L0 SSTs a point
    /// lookup probes them concurrently instead of one-by-one. A value of <c>1</c> keeps both paths
    /// sequential. Parallel point-lookup probing only engages past an internal L0 count threshold, so a
    /// store with a small, well-compacted L0 still uses the cheaper newest-first short-circuit.
    /// </summary>
    /// <value>The default value is <see cref="System.Environment.ProcessorCount"/>.</value>
    public int MaxReadParallelism { get; set; } = Environment.ProcessorCount;
}
