using Silex;
using Silex.Blocks;
using Silex.BloomFilters;

namespace Silex.DbBench;

internal sealed class BenchmarkOptions
{
    public string Benchmarks { get; set; } = "fillseq,readrandom,readseq";

    public long Num { get; set; } = 1_000_000;

    public long Reads { get; set; } = -1;

    public int ValueSize { get; set; } = 100;

    public int KeySize { get; set; } = 16;

    public string Db { get; set; } = string.Empty;

    public bool UseExistingDb { get; set; }

    public int Seed { get; set; }

    public bool Histogram { get; set; }

    public int Threads { get; set; } = 1;

    public long WriteBufferSize { get; set; } = 64L * 1024 * 1024;

    public int MaxWriteBufferNumber { get; set; } = 50;

    public int BloomBits { get; set; } = 10;

    public int BlockSize { get; set; } = 4 * 1024;

    public long CacheSize { get; set; } = 8L * 1024 * 1024;

    public SstCompression Compression { get; set; } = SstCompression.Lz4;

    public int CompressionLevel { get; set; }

    public double CompressionRatio { get; set; } = 1;

    public int SeekNexts { get; set; }

    public CompactionStrategy Compaction { get; set; } = CompactionStrategy.Tiered;

    public bool Wal { get; set; } = true;

    public bool WalSync { get; set; }

    public int Level0FileNumCompactionTrigger { get; set; } = 4;

    public int NumLevels { get; set; } = 7;

    public long MaxBytesForLevelBase { get; set; } = 256 * 1024;

    public int MaxBytesForLevelMultiplier { get; set; } = 10;

    public long TargetSstSize { get; set; } = 2L * 1024 * 1024;

    public int UniversalMaxReadAmp { get; set; } = 8;

    public int UniversalMaxSizeAmplificationPercent { get; set; } = 200;

    public int UniversalSizeRatio { get; set; } = 1;

    public int UniversalMinMergeWidth { get; set; } = 2;

    public int CompactionParallelism { get; set; } = Environment.ProcessorCount;

    public int ReadParallelism { get; set; } = Environment.ProcessorCount;

    public long EffectiveReads => Reads < 0 ? Num : Reads;

    public StorageOptions ToStorageOptions(bool walSync)
    {
        var blockSize = ToUInt16("--block_size", BlockSize);

        return new StorageOptions
        {
            MemTableSizeLimit = Positive("--write_buffer_size", WriteBufferSize),
            MemTableMaxCount = ToUInt16("--max_write_buffer_number", MaxWriteBufferNumber),
            BlockSize = blockSize,
            BlockCacheSizeLimit = NonNegative("--cache_size", CacheSize),
            Compression = Compression,
            CompressionLevel = CompressionLevel,
            BloomFilterFactory = new RocksStyleBloomFilterFactory(NonNegative("--bloom_bits", BloomBits)),
            UseWriteAheadLog = Wal,
            SyncWriteAheadLogToDisk = Wal && (WalSync || walSync),
            CompactionStrategy = Compaction,
            MaxCompactionTiers = Positive("--universal_max_read_amp", UniversalMaxReadAmp),
            MaxSizeAmplificationPercent = Positive("--universal_max_size_amplification_percent", UniversalMaxSizeAmplificationPercent),
            SizeRatioPercent = NonNegative("--universal_size_ratio", UniversalSizeRatio),
            MinMergeWidth = Positive("--universal_min_merge_width", UniversalMinMergeWidth),
            Level0CompactionThreshold = Positive("--level0_file_num_compaction_trigger", Level0FileNumCompactionTrigger),
            BaseLevelTargetBytes = Positive("--max_bytes_for_level_base", MaxBytesForLevelBase),
            LevelSizeMultiplier = Positive("--max_bytes_for_level_multiplier", MaxBytesForLevelMultiplier),
            MaxLevels = Positive("--num_levels", NumLevels),
            TargetSstSizeBytes = Positive("--target_file_size_base", TargetSstSize),
            MaxCompactionParallelism = Positive("--compaction_parallelism", CompactionParallelism),
            MaxReadParallelism = Positive("--read_parallelism", ReadParallelism),
        };
    }

    private static int NonNegative(string name, int value)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(name, value, $"{name} must be non-negative.");
        }

        return value;
    }

    private static long NonNegative(string name, long value)
    {
        if (value < 0)
        {
            throw new ArgumentOutOfRangeException(name, value, $"{name} must be non-negative.");
        }

        return value;
    }

    private static int Positive(string name, int value)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(name, value, $"{name} must be positive.");
        }

        return value;
    }

    private static long Positive(string name, long value)
    {
        if (value <= 0)
        {
            throw new ArgumentOutOfRangeException(name, value, $"{name} must be positive.");
        }

        return value;
    }

    private static ushort ToUInt16(string name, int value)
    {
        if (value is <= 0 or > ushort.MaxValue)
        {
            throw new ArgumentOutOfRangeException(name, value, $"{name} must be in the range 1..{ushort.MaxValue}.");
        }

        return (ushort)value;
    }

    private sealed class RocksStyleBloomFilterFactory(int bitsPerKey) : IBloomFilterFactory
    {
        public IBloomFilter CreateBloomFilter(int n, double p)
        {
            if (bitsPerKey == 0)
            {
                return DisabledBloomFilter.Instance;
            }

            var probability = Math.Pow(0.6185, bitsPerKey);

            if (probability <= 0)
            {
                probability = double.Epsilon;
            }

            return new BloomFilter(n, probability);
        }

        public IBloomFilter CreateBloomFilter(ReadOnlySpan<byte> bytes, int k)
        {
            if (k == 0)
            {
                return DisabledBloomFilter.Instance;
            }

            return new BloomFilter(bytes.ToArray(), k);
        }

        public IBloomFilter CreateBloomFilterFromOwnedBytes(byte[] bytes, int k, int algorithmVersion)
        {
            if (k == 0)
            {
                return DisabledBloomFilter.Instance;
            }

            return new BloomFilter(bytes, k, algorithmVersion);
        }
    }

    private sealed class DisabledBloomFilter : IBloomFilter
    {
        public static readonly DisabledBloomFilter Instance = new();

        public int K => 0;

        public void Add(ReadOnlySpan<byte> value)
        {
        }

        public bool Probe(ReadOnlySpan<byte> item) => true;

        public ReadOnlySpan<byte> GetBytes() => [];
    }
}
