using System.CommandLine;
using Silex;

namespace Silex.DbBench;

/// <summary>
/// Defines the <c>db_bench</c>-compatible command line using System.CommandLine and translates a parse
/// result into a <see cref="BenchmarkOptions"/>.
/// </summary>
internal static class CommandLine
{
    private static readonly Option<string> Benchmarks = new("--benchmarks")
    {
        Description = "Comma-separated list: fillseq, fillrandom, fillsync, overwrite, readrandom, readmissing, readseq, seekrandom, deleterandom.",
        DefaultValueFactory = _ => "fillseq,readrandom,readseq",
    };

    private static readonly Option<long> Num = new("--num")
    {
        Description = "Number of key/value entries the database is filled with.",
        DefaultValueFactory = _ => 1_000_000,
    };

    private static readonly Option<long> Reads = new("--reads")
    {
        Description = "Number of read operations per thread (defaults to --num).",
        DefaultValueFactory = _ => -1,
    };

    private static readonly Option<int> ValueSize = new("--value_size")
    {
        Description = "Size of each value in bytes.",
        DefaultValueFactory = _ => 100,
    };

    private static readonly Option<int> KeySize = new("--key_size")
    {
        Description = "Size of each key in bytes.",
        DefaultValueFactory = _ => 16,
    };

    private static readonly Option<string> Db = new("--db")
    {
        Description = "Database directory (defaults to a temp folder).",
        DefaultValueFactory = _ => string.Empty,
    };

    private static readonly Option<bool> UseExistingDb = new("--use_existing_db")
    {
        Description = "Reuse (do not wipe) the database directory for fill benchmarks.",
    };

    private static readonly Option<int> Seed = new("--seed")
    {
        Description = "Base RNG seed (each thread offsets it by its id).",
        DefaultValueFactory = _ => 0,
    };

    private static readonly Option<bool> Histogram = new("--histogram")
    {
        Description = "Collect and print per-operation latency percentiles.",
    };

    private static readonly Option<int> Threads = new("--threads")
    {
        Description = "Number of concurrent client threads.",
        DefaultValueFactory = _ => 1,
    };

    private static readonly Option<long> WriteBufferSize = new("--write_buffer_size")
    {
        Description = "Memtable size limit in bytes before it is frozen/flushed.",
        DefaultValueFactory = _ => 64L * 1024 * 1024,
    };

    private static readonly Option<int> MaxWriteBufferNumber = new("--max_write_buffer_number")
    {
        Description = "Maximum number of memtables retained before Silex flushes one to L0.",
        DefaultValueFactory = _ => 50,
    };

    private static readonly Option<int> BloomBits = new("--bloom_bits")
    {
        Description = "Bloom filter bits per key. Use 0 to disable Bloom filters.",
        DefaultValueFactory = _ => 10,
    };

    private static readonly Option<int> BlockSize = new("--block_size")
    {
        Description = "SSTable block size in bytes.",
        DefaultValueFactory = _ => 4 * 1024,
    };

    private static readonly Option<long> CacheSize = new("--cache_size")
    {
        Description = "Block cache size limit in bytes.",
        DefaultValueFactory = _ => 8L * 1024 * 1024,
    };

    private static readonly Option<int> SeekNexts = new("--seek_nexts")
    {
        Description = "Entries read after each seek in seekrandom.",
        DefaultValueFactory = _ => 0,
    };

    // Silex-specific knobs.
    private static readonly Option<CompactionStrategy> Compaction = new("--compaction")
    {
        Description = "Compaction strategy: None, Tiered or Leveled.",
        DefaultValueFactory = _ => CompactionStrategy.Tiered,
    };

    private static readonly Option<string?> CompactionStyle = new("--compaction_style")
    {
        Description = "RocksDB-compatible compaction style: 0/level/leveled or 1/universal/tiered.",
    };

    private static readonly Option<bool> Wal = new("--wal")
    {
        Description = "Enable the write-ahead log.",
        DefaultValueFactory = _ => true,
    };

    private static readonly Option<bool> WalSync = new("--wal_sync")
    {
        Description = "fsync the WAL on every write.",
    };

    private static readonly Option<int> Level0FileNumCompactionTrigger = new("--level0_file_num_compaction_trigger")
    {
        Description = "Number of L0 SSTs that triggers leveled L0-to-L1 compaction.",
        DefaultValueFactory = _ => 4,
    };

    private static readonly Option<int> NumLevels = new("--num_levels")
    {
        Description = "Maximum number of levels below L0 for leveled compaction.",
        DefaultValueFactory = _ => 7,
    };

    private static readonly Option<long> MaxBytesForLevelBase = new("--max_bytes_for_level_base")
    {
        Description = "Target bytes for the base level (L1) in leveled compaction.",
        DefaultValueFactory = _ => 256L * 1024,
    };

    private static readonly Option<int> MaxBytesForLevelMultiplier = new("--max_bytes_for_level_multiplier")
    {
        Description = "Size multiplier between adjacent leveled compaction levels.",
        DefaultValueFactory = _ => 10,
    };

    private static readonly Option<long> TargetSstSize = new("--target_sst_size")
    {
        Description = "Target size of a compacted SSTable in bytes.",
        DefaultValueFactory = _ => 2L * 1024 * 1024,
    };

    private static readonly Option<long?> TargetFileSizeBase = new("--target_file_size_base")
    {
        Description = "RocksDB-compatible alias for --target_sst_size.",
    };

    private static readonly Option<int> UniversalMaxReadAmp = new("--universal_max_read_amp")
    {
        Description = "Maximum sorted runs tolerated before tiered/universal compaction starts merging.",
        DefaultValueFactory = _ => 8,
    };

    private static readonly Option<int> UniversalMaxSizeAmplificationPercent = new("--universal_max_size_amplification_percent")
    {
        Description = "Space-amplification trigger percentage for tiered/universal compaction.",
        DefaultValueFactory = _ => 200,
    };

    private static readonly Option<int> UniversalSizeRatio = new("--universal_size_ratio")
    {
        Description = "Size-ratio trigger percentage for tiered/universal compaction.",
        DefaultValueFactory = _ => 1,
    };

    private static readonly Option<int> UniversalMinMergeWidth = new("--universal_min_merge_width")
    {
        Description = "Minimum number of tiers participating in a tiered/universal merge.",
        DefaultValueFactory = _ => 2,
    };

    private static readonly Option<int> CompactionParallelism = new("--compaction_parallelism")
    {
        Description = "Max degree of parallelism for leveled subcompactions.",
        DefaultValueFactory = _ => Environment.ProcessorCount,
    };

    private static readonly Option<int> ReadParallelism = new("--read_parallelism")
    {
        Description = "Max degree of parallelism for SST loading and L0 probing.",
        DefaultValueFactory = _ => Environment.ProcessorCount,
    };

    private static readonly Option<int?> MaxBackgroundCompactions = new("--max_background_compactions")
    {
        Description = "Accepted for db_bench command compatibility; Silex runs one background compaction loop.",
    };

    // Accepted-but-ignored db_bench flags, declared so they don't error out; they emit a warning instead.
    private static readonly Option<string?> CompressionType = new("--compression_type")
    {
        Description = "Accepted for db_bench compatibility. Silex stores values uncompressed; use none for fair RocksDB runs.",
    };

    private static readonly Option<double?> CompressionRatio = new("--compression_ratio")
    {
        Description = "Ignored: Silex stores values uncompressed.",
    };

    private static readonly Option<int?> BatchSize = new("--batch_size")
    {
        Description = "Ignored unless 1: Silex has no write-batch API.",
    };

    public static RootCommand BuildRootCommand(Func<BenchmarkOptions, List<string>, Task<int>> run)
    {
        var root = new RootCommand("Silex DbBench — a RocksDB db_bench-style benchmark tool for the Silex LSM store.")
        {
            Benchmarks, Num, Reads, ValueSize, KeySize, Db, UseExistingDb, Seed, Histogram, Threads,
            WriteBufferSize, MaxWriteBufferNumber, BloomBits, BlockSize, CacheSize, SeekNexts,
            Compaction, CompactionStyle, Wal, WalSync,
            Level0FileNumCompactionTrigger, NumLevels, MaxBytesForLevelBase, MaxBytesForLevelMultiplier,
            TargetSstSize, TargetFileSizeBase,
            UniversalMaxReadAmp, UniversalMaxSizeAmplificationPercent, UniversalSizeRatio, UniversalMinMergeWidth,
            CompactionParallelism, ReadParallelism, MaxBackgroundCompactions,
            CompressionType, CompressionRatio, BatchSize,
        };

        root.SetAction((parseResult, _) =>
        {
            try
            {
                var (options, warnings) = ToOptions(parseResult);
                return run(options, warnings);
            }
            catch (ArgumentException ex)
            {
                Console.Error.WriteLine($"error: {ex.Message}");
                return Task.FromResult(1);
            }
        });

        return root;
    }

    private static (BenchmarkOptions options, List<string> warnings) ToOptions(ParseResult parseResult)
    {
        var warnings = new List<string>();

        var targetFileSizeBase = parseResult.GetValue(TargetFileSizeBase);
        var compactionStyle = parseResult.GetValue(CompactionStyle);

        var options = new BenchmarkOptions
        {
            Benchmarks = parseResult.GetValue(Benchmarks)!,
            Num = parseResult.GetValue(Num),
            Reads = parseResult.GetValue(Reads),
            ValueSize = parseResult.GetValue(ValueSize),
            KeySize = parseResult.GetValue(KeySize),
            Db = parseResult.GetValue(Db)!,
            UseExistingDb = parseResult.GetValue(UseExistingDb),
            Seed = parseResult.GetValue(Seed),
            Histogram = parseResult.GetValue(Histogram),
            Threads = Math.Max(1, parseResult.GetValue(Threads)),
            WriteBufferSize = parseResult.GetValue(WriteBufferSize),
            MaxWriteBufferNumber = parseResult.GetValue(MaxWriteBufferNumber),
            BloomBits = parseResult.GetValue(BloomBits),
            BlockSize = parseResult.GetValue(BlockSize),
            CacheSize = parseResult.GetValue(CacheSize),
            SeekNexts = parseResult.GetValue(SeekNexts),
            Compaction = compactionStyle == null ? parseResult.GetValue(Compaction) : ParseCompactionStyle(compactionStyle),
            Wal = parseResult.GetValue(Wal),
            WalSync = parseResult.GetValue(WalSync),
            Level0FileNumCompactionTrigger = parseResult.GetValue(Level0FileNumCompactionTrigger),
            NumLevels = parseResult.GetValue(NumLevels),
            MaxBytesForLevelBase = parseResult.GetValue(MaxBytesForLevelBase),
            MaxBytesForLevelMultiplier = parseResult.GetValue(MaxBytesForLevelMultiplier),
            TargetSstSize = targetFileSizeBase ?? parseResult.GetValue(TargetSstSize),
            UniversalMaxReadAmp = parseResult.GetValue(UniversalMaxReadAmp),
            UniversalMaxSizeAmplificationPercent = parseResult.GetValue(UniversalMaxSizeAmplificationPercent),
            UniversalSizeRatio = parseResult.GetValue(UniversalSizeRatio),
            UniversalMinMergeWidth = parseResult.GetValue(UniversalMinMergeWidth),
            CompactionParallelism = Math.Max(1, parseResult.GetValue(CompactionParallelism)),
            ReadParallelism = Math.Max(1, parseResult.GetValue(ReadParallelism)),
        };

        var compressionType = parseResult.GetValue(CompressionType);
        if (compressionType != null && !compressionType.Equals("none", StringComparison.OrdinalIgnoreCase))
        {
            warnings.Add($"--compression_type={compressionType} ignored: Silex stores values uncompressed. Use --compression_type=none for a fair RocksDB comparison.");
        }

        var compressionRatio = parseResult.GetValue(CompressionRatio);
        if (compressionRatio is not null && compressionRatio != 1)
        {
            warnings.Add("--compression_ratio ignored: Silex generates uncompressed random values. Use --compression_ratio=1 for a fair RocksDB comparison.");
        }

        var maxBackgroundCompactions = parseResult.GetValue(MaxBackgroundCompactions);
        if (maxBackgroundCompactions > 1)
        {
            warnings.Add("--max_background_compactions accepted for compatibility, but Silex currently runs one background compaction loop.");
        }

        var batch = parseResult.GetValue(BatchSize);
        if (batch is not null and not 1)
        {
            warnings.Add("--batch_size ignored: Silex has no write-batch API; every write is an individual Put.");
        }

        return (options, warnings);
    }

    private static CompactionStrategy ParseCompactionStyle(string value) => value.ToLowerInvariant() switch
    {
        "0" or "level" or "leveled" => CompactionStrategy.Leveled,
        "1" or "universal" or "tiered" => CompactionStrategy.Tiered,
        "none" => CompactionStrategy.None,
        _ => throw new ArgumentException($"Unsupported --compaction_style='{value}'. Use 0/level, 1/universal, or none."),
    };
}
