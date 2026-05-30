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

    private static readonly Option<bool> Wal = new("--wal")
    {
        Description = "Enable the write-ahead log.",
        DefaultValueFactory = _ => true,
    };

    private static readonly Option<bool> WalSync = new("--wal_sync")
    {
        Description = "fsync the WAL on every write.",
    };

    private static readonly Option<long> TargetSstSize = new("--target_sst_size")
    {
        Description = "Target size of a compacted SSTable in bytes.",
        DefaultValueFactory = _ => 2L * 1024 * 1024,
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

    // Accepted-but-ignored db_bench flags, declared so they don't error out; they emit a warning instead.
    private static readonly Option<string?> CompressionType = new("--compression_type")
    {
        Description = "Ignored: Silex stores values uncompressed.",
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
            WriteBufferSize, BlockSize, CacheSize, SeekNexts,
            Compaction, Wal, WalSync, TargetSstSize, CompactionParallelism, ReadParallelism,
            CompressionType, CompressionRatio, BatchSize,
        };

        root.SetAction((parseResult, _) =>
        {
            var (options, warnings) = ToOptions(parseResult);
            return run(options, warnings);
        });

        return root;
    }

    private static (BenchmarkOptions options, List<string> warnings) ToOptions(ParseResult parseResult)
    {
        var warnings = new List<string>();

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
            BlockSize = parseResult.GetValue(BlockSize),
            CacheSize = parseResult.GetValue(CacheSize),
            SeekNexts = parseResult.GetValue(SeekNexts),
            Compaction = parseResult.GetValue(Compaction),
            Wal = parseResult.GetValue(Wal),
            WalSync = parseResult.GetValue(WalSync),
            TargetSstSize = parseResult.GetValue(TargetSstSize),
            CompactionParallelism = Math.Max(1, parseResult.GetValue(CompactionParallelism)),
            ReadParallelism = Math.Max(1, parseResult.GetValue(ReadParallelism)),
        };

        if (parseResult.GetValue(CompressionType) != null || parseResult.GetValue(CompressionRatio) != null)
        {
            warnings.Add("--compression_* ignored: Silex stores values uncompressed. Run RocksDB with compression disabled for a fair comparison.");
        }

        var batch = parseResult.GetValue(BatchSize);
        if (batch is not null and not 1)
        {
            warnings.Add("--batch_size ignored: Silex has no write-batch API; every write is an individual Put.");
        }

        return (options, warnings);
    }
}
