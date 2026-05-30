using Silex;

namespace Silex.DbBench;

/// <summary>
/// Parsed configuration for a benchmark run. Flag names (wired up in <see cref="CommandLine"/>) mirror
/// RocksDB's <c>db_bench</c> where an equivalent concept exists so the same invocation can be used for
/// cross-engine comparison; Silex-specific knobs are added for tuning during perf work.
/// </summary>
internal sealed class BenchmarkOptions
{
    // db_bench-compatible flags.
    public string Benchmarks { get; set; } = "fillseq,readrandom,readseq";
    public long Num { get; set; } = 1_000_000;
    public long Reads { get; set; } = -1; // -1 => same as Num
    public int ValueSize { get; set; } = 100;
    public int KeySize { get; set; } = 16;
    public string Db { get; set; } = string.Empty; // empty => temp folder
    public bool UseExistingDb { get; set; }
    public int Seed { get; set; } = 0;
    public bool Histogram { get; set; }
    public int Threads { get; set; } = 1;
    public long WriteBufferSize { get; set; } = 64L * 1024 * 1024; // MemTableSizeLimit
    public int BlockSize { get; set; } = 4 * 1024;
    public long CacheSize { get; set; } = 8L * 1024 * 1024; // block cache
    public int SeekNexts { get; set; } = 0;

    // Silex-specific knobs (no db_bench equivalent, used for tuning / perf work).
    public CompactionStrategy Compaction { get; set; } = CompactionStrategy.Tiered;
    public bool Wal { get; set; } = true;
    public bool WalSync { get; set; } = false;
    public long TargetSstSize { get; set; } = 2L * 1024 * 1024;
    public int CompactionParallelism { get; set; } = Environment.ProcessorCount;
    public int ReadParallelism { get; set; } = Environment.ProcessorCount;

    public long EffectiveReads => Reads < 0 ? Num : Reads;

    /// <summary>Builds the engine options shared by every benchmark in the run.</summary>
    public StorageOptions ToStorageOptions(bool walSyncOverride)
    {
        return new StorageOptions
        {
            MemTableSizeLimit = WriteBufferSize,
            BlockSize = (ushort)Math.Clamp(BlockSize, 1, ushort.MaxValue),
            BlockCacheSizeLimit = CacheSize,
            UseWriteAheadLog = Wal,
            SyncWriteAheadLogToDisk = walSyncOverride || WalSync,
            CompactionStrategy = Compaction,
            TargetSstSizeBytes = TargetSstSize,
            MaxCompactionParallelism = CompactionParallelism,
            MaxReadParallelism = ReadParallelism,
        };
    }
}
