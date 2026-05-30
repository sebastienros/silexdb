namespace Silex;

/// <summary>
/// Selects how on-disk SSTs are compacted in the background to bound read amplification and reclaim
/// space from overwritten and deleted entries.
/// </summary>
public enum CompactionStrategy
{
    /// <summary>
    /// No compaction. Flushed SSTs accumulate without ever being merged. Read amplification grows
    /// without bound and deleted/overwritten data is never reclaimed.
    /// </summary>
    None,

    /// <summary>
    /// Tiered (a.k.a. RocksDB universal) compaction. Each flushed memtable forms a new sorted run; runs
    /// are merged together when they accumulate, trading higher read/space amplification for the lowest
    /// write amplification. Best for write-heavy workloads.
    /// </summary>
    Tiered,
}
