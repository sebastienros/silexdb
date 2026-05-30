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

    /// <summary>
    /// Leveled compaction. SSTs are organised into levels of geometrically increasing size; each level
    /// (below L0) is a single sorted run with non-overlapping key ranges. Trades higher write
    /// amplification for the lowest read and space amplification. Best for read-heavy workloads. Requires
    /// a manifest to record which SST belongs to which level across restarts.
    /// </summary>
    Leveled,
}
