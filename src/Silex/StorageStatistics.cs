using System.Diagnostics;

namespace Silex;

/// <summary>Count, total, maximum and latest value of a measured duration.</summary>
/// <remarks>
/// <see cref="Max"/>, <see cref="Last"/> and <see cref="Average"/> are <c>null</c> until at least one sample has
/// been recorded, so "never measured" is distinguishable from "measured as zero".
/// </remarks>
/// <param name="Count">Number of samples recorded since the instance was opened.</param>
/// <param name="Total">Sum of all samples.</param>
/// <param name="Max">Largest sample, or <c>null</c> when <see cref="Count"/> is zero.</param>
/// <param name="Last">Most recent sample, or <c>null</c> when <see cref="Count"/> is zero.</param>
public readonly record struct DurationStatistics(long Count, TimeSpan Total, TimeSpan? Max, TimeSpan? Last)
{
    /// <summary>Mean sample, or <c>null</c> when no samples exist.</summary>
    public TimeSpan? Average => Count == 0 ? null : Total / Count;

    internal static DurationStatistics FromTicks(long count, long totalTicks, long maxTicks, long lastTicks) =>
        count == 0
            ? default
            : new DurationStatistics(count, ToTimeSpan(totalTicks), ToTimeSpan(maxTicks), ToTimeSpan(lastTicks));

    internal static TimeSpan ToTimeSpan(long stopwatchTicks) => Stopwatch.GetElapsedTime(0, Math.Max(0, stopwatchTicks));
}

/// <summary>Signals that the engine is accumulating work faster than it retires it.</summary>
/// <remarks>
/// These are observations for a higher layer's admission control. The engine never delays or rejects writes
/// because of them.
/// </remarks>
[Flags]
public enum StoragePressureCauses
{
    /// <summary>No pressure signal is raised.</summary>
    None = 0,
    /// <summary>The instance is faulted and refuses operations.</summary>
    Faulted = 1,
    /// <summary>Immutable memtables are waiting for flush and their count has reached <c>MemTableMaxCount</c>, so a flush is due.</summary>
    MemTableLimit = 2,
    /// <summary>Level-0 has reached the table count at which the configured strategy compacts it.</summary>
    Level0Backlog = 4,
    /// <summary>The configured strategy currently has compaction work selected or levels over their target.</summary>
    CompactionBacklog = 8,
}

/// <summary>Tables and on-disk bytes of one level.</summary>
/// <param name="Level">0 for level-0 (flushed memtables and tiered runs), then 1, 2, ... for leveled compaction.</param>
/// <param name="TableCount">Number of SST files.</param>
/// <param name="Bytes">Total SST file bytes (compressed, as stored).</param>
public sealed record StorageLevelStatistics(int Level, int TableCount, long Bytes);

/// <summary>Copied metadata of a currently published SST. Keys are physical encoded keys, including MVCC versions.</summary>
public sealed record StorageTableStatistics(long FileId, int Level, long Bytes, int Blocks, byte[] FirstKey, byte[] LastKey);

/// <summary>Cumulative flush or compaction activity since the instance was opened.</summary>
/// <param name="Duration">Count and wall time of completed operations; failed operations are not included.</param>
/// <param name="IsRunning">Whether one is executing right now.</param>
/// <param name="InputBytes">Bytes consumed: memtable bytes for flushes, SST bytes for compactions.</param>
/// <param name="OutputBytes">SST bytes produced.</param>
public sealed record StorageMaintenanceStatistics(
    DurationStatistics Duration, bool IsRunning, long InputBytes, long OutputBytes);

/// <summary>Write-ahead-log activity since the instance was opened.</summary>
/// <param name="Enabled">Whether a write-ahead log is configured. When false every other member is zero or empty.</param>
/// <param name="SynchronizesEveryWrite">Whether each acknowledged write waits for a disk flush.</param>
/// <param name="BytesWritten">Frame bytes appended.</param>
/// <param name="Sync">Disk-flush latency of every WAL synchronization, including explicit <c>SynchronizeAsync</c>.</param>
public sealed record StorageWalStatistics(
    bool Enabled, bool SynchronizesEveryWrite, long BytesWritten, DurationStatistics Sync);

/// <summary>Block cache effectiveness.</summary>
/// <param name="Enabled">Whether the configured size limit is greater than zero. When false nothing is retained.</param>
/// <param name="SizeLimitBytes">Configured limit.</param>
/// <param name="SizeBytes">Bytes currently resident.</param>
/// <param name="EntryCount">Blocks currently resident.</param>
/// <param name="Hits">Lookups served from a resident block.</param>
/// <param name="Misses">Lookups that had to read and decode the block from its SST.</param>
/// <param name="Evictions">Blocks removed to stay within the limit.</param>
public sealed record StorageCacheStatistics(
    bool Enabled, long SizeLimitBytes, long SizeBytes, long EntryCount, long Hits, long Misses, long Evictions)
{
    /// <summary>Hits divided by lookups, or <c>null</c> when nothing was looked up.</summary>
    public double? HitRatio => Hits + Misses == 0 ? null : (double)Hits / (Hits + Misses);
}

/// <summary>Failures of work the engine runs on its own.</summary>
/// <param name="Count">Failures since the instance was opened. A background failure also faults the instance.</param>
/// <param name="Last">The most recent failure, or <c>null</c> when there were none.</param>
/// <param name="LastAt">When <paramref name="Last"/> occurred, or <c>null</c> when there were none.</param>
public sealed record StorageBackgroundFailureStatistics(long Count, Exception? Last, DateTimeOffset? LastAt);

/// <summary>A point-in-time view of the physical state of a plain LSM engine.</summary>
/// <remarks>
/// <para>
/// Counters are cumulative since this instance was opened and are read without stopping writers, so members
/// are individually consistent but not a single atomic cut. Structure (tables and memtables) is a consistent
/// cut. Nothing here is a logical row count or selectivity statistic: all sizes are physical bytes.
/// </para>
/// <para>
/// A measurement that cannot be taken is <c>null</c> or flagged unavailable rather than reported as zero.
/// </para>
/// </remarks>
public sealed record LsmStorageStatistics
{
    /// <summary>Instance health.</summary>
    public required StorageHealth Health { get; init; }

    /// <summary>The first unsafe failure, or <c>null</c> while healthy.</summary>
    public Exception? Fault { get; init; }

    /// <summary>Raised pressure signals; see <see cref="StoragePressureCauses"/>.</summary>
    public required StoragePressureCauses Pressure { get; init; }

    /// <summary>Bytes held by the memtable currently accepting writes.</summary>
    public required long ActiveMemTableBytes { get; init; }

    /// <summary>Frozen memtables waiting to be flushed; each is a pending flush.</summary>
    public required int ImmutableMemTableCount { get; init; }

    /// <summary>Bytes held by frozen memtables.</summary>
    public required long ImmutableMemTableBytes { get; init; }

    /// <summary>Configured <c>MemTableMaxCount</c>.</summary>
    public required int MemTableCountLimit { get; init; }

    /// <summary>Per-level SST counts and bytes. Level 0 is always present.</summary>
    public required IReadOnlyList<StorageLevelStatistics> Levels { get; init; }

    /// <summary>Total SST bytes across all levels (physical, as stored).</summary>
    public required long TotalTableBytes { get; init; }

    /// <summary>Total SST files across all levels.</summary>
    public required int TotalTableCount { get; init; }

    /// <summary>The configured compaction strategy.</summary>
    public required CompactionStrategy CompactionStrategy { get; init; }

    /// <summary>
    /// Bytes of SSTs the configured strategy would compact next (tiered: the selected run suffix; leveled: level 0
    /// once it reaches its threshold plus each level's excess over its target). <c>null</c> when compaction is
    /// disabled, because no backlog can be defined. This is the data that would be rewritten, not the output.
    /// </summary>
    public long? CompactionBacklogBytes { get; init; }

    /// <summary>Memtable flush activity.</summary>
    public required StorageMaintenanceStatistics Flush { get; init; }

    /// <summary>SST compaction activity.</summary>
    public required StorageMaintenanceStatistics Compaction { get; init; }

    /// <summary>
    /// Time writers waited for the memtable writer lock because another writer (or a maintenance structure
    /// change) held it. The engine has no deliberate write stall: this measures only contended acquisitions,
    /// which include time spent behind a WAL disk flush.
    /// </summary>
    public required DurationStatistics WriterLockWait { get; init; }

    /// <summary>Writes (single operations, batches and commit groups) applied by the engine.</summary>
    public required long WriteOperations { get; init; }

    /// <summary>Write-ahead-log activity.</summary>
    public required StorageWalStatistics Wal { get; init; }

    /// <summary>Block cache effectiveness.</summary>
    public required StorageCacheStatistics Cache { get; init; }

    /// <summary>Failures of background flush/compaction.</summary>
    public required StorageBackgroundFailureStatistics BackgroundFailures { get; init; }

    /// <summary>Key and value bytes accepted from callers since open.</summary>
    public required long UserBytesWritten { get; init; }

    /// <summary>
    /// (WAL bytes + flush output + compaction output) divided by <see cref="UserBytesWritten"/>, or <c>null</c>
    /// when nothing was written. Counts only bytes written since open and only completed maintenance, so it
    /// understates amplification while a backlog exists.
    /// </summary>
    public double? WriteAmplification { get; init; }

    /// <summary>
    /// Total SST bytes divided by the size of the largest level (leveled) or run (tiered), a heuristic upper
    /// indication of how many times the bulk of the data may be stored. <c>null</c> without SSTs. It cannot
    /// observe logical data size and is not a measured space overhead.
    /// </summary>
    public double? SpaceAmplificationEstimate { get; init; }
}

/// <summary>An approximate physical size for a key range.</summary>
/// <remarks>
/// <para>
/// The estimate is derived from SST block-index boundaries without reading data. Every block that may contain a
/// key in the range is counted whole, so it overstates by up to two boundary blocks per table and counts shadowed
/// versions and tombstones that compaction has not yet reclaimed. The same data in several levels is counted in
/// each.
/// </para>
/// <para>
/// It is <b>not</b> an entry count, a logical cardinality, or a query selectivity statistic, and an empty
/// result does not prove that no key exists: unflushed memtables are not range-scoped, see
/// <see cref="UnflushedMemTableBytesUpperBound"/>.
/// </para>
/// </remarks>
/// <param name="ApproximateStoredBytes">Stored (possibly compressed) bytes of the overlapping SST blocks.</param>
/// <param name="ApproximateDecodedBytes">Uncompressed bytes of the same blocks.</param>
/// <param name="OverlappingTables">SSTs whose key span intersects the range.</param>
/// <param name="OverlappingBlocks">Blocks counted.</param>
/// <param name="UnflushedMemTableBytesUpperBound">
/// Total bytes of every memtable, not restricted to the range. Memtables carry no block index, so this is an
/// upper bound on unflushed data that could intersect the range.
/// </param>
public sealed record StorageRangeEstimate(
    long ApproximateStoredBytes,
    long ApproximateDecodedBytes,
    int OverlappingTables,
    int OverlappingBlocks,
    long UnflushedMemTableBytesUpperBound);

/// <summary>Garbage collection activity of an <see cref="MvccStorage"/> since it was opened.</summary>
/// <param name="IsRunning">Whether a collection call is executing.</param>
/// <param name="PassesCompleted">Calls that returned a result, whether or not they covered the whole keyspace.</param>
/// <param name="PassesFailed">Calls that ended with an error other than cancellation or disposal.</param>
/// <param name="PassesCanceled">Calls ended by their cancellation token.</param>
/// <param name="VersionsExamined">Versions examined by all calls; chunks applied by a failed call are included.</param>
/// <param name="VersionsDeleted">Versions deleted by all calls; chunks applied by a failed call are included.</param>
/// <param name="Duration">Scan-and-delete time of completed calls.</param>
/// <param name="LastResult">The most recent completed call's result, or <c>null</c> when none completed.</param>
/// <param name="LastFailure">The most recent failure, or <c>null</c> when none occurred.</param>
/// <param name="LastFailureAt">When <paramref name="LastFailure"/> occurred, or <c>null</c>.</param>
public sealed record MvccGarbageCollectionStatistics(
    bool IsRunning,
    long PassesCompleted,
    long PassesFailed,
    long PassesCanceled,
    long VersionsExamined,
    long VersionsDeleted,
    DurationStatistics Duration,
    MvccGarbageCollectionResult? LastResult,
    Exception? LastFailure,
    DateTimeOffset? LastFailureAt);

/// <summary>A point-in-time view of an <see cref="MvccStorage"/> and the engine beneath it.</summary>
/// <remarks>
/// A "read view" is anything that pins a sequence: snapshots, transactions, ordered-read cursors and reads in
/// flight. The oldest pinned view bounds what garbage collection may reclaim. Counters are cumulative since
/// open.
/// </remarks>
public sealed record MvccStorageStatistics
{
    /// <summary>The sequence visible to new reads.</summary>
    public required long PublishedSequence { get; init; }

    /// <summary>Number of pinned read views (a sequence pinned twice counts twice).</summary>
    public required int ActiveReadViews { get; init; }

    /// <summary>Number of distinct sequences pinned.</summary>
    public required int DistinctPinnedSequences { get; init; }

    /// <summary>The oldest sequence pinned by a read view, or <c>null</c> when none is pinned.</summary>
    public long? OldestRetainedSequence { get; init; }

    /// <summary>
    /// <see cref="PublishedSequence"/> minus <see cref="OldestRetainedSequence"/>: how many commits the oldest
    /// view is behind. <c>null</c> when no view is pinned. It bounds, but does not count, retained versions.
    /// </summary>
    public long? RetainedSequenceSpan { get; init; }

    /// <summary>
    /// Time since the oldest pinned sequence was first pinned while its registry entry was continuously
    /// non-empty. It can exceed the age of the longest-lived remaining view when later views pinned the same
    /// sequence, never the reverse. <c>null</c> when no view is pinned.
    /// </summary>
    public TimeSpan? OldestRetainedViewAge { get; init; }

    /// <summary>Commits published since open (the sequence advance); not a count of versions written.</summary>
    public required long CommitsPublished { get; init; }

    /// <summary>
    /// Physical size of the entire version keyspace: live data, retained history and not-yet-compacted
    /// tombstones together. Subtract nothing from it to obtain logical size; the engine cannot.
    /// </summary>
    public required StorageRangeEstimate VersionStorage { get; init; }

    /// <summary>Commit-gate acquisitions; <see cref="DurationStatistics.Count"/> counts only acquisitions that had to wait.</summary>
    public required long CommitGateAcquisitions { get; init; }

    /// <summary>Callers blocked on the commit gate right now.</summary>
    public required int CommitGateWaiters { get; init; }

    /// <summary>Wait times of acquisitions that found the commit gate held.</summary>
    public required DurationStatistics CommitGateWait { get; init; }

    /// <summary>Whether durable group commit is configured.</summary>
    public required bool GroupCommitEnabled { get; init; }

    /// <summary>Commit groups made durable together; zero when group commit is off.</summary>
    public required long CommitGroups { get; init; }

    /// <summary>Transactions that were accepted into commit groups.</summary>
    public required long GroupedCommits { get; init; }

    /// <summary>Commit requests queued for group commit right now; <c>null</c> when group commit is off.</summary>
    public int? PendingCommitRequests { get; init; }

    /// <summary>Garbage collection activity.</summary>
    public required MvccGarbageCollectionStatistics GarbageCollection { get; init; }

    /// <summary>The engine beneath the MVCC layer.</summary>
    public required LsmStorageStatistics Engine { get; init; }
}
