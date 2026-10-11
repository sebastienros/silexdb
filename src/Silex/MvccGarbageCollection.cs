namespace Silex;

/// <summary>Why an incremental MVCC garbage collection call returned.</summary>
public enum MvccGarbageCollectionStopReason
{
    /// <summary>The whole version keyspace was examined; there is nothing to resume.</summary>
    Completed,
    /// <summary><see cref="MvccGarbageCollectionOptions.MaxVersionsExamined"/> was reached.</summary>
    ExaminedLimit,
    /// <summary><see cref="MvccGarbageCollectionOptions.MaxVersionsDeleted"/> was reached.</summary>
    DeletedLimit,
    /// <summary><see cref="MvccGarbageCollectionOptions.MaxDuration"/> elapsed.</summary>
    TimeBudget,
}

/// <summary>
/// An opaque-by-contract position in the physical version keyspace at which a later collection can resume.
/// </summary>
/// <remarks>
/// A cursor is only a scan hint. It carries no retention decision: every call recomputes which versions are
/// required from the live snapshot registry, so a stale cursor (even from before a restart or from another
/// process) can never cause a needed version to be deleted. Versions behind the cursor are simply not
/// re-examined until a later call starts without one.
/// </remarks>
public sealed class MvccGarbageCollectionCursor
{
    private readonly byte[] _position;

    /// <summary>Restores a cursor previously obtained from <see cref="Position"/>.</summary>
    public MvccGarbageCollectionCursor(ReadOnlySpan<byte> position) => _position = position.ToArray();

    internal MvccGarbageCollectionCursor(byte[] ownedPosition) => _position = ownedPosition;

    /// <summary>The persistable position bytes; validated when the cursor is used.</summary>
    public ReadOnlyMemory<byte> Position => _position;

    internal byte[] PositionArray => _position;
}

/// <summary>Limits and behavior for one incremental MVCC garbage collection call.</summary>
public sealed class MvccGarbageCollectionOptions
{
    /// <summary>Maximum physical versions examined by the call. Defaults to unlimited.</summary>
    public long MaxVersionsExamined { get; init; } = long.MaxValue;

    /// <summary>Maximum obsolete versions deleted by the call. Defaults to unlimited.</summary>
    public long MaxVersionsDeleted { get; init; } = long.MaxValue;

    /// <summary>
    /// Elapsed-time budget for the call's scan and delete work, checked between small groups of versions and
    /// between chunks. Each call examines at least one version, so repeated calls always progress.
    /// Optional flush and compaction run after the budget and are not interruptible except by cancellation.
    /// </summary>
    public TimeSpan? MaxDuration { get; init; }

    /// <summary>
    /// Maximum versions examined per chunk. A chunk is scanned without the commit gate and its obsolete
    /// versions are deleted in one atomic batch while holding the gate, so this bounds both temporary memory
    /// and commit-gate hold time. Defaults to 1024.
    /// </summary>
    public int ChunkSize { get; init; } = 1024;

    /// <summary>
    /// Maximum bytes of obsolete keys buffered per chunk (a single larger key still forms its own chunk).
    /// Defaults to 1 MiB.
    /// </summary>
    public int MaxChunkBytes { get; init; } = 1 << 20;

    /// <summary>
    /// When <c>true</c> and the call deleted versions, flushes and compacts afterward without the commit gate.
    /// </summary>
    public bool Compact { get; init; }

    /// <summary>Position returned by a previous call; <c>null</c> starts from the beginning.</summary>
    public MvccGarbageCollectionCursor? Resume { get; init; }

    /// <summary>Receives a report after every chunk is applied.</summary>
    public IProgress<MvccGarbageCollectionProgress>? Progress { get; init; }

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxVersionsExamined);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxVersionsDeleted);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ChunkSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(MaxChunkBytes);
        if (MaxDuration is { } duration && duration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxDuration));
        }
    }
}

/// <summary>Cumulative progress of one call after a chunk was durably applied.</summary>
/// <param name="VersionsExamined">Versions examined by the call so far.</param>
/// <param name="VersionsDeleted">Obsolete versions deleted by the call so far; they are already committed.</param>
/// <param name="Resume">Where the next call can continue; progress never needs to be rolled back.</param>
public readonly record struct MvccGarbageCollectionProgress(
    long VersionsExamined, long VersionsDeleted, MvccGarbageCollectionCursor? Resume);

/// <summary>Outcome of one incremental MVCC garbage collection call.</summary>
/// <param name="VersionsExamined">Physical versions examined.</param>
/// <param name="VersionsDeleted">Obsolete versions deleted.</param>
/// <param name="StopReason">Why the call returned.</param>
/// <param name="Resume">Pass as <see cref="MvccGarbageCollectionOptions.Resume"/>; <c>null</c> when completed.</param>
/// <param name="Elapsed">Time spent scanning and deleting (excluding compaction).</param>
/// <param name="Compacted">Whether flush and compaction ran.</param>
public readonly record struct MvccGarbageCollectionResult(
    long VersionsExamined,
    long VersionsDeleted,
    MvccGarbageCollectionStopReason StopReason,
    MvccGarbageCollectionCursor? Resume,
    TimeSpan Elapsed,
    bool Compacted)
{
    /// <summary>Whether the whole keyspace was covered.</summary>
    public bool IsCompleted => StopReason == MvccGarbageCollectionStopReason.Completed;
}
