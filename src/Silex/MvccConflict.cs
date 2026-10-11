namespace Silex;

/// <summary>The dependency invalidated by a post-snapshot write.</summary>
public enum MvccConflictKind
{
    WrittenKey,
    TrackedPointRead,
    TrackedRange,
}

/// <summary>Owned diagnostic data for the first conflict detected during commit validation.</summary>
/// <remarks>
/// Keys and bounds survive transaction disposal. This is not a list of all conflicts or a retry policy.
/// An earlier accepted group member can cause a conflict before its durability or publication succeeds;
/// <see cref="ConflictingSequence"/> identifies its reserved sequence, not proof of a durable write.
/// </remarks>
public sealed class MvccConflict
{
    internal MvccConflict(MvccConflictKind kind, ReadOnlySpan<byte> key, long snapshotSequence,
        long conflictingSequence, OrderedReadRange? range = null)
    {
        Kind = kind;
        Key = key.ToArray();
        SnapshotSequence = snapshotSequence;
        ConflictingSequence = conflictingSequence;
        Range = range;
    }

    public MvccConflictKind Kind { get; }
    /// <summary>The encoded user key, including for a range conflict. Empty memory means the empty key.</summary>
    public ReadOnlyMemory<byte> Key { get; }
    public long SnapshotSequence { get; }
    public long ConflictingSequence { get; }
    /// <summary>The registered interval for a range conflict; otherwise null.</summary>
    public OrderedReadRange? Range { get; }
}
