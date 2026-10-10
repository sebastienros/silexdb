namespace Silex;

/// <summary>Owned binary key bounds. Null means unbounded; empty memory is the empty key.</summary>
public sealed class OrderedReadRange
{
    public OrderedReadRange(
        ReadOnlyMemory<byte>? lower = null,
        bool lowerInclusive = true,
        ReadOnlyMemory<byte>? upper = null,
        bool upperInclusive = true)
    {
        Lower = lower.HasValue ? new ReadOnlyMemory<byte>(lower.Value.ToArray()) : (ReadOnlyMemory<byte>?)null;
        Upper = upper.HasValue ? new ReadOnlyMemory<byte>(upper.Value.ToArray()) : (ReadOnlyMemory<byte>?)null;
        LowerInclusive = lowerInclusive;
        UpperInclusive = upperInclusive;
    }

    public ReadOnlyMemory<byte>? Lower { get; }
    public ReadOnlyMemory<byte>? Upper { get; }
    public bool LowerInclusive { get; }
    public bool UpperInclusive { get; }

    internal bool IsEmpty => Lower is { } lower && Upper is { } upper &&
        (lower.Span.SequenceCompareTo(upper.Span) > 0 ||
         (lower.Span.SequenceEqual(upper.Span) && (!LowerInclusive || !UpperInclusive)));

    internal bool Before(ReadOnlySpan<byte> key) => Lower is { } lower &&
        (key.SequenceCompareTo(lower.Span) < 0 || (!LowerInclusive && key.SequenceEqual(lower.Span)));

    internal bool After(ReadOnlySpan<byte> key) => Upper is { } upper &&
        (key.SequenceCompareTo(upper.Span) > 0 || (!UpperInclusive && key.SequenceEqual(upper.Span)));
}

public enum OrderedReadDirection
{
    Forward,
    Reverse,
}

public enum OrderedReadConsistency
{
    /// <summary>No historical view. Concurrent changes may affect this or subsequent batches.</summary>
    Live,
    /// <summary>All batches read the cursor's retained MVCC sequence.</summary>
    Snapshot,
}
