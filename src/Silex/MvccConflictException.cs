namespace Silex;

/// <summary>
/// Thrown when an optimistic MVCC transaction observes a conflicting committed write.
/// </summary>
public sealed class MvccConflictException : Exception
{
    public MvccConflictException()
        : base("The transaction conflicts with a write committed after its snapshot was created.")
    {
    }

    /// <summary>Creates an exception carrying commit-validation diagnostics.</summary>
    public MvccConflictException(MvccConflict conflict)
        : this()
    {
        ArgumentNullException.ThrowIfNull(conflict);
        Conflict = conflict;
    }

    /// <summary>Validation diagnostics, or null when constructed with the parameterless constructor.</summary>
    public MvccConflict? Conflict { get; }
}
