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
}
