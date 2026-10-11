namespace Silex;

/// <summary>An opaque checkpoint of one transaction's staged writes and tracked reads.</summary>
/// <remarks>
/// Handles belong to their creating transaction. A default, released, or invalidated handle cannot be used.
/// Savepoints do not capture committed storage history or change the transaction's starting sequence.
/// </remarks>
public readonly struct MvccSavepoint
{
    internal MvccSavepoint(MvccWriteBuffer owner, long id)
    {
        Owner = owner;
        Id = id;
    }

    internal MvccWriteBuffer? Owner { get; }
    internal long Id { get; }
}
