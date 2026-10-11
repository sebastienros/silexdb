namespace Silex;

/// <summary>Outcome of one bounded <see cref="MvccTransaction.DeleteRangeAsync"/> call.</summary>
/// <param name="DeletedCount">Keys visible to the transaction (snapshot plus its own writes) that were staged for deletion.</param>
/// <param name="Remaining">Null when the whole requested range was processed; otherwise the unprocessed suffix, to be passed to the next call.</param>
public readonly record struct RangeDeleteResult(int DeletedCount, OrderedReadRange? Remaining)
{
    public bool IsComplete => Remaining is null;
}
