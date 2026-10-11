namespace Silex;

/// <summary>A staging operation would exceed a transaction resource budget.</summary>
/// <remarks>The rejected operation changes no staged state and does not consume the transaction.</remarks>
public sealed class TransactionLimitException : InvalidOperationException
{
    internal TransactionLimitException(string resource, long limit, long requested)
        : base($"Transaction {resource} limit is {limit}; the operation requires {requested}.")
    {
        Resource = resource;
        Limit = limit;
        Requested = requested;
    }

    public string Resource { get; }
    public long Limit { get; }
    public long Requested { get; }
}

internal readonly record struct TransactionLimits(long Bytes, int Mutations, int TrackedReads, int Savepoints, int ReadRanges = int.MaxValue)
{
    public static TransactionLimits Unlimited => new(long.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue, int.MaxValue);

    public static TransactionLimits FromOptions(StorageOptions options)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(options.MvccTransactionMaxBytes);
        ArgumentOutOfRangeException.ThrowIfNegative(options.MvccTransactionMaxMutations);
        ArgumentOutOfRangeException.ThrowIfNegative(options.MvccTransactionMaxTrackedReads);
        ArgumentOutOfRangeException.ThrowIfNegative(options.MvccTransactionMaxSavepoints);
        ArgumentOutOfRangeException.ThrowIfNegative(options.MvccTransactionMaxTrackedReadRanges);
        return new(options.MvccTransactionMaxBytes, options.MvccTransactionMaxMutations,
            options.MvccTransactionMaxTrackedReads, options.MvccTransactionMaxSavepoints,
            options.MvccTransactionMaxTrackedReadRanges);
    }
}
