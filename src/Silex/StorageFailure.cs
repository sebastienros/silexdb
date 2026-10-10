namespace Silex;

/// <summary>The outcome of a failed mutation, including what recovery may observe.</summary>
public enum StorageCommitOutcome
{
    /// <summary>No mutation was applied and no WAL frame was submitted.</summary>
    NotCommitted,
    /// <summary>The mutation may be recovered; memory may contain only part of a batch.</summary>
    Indeterminate,
    /// <summary>The entire mutation was applied and reached the configured WAL boundary.</summary>
    Committed
}

/// <summary>Whether an instance can accept operations.</summary>
public enum StorageHealth
{
    Healthy,
    Faulted,
    Disposed
}

/// <summary>A mutation failed. Inspect <see cref="Outcome"/> before deciding whether to retry.</summary>
public sealed class StorageOperationException : IOException
{
    public StorageCommitOutcome Outcome { get; }

    internal StorageOperationException(StorageCommitOutcome outcome, Exception cause)
        : base($"Storage mutation failed with outcome {outcome}.", cause) => Outcome = outcome;
}

/// <summary>The instance is unsafe to use. Dispose it without flushing and reopen for recovery.</summary>
public sealed class StorageFaultedException : InvalidOperationException
{
    internal StorageFaultedException(Exception cause)
        : base("The storage instance is faulted. Dispose it and reopen to recover; do not blindly retry indeterminate mutations.", cause)
    {
    }
}

internal enum StorageFaultPoint
{
    WalBeforeWrite,
    WalDuringWrite,
    WalAfterWrite,
    WalBeforeSync,
    WalAfterSync,
    BatchBeforeApply,
    BatchAfterApplyEntry,
    WriteBeforePublication,
    MvccBeforePublication,
    MemTableBeforeCreate,
    RecoveryBeforeReplay,
    RecoveryAfterApplyEntry,
    FlushBeforeBuild,
    FlushBeforeInstall,
    FlushAfterInstall,
    CompactionBeforeBuild,
    CompactionBeforeInstall,
    CompactionAfterInstall,
    ManifestBeforePublish,
    ManifestAfterPublish
}
