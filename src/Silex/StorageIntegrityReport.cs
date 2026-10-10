namespace Silex;

public enum StorageIntegrityIssueKind
{
    MissingSst,
    CorruptSst,
    CorruptWal,
    CorruptManifest,
    IncompleteWalTail,
    IncompletePhysicalCopy
}

/// <summary>An on-disk integrity finding, including the affected path and recovery guidance.</summary>
public sealed record StorageIntegrityIssue(StorageIntegrityIssueKind Kind, string Path, string Message)
{
    /// <summary>An incomplete WAL tail is uncommitted and does not imply lost committed data.</summary>
    public bool IsError => Kind != StorageIntegrityIssueKind.IncompleteWalTail;
}

/// <summary>
/// Findings from an offline inspection. Completeness is relative to the committed manifest and
/// surviving WALs; files lost from a legacy manifest-less store cannot be detected.
/// </summary>
public sealed class StorageIntegrityReport
{
    internal StorageIntegrityReport(IEnumerable<StorageIntegrityIssue> issues)
    {
        Issues = Array.AsReadOnly(issues.ToArray());
    }

    public IReadOnlyList<StorageIntegrityIssue> Issues { get; }
    public bool IsRecoveryComplete => !Issues.Any(issue => issue.IsError);
}

/// <summary>Opening failed because the committed database cannot be completely recovered.</summary>
public sealed class StorageRecoveryException : IOException
{
    internal StorageRecoveryException(StorageIntegrityReport report)
        : base("Storage recovery is incomplete. " + string.Join(" ", report.Issues.Select(issue => issue.Message)))
    {
        Report = report;
    }

    public StorageIntegrityReport Report { get; }
}
