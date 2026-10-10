using System.Text.Json;
using Silex.Blocks;
using Silex.Tables;
using Silex.Wal;

namespace Silex;

public sealed partial class LsmStorage
{
    /// <summary>
    /// Exclusively owns the directory while checking live SST metadata, data blocks, and WAL frames.
    /// Does not change recovery files; may create the persistent ownership file.
    /// </summary>
    /// <remarks>Orphan SSTs and WALs superseded by verified committed SSTs are not needed for recovery and are skipped.</remarks>
    public static async Task<StorageIntegrityReport> VerifyIntegrityAsync(
        string path, StorageOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();
        if (!Directory.Exists(path))
        {
            throw new DirectoryNotFoundException($"Storage directory '{path}' does not exist.");
        }

        using var ownership = DirectoryOwnership.Acquire(path);
        return await VerifyIntegrityOwnedAsync(path, options, cancellationToken);
    }

    private static async Task<StorageIntegrityReport> VerifyIntegrityOwnedAsync(
        string path, StorageOptions options, CancellationToken cancellationToken, bool allowPendingCopy = false)
    {
        var issues = new List<StorageIntegrityIssue>();
        var pendingPath = Path.Combine(path, StorageBackup.PendingFileName);
        if (!allowPendingCopy && File.Exists(pendingPath))
        {
            issues.Add(new(StorageIntegrityIssueKind.IncompletePhysicalCopy, pendingPath,
                "The physical backup/restore did not finish. Restore a completed backup to an empty destination; do not accept this partial file set."));
            return new StorageIntegrityReport(issues);
        }
        Manifest? manifest = null;
        try
        {
            manifest = Manifest.TryRead(path);
        }
        catch (Exception exception) when (IsCorruption(exception))
        {
            issues.Add(new(StorageIntegrityIssueKind.CorruptManifest, Manifest.GetPath(path),
                $"Manifest '{Manifest.GetPath(path)}' is corrupt: {exception.Message} Restore the manifest and its SSTs from a consistent backup."));
        }

        var files = Directory.EnumerateFiles(path, "*.sst")
            .Select(filename => (filename, id: TryParseId(filename)))
            .Where(item => item.id.HasValue)
            .ToDictionary(item => item.id!.Value, item => item.filename);
        var liveIds = manifest?.AllSstIds() ?? files.Keys;
        var validSstIds = new HashSet<long>();
        foreach (var id in liveIds)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!files.TryGetValue(id, out var filename))
            {
                issues.Add(MissingSstIssue(path, id));
                continue;
            }

            try
            {
                using var blockBuilder = new BlockBuilder(options.BlockEncoderFactory.Create());
                using var table = await SsTable.LoadSsTableAsync(filename,
                    options.SsTableEncoderFactory.Create(), blockBuilder,
                    options.BloomFilterFactory, id, cancellationToken);
                for (var blockIndex = 0; blockIndex < table.BlockMetadata.Length; blockIndex++)
                {
                    if (blockIndex > 0 && table.BlockMetadata[blockIndex - 1].LastKeySpan.SequenceCompareTo(
                        table.BlockMetadata[blockIndex].FirstKeySpan) >= 0)
                    {
                        throw new InvalidDataException("The SST index contains overlapping or unordered blocks.");
                    }

                    using var block = await table.ReadBlockAsync(blockIndex, cancellationToken);
                    if (block == null)
                    {
                        throw new InvalidDataException($"SST block {blockIndex} could not be decoded.");
                    }

                    VerifyBlock(block, table.BlockMetadata[blockIndex]);
                }

                validSstIds.Add(id);
            }
            catch (Exception exception) when (IsCorruption(exception) || exception is FileNotFoundException)
            {
                issues.Add(new(StorageIntegrityIssueKind.CorruptSst, filename,
                    $"SST '{filename}' is unreadable or corrupt: {exception.Message} Restore this file from backup or use read-only salvage."));
            }
        }

        foreach (var filename in Directory.EnumerateFiles(path, "*.wal"))
        {
            cancellationToken.ThrowIfCancellationRequested();
            // A committed, verified SST supersedes its WAL, even if the stale WAL is damaged.
            var id = TryParseId(filename);
            if (!id.HasValue || validSstIds.Contains(id.Value))
            {
                continue;
            }

            try
            {
                if (WriteAheadLog.Verify(filename))
                {
                    issues.Add(new(StorageIntegrityIssueKind.IncompleteWalTail, filename,
                        $"WAL '{filename}' has an incomplete uncommitted tail. Earlier committed frames remain recoverable; no repair is required."));
                }
            }
            catch (Exception exception) when (IsCorruption(exception))
            {
                issues.Add(new(StorageIntegrityIssueKind.CorruptWal, filename,
                    $"WAL '{filename}' is corrupt: {exception.Message} Restore it from backup or use read-only salvage to inspect the valid prefix."));
            }
        }

        return new StorageIntegrityReport(issues);
    }

    /// <summary>
    /// Opens surviving live SSTs and valid WAL prefixes for read-only, best-effort extraction.
    /// Does not change recovery files; may create the persistent ownership file.
    /// A corrupt manifest must be restored first.
    /// </summary>
    public static async Task<LsmStorage> OpenSalvageAsync(
        string path, StorageOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();
        if (!Directory.Exists(path))
        {
            throw new DirectoryNotFoundException($"Storage directory '{path}' does not exist.");
        }
        var ownership = DirectoryOwnership.Acquire(path);
        try
        {
            var report = await VerifyIntegrityOwnedAsync(path, options, cancellationToken);
            if (report.Issues.Any(issue => issue.Kind is StorageIntegrityIssueKind.CorruptManifest or StorageIntegrityIssueKind.IncompletePhysicalCopy))
            {
                throw new StorageRecoveryException(report);
            }
            return await OpenOwnedAsync(path, options, ownership, cancellationToken, report);
        }
        catch
        {
            ownership.Dispose();
            throw;
        }
    }

    /// <summary>True for a salvage handle, which cannot mutate the source database.</summary>
    public bool IsReadOnly => RecoveryReport != null;

    /// <summary>Integrity findings captured before salvage; null for a normal handle.</summary>
    public StorageIntegrityReport? RecoveryReport { get; private init; }

    private static StorageIntegrityIssue MissingSstIssue(string path, long id)
    {
        var filename = Path.Combine(path, $"{id}.sst");
        return new(StorageIntegrityIssueKind.MissingSst, filename,
            $"Manifest-referenced SST '{filename}' is missing; committed data may be lost even if a WAL survives. Restore it from a consistent backup or use OpenSalvageAsync for read-only extraction.");
    }

    private static bool IsCorruption(Exception exception) =>
        exception is InvalidDataException or EndOfStreamException or JsonException
            or ArgumentException or OverflowException or NotSupportedException;

    private static void VerifyBlock(Block block, BlockMetadata metadata)
    {
        var dataEnd = block.Memory.Length - (block.Offsets.Count + 1) * sizeof(ushort);
        if (block.Offsets.Count == 0 || dataEnd <= 0 || block.Offsets[0] != 0)
        {
            throw new InvalidDataException("The SST block has an invalid entry offset section.");
        }

        ReadOnlyMemory<byte> previousKey = default;
        for (var index = 0; index < block.Offsets.Count; index++)
        {
            var offset = block.Offsets[index];
            var end = index + 1 < block.Offsets.Count ? block.Offsets[index + 1] : dataEnd;
            if (offset >= end || end > dataEnd)
            {
                throw new InvalidDataException("The SST block has an invalid entry extent.");
            }

            block.GetRawEntry(index, out var key, out _, out _);
            if (key.IsEmpty || (index > 0 && previousKey.Span.SequenceCompareTo(key.Span) >= 0)
                || (index == 0 && !metadata.FirstKeySpan.SequenceEqual(key.Span))
                || (index == block.Offsets.Count - 1 && !metadata.LastKeySpan.SequenceEqual(key.Span)))
            {
                throw new InvalidDataException("The SST block keys are unordered or do not match the index.");
            }

            var entry = block.GetEntry(offset);
            if (entry.BlockOffset + entry.StoredValueLength != end)
            {
                throw new InvalidDataException("The SST block entry does not match its recorded extent.");
            }

            previousKey = key;
        }
    }

    private void CheckWritable()
    {
        CheckDisposed();
        if (IsReadOnly)
        {
            throw new InvalidOperationException("Salvage storage is read-only. Extract surviving entries to a separate database.");
        }
    }
}
