namespace Silex;

public sealed partial class LsmStorage
{
    /// <summary>Creates a complete, independently recoverable physical checkpoint in an empty directory.</summary>
    /// <remarks>
    /// Includes writes serialized before capture; later writes are excluded. Blocks writes and maintenance
    /// during capture, pins lifetime until completion, and does not flush or change source files.
    /// WAL-disabled memory tables are written to backup-only SSTs. Salvage/faulted handles are rejected.
    /// Cancellation is checked during copying and verification; synchronous write-lock waits cannot be interrupted.
    /// On failure, removes only files created by this call, leaving the destination directory and LOCK.
    /// Cleanup failures are reported as AggregateException. Do not open or modify a completed backup;
    /// use RestoreBackupAsync. Do not reenter from borrowed-read callbacks.
    /// </remarks>
    public Task CreateBackupAsync(string destination, CancellationToken cancellationToken = default) =>
        CreateBackupCoreAsync(destination, mvcc: false, cancellationToken);

    internal async Task CreateBackupCoreAsync(string destination, bool mvcc, CancellationToken cancellationToken)
    {
        using var operation = _inner.Lifetime.Enter();
        CheckWritable();
        cancellationToken.ThrowIfCancellationRequested();
        destination = StorageBackup.NormalizePath(destination);
        StorageBackup.CheckDisjoint(StorageBackup.NormalizePath(_inner.StoragePath), destination);
        using var ownership = StorageBackup.PrepareDestination(destination);
        var created = new List<string>();
        try
        {
            StorageBackup.Begin(destination, created);
            await _inner.CopyBackupAsync(destination, created, cancellationToken).ConfigureAwait(false);
            var backup = new StorageBackup { Mvcc = mvcc };
            foreach (var path in Directory.EnumerateFiles(destination))
            {
                if (Path.GetFileName(path) is not (DirectoryOwnership.FileName or StorageBackup.PendingFileName))
                {
                    backup.Files.Add(StorageBackup.DescribeFile(path, cancellationToken));
                }
            }
            backup.HasWal = backup.Files.Any(file => file.Name.EndsWith(".wal", StringComparison.Ordinal));
            await ValidateBackupRecoveryAsync(destination, _inner.BackupOptions, ownership, mvcc, cancellationToken).ConfigureAwait(false);
            _inner.FaultInjector?.Invoke(StorageFaultPoint.BackupBeforePublish);
            backup.Publish(destination, created, cancellationToken);
            File.Delete(Path.Combine(destination, StorageBackup.PendingFileName));
        }
        catch (Exception exception)
        {
            StorageBackup.Cleanup(created, exception);
            throw;
        }
    }

    /// <summary>Validates an immutable LSM backup and restores it into an empty, closed storage directory.</summary>
    /// <remarks>
    /// Exclusively owns both directories. Neither may contain the other; directory links are resolved.
    /// Recovery file links, incomplete backups, unexpected files, checksum mismatches, and unhealthy recovery
    /// are rejected. Use matching encoding factories and enable WAL recovery for backups containing WALs.
    /// Validation does not open the source for writing or delete its evidence. No existing destination data
    /// is replaced. Cancellation/failure removes files created by this call, never the directory or LOCK;
    /// cleanup failure is explicit. Successful completion leaves a normally openable database, not a backup.
    /// Requests file-content synchronization, not directory synchronization/power-loss certification.
    /// </remarks>
    public static Task RestoreBackupAsync(string source, string destination, StorageOptions options,
        CancellationToken cancellationToken = default) =>
        RestoreBackupCoreAsync(source, destination, options, mvcc: false, cancellationToken);

    internal static async Task RestoreBackupCoreAsync(string source, string destination, StorageOptions options,
        bool mvcc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();
        source = StorageBackup.NormalizePath(source);
        destination = StorageBackup.NormalizePath(destination);
        StorageBackup.CheckDisjoint(source, destination);
        if (!Directory.Exists(source))
        {
            throw new DirectoryNotFoundException($"Backup directory '{source}' does not exist.");
        }
        StorageBackup.CheckLockFile(source);
        using var sourceOwnership = DirectoryOwnership.Acquire(source);
        using var destinationOwnership = StorageBackup.PrepareDestination(destination);
        var created = new List<string>();
        try
        {
            if (File.Exists(Path.Combine(source, StorageBackup.PendingFileName)))
            {
                throw new InvalidDataException("The backup copy did not complete.");
            }
            var backup = StorageBackup.Read(source, mvcc, options);
            backup.VerifyFiles(source, cancellationToken);
            await ValidateBackupRecoveryAsync(source, options, sourceOwnership, mvcc, cancellationToken).ConfigureAwait(false);
            StorageBackup.Begin(destination, created);
            await Task.Run(() =>
            {
                foreach (var file in backup.Files)
                {
                    options.FaultInjector?.Invoke(StorageFaultPoint.BackupBeforeCopy);
                    StorageBackup.CopyFile(Path.Combine(source, file.Name), Path.Combine(destination, file.Name), created, cancellationToken);
                    options.FaultInjector?.Invoke(StorageFaultPoint.BackupAfterCopy);
                }
                backup.VerifyFiles(destination, cancellationToken);
            }, cancellationToken).ConfigureAwait(false);
            await ValidateBackupRecoveryAsync(destination, options, destinationOwnership, mvcc, cancellationToken).ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            File.Delete(Path.Combine(destination, StorageBackup.PendingFileName));
        }
        catch (Exception exception)
        {
            StorageBackup.Cleanup(created, exception);
            throw;
        }
    }

    private static async Task ValidateBackupRecoveryAsync(string path, StorageOptions options,
        DirectoryOwnership ownership, bool mvcc, CancellationToken cancellationToken)
    {
        var report = await VerifyIntegrityOwnedAsync(path, options, cancellationToken, allowPendingCopy: true).ConfigureAwait(false);
        if (!report.IsRecoveryComplete)
        {
            throw new StorageRecoveryException(report);
        }
        if (Manifest.TryRead(path) == null)
        {
            throw new InvalidDataException("A physical backup requires a committed manifest.");
        }
        if (mvcc)
        {
            var storage = await OpenOwnedAsync(path, options, ownership, cancellationToken, report).ConfigureAwait(false);
            try
            {
                await MvccStorage.ValidateBackupMetadataAsync(storage, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                // The enclosing backup operation, not this read-only validation handle, owns the LOCK.
                storage._inner.Ownership = null;
                await storage.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
