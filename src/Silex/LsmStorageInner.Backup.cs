using Silex.MemTables;

namespace Silex;

internal sealed partial class LsmStorageInner
{
    internal StorageOptions BackupOptions => new()
    {
        UseWriteAheadLog = _useWriteAheadLog,
        BlockEncoderFactory = _blockEncoderFactory,
        SsTableEncoderFactory = _ssTableEncoderFactory,
        BloomFilterFactory = _bloomFilterFactory
    };

    internal async Task CopyBackupAsync(string destination, List<string> created, CancellationToken cancellationToken)
    {
        await _maintenanceLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // Thread-affine locks must not cross awaits. Backup I/O never runs on a caller's UI context.
            await Task.Run(() =>
            {
                Manifest manifest;
                _currentMemTableLock.EnterWriteLock();
                try
                {
                    CheckHealthy();
                    cancellationToken.ThrowIfCancellationRequested();
                    manifest = Manifest.TryRead(StoragePath)
                        ?? throw new InvalidDataException("An open store must have a committed manifest.");
                    foreach (var id in manifest.AllSstIds())
                    {
                        Copy(GetSstPath(id));
                    }
                    foreach (var memTable in _state.ImmutableMemTables)
                    {
                        CopyMemory(memTable);
                    }
                    CopyMemory(_state.CurrentMemTable);
                    var manifestPath = Manifest.GetPath(destination);
                    created.Add(manifestPath + ".tmp");
                    created.Add(manifestPath);
                    manifest.Write(destination);
                }
                finally
                {
                    _currentMemTableLock.ExitWriteLock();
                }

                void Copy(string path)
                {
                    FaultInjector?.Invoke(StorageFaultPoint.BackupBeforeCopy);
                    StorageBackup.CopyFile(path, Path.Combine(destination, Path.GetFileName(path)), created, cancellationToken);
                    FaultInjector?.Invoke(StorageFaultPoint.BackupAfterCopy);
                }

                void CopyMemory(IMemTable memTable)
                {
                    if (_useWriteAheadLog)
                    {
                        Copy(GetWalPath(memTable.Id));
                    }
                    else if (memTable.Count != 0)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var path = Path.Combine(destination, $"{memTable.Id}.sst");
                        created.Add(path + ".tmp");
                        created.Add(path);
                        using var builder = _ssTableBuilderFactory.CreateSsTableBuilder(
                            path, _ssTableEncoderFactory.Create(), _blockEncoderFactory.Create(), _bloomFilterFactory,
                            memTable.Count, _compression, _compressionLevel, _minimumCompressionSavingsPercent);
                        memTable.FlushAsync(builder, cancellationToken).GetAwaiter().GetResult();
                        using var table = builder.BuildAsync(cancellationToken).GetAwaiter().GetResult();
                        cancellationToken.ThrowIfCancellationRequested();
                        SynchronizeFile(path, StorageFaultPoint.SynchronizationBeforeSst, StorageFaultPoint.SynchronizationAfterSst);
                        manifest.L0.Add(memTable.Id);
                    }
                }
            }, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _maintenanceLock.Release();
        }
    }
}
