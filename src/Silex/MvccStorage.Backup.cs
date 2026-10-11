namespace Silex;

public sealed partial class MvccStorage
{
    /// <summary>Creates a physical backup at one published commit boundary, excluding staged transactions.</summary>
    /// <remarks>
    /// Serializes with commit publication, group commit, garbage collection, and shutdown.
    /// Includes all previously completed commits and retains stored versions, not application snapshot handles.
    /// Uses LsmStorage.CreateBackupAsync ownership, cancellation, cleanup, and synchronization contracts.
    /// </remarks>
    public async Task CreateBackupAsync(string destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        await EnterCommitGateAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            CheckDisposed();
            await _storage.CreateBackupCoreAsync(destination, mvcc: true, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _commitGate.Release();
        }
    }

    /// <summary>Restores a validated MVCC physical backup into an empty, closed directory.</summary>
    /// <remarks>Uses LsmStorage.RestoreBackupAsync contracts and additionally verifies MVCC format/sequence metadata.</remarks>
    public static Task RestoreBackupAsync(string source, string destination, StorageOptions options,
        CancellationToken cancellationToken = default) =>
        LsmStorage.RestoreBackupCoreAsync(source, destination, options, mvcc: true, cancellationToken);

    internal static async Task ValidateBackupMetadataAsync(LsmStorage storage, CancellationToken cancellationToken)
    {
        var format = new byte[_formatValue.Length];
        if (await storage.GetRawAsync(_formatKey, format, cancellationToken).ConfigureAwait(false) != _formatValue.Length
            || !format.AsSpan().SequenceEqual(_formatValue))
        {
            throw new InvalidDataException("The backup does not contain a supported MVCC format.");
        }
        var allocated = await ReadSequenceAsync(storage, _allocatedSequenceKey, cancellationToken).ConfigureAwait(false);
        var published = await ReadSequenceAsync(storage, _publishedSequenceKey, cancellationToken).ConfigureAwait(false);
        if (allocated < published)
        {
            throw new InvalidDataException("The MVCC backup allocated sequence is older than its published sequence.");
        }
    }
}
