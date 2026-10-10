using System.Buffers;
using System.Security.Cryptography;
using System.Text.Json;

namespace Silex;

internal sealed class StorageBackup
{
    internal const string FileName = "backup";
    internal const string PendingFileName = "backup.pending";
    private const int BufferSize = 64 * 1024;

    public int Version { get; set; } = 1;
    public bool Mvcc { get; set; }
    public bool HasWal { get; set; }
    public List<BackupFile> Files { get; set; } = [];

    internal sealed class BackupFile
    {
        public string Name { get; set; } = "";
        public long Length { get; set; }
        public string Sha256 { get; set; } = "";
    }

    internal static string NormalizePath(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);
        var fullPath = Path.GetFullPath(path);
        var root = Path.GetPathRoot(fullPath)!;
        var current = root;
        foreach (var segment in fullPath[root.Length..].Split(
            [Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries))
        {
            current = Path.Combine(current, segment);
            var info = new DirectoryInfo(current);
            if (info.LinkTarget != null)
            {
                current = NormalizePath(info.ResolveLinkTarget(returnFinalTarget: true)?.FullName
                    ?? throw new IOException($"Cannot resolve directory link '{current}'."));
            }
        }
        return Path.TrimEndingDirectorySeparator(current);
    }

    internal static void CheckDisjoint(string source, string destination)
    {
        // Case-insensitive checks are deliberately conservative on case-sensitive filesystems.
        if (string.Equals(source, destination, StringComparison.OrdinalIgnoreCase)
            || source.StartsWith(Prefix(destination), StringComparison.OrdinalIgnoreCase)
            || destination.StartsWith(Prefix(source), StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("Source and destination directories must be distinct and neither may contain the other.");
        }
        static string Prefix(string path) => Path.EndsInDirectorySeparator(path) ? path : path + Path.DirectorySeparatorChar;
    }

    internal static DirectoryOwnership PrepareDestination(string path)
    {
        Directory.CreateDirectory(path);
        CheckLockFile(path);
        var ownership = DirectoryOwnership.Acquire(path);
        try
        {
            if (Directory.EnumerateFileSystemEntries(path).Any(entry => Path.GetFileName(entry) != DirectoryOwnership.FileName))
            {
                throw new IOException("Backup and restore require an empty destination (only a persistent LOCK file is allowed).");
            }
            return ownership;
        }
        catch
        {
            ownership.Dispose();
            throw;
        }
    }

    internal static void CheckLockFile(string path)
    {
        var filename = Path.Combine(path, DirectoryOwnership.FileName);
        if (File.Exists(filename) || new FileInfo(filename).LinkTarget != null)
        {
            CheckRegularFile(filename);
        }
    }

    internal static void Begin(string path, List<string> created)
    {
        var filename = Path.Combine(path, PendingFileName);
        using var output = new FileStream(filename, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        created.Add(filename);
        output.Write("Silex incomplete physical copy"u8);
        output.Flush(flushToDisk: true);
    }

    internal static void CheckRegularFile(string path)
    {
        var attributes = File.GetAttributes(path);
        if ((attributes & (FileAttributes.ReparsePoint | FileAttributes.Directory)) != 0)
        {
            throw new IOException($"Backup recovery files must be regular files, not links: '{path}'.");
        }
    }

    internal static void CopyFile(string source, string destination, List<string> created, CancellationToken cancellationToken)
    {
        CheckRegularFile(source);
        using var input = new FileStream(source, FileMode.Open, FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete, BufferSize, FileOptions.SequentialScan);
        using var output = new FileStream(destination, FileMode.CreateNew, FileAccess.Write, FileShare.None,
            BufferSize, FileOptions.SequentialScan);
        created.Add(destination);
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        try
        {
            int length;
            while ((length = input.Read(buffer, 0, BufferSize)) != 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                output.Write(buffer, 0, length);
            }
            cancellationToken.ThrowIfCancellationRequested();
            output.Flush(flushToDisk: true);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal static BackupFile DescribeFile(string path, CancellationToken cancellationToken)
    {
        CheckRegularFile(path);
        using var input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read,
            BufferSize, FileOptions.SequentialScan);
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var buffer = ArrayPool<byte>.Shared.Rent(BufferSize);
        try
        {
            int length;
            while ((length = input.Read(buffer, 0, BufferSize)) != 0)
            {
                cancellationToken.ThrowIfCancellationRequested();
                hash.AppendData(buffer, 0, length);
            }
            return new BackupFile
            {
                Name = Path.GetFileName(path), Length = input.Length,
                Sha256 = Convert.ToHexString(hash.GetHashAndReset())
            };
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    internal void Publish(string path, List<string> created, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var filename = Path.Combine(path, FileName);
        using var output = new FileStream(filename, FileMode.CreateNew, FileAccess.Write, FileShare.None);
        created.Add(filename);
        JsonSerializer.Serialize(output, this);
        output.Flush(flushToDisk: true);
    }

    internal static StorageBackup Read(string path, bool mvcc, StorageOptions options)
    {
        var filename = Path.Combine(path, FileName);
        CheckRegularFile(filename);
        using var input = File.OpenRead(filename);
        var backup = JsonSerializer.Deserialize<StorageBackup>(input);
        if (backup == null || backup.Version != 1 || backup.Mvcc != mvcc || backup.Files == null
            || backup.Files.Count == 0 || (backup.HasWal && !options.UseWriteAheadLog))
        {
            throw new InvalidDataException("Unsupported backup, wrong storage facade, or WAL recovery is disabled.");
        }
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var file in backup.Files)
        {
            if (file == null || string.IsNullOrEmpty(file.Name) || Path.GetFileName(file.Name) != file.Name
                || file.Length < 0 || file.Sha256 == null || file.Sha256.Length != 64 || !names.Add(file.Name)
                || (file.Name != "manifest" && !IsRecoveryName(file.Name)))
            {
                throw new InvalidDataException("The backup file inventory is invalid.");
            }
        }
        if (!names.Contains("manifest") || backup.HasWal != backup.Files.Any(file => file.Name.EndsWith(".wal", StringComparison.Ordinal)))
        {
            throw new InvalidDataException("The backup inventory is missing required recovery state.");
        }
        return backup;

        static bool IsRecoveryName(string name) =>
            (name.EndsWith(".sst", StringComparison.Ordinal) || name.EndsWith(".wal", StringComparison.Ordinal))
            && long.TryParse(Path.GetFileNameWithoutExtension(name), out var id) && id >= 0
            && name == $"{id}{Path.GetExtension(name)}";
    }

    internal void VerifyFiles(string path, CancellationToken cancellationToken)
    {
        var expected = new HashSet<string>(Files.Select(file => file.Name), StringComparer.Ordinal);
        foreach (var entry in Directory.EnumerateFileSystemEntries(path))
        {
            var name = Path.GetFileName(entry);
            if (name != FileName && name != DirectoryOwnership.FileName && name != PendingFileName && !expected.Contains(name))
            {
                throw new InvalidDataException($"Backup contains an unexpected entry '{name}'.");
            }
        }
        foreach (var file in Files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var actual = DescribeFile(Path.Combine(path, file.Name), cancellationToken);
            if (actual.Length != file.Length || actual.Sha256 != file.Sha256)
            {
                throw new InvalidDataException($"Backup file '{file.Name}' does not match its length/checksum.");
            }
        }
    }

    internal static void Cleanup(List<string> created, Exception cause)
    {
        List<Exception>? failures = null;
        for (var i = created.Count - 1; i >= 0; i--)
        {
            if (Path.GetFileName(created[i]) == PendingFileName && failures != null)
            {
                continue;
            }
            try
            {
                File.Delete(created[i]);
            }
            catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
            {
                (failures ??= [cause]).Add(exception);
            }
        }
        if (failures != null)
        {
            throw new AggregateException("Backup/restore failed and destination cleanup was incomplete. Do not use the partial destination.", failures);
        }
    }
}
