using System.Diagnostics;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Silex.Tool;

internal sealed class Registry
{
    public string DirectoryPath { get; }

    public Registry()
    {
        DirectoryPath = Path.GetFullPath(Environment.GetEnvironmentVariable("SILEX_STATE_DIRECTORY")
            ?? Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "Silex", "servers"));
        if (new DirectoryInfo(DirectoryPath).LinkTarget is not null)
            throw new IOException("The server state directory must not be a symbolic link.");
        if (OperatingSystem.IsWindows())
        {
            var security = new DirectorySecurity();
            security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
            security.AddAccessRule(new FileSystemAccessRule(
                WindowsIdentity.GetCurrent().User ?? throw new IOException("Cannot identify the current Windows user."),
                FileSystemRights.FullControl, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                PropagationFlags.None, AccessControlType.Allow));
            var directory = new DirectoryInfo(DirectoryPath);
            if (directory.Exists) directory.SetAccessControl(security);
            else directory.Create(security);
        }
        else
        {
            Directory.CreateDirectory(DirectoryPath, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
            File.SetUnixFileMode(DirectoryPath, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
        }
    }

    public static void ValidateName(string name)
    {
        if (string.IsNullOrEmpty(name) || name.Length > 64 ||
            !name.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_'))
            throw new ArgumentException("Names must contain 1-64 ASCII letters, digits, hyphens, or underscores.");
    }

    public string FilePath(string name, string suffix)
    {
        ValidateName(name);
        var path = Path.Combine(DirectoryPath, name + suffix);
        if (new FileInfo(path).LinkTarget is not null)
            throw new IOException($"Server state must not be a symbolic link: '{path}'.");
        return path;
    }

    public FileStream Acquire(string name, string suffix) =>
        new(FilePath(name, suffix), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

    public FileStream? TryAcquire(string name, string suffix)
    {
        try { return Acquire(name, suffix); }
        catch (IOException exception) when ((exception.HResult & 0xffff) is 11 or 32 or 33)
        {
            return null;
        }
    }

    public ServerRecord? Read(string name)
    {
        var path = FilePath(name, ".json");
        if (!File.Exists(path)) return null;
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var record = JsonSerializer.Deserialize(file, ToolJsonContext.Default.ServerRecord)
            ?? throw new InvalidDataException($"Invalid server record '{path}'.");
        if (record.Name != name || !Uri.TryCreate(record.Endpoint, UriKind.Absolute, out var endpoint) ||
            endpoint.Scheme != "http" || endpoint.Host is not ("127.0.0.1" or "[::1]") || endpoint.AbsolutePath != "/" ||
            endpoint.UserInfo.Length != 0 || endpoint.Query.Length != 0 || endpoint.Fragment.Length != 0 ||
            string.IsNullOrWhiteSpace(record.InstanceId) || record.ProcessId <= 0 ||
            string.IsNullOrWhiteSpace(record.Database) || record.Mode is not ("lsm" or "mvcc") ||
            string.IsNullOrWhiteSpace(record.Log))
            throw new InvalidDataException($"Invalid local server identity '{path}'.");
        return record;
    }

    public void Write<T>(string name, string suffix, T value, JsonTypeInfo<T> type) =>
        WriteFile(FilePath(name, suffix), value, type, privateFile: true);

    internal static void WriteFile<T>(string path, T value, JsonTypeInfo<T> type, bool privateFile)
    {
        if (new FileInfo(path).LinkTarget is not null)
            throw new IOException($"Server state must not be a symbolic link: '{path}'.");
        var temp = path + "." + Guid.NewGuid().ToString("N") + ".tmp";
        var options = new FileStreamOptions
        {
            Mode = FileMode.CreateNew, Access = FileAccess.Write, Share = FileShare.None
        };
        if (privateFile && !OperatingSystem.IsWindows())
            options.UnixCreateMode = UnixFileMode.UserRead | UnixFileMode.UserWrite;
        try
        {
            using (var file = new FileStream(temp, options))
            {
                JsonSerializer.Serialize(file, value, type);
                file.Flush(flushToDisk: true);
            }
            File.Move(temp, path, overwrite: true);
        }
        finally { File.Delete(temp); }
    }

    public void Remove(ServerRecord record)
    {
        if (Read(record.Name)?.InstanceId == record.InstanceId)
            File.Delete(FilePath(record.Name, ".json"));
    }

    public async Task<ServerStatus[]> ListAsync(CancellationToken cancellationToken)
    {
        var result = new List<ServerStatus>();
        foreach (var path in Directory.EnumerateFiles(DirectoryPath, "*.json").Order(StringComparer.Ordinal))
        {
            if (path.EndsWith(".launch.json", StringComparison.Ordinal)) continue;
            var record = Read(Path.GetFileNameWithoutExtension(path));
            if (record is null) continue;
            using var client = new ServerClient(record);
            var running = await client.IsRunningAsync(cancellationToken);
            result.Add(new(record.Name, running ? "running" : "unreachable", record.ProcessId,
                record.Endpoint, record.Database, record.Mode, record.Log));
        }
        return result.ToArray();
    }

    public static ProcessStartInfo ChildStartInfo(string configurationPath)
    {
        var executable = Environment.ProcessPath ?? throw new IOException("Cannot determine the current executable.");
        var start = new ProcessStartInfo(executable)
        {
            UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardOutput = true, RedirectStandardError = true
        };
        if (Path.GetFileNameWithoutExtension(executable).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            start.ArgumentList.Add(Environment.GetCommandLineArgs()[0]);
        start.ArgumentList.Add("serve");
        start.ArgumentList.Add("--config");
        start.ArgumentList.Add(configurationPath);
        start.ArgumentList.Add("--background");
        return start;
    }

    public async Task StartDetachedAsync(ServerConfiguration configuration, CancellationToken cancellationToken)
    {
        using var launchLock = Acquire(configuration.Name, ".launch.lock");
        var existing = Read(configuration.Name);
        if (existing is not null)
        {
            using var client = new ServerClient(existing);
            if (await client.IsRunningAsync(cancellationToken))
                throw new IOException($"Server '{configuration.Name}' is already running.");
        }
        configuration = configuration with { InstanceId = Guid.NewGuid().ToString("N") };
        Write(configuration.Name, ".launch.json", configuration, ToolJsonContext.Default.ServerConfiguration);
        var path = FilePath(configuration.Name, ".launch.json");
        using var process = Process.Start(ChildStartInfo(path)) ?? throw new IOException("Could not start the server.");
        try
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(30));
            while (true)
            {
                if (process.HasExited)
                    throw new IOException($"Server startup failed (exit {process.ExitCode}): {await process.StandardError.ReadToEndAsync(cancellationToken)} See '{FilePath(configuration.Name, ".log")}'.");
                var record = Read(configuration.Name);
                if (record?.InstanceId == configuration.InstanceId)
                {
                    using var client = new ServerClient(record);
                    if (await client.IsRunningAsync(timeout.Token))
                    {
                        Output.Report(new ServerStatus(record.Name, "running", record.ProcessId, record.Endpoint,
                            record.Database, record.Mode, record.Log), ToolJsonContext.Default.ServerStatus, compact: true);
                        return;
                    }
                }
                await Task.Delay(100, timeout.Token);
            }
        }
        catch
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync(CancellationToken.None);
            }
            throw;
        }
        finally
        {
            File.Delete(path);
        }
    }
}
