using System.Diagnostics;
using System.Text.Json;
using Silex.Tool;

namespace Silex.Tool.Tests;

public class CliTests
{
    [Test]
    [Arguments("lsm")]
    [Arguments("mvcc")]
    public async Task OfflineOperationsAreBinarySafeAndReadOnly(string mode)
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        await Run(0, "init", "--db", db, "--mode", mode);
        await Run(0, "put", "--db", db, "--mode", mode, "--key", "00FF", "--key-encoding", "hex",
            "--value", "AA00FF", "--value-encoding", "hex");
        var before = Snapshot(db);
        var read = await Run(0, "get", "--db", db, "--mode", mode, "--key", "00FF", "--key-encoding", "hex", "--value-encoding", "hex");
        await Assert.That(read.Output.Trim()).IsEqualTo("AA00FF");
        var scan = await Run(0, "scan", "--db", db, "--mode", mode, "--prefix", "00", "--key-encoding", "hex");
        using var entry = JsonDocument.Parse(scan.Output);
        await Assert.That(entry.RootElement.GetProperty("key").GetString()).IsEqualTo("AP8=");
        await Assert.That(entry.RootElement.GetProperty("value").GetString()).IsEqualTo("qgD/");
        await Run(0, "info", "--db", db, "--mode", mode, "--json");
        await Run(0, "verify", "--db", db, "--mode", mode, "--json");
        await Assert.That(Snapshot(db)).IsEqualTo(before);
        var backup = Path.Combine(folder.Path, "backup");
        var restored = Path.Combine(folder.Path, "restored");
        await Run(0, "backup", "--db", db, "--mode", mode, "--to", backup);
        await Run(0, "restore", "--mode", mode, "--from", backup, "--to", restored);
        await Run(0, "get", "--db", restored, "--mode", mode, "--key", "00FF", "--key-encoding", "hex", "--value-encoding", "hex");
        await Run(0, "delete", "--db", db, "--mode", mode, "--key", "00FF", "--key-encoding", "hex");
        await Run(2, "get", "--db", db, "--mode", mode, "--key", "00FF", "--key-encoding", "hex");
    }

    [Test]
    public async Task MissingDatabaseAndInvalidInputDoNotCreateData()
    {
        using var folder = new TestFolder();
        var missing = Path.Combine(folder.Path, "missing");
        await Run(1, "get", "--db", missing, "--key", "a");
        await Assert.That(Directory.Exists(missing)).IsFalse();
        await Run(1, "init", "--db", missing, "--mode", "invalid");
        await Assert.That(Directory.Exists(missing)).IsFalse();
        await Run(1, "put", "--db", missing, "--key", "a");
    }

    [Test]
    public async Task RangesReverseLimitsAndRawValuesPreserveBytes()
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        await Run(0, "init", "--db", db);
        foreach (var key in new[] { "a", "b", "c", "d" })
            await Run(0, "put", "--db", db, "--key", key, "--value", "FF00AA", "--value-encoding", "hex");
        var scan = await Run(0, "scan", "--db", db, "--from", "b", "--to", "d", "--reverse", "--limit", "1");
        using var row = JsonDocument.Parse(scan.Output);
        await Assert.That(row.RootElement.GetProperty("key").GetString()).IsEqualTo("Yw==");
        await Run(1, "scan", "--db", db, "--prefix", "a", "--from", "b");
        await Run(1, "scan", "--db", db, "--limit", "0");
        await Run(1, "scan", "--db", db, "--key-encoding", "invalid");
        using var raw = Process.Start(StartInfo(null, ["get", "--db", db, "--key", "a", "--value-encoding", "raw"]))!;
        using var output = new MemoryStream();
        var copy = raw.StandardOutput.BaseStream.CopyToAsync(output);
        var errors = raw.StandardError.ReadToEndAsync();
        await raw.WaitForExitAsync();
        await copy;
        await Assert.That(raw.ExitCode).IsEqualTo(0);
        await Assert.That(await errors).IsEqualTo("");
        await Assert.That(Convert.ToHexString(output.ToArray())).IsEqualTo("FF00AA");
    }

    [Test]
    public async Task StartupFailuresAreVisibleAndDoNotLeaveRegisteredServers()
    {
        using var folder = new TestFolder();
        var state = Path.Combine(folder.Path, "state");
        var result = await RunWithState(state, 1, "serve", "--db", Path.Combine(folder.Path, "missing"), "--detach");
        await Assert.That(result.Error.Contains("startup failed", StringComparison.OrdinalIgnoreCase)).IsTrue();
        await Assert.That(result.Error.Contains("ObjectDisposedException", StringComparison.Ordinal)).IsFalse();
        await Assert.That(File.Exists(Path.Combine(state, "local.json"))).IsFalse();
        var ps = await RunWithState(state, 0, "ps", "--json");
        await Assert.That(ps.Output.Trim()).IsEqualTo("[]");
    }

    [Test]
    [NotInParallel]
    public async Task RegistryLockContentionIsRetryableUntilOwnershipIsReleased()
    {
        using var folder = new TestFolder();
        var previous = Environment.GetEnvironmentVariable("SILEX_STATE_DIRECTORY");
        Environment.SetEnvironmentVariable("SILEX_STATE_DIRECTORY", folder.Path);
        try
        {
            var registry = new Registry();
            using (var ownership = registry.Acquire("contended", ".lock"))
                await Assert.That(registry.TryAcquire("contended", ".lock")).IsNull();
            using var acquired = registry.TryAcquire("contended", ".lock");
            await Assert.That(acquired).IsNotNull();
            await Assert.That(() => registry.TryAcquire("../invalid", ".lock")).Throws<ArgumentException>();
        }
        finally { Environment.SetEnvironmentVariable("SILEX_STATE_DIRECTORY", previous); }
    }

    [Test]
    public async Task ForegroundHostBlocksUntilStopped()
    {
        using var folder = new TestFolder();
        var state = Path.Combine(folder.Path, "state");
        using var process = Process.Start(StartInfo(state, ["serve", "--db", Path.Combine(folder.Path, "db"), "--create", "--name", "foreground"]))!;
        var output = process.StandardOutput.ReadToEndAsync();
        var errors = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        try
        {
            var recordPath = Path.Combine(state, "foreground.json");
            while (!File.Exists(recordPath))
            {
                if (process.HasExited) throw new IOException(await errors);
                await Task.Delay(50, timeout.Token);
            }
            using var record = JsonDocument.Parse(await File.ReadAllTextAsync(recordPath, timeout.Token));
            await Assert.That(record.RootElement.GetProperty("processId").GetInt32()).IsEqualTo(process.Id);
            await Assert.That(process.HasExited).IsFalse();
            await RunWithState(state, 0, "stop", "foreground");
            await process.WaitForExitAsync(timeout.Token);
            await output.WaitAsync(timeout.Token);
            await errors.WaitAsync(timeout.Token);
            await Assert.That(process.ExitCode).IsEqualTo(0);
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync();
            }
        }
    }

    [Test]
    public async Task DetachedHostSupportsAuthenticatedOperationsAndGracefulShutdown()
    {
        using var folder = new TestFolder();
        var state = Path.Combine(folder.Path, "state");
        var db = Path.Combine(folder.Path, "db");
        var backups = Path.Combine(folder.Path, "backups");
        var configuration = Path.Combine(folder.Path, "server.json");
        await File.WriteAllTextAsync(configuration, """
            {"name":"configured","database":"db","mode":"mvcc","allowWrites":false,"backupRoot":"backups"}
            """);
        await Run(0, "init", "--db", db, "--mode", "mvcc");
        await RunWithState(state, 0, "serve", "--config", configuration, "--name", "test", "--allow-writes", "--detach");
        try
        {
            var ps = await RunWithState(state, 0, "ps", "--json");
            using var listing = JsonDocument.Parse(ps.Output);
            await Assert.That(listing.RootElement[0].GetProperty("state").GetString()).IsEqualTo("running");
            await Assert.That(ps.Output.Contains("token", StringComparison.OrdinalIgnoreCase)).IsFalse();
            using var record = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(state, "test.json")));
            using var unauthorized = new HttpClient();
            using var response = await unauthorized.GetAsync(record.RootElement.GetProperty("endpoint").GetString() + "/v1/stats");
            await Assert.That((int)response.StatusCode).IsEqualTo(401);
            await RunWithState(state, 0, "put", "--server", "test", "--key", "user:1", "--value", "");
            var read = await RunWithState(state, 0, "get", "--server", "test", "--key", "user:1");
            await Assert.That(read.Output.Trim()).IsEqualTo("");
            await RunWithState(state, 0, "scan", "--server", "test", "--prefix", "user:");
            await RunWithState(state, 0, "stats", "--server", "test", "--json");
            await Run(1, "get", "--db", db, "--mode", "mvcc", "--key", "user:1");
            await RunWithState(state, 1, "serve", "--db", db, "--name", "test", "--detach");
            await RunWithState(state, 1, "backup", "--server", "test", "--to", "../escape");
            await RunWithState(state, 0, "backup", "--server", "test", "--to", "checkpoint");
            await Assert.That(File.Exists(Path.Combine(backups, "checkpoint", "backup"))).IsTrue();
        }
        finally { await RunWithState(state, 0, "stop", "test"); }
        await Assert.That(File.Exists(Path.Combine(state, "test.json"))).IsFalse();
        await Run(0, "get", "--db", db, "--mode", "mvcc", "--key", "user:1");
        await RunWithState(state, 0, "serve", "--db", db, "--mode", "mvcc", "--name", "readonly", "--detach");
        try { await RunWithState(state, 1, "put", "--server", "readonly", "--key", "a", "--value", "b"); }
        finally { await RunWithState(state, 0, "stop", "readonly"); }
    }

    [Test]
    public async Task ReadOnlyOpeningRejectsIncompleteRecoveryAndMutations()
    {
        using var folder = new TestFolder();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };
        await using (var writable = await LsmStorage.OpenAsync(folder.Path, options))
            writable.Put("key"u8, "value"u8);
        await using (var readOnly = await LsmStorage.OpenReadOnlyAsync(folder.Path, options))
        {
            await Assert.That(readOnly.IsReadOnly).IsTrue();
            await Assert.That(() => readOnly.Put("key"u8, "other"u8)).Throws<InvalidOperationException>();
        }
        File.Delete(Directory.GetFiles(folder.Path, "*.sst").Single());
        await Assert.That(async () => await LsmStorage.OpenReadOnlyAsync(folder.Path, options)).Throws<StorageRecoveryException>();
    }

    internal static string Snapshot(string path) => string.Join("\n", Directory.GetFiles(path)
        .Where(file => System.IO.Path.GetFileName(file) != "LOCK").Order(StringComparer.Ordinal)
        .Select(file => System.IO.Path.GetFileName(file) + ":" + Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(File.ReadAllBytes(file)))));

    internal static Task<(string Output, string Error)> Run(int exit, params string[] arguments) => RunWithState(null, exit, arguments);

    internal static async Task<(string Output, string Error)> RunWithState(string? state, int exit, params string[] arguments)
    {
        using var process = Process.Start(StartInfo(state, arguments))!;
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        try
        {
            await process.WaitForExitAsync(timeout.Token);
            var output = await stdout.WaitAsync(timeout.Token);
            var error = await stderr.WaitAsync(timeout.Token);
            if (process.ExitCode != exit)
                throw new InvalidOperationException($"{string.Join(' ', arguments)}: expected {exit}, got {process.ExitCode}.\n{output}\n{error}");
            return (output, error);
        }
        catch
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync();
            }
            throw;
        }
    }

    internal static ProcessStartInfo StartInfo(string? state, string[] arguments)
    {
        var executable = Environment.GetEnvironmentVariable("SILEX_TEST_EXECUTABLE");
        var assembly = executable?.EndsWith(".dll", StringComparison.OrdinalIgnoreCase) == true ? executable : null;
        var start = new ProcessStartInfo(assembly is not null ? "dotnet" : executable ?? "dotnet")
        {
            UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true
        };
        if (assembly is not null || executable is null) start.ArgumentList.Add(assembly ?? typeof(Cli).Assembly.Location);
        foreach (var argument in arguments) start.ArgumentList.Add(argument);
        if (state is not null) start.Environment["SILEX_STATE_DIRECTORY"] = state;
        return start;
    }

    internal sealed class TestFolder : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "silex-tool-tests", Guid.NewGuid().ToString("N"));
        public TestFolder() => Directory.CreateDirectory(Path);
        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}
