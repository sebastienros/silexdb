using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using Silex.Tool;
using static Silex.Tool.Tests.CliTests;

namespace Silex.Tool.Tests;

[NotInParallel]
public class AuthenticationTests
{
    [Test]
    [Arguments("lsm")]
    [Arguments("mvcc")]
    public async Task CredentialsUseTheDatabaseBoundaryAndRotateAcrossRestarts(string mode)
    {
        using var folder = new TestFolder();
        var state = Path.Combine(folder.Path, "state");
        var db = Path.Combine(folder.Path, "db");
        var backup = Path.Combine(folder.Path, "backups");
        var credentialsPath = Path.Combine(db, ServerCredentials.FileName);
        var recordPath = Path.Combine(state, "auth.json");
        await Run(0, "init", "--db", db, "--mode", mode);
        await Run(0, "put", "--db", db, "--mode", mode, "--key", "key", "--value", "value");
        var probe = Path.Combine(db, "permission-probe");
        await File.WriteAllTextAsync(probe, "");
        var stale = new ServerCredentials(Guid.NewGuid().ToString("N"), new string('0', 64));
        ServerCredentials.Write(db, stale);
        string[] serve = ["serve", "--db", db, "--mode", mode, "--name", "auth", "--backup-root", backup, "--detach"];
        await RunWithState(state, 0, serve);
        ServerRecord first;
        ServerCredentials credentials;
        try
        {
            var registration = await File.ReadAllTextAsync(recordPath);
            first = JsonSerializer.Deserialize(registration, ToolJsonContext.Default.ServerRecord)!;
            credentials = ServerCredentials.Read(db);
            await Assert.That(credentials.InstanceId).IsEqualTo(first.InstanceId);
            await Assert.That(credentials.Token).IsNotEqualTo(stale.Token);
            await Assert.That(credentials.Token.Length).IsEqualTo(64);
            using (var json = JsonDocument.Parse(registration))
                await Assert.That(json.RootElement.TryGetProperty("token", out _)).IsFalse();
            await Assert.That(registration.Contains(credentials.Token, StringComparison.Ordinal)).IsFalse();
            if (!OperatingSystem.IsWindows())
                await Assert.That(File.GetUnixFileMode(credentialsPath)).IsEqualTo(File.GetUnixFileMode(probe));

            using var http = Http(first);
            using (var missing = await http.GetAsync("v1/stats"))
                await Assert.That(missing.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", stale.Token);
            using (var invalid = await http.GetAsync("v1/stats"))
                await Assert.That(invalid.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", credentials.Token);
            using (var valid = await http.GetAsync("v1/stats"))
                await Assert.That(valid.StatusCode).IsEqualTo(HttpStatusCode.OK);
            var ps = await RunWithState(state, 0, "ps", "--json");
            await Assert.That(ps.Output.Contains(credentials.Token, StringComparison.Ordinal)).IsFalse();
            await RunWithState(state, 0, "get", "--server", "auth", "--key", "key");
            var dump = Path.Combine(folder.Path, "dump");
            await RunWithState(state, 0, "export", "--server", "auth", "--to", dump);
            var rows = await File.ReadAllLinesAsync(dump);
            await Assert.That(rows.Length).IsEqualTo(1);
            using (var row = JsonDocument.Parse(rows[0]))
                await Assert.That(row.RootElement.GetProperty("key").GetString()).IsEqualTo("a2V5");
            await RunWithState(state, 0, "backup", "--server", "auth", "--to", "checkpoint");
            await Assert.That(File.Exists(Path.Combine(backup, "checkpoint", ServerCredentials.FileName))).IsFalse();
            await Assert.That((await File.ReadAllTextAsync(Path.Combine(backup, "checkpoint", "backup")))
                .Contains(ServerCredentials.FileName, StringComparison.Ordinal)).IsFalse();
        }
        finally { await RunWithState(state, 0, "stop", "auth"); }
        await Assert.That(File.Exists(credentialsPath)).IsFalse();
        await Assert.That(Directory.GetFiles(db, ServerCredentials.FileName + ".*.tmp").Length).IsEqualTo(0);

        ServerCredentials.Write(db, credentials);
        await File.WriteAllTextAsync(recordPath, JsonSerializer.Serialize(first, ToolJsonContext.Default.ServerRecord));
        var stopped = await RunWithState(state, 0, "ps", "--json");
        using (var json = JsonDocument.Parse(stopped.Output))
            await Assert.That(json.RootElement[0].GetProperty("state").GetString()).IsEqualTo("unreachable");
        await RunWithState(state, 0, serve);
        try
        {
            var second = JsonSerializer.Deserialize(await File.ReadAllTextAsync(recordPath), ToolJsonContext.Default.ServerRecord)!;
            var current = ServerCredentials.Read(db);
            await Assert.That(current.InstanceId).IsEqualTo(second.InstanceId);
            await Assert.That(current.Token).IsNotEqualTo(credentials.Token);
            using var http = Http(second);
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", credentials.Token);
            using var rejected = await http.GetAsync("v1/stats");
            await Assert.That(rejected.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
            using var old = new ServerClient(first);
            await Assert.That(await old.IsRunningAsync(CancellationToken.None)).IsFalse();
            await RunWithState(state, 0, "get", "--server", "auth", "--key", "key");
        }
        finally { await RunWithState(state, 0, "stop", "auth"); }
        await Assert.That(File.Exists(credentialsPath)).IsFalse();
        await Assert.That(File.Exists(recordPath)).IsFalse();
    }

    [Test]
    public async Task MissingAndInvalidCredentialsFailBeforeImportTransport()
    {
        using var folder = new TestFolder();
        var instance = Guid.NewGuid().ToString("N");
        var record = new ServerRecord("missing", instance, "http://127.0.0.1:1", 1, folder.Path, "lsm", "unused");
        using var missing = new ServerClient(record);
        await Assert.That(await missing.IsRunningAsync(CancellationToken.None)).IsFalse();
        using var input = new MemoryStream();
        await Assert.That(async () => await missing.ImportAsync(input, "auto", CancellationToken.None)).Throws<FileNotFoundException>();

        var path = Path.Combine(folder.Path, ServerCredentials.FileName);
        await File.WriteAllTextAsync(path, """{"instanceId":"invalid","token":"short"}""");
        await Assert.That(() => ServerCredentials.Read(folder.Path)).Throws<InvalidDataException>();
        await File.WriteAllTextAsync(path, """{"instanceId":"invalid","token":"short","extra":true}""");
        await Assert.That(() => ServerCredentials.Read(folder.Path)).Throws<JsonException>();
        ServerCredentials.Write(folder.Path, new(Guid.NewGuid().ToString("N"), new string('0', 64)));
        using var changed = new ServerClient(record);
        await Assert.That(await changed.IsRunningAsync(CancellationToken.None)).IsFalse();
        await Assert.That(async () => await changed.ImportAsync(input, "auto", CancellationToken.None)).Throws<InvalidDataException>();
    }

    [Test]
    public async Task FailedCredentialPublicationCleansUpAndReleasesDatabaseOwnership()
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        var state = Path.Combine(folder.Path, "state");
        await Run(0, "init", "--db", db);
        Directory.CreateDirectory(Path.Combine(db, ServerCredentials.FileName));
        await RunWithState(state, 1, "serve", "--db", db, "--name", "failed", "--detach");
        await Assert.That(File.Exists(Path.Combine(state, "failed.json"))).IsFalse();
        await Assert.That(Directory.GetFiles(db, ServerCredentials.FileName + ".*.tmp").Length).IsEqualTo(0);
        await Run(0, "info", "--db", db);
    }

    [Test]
    public async Task CredentialCleanupOnlyRemovesItsOwnInstance()
    {
        using var folder = new TestFolder();
        var credentials = new ServerCredentials(Guid.NewGuid().ToString("N"), new string('0', 64));
        ServerCredentials.Write(folder.Path, credentials);
        ServerCredentials.Remove(folder.Path, "other");
        await Assert.That(ServerCredentials.Read(folder.Path)).IsEqualTo(credentials);
        ServerCredentials.Remove(folder.Path, credentials.InstanceId);
        await Assert.That(File.Exists(Path.Combine(folder.Path, ServerCredentials.FileName))).IsFalse();
    }

    private static HttpClient Http(ServerRecord record) => new(new SocketsHttpHandler { UseProxy = false, AllowAutoRedirect = false })
    {
        BaseAddress = new Uri(record.Endpoint), Timeout = TimeSpan.FromSeconds(15)
    };
}
