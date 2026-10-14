using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Silex.Tool;
using static Silex.Tool.Tests.CliTests;

namespace Silex.Tool.Tests;

[NotInParallel]
public class LoopbackTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task BothLoopbackFamiliesShareTheSelectedPortAndRequireAuthentication(bool explicitPort)
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        var state = Path.Combine(folder.Path, "state");
        var arguments = new List<string>
        {
            "serve", "--db", db, "--create", "--name", "loopback", "--allow-writes", "--detach"
        };
        var selectedPort = 0;
        if (explicitPort)
        {
            var probe = new TcpListener(IPAddress.Loopback, 0);
            probe.Start();
            selectedPort = ((IPEndPoint)probe.LocalEndpoint).Port;
            probe.Stop();
            arguments.AddRange(["--port", selectedPort.ToString(System.Globalization.CultureInfo.InvariantCulture)]);
        }
        await RunWithState(state, 0, arguments.ToArray());
        try
        {
            var recordPath = Path.Combine(state, "loopback.json");
            var record = JsonSerializer.Deserialize(await File.ReadAllTextAsync(recordPath), ToolJsonContext.Default.ServerRecord)!;
            var ipv4 = new Uri(record.Endpoint);
            await Assert.That(ipv4.Host).IsEqualTo("127.0.0.1");
            if (explicitPort) await Assert.That(ipv4.Port).IsEqualTo(selectedPort);
            var ipv6 = new UriBuilder(ipv4) { Host = "::1" }.Uri;
            var credentials = ServerCredentials.Read(db);
            foreach (var endpoint in new[] { ipv4, ipv6 })
            {
                using var http = new HttpClient(new SocketsHttpHandler { UseProxy = false, AllowAutoRedirect = false })
                {
                    BaseAddress = endpoint, Timeout = TimeSpan.FromSeconds(10)
                };
                using (var rejected = await http.GetAsync("v1/identity"))
                    await Assert.That(rejected.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
                http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", credentials.Token);
                using var accepted = await http.GetAsync("v1/identity");
                await Assert.That(accepted.StatusCode).IsEqualTo(HttpStatusCode.OK);
                using var identity = JsonDocument.Parse(await accepted.Content.ReadAsStringAsync());
                await Assert.That(identity.RootElement.GetProperty("instanceId").GetString()).IsEqualTo(record.InstanceId);
            }

            var localAddress = NetworkInterface.GetAllNetworkInterfaces()
                .Where(network => network.OperationalStatus == OperationalStatus.Up)
                .SelectMany(network => network.GetIPProperties().UnicastAddresses)
                .Select(address => address.Address)
                .FirstOrDefault(address => address.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(address));
            if (localAddress is not null)
            {
                await RejectLanListenerAsync(localAddress, ipv4.Port);
                await RejectNonLoopbackPeerAsync(localAddress, ipv4.Port, credentials.Token);
            }

            await File.WriteAllTextAsync(recordPath,
                JsonSerializer.Serialize(record with { Endpoint = ipv6.AbsoluteUri }, ToolJsonContext.Default.ServerRecord));
            await RunWithState(state, 0, "stats", "--server", "loopback");
            await RunWithState(state, 0, "put", "--server", "loopback", "--key", "key", "--value", "value");
            await RunWithState(state, 0, "get", "--server", "loopback", "--key", "key");
            var dump = Path.Combine(folder.Path, "dump");
            await RunWithState(state, 0, "export", "--server", "loopback", "--to", dump, "--format", "binary");
            await RunWithState(state, 0, "import", "--server", "loopback", "--from", dump);
        }
        finally { await RunWithState(state, 0, "stop", "loopback"); }
        await Assert.That(File.Exists(Path.Combine(db, ServerCredentials.FileName))).IsFalse();
    }

    [Test]
    [Arguments("http://0.0.0.0:12345")]
    [Arguments("http://[::]:12345")]
    [Arguments("http://192.0.2.1:12345")]
    [Arguments("http://[2001:db8::1]:12345")]
    [Arguments("http://localhost:12345")]
    [Arguments("http://127.0.0.2:12345")]
    [Arguments("https://127.0.0.1:12345")]
    [Arguments("http://127.0.0.1:12345/path")]
    [Arguments("http://[::1]:12345/path")]
    public async Task RegistryRejectsNonLoopbackOrNonCanonicalEndpoints(string endpoint)
    {
        using var folder = new TestFolder();
        var record = new ServerRecord("invalid", Guid.NewGuid().ToString("N"), endpoint, 1, folder.Path, "lsm", "unused");
        await File.WriteAllTextAsync(Path.Combine(folder.Path, "invalid.json"),
            JsonSerializer.Serialize(record, ToolJsonContext.Default.ServerRecord));
        var result = await RunWithState(folder.Path, 1, "ps");
        await Assert.That(result.Error.Contains("Invalid local server identity", StringComparison.Ordinal)).IsTrue();
    }

    [Test]
    [Arguments("http://0.0.0.0:0")]
    [Arguments("http://[::]:0")]
    public async Task AmbientKestrelConfigurationCannotAddPublicListeners(string endpoint)
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        var state = Path.Combine(folder.Path, "state");
        await Run(0, "init", "--db", db);
        var start = StartInfo(state, ["serve", "--db", db, "--name", "restricted"]);
        start.Environment["Kestrel__Endpoints__Public__Url"] = endpoint;
        using var process = Process.Start(start)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        try
        {
            await process.WaitForExitAsync(timeout.Token);
            await output.WaitAsync(timeout.Token);
            var message = await error.WaitAsync(timeout.Token);
            await Assert.That(process.ExitCode).IsEqualTo(1);
            await Assert.That(message.Contains("loopback listeners", StringComparison.Ordinal)).IsTrue();
        }
        finally
        {
            if (!process.HasExited)
            {
                process.Kill(entireProcessTree: true);
                await process.WaitForExitAsync();
            }
        }
        await Assert.That(File.Exists(Path.Combine(state, "restricted.json"))).IsFalse();
        await Assert.That(File.Exists(Path.Combine(db, ServerCredentials.FileName))).IsFalse();
        await Run(0, "info", "--db", db);
    }

    private static async Task RejectLanListenerAsync(IPAddress address, int port)
    {
        using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try { await socket.ConnectAsync(new IPEndPoint(address, port), timeout.Token); }
        catch (SocketException) { return; }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested) { return; }
        throw new IOException("The developer host port was reachable through a non-loopback interface.");
    }

    private static async Task RejectNonLoopbackPeerAsync(IPAddress address, int port, string token)
    {
        using var client = new TcpClient(AddressFamily.InterNetwork);
        client.Client.Bind(new IPEndPoint(address, 0));
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        try { await client.ConnectAsync(IPAddress.Loopback, port, timeout.Token); }
        catch (SocketException) { return; }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested) { return; }
        using var stream = client.GetStream();
        await stream.WriteAsync(Encoding.ASCII.GetBytes(
            $"GET /v1/stats HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"), timeout.Token);
        using var reader = new StreamReader(stream, Encoding.ASCII);
        await Assert.That(await reader.ReadLineAsync(timeout.Token)).IsEqualTo("HTTP/1.1 403 Forbidden");
    }
}
