using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.RegularExpressions;
using Silex.Tool;
using static Silex.Tool.Tests.CliTests;

namespace Silex.Tool.Tests;

[NotInParallel]
public class UiTests
{
    [Test]
    [Arguments("lsm")]
    [Arguments("mvcc")]
    public async Task UiStartsAndAttachesWithEmbeddedAssetsAndPublishedTables(string mode)
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        var state = Path.Combine(folder.Path, "state");
        await Run(0, "init", "--db", db, "--mode", mode);
        var launch = await RunWithState(state, 0, "ui", "--db", db, "--mode", mode, "--name", "studio", "--allow-writes", "--no-open");
        try
        {
            var url = new Uri(launch.Output.Split('\n', StringSplitOptions.RemoveEmptyEntries)[^1].Trim());
            await Assert.That(url.AbsolutePath).IsEqualTo("/ui/");
            await Assert.That(url.Query).IsEqualTo("");
            var record = JsonSerializer.Deserialize(await File.ReadAllTextAsync(Path.Combine(state, "studio.json")), ToolJsonContext.Default.ServerRecord)!;
            await Assert.That(url.Fragment).IsEqualTo("#token=" + ServerCredentials.Read(db).Token);
            using var http = new HttpClient(new SocketsHttpHandler { UseProxy = false, AllowAutoRedirect = false }) { BaseAddress = new Uri(record.Endpoint) };
            using var shell = await http.GetAsync("ui/");
            await Assert.That(shell.StatusCode).IsEqualTo(HttpStatusCode.OK);
            await Assert.That(shell.Headers.GetValues("Content-Security-Policy").Single().Contains("frame-ancestors 'none'")).IsTrue();
            var html = await shell.Content.ReadAsStringAsync();
            await Assert.That(html.Contains("Silex Studio")).IsTrue();
            await Assert.That(html.Contains(ServerCredentials.Read(db).Token)).IsFalse();
            foreach (Match asset in Regex.Matches(html, "(?:src|href)=\"(/ui/assets/[^\"]+)\""))
            {
                using var response = await http.GetAsync(asset.Groups[1].Value);
                await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
                await Assert.That((await response.Content.ReadAsByteArrayAsync()).Length > 0).IsTrue();
            }
            using (var unauthorized = await http.GetAsync("v1/tables"))
                await Assert.That(unauthorized.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", ServerCredentials.Read(db).Token);
            await RunWithState(state, 0, "put", "--server", "studio", "--key", "a", "--value", "value");
            using (var compact = await http.PostAsync("v1/compact", new StringContent("{}")))
                await Assert.That(compact.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
            var orphan = Path.Combine(db, "999999999.sst");
            await File.WriteAllTextAsync(orphan, "not a published table");
            try
            {
                using var tables = await http.GetAsync("v1/tables");
                var inventory = JsonSerializer.Deserialize(await tables.Content.ReadAsStringAsync(), ToolJsonContext.Default.StorageTableStatisticsArray)!;
                await Assert.That(inventory.Length > 0).IsTrue();
                await Assert.That(inventory.Any(table => table.FileId == 999999999)).IsFalse();
                await Assert.That(inventory.All(table => table.Bytes > 0 && table.Blocks > 0)).IsTrue();
            }
            finally { File.Delete(orphan); }
            var attached = await RunWithState(state, 0, "ui", "--server", "studio", "--no-open");
            await Assert.That(attached.Output.Trim()).IsEqualTo(url.AbsoluteUri);
            await RunWithState(state, 1, "ui", "--server", "studio", "--db", db, "--no-open");
        }
        finally { await RunWithState(state, 0, "stop", "studio"); }
    }

    [Test]
    public async Task UiDoesNotBypassReadOnlyMutationPolicy()
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        var state = Path.Combine(folder.Path, "state");
        await Run(0, "init", "--db", db);
        await RunWithState(state, 0, "ui", "--db", db, "--name", "readonly-ui", "--no-open");
        try
        {
            var record = JsonSerializer.Deserialize(await File.ReadAllTextAsync(Path.Combine(state, "readonly-ui.json")), ToolJsonContext.Default.ServerRecord)!;
            using var http = new HttpClient { BaseAddress = new Uri(record.Endpoint) };
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", ServerCredentials.Read(db).Token);
            using var response = await http.PostAsync("v1/compact", new StringContent("invalid"));
            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Forbidden);
        }
        finally { await RunWithState(state, 0, "stop", "readonly-ui"); }
    }
}
