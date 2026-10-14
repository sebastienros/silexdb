using System.Net;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using Silex.Tool;
using static Silex.Tool.Tests.CliTests;

namespace Silex.Tool.Tests;

[NotInParallel]
public class RoutingTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RoutingPreservesAuthenticationMethodsPathsAndExplicitJson(bool allowWrites)
    {
        using var folder = new TestFolder();
        var state = Path.Combine(folder.Path, "state");
        var db = Path.Combine(folder.Path, "db");
        await Run(0, "init", "--db", db);
        await Run(0, "put", "--db", db, "--key", "key", "--value", "value");
        var arguments = new List<string>
        {
            "serve", "--db", db, "--name", "routing", "--backup-root", Path.Combine(folder.Path, "backups"), "--detach"
        };
        if (allowWrites) arguments.Add("--allow-writes");
        await RunWithState(state, 0, arguments.ToArray());
        try
        {
            var record = JsonSerializer.Deserialize(await File.ReadAllTextAsync(Path.Combine(state, "routing.json")),
                ToolJsonContext.Default.ServerRecord) ?? throw new IOException("Missing server registration.");
            using var http = new HttpClient(new SocketsHttpHandler { UseProxy = false, AllowAutoRedirect = false })
            {
                BaseAddress = new Uri(record.Endpoint), Timeout = TimeSpan.FromSeconds(15)
            };
            foreach (var path in new[] { "/v1/stats", "/unknown", "/v1/stats/" })
            {
                using var unauthorized = await http.GetAsync(path);
                await Assert.That(unauthorized.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
                await ErrorAsync(unauthorized, "Authentication required.");
            }
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("bearer", ServerCredentials.Read(db).Token);
            foreach (var path in new[]
            {
                "/unknown", "/v1/stats/", "/V1/stats", "/v1/STATS", "/v1/read", "/v1/operations/", "/v1/operations/missing/extra"
            })
            {
                using var unknown = await http.GetAsync(path);
                await Assert.That(unknown.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
                await ErrorAsync(unknown, "Unknown administration operation.");
            }
            foreach (var path in new[] { "/v1/stats", "/V1/put", "/v1/PUT", "/v1/put/" })
            {
                using var wrongMethod = await http.PostAsync(path, new StringContent("{}"));
                await Assert.That(wrongMethod.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
                await ErrorAsync(wrongMethod, "Unknown administration operation.");
            }
            using (var stats = await http.GetAsync("/v1/stats?ignored=true"))
                await Assert.That(stats.StatusCode).IsEqualTo(HttpStatusCode.OK);
            using (var read = await http.PostAsync("/v1/read", new StringContent("""{"key":"a2V5"}""", Encoding.UTF8, "text/plain")))
            {
                await Assert.That(read.StatusCode).IsEqualTo(HttpStatusCode.OK);
                await Assert.That(read.Content.Headers.ContentType!.MediaType).IsEqualTo("application/octet-stream");
                await Assert.That(await read.Content.ReadAsStringAsync()).IsEqualTo("value");
            }
            using (var missing = await http.PostAsync("/v1/read", new StringContent("""{"key":"bWlzc2luZw=="}""")))
            {
                await Assert.That(missing.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
                await Assert.That(await missing.Content.ReadAsStringAsync()).IsEqualTo("");
            }
            using (var invalid = await http.PostAsync("/v1/read", new StringContent("""{"key":"a2V5","unknown":true}""")))
            {
                await Assert.That(invalid.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
                await ErrorAsync(invalid);
            }
            foreach (var path in new[] { "/v1/put", "/v1/delete", "/v1/import" })
            {
                using var mutation = await http.PostAsync(path, new StringContent("invalid JSON"));
                await Assert.That(mutation.StatusCode).IsEqualTo(allowWrites ? HttpStatusCode.BadRequest : HttpStatusCode.Forbidden);
                await ErrorAsync(mutation);
            }
            var backup = await RunWithState(state, 0, "backup", "--server", "routing", "--to", "checkpoint");
            using var operation = JsonDocument.Parse(backup.Output);
            var id = operation.RootElement.GetProperty("id").GetString();
            using var status = await http.GetAsync("/v1/operations/" + id);
            await Assert.That(status.StatusCode).IsEqualTo(HttpStatusCode.OK);
            using var completed = JsonDocument.Parse(await status.Content.ReadAsStringAsync());
            await Assert.That(completed.RootElement.GetProperty("state").GetString()).IsEqualTo("completed");
            foreach (var path in new[] { "/v1/operations/" + id + "/", "/v1/OPERATIONS/" + id, "/V1/operations/" + id })
            {
                using var unknown = await http.GetAsync(path);
                await Assert.That(unknown.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
                await ErrorAsync(unknown, "Unknown administration operation.");
            }
        }
        finally { await RunWithState(state, 0, "stop", "routing"); }
    }

    private static async Task ErrorAsync(HttpResponseMessage response, string? expected = null)
    {
        await Assert.That(response.Content.Headers.ContentType!.MediaType).IsEqualTo("application/json");
        using var error = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
        if (expected is not null)
            await Assert.That(error.RootElement.GetProperty("error").GetString()).IsEqualTo(expected);
        else await Assert.That(error.RootElement.GetProperty("error").GetString()).IsNotNull();
    }
}
