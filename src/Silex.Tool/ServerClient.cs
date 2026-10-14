using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Silex.Tool;

internal sealed class ServerClient : IDisposable
{
    private readonly ServerRecord _record;
    private readonly HttpClient _http;
    private HttpClient? _bulkHttp;
    private ServerCredentials? _credentials;
    private AuthenticationHeaderValue? _authorization;

    private ServerCredentials Credentials => _credentials ??= ServerCredentials.Read(_record.Database);

    private AuthenticationHeaderValue Authorization
    {
        get
        {
            if (_authorization is not null) return _authorization;
            var credentials = Credentials;
            if (credentials.InstanceId != _record.InstanceId)
                throw new InvalidDataException("The server credentials belong to a different instance. Refresh the server registration.");
            return _authorization = new AuthenticationHeaderValue("Bearer", credentials.Token);
        }
    }

    public ServerClient(ServerRecord record)
    {
        _record = record;
        _http = new HttpClient(new SocketsHttpHandler
        {
            AllowAutoRedirect = false, UseProxy = false, ConnectTimeout = TimeSpan.FromSeconds(2)
        })
        {
            BaseAddress = new Uri(record.Endpoint), Timeout = TimeSpan.FromMinutes(2)
        };
    }

    public async Task<bool> IsRunningAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(2));
        try
        {
            if (Credentials.InstanceId != _record.InstanceId) return false;
            var identity = await GetAsync("identity", ToolJsonContext.Default.ServerIdentity, timeout.Token);
            return identity.InstanceId == _record.InstanceId && identity.ProcessId == _record.ProcessId;
        }
        catch (HttpRequestException) { return false; }
        catch (FileNotFoundException) { return false; }
        catch (DirectoryNotFoundException) { return false; }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested) { return false; }
    }

    public async Task<T> GetAsync<T>(string path, JsonTypeInfo<T> type, CancellationToken cancellationToken)
    {
        _http.DefaultRequestHeaders.Authorization ??= Authorization;
        using var response = await _http.GetAsync("v1/" + path, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        return await response.Content.ReadFromJsonAsync(type, cancellationToken)
            ?? throw new InvalidDataException("The server returned an empty response.");
    }

    private async Task<HttpResponseMessage> PostAsync<T>(string path, T body, JsonTypeInfo<T> type,
        CancellationToken cancellationToken)
    {
        _http.DefaultRequestHeaders.Authorization ??= Authorization;
        using var request = new HttpRequestMessage(HttpMethod.Post, "v1/" + path)
        {
            Content = JsonContent.Create(body, type)
        };
        var response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
        try
        {
            if (response.StatusCode != HttpStatusCode.NotFound)
                await EnsureSuccessAsync(response, cancellationToken);
            return response;
        }
        catch
        {
            response.Dispose();
            throw;
        }
    }

    public async Task<bool> ReadAsync(byte[] key, Stream output, CancellationToken cancellationToken)
    {
        using var response = await PostAsync("read", new KeyRequest(key), ToolJsonContext.Default.KeyRequest, cancellationToken);
        if (response.StatusCode == HttpStatusCode.NotFound) return false;
        await response.Content.CopyToAsync(output, cancellationToken);
        return true;
    }

    public async Task PutAsync(byte[] key, byte[] value, CancellationToken cancellationToken)
    {
        using var response = await PostAsync("put", new PutRequest(key, value), ToolJsonContext.Default.PutRequest, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
    }

    public async Task DeleteAsync(byte[] key, CancellationToken cancellationToken)
    {
        using var response = await PostAsync("delete", new KeyRequest(key), ToolJsonContext.Default.KeyRequest, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
    }

    public async Task ScanAsync(ScanRequest request, Stream output, CancellationToken cancellationToken)
    {
        using var response = await PostAsync("scan", request, ToolJsonContext.Default.ScanRequest, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        await response.Content.CopyToAsync(output, cancellationToken);
    }

    private HttpClient BulkHttp
    {
        get
        {
            if (_bulkHttp is not null) return _bulkHttp;
            var authorization = Authorization;
            return _bulkHttp = new HttpClient(new SocketsHttpHandler
            {
                AllowAutoRedirect = false, UseProxy = false, ConnectTimeout = TimeSpan.FromSeconds(2)
            })
            {
                BaseAddress = new Uri(_record.Endpoint), Timeout = System.Threading.Timeout.InfiniteTimeSpan,
                DefaultRequestHeaders = { Authorization = authorization }
            };
        }
    }

    public async Task ExportAsync(ExportRequest export, Stream output, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(BulkTransfer.Timeout);
        using var request = new HttpRequestMessage(HttpMethod.Post, "v1/export")
        {
            Content = JsonContent.Create(export, ToolJsonContext.Default.ExportRequest)
        };
        using var response = await BulkHttp.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token);
        await EnsureSuccessAsync(response, timeout.Token);
        await response.Content.CopyToAsync(output, timeout.Token);
    }

    public async Task<ImportResult> ImportAsync(Stream input, string format, CancellationToken cancellationToken)
    {
        DumpWriter.ValidateFormat(format, allowAuto: true);
        var http = BulkHttp;
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(BulkTransfer.Timeout);
        using var request = new HttpRequestMessage(HttpMethod.Post, "v1/import?format=" + format)
        {
            Content = new StreamContent(input, 64 * 1024)
        };
        try
        {
            using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token);
            await EnsureSuccessAsync(response, timeout.Token);
            return await response.Content.ReadFromJsonAsync(ToolJsonContext.Default.ImportResult, timeout.Token)
                ?? throw new InvalidDataException("The server returned no import result.");
        }
        catch (Exception exception) when (exception is OperationCanceledException or IOException or JsonException ||
            exception is HttpRequestException { StatusCode: null })
        {
            throw new IOException("Import transport failed; committed progress could not be confirmed. The import was not retried.", exception);
        }
    }

    public async Task BackupAsync(string name, CancellationToken cancellationToken)
    {
        using var response = await PostAsync("backups", new BackupRequest(name), ToolJsonContext.Default.BackupRequest, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        var operation = await response.Content.ReadFromJsonAsync(ToolJsonContext.Default.OperationStatus, cancellationToken)
            ?? throw new InvalidDataException("Missing backup operation.");
        while (operation.State == "running")
        {
            await Task.Delay(200, cancellationToken);
            operation = await GetAsync("operations/" + operation.Id, ToolJsonContext.Default.OperationStatus, cancellationToken);
        }
        if (operation.State != "completed")
            throw new IOException($"Backup {operation.State}: {operation.Error}");
        Output.Report(operation, ToolJsonContext.Default.OperationStatus, compact: true);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!await IsRunningAsync(cancellationToken))
            throw new IOException("The registered server is unreachable or its identity does not match. No process was killed.");
        using var response = await PostAsync("stop", new KeyRequest([]), ToolJsonContext.Default.KeyRequest, cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        var registry = new Registry();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));
        while (true)
        {
            var current = registry.Read(_record.Name);
            if (current is not null && current.InstanceId != _record.InstanceId) return;
            if (current is null)
            {
                using var ownership = registry.TryAcquire(_record.Name, ".lock");
                if (ownership is not null) return;
            }
            await Task.Delay(100, timeout.Token);
        }
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode) return;
        var error = await response.Content.ReadFromJsonAsync(ToolJsonContext.Default.ErrorResponse, cancellationToken);
        throw new HttpRequestException(error is null ? $"Server returned {(int)response.StatusCode}."
            : error.Error + (error.CommitOutcome is null ? "" : $" Commit outcome: {error.CommitOutcome}."),
            null, response.StatusCode);
    }

    public void Dispose()
    {
        _bulkHttp?.Dispose();
        _http.Dispose();
    }
}
