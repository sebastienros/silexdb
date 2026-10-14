using System.ComponentModel;
using System.Diagnostics;
using System.Net;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Transport.Sockets;

namespace Silex.Tool;

internal static partial class ServerHost
{
    internal const int MaxPayloadBytes = 16 * 1024 * 1024;

    public static async Task<int> RunAsync(ServerConfiguration configuration, bool background, CancellationToken cancellationToken)
    {
        configuration.Validate();
        var registry = new Registry();
        using var ownership = registry.Acquire(configuration.Name, ".lock");
        var logPath = registry.FilePath(configuration.Name, ".log");
        using var log = background ? Detach(logPath) : null;
        await using var database = await DatabaseHandle.OpenAsync(configuration.Database!, configuration.Mode,
            readOnly: false, configuration.Create, configuration.UseWriteAheadLog, cancellationToken);
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { Args = [] });
        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole();
        builder.WebHost.ConfigureKestrel(options =>
        {
            options.Listen(IPAddress.Loopback, configuration.Port);
            options.Listen(IPAddress.IPv6Loopback, configuration.Port);
            options.Limits.MaxRequestBodySize = MaxPayloadBytes * 4L / 3 + 4096;
        });
        builder.WebHost.UseSockets(options =>
        {
            var port = configuration.Port;
            var gate = new object();
            options.CreateBoundListenSocket = endpoint =>
            {
                if (endpoint is not IPEndPoint address || address.Port != configuration.Port ||
                    (!address.Address.Equals(IPAddress.Loopback) && !address.Address.Equals(IPAddress.IPv6Loopback)))
                    throw new InvalidOperationException("The developer host only supports its configured IPv4 and IPv6 loopback listeners.");
                lock (gate)
                {
                    // Keep the first socket bound while sharing its assigned port with the second family.
                    var socket = SocketTransportOptions.CreateDefaultBoundListenSocket(new IPEndPoint(address.Address, port));
                    port = ((IPEndPoint)socket.LocalEndPoint!).Port;
                    return socket;
                }
            };
        });
        await using var app = builder.Build();
        var token = Convert.ToHexString(RandomNumberGenerator.GetBytes(32));
        var authorization = "Bearer " + token;
        var identity = new ServerIdentity(configuration.Name, configuration.InstanceId ?? Guid.NewGuid().ToString("N"),
            Environment.ProcessId, database.Path, database.Mode, configuration.AllowWrites);
        using var concurrency = new SemaphoreSlim(8, 8);
        using var mutations = new SemaphoreSlim(1, 1);
        var backups = new BackupOperation(database, configuration.BackupRoot, app.Logger, app.Lifetime.ApplicationStopping);
        app.UseRouting();
        app.Use(async (HttpContext context, RequestDelegate next) =>
        {
            if (context.Connection.RemoteIpAddress is not { } remote || !IPAddress.IsLoopback(remote))
            {
                app.Logger.LogWarning("Rejected a non-loopback administration connection.");
                context.Response.StatusCode = StatusCodes.Status403Forbidden;
                await ReplyAsync(context, new ErrorResponse("Only loopback connections are allowed."), ToolJsonContext.Default.ErrorResponse);
                return;
            }
            if (context.GetEndpoint()?.Metadata.GetMetadata<UiAssets.PublicAsset>() is not null)
            {
                await next(context);
                return;
            }
            if (!Authorized(context.Request.Headers.Authorization.ToString(), authorization))
            {
                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                await ReplyAsync(context, new ErrorResponse("Authentication required."), ToolJsonContext.Default.ErrorResponse);
                return;
            }
            if (!await concurrency.WaitAsync(0, context.RequestAborted))
            {
                context.Response.StatusCode = StatusCodes.Status429TooManyRequests;
                await ReplyAsync(context, new ErrorResponse("Too many active operations."), ToolJsonContext.Default.ErrorResponse);
                return;
            }
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(context.RequestAborted, app.Lifetime.ApplicationStopping);
            var policy = context.GetEndpoint()?.Metadata.GetMetadata<RequestPolicy>();
            if (policy is not null && !policy.Matches(context.Request)) policy = null;
            timeout.CancelAfter(policy?.Bulk == true ? BulkTransfer.Timeout : TimeSpan.FromSeconds(30));
            try
            {
                if (policy is null)
                {
                    await UnknownAsync(context);
                    return;
                }
                if (policy.StreamingBody)
                {
                    var bodyLimit = context.Features.Get<IHttpMaxRequestBodySizeFeature>()
                        ?? throw new InvalidOperationException("The server cannot configure bulk request limits.");
                    bodyLimit.MaxRequestBodySize = null;
                }
                if (policy.RequiresWrites && !configuration.AllowWrites)
                {
                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
                    await ReplyAsync(context, new ErrorResponse("Writes are disabled. Start with --allow-writes to enable mutations."),
                        ToolJsonContext.Default.ErrorResponse);
                    return;
                }
                context.Features.Set(timeout);
                if (policy.RequiresWrites)
                {
                    await mutations.WaitAsync(timeout.Token);
                    try { await next(context); }
                    finally { mutations.Release(); }
                }
                else await next(context);
            }
            catch (OperationCanceledException) when (timeout.IsCancellationRequested)
            {
                app.Logger.LogWarning("Administration request canceled or timed out.");
                context.Abort();
            }
            catch (Exception exception)
            {
                app.Logger.LogError(exception, "Administration request failed.");
                if (context.Response.HasStarted || timeout.IsCancellationRequested) context.Abort();
                else
                {
                    context.Response.StatusCode = exception switch
                    {
                        BadHttpRequestException badRequest => badRequest.StatusCode,
                        ArgumentException or JsonException or InvalidDataException => StatusCodes.Status400BadRequest,
                        BulkImportException { InnerException: ArgumentException or JsonException or InvalidDataException or EndOfStreamException } =>
                            StatusCodes.Status400BadRequest,
                        InvalidOperationException => StatusCodes.Status409Conflict,
                        _ => StatusCodes.Status500InternalServerError
                    };
                    await ReplyAsync(context, new ErrorResponse(exception.Message,
                        exception is StorageOperationException operation ? operation.Outcome.ToString() :
                            exception.InnerException is StorageOperationException inner ? inner.Outcome.ToString() : null,
                        exception is BulkImportException import ? import.CommittedRecords : null),
                        ToolJsonContext.Default.ErrorResponse);
                }
            }
            finally
            {
                context.Features.Set<CancellationTokenSource>(null);
                concurrency.Release();
            }
        });
        MapEndpoints(app, database, identity, backups, mutations);
        UiAssets.Map(app);

        ServerRecord? record = null;
        var credentialsWritten = false;
        try
        {
            await app.StartAsync(cancellationToken);
            record = new(configuration.Name, identity.InstanceId, app.Urls.Single(url => new Uri(url).Host == "127.0.0.1"), Environment.ProcessId,
                database.Path, database.Mode, logPath);
            ServerCredentials.Write(database.Path, new(identity.InstanceId, token));
            credentialsWritten = true;
            registry.Write(record.Name, ".json", record, ToolJsonContext.Default.ServerRecord);
            Console.Error.WriteLine($"silex server '{record.Name}' ready at {string.Join(", ", app.Urls)}; database '{record.Database}'.");
            await app.WaitForShutdownAsync(cancellationToken);
        }
        finally
        {
            await app.StopAsync(CancellationToken.None);
            await backups.DrainAsync();
            try
            {
                if (credentialsWritten) ServerCredentials.Remove(database.Path, identity.InstanceId);
            }
            finally
            {
                await database.DisposeAsync();
                if (record is not null) registry.Remove(record);
            }
        }
        return 0;
    }

    private static bool Authorized(string actual, string expected) =>
        actual.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase) &&
        CryptographicOperations.FixedTimeEquals(MemoryMarshal.AsBytes(actual.AsSpan(7).Trim()),
            MemoryMarshal.AsBytes(expected.AsSpan(7)));

    private static void MapEndpoints(WebApplication app, DatabaseHandle database, ServerIdentity identity,
        BackupOperation backups, SemaphoreSlim mutations)
    {
        var api = app.MapGroup("/v1");
        MapGet("/tables", async context =>
            await ReplyAsync(context, await database.TablesAsync(RequestCancellation(context)), ToolJsonContext.Default.StorageTableStatisticsArray));
        MapPost("/compact", async context =>
        {
            await database.CompactAsync(RequestCancellation(context));
            context.Response.StatusCode = StatusCodes.Status204NoContent;
        }, bulk: true, requiresWrites: true);
        MapGet("/identity", context => ReplyAsync(context, identity, ToolJsonContext.Default.ServerIdentity));
        MapGet("/stats", async context =>
        {
            await ReplyAsync(context, await database.StatisticsAsync(RequestCancellation(context)), ToolJsonContext.Default.DatabaseStatistics);
        });
        MapGet("/info", async context =>
        {
            await ReplyAsync(context, await database.InfoAsync(RequestCancellation(context)), ToolJsonContext.Default.DatabaseInfo);
        });
        api.MapGet("/operations/{id}", (RequestDelegate)(async context =>
        {
            if (backups.Find(context.Request.Path.Value!["/v1/operations/".Length..]) is { } operation)
                await ReplyAsync(context, operation, ToolJsonContext.Default.OperationStatus);
            else await UnknownAsync(context);
        })).WithMetadata(new RequestPolicy("GET", "/v1/operations/", Prefix: true));

        MapPost("/read", async context =>
        {
            var cancellationToken = RequestCancellation(context);
            var read = await ReadAsync(context, ToolJsonContext.Default.KeyRequest, cancellationToken);
            using var buffer = new LimitedBuffer(MaxPayloadBytes);
            if (!await database.ReadAsync(read.Key, buffer, cancellationToken))
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
                return;
            }
            context.Response.ContentType = "application/octet-stream";
            context.Response.ContentLength = buffer.WrittenMemory.Length;
            await context.Response.Body.WriteAsync(buffer.WrittenMemory, cancellationToken);
        });
        MapPost("/scan", async context =>
        {
            var cancellationToken = RequestCancellation(context);
            var scan = await ReadAsync(context, ToolJsonContext.Default.ScanRequest, cancellationToken);
            context.Response.ContentType = "application/x-ndjson";
            await database.ScanAsync(scan, context.Response.Body, cancellationToken);
        });
        MapPost("/export", async context =>
        {
            var cancellationToken = RequestCancellation(context);
            var export = await ReadAsync(context, ToolJsonContext.Default.ExportRequest, cancellationToken);
            DumpWriter.ValidateFormat(export.Format);
            if (export.Live && database.Mode != "lsm")
                throw new ArgumentException("--live is only valid for online plain-LSM exports.");
            var quiesce = database.Mode == "lsm" && !export.Live;
            if (quiesce) await mutations.WaitAsync(cancellationToken);
            try
            {
                context.Response.ContentType = export.Format == "binary" ? "application/octet-stream" : "application/x-ndjson";
                context.Response.Headers["X-Silex-Consistency"] = database.Mode == "mvcc" ? "snapshot" : quiesce ? "quiesced" : "live";
                await BulkTransfer.WriteExportAsync(context.Response.Body, gzip: false,
                    async (output, token) => { await database.ExportAsync(export, output, token); }, cancellationToken);
            }
            finally { if (quiesce) mutations.Release(); }
        }, bulk: true);
        MapPost("/import", async context =>
        {
            var format = context.Request.Query["format"].ToString();
            if (format.Length == 0) format = "auto";
            var imported = await BulkTransfer.ImportAsync(database, context.Request.Body, format, RequestCancellation(context));
            await ReplyAsync(context, imported, ToolJsonContext.Default.ImportResult);
        }, bulk: true, requiresWrites: true, streamingBody: true);
        MapPost("/put", async context =>
        {
            var put = await ReadAsync(context, ToolJsonContext.Default.PutRequest, RequestCancellation(context));
            ArgumentNullException.ThrowIfNull(put.Value);
            database.Put(put.Key, put.Value);
            context.Response.StatusCode = StatusCodes.Status204NoContent;
        }, requiresWrites: true);
        MapPost("/delete", async context =>
        {
            var delete = await ReadAsync(context, ToolJsonContext.Default.KeyRequest, RequestCancellation(context));
            database.Delete(delete.Key);
            context.Response.StatusCode = StatusCodes.Status204NoContent;
        }, requiresWrites: true);
        MapPost("/backups", async context =>
        {
            var backup = await ReadAsync(context, ToolJsonContext.Default.BackupRequest, RequestCancellation(context));
            context.Response.StatusCode = StatusCodes.Status202Accepted;
            await ReplyAsync(context, backups.Start(backup.Name), ToolJsonContext.Default.OperationStatus);
        });
        MapPost("/stop", context =>
        {
            context.Response.StatusCode = StatusCodes.Status204NoContent;
            context.Response.OnCompleted(() =>
            {
                app.Lifetime.StopApplication();
                return Task.CompletedTask;
            });
            return Task.CompletedTask;
        });

        void MapGet(string path, RequestDelegate handler) =>
            api.MapGet(path, handler).WithMetadata(new RequestPolicy("GET", "/v1" + path));

        void MapPost(string path, RequestDelegate handler, bool bulk = false, bool requiresWrites = false, bool streamingBody = false) =>
            api.MapPost(path, handler).WithMetadata(new RequestPolicy("POST", "/v1" + path, bulk, requiresWrites, streamingBody));
    }

    private sealed record RequestPolicy(string Method, string Path, bool Bulk = false, bool RequiresWrites = false,
        bool StreamingBody = false, bool Prefix = false)
    {
        // Preserve the protocol's exact matching rather than routing's case/trailing-slash aliases.
        public bool Matches(HttpRequest request) =>
            string.Equals(request.Method, Method, StringComparison.Ordinal) &&
            (Prefix ? request.Path.Value?.StartsWith(Path, StringComparison.Ordinal) == true :
                string.Equals(request.Path.Value, Path, StringComparison.Ordinal));
    }

    private static CancellationToken RequestCancellation(HttpContext context) =>
        context.Features.Get<CancellationTokenSource>()!.Token;

    private static async Task UnknownAsync(HttpContext context)
    {
        context.Response.StatusCode = StatusCodes.Status404NotFound;
        await ReplyAsync(context, new ErrorResponse("Unknown administration operation."), ToolJsonContext.Default.ErrorResponse);
    }

    private static async Task<T> ReadAsync<T>(HttpContext context, JsonTypeInfo<T> type, CancellationToken cancellationToken) =>
        await JsonSerializer.DeserializeAsync(context.Request.Body, type, cancellationToken)
            ?? throw new ArgumentException("A JSON request body is required.");

    private static async Task ReplyAsync<T>(HttpContext context, T value, JsonTypeInfo<T> type)
    {
        context.Response.ContentType = "application/json";
        await JsonSerializer.SerializeAsync(context.Response.Body, value, type, context.RequestAborted);
    }

    private static DetachedLogging Detach(string logPath)
    {
        if (!OperatingSystem.IsWindows() && SetSid() < 0)
            throw new Win32Exception(Marshal.GetLastPInvokeError(), "Could not detach the server session.");
        var stream = new FileStream(logPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        if (!OperatingSystem.IsWindows())
        {
            var descriptor = stream.SafeFileHandle.DangerousGetHandle().ToInt32();
            if (Dup2(descriptor, 1) < 0 || Dup2(descriptor, 2) < 0)
            {
                stream.Dispose();
                throw new Win32Exception(Marshal.GetLastPInvokeError(), "Could not redirect server logs.");
            }
        }
        return new DetachedLogging(stream);
    }

    [LibraryImport("libc", EntryPoint = "setsid", SetLastError = true)]
    private static partial int SetSid();
    [LibraryImport("libc", EntryPoint = "dup2", SetLastError = true)]
    private static partial int Dup2(int oldDescriptor, int newDescriptor);

    private sealed class DetachedLogging : IDisposable
    {
        private readonly TextWriter _previousOut = Console.Out;
        private readonly TextWriter _previousError = Console.Error;
        private readonly StreamWriter _writer;

        public DetachedLogging(Stream stream)
        {
            _writer = new StreamWriter(stream) { AutoFlush = true };
            Console.SetOut(_writer);
            Console.SetError(_writer);
        }

        public void Dispose()
        {
            Console.SetOut(_previousOut);
            Console.SetError(_previousError);
            _writer.Dispose();
        }
    }

    private sealed class BackupOperation(DatabaseHandle database, string? root, ILogger logger, CancellationToken shutdown)
    {
        private readonly object _gate = new();
        private OperationStatus? _status;
        private readonly Dictionary<string, OperationStatus> _history = [];
        private readonly Queue<string> _order = [];
        private Task _task = Task.CompletedTask;

        public OperationStatus Start(string name)
        {
            Registry.ValidateName(name);
            if (root is null) throw new InvalidOperationException("Backups require a configured backup root.");
            var destination = Path.Combine(root, name);
            if (Directory.Exists(destination) || new DirectoryInfo(destination).LinkTarget is not null)
                throw new IOException("The backup destination already exists. Choose a new backup name.");
            lock (_gate)
            {
                if (!_task.IsCompleted) throw new InvalidOperationException("A backup is already running.");
                _status = new(Guid.NewGuid().ToString("N"), "running");
                var initial = _status;
                _history.Add(initial.Id, initial);
                _order.Enqueue(initial.Id);
                if (_order.Count > 64) _history.Remove(_order.Dequeue());
                _task = Task.Run(async () =>
                {
                    OperationStatus result;
                    try
                    {
                        await database.BackupAsync(destination, shutdown);
                        result = initial with { State = "completed" };
                    }
                    catch (OperationCanceledException) when (shutdown.IsCancellationRequested)
                    {
                        logger.LogWarning("Backup {Id} canceled during shutdown.", initial.Id);
                        result = initial with { State = "canceled", Error = "Server is shutting down." };
                    }
                    catch (Exception exception)
                    {
                        logger.LogError(exception, "Backup {Id} failed.", initial.Id);
                        result = initial with { State = "failed", Error = exception.Message };
                    }
                    lock (_gate)
                    {
                        _status = result;
                        _history[result.Id] = result;
                    }
                }, CancellationToken.None);
                return initial;
            }
        }

        public OperationStatus? Find(string id)
        {
            lock (_gate) return _history.GetValueOrDefault(id);
        }

        public Task DrainAsync()
        {
            lock (_gate) return _task;
        }
    }
}
