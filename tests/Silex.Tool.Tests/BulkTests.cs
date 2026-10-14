using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Silex.Tool;
using static Silex.Tool.Tests.CliTests;

namespace Silex.Tool.Tests;

[NotInParallel]
public class BulkTests
{
    [Test]
    [Arguments("lsm", "jsonl", false)]
    [Arguments("lsm", "jsonl", true)]
    [Arguments("lsm", "binary", false)]
    [Arguments("lsm", "binary", true)]
    [Arguments("mvcc", "jsonl", false)]
    [Arguments("mvcc", "jsonl", true)]
    [Arguments("mvcc", "binary", false)]
    [Arguments("mvcc", "binary", true)]
    public async Task OfflineRoundTripsPreserveBytesAcrossModesAndUpsert(string mode, string format, bool gzip)
    {
        using var folder = new TestFolder();
        var source = Path.Combine(folder.Path, "source");
        var target = Path.Combine(folder.Path, "target");
        var input = Path.Combine(folder.Path, "input.jsonl");
        await File.WriteAllTextAsync(input, Rows(1100));
        await Run(0, "init", "--db", source, "--mode", mode);
        var imported = await Run(0, "import", "--db", source, "--mode", mode, "--from", input, "--json");
        using (var report = JsonDocument.Parse(imported.Output))
        {
            await Assert.That(report.RootElement.GetProperty("records").GetInt64()).IsEqualTo(1100L);
            await Assert.That(report.RootElement.GetProperty("batches").GetInt64()).IsEqualTo(2L);
        }
        var before = Snapshot(source);
        var dump = Path.Combine(folder.Path, "dump");
        var arguments = new List<string> { "export", "--db", source, "--mode", mode, "--to", dump, "--format", format };
        if (gzip) arguments.Add("--gzip");
        await Run(0, arguments.ToArray());
        await Assert.That(Snapshot(source)).IsEqualTo(before);
        var otherMode = mode == "lsm" ? "mvcc" : "lsm";
        await Run(0, "init", "--db", target, "--mode", otherMode);
        await Run(0, "put", "--db", target, "--mode", otherMode, "--key", "untouched", "--value", "keep");
        await Run(0, "put", "--db", target, "--mode", otherMode, "--key", "00000000", "--key-encoding", "hex", "--value", "old");
        await Run(0, "import", "--db", target, "--mode", otherMode, "--from", dump);
        var last = await Run(0, "get", "--db", target, "--mode", otherMode, "--key", "0000044B", "--key-encoding", "hex", "--value-encoding", "hex");
        await Assert.That(last.Output.Trim()).IsEqualTo("FF0000044B");
        var first = await Run(0, "get", "--db", target, "--mode", otherMode, "--key", "00000000", "--key-encoding", "hex", "--value-encoding", "hex");
        await Assert.That(first.Output.Trim()).IsEqualTo("FF00000000");
        var untouched = await Run(0, "get", "--db", target, "--mode", otherMode, "--key", "untouched");
        await Assert.That(untouched.Output.Trim()).IsEqualTo("keep");
        var scan = await Run(0, "scan", "--db", target, "--mode", otherMode, "--limit", "2000");
        await Assert.That(scan.Output.Split('\n', StringSplitOptions.RemoveEmptyEntries).Length).IsEqualTo(1101);
    }

    [Test]
    public async Task ExportHasNoMillionRowScanLimit()
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        await using (var storage = await LsmStorage.OpenAsync(db, new StorageOptions { FlushPeriod = TimeSpan.Zero }))
        {
            var batch = new LsmWriteBatch();
            var key = new byte[4];
            for (var i = 0; i < 1_000_001; i++)
            {
                BinaryPrimitives.WriteInt32BigEndian(key, i);
                batch.Put(key, "v"u8);
                if (batch.Count == 1024)
                {
                    storage.Write(batch);
                    batch.Clear();
                }
            }
            if (batch.Count > 0) storage.Write(batch);
        }
        var dump = Path.Combine(folder.Path, "dump");
        await Run(0, "export", "--db", db, "--to", dump, "--format", "binary");
        await using var input = File.OpenRead(dump);
        using var reader = await DumpReader.OpenAsync(input, "binary", CancellationToken.None);
        var count = 0L;
        while (await reader.ReadAsync(CancellationToken.None) is not null) count++;
        await Assert.That(count).IsEqualTo(1_000_001L);
    }

    [Test]
    public async Task ExportFiltersReverseAndStdoutPipelineWork()
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        await Run(0, "init", "--db", db);
        foreach (var key in new[] { "a", "b", "c", "d" })
            await Run(0, "put", "--db", db, "--key", key, "--value", "");
        var dump = Path.Combine(folder.Path, "dump");
        await Run(0, "export", "--db", db, "--from", "b", "--to-key", "d", "--reverse", "--to", dump);
        var lines = await File.ReadAllLinesAsync(dump);
        await Assert.That(lines.Length).IsEqualTo(2);
        using (var row = JsonDocument.Parse(lines[0]))
            await Assert.That(row.RootElement.GetProperty("key").GetString()).IsEqualTo("Yw==");
        await Run(1, "export", "--db", db, "--prefix", "b", "--from", "b", "--to", Path.Combine(folder.Path, "invalid"));
        await Run(1, "export", "--db", db, "--live", "--to", Path.Combine(folder.Path, "invalid"));
        await Run(1, "export", "--db", db, "--to", dump);
        await Assert.That(await File.ReadAllLinesAsync(dump)).IsEquivalentTo(lines);

        var target = Path.Combine(folder.Path, "target");
        await Run(0, "init", "--db", target);
        using var exporter = Process.Start(StartInfo(null, ["export", "--db", db, "--prefix", "a", "--format", "binary", "--gzip", "--to", "-"]))!;
        var start = StartInfo(null, ["import", "--db", target, "--from", "-"]);
        start.RedirectStandardInput = true;
        using var importer = Process.Start(start)!;
        var exportErrors = exporter.StandardError.ReadToEndAsync();
        var importErrors = importer.StandardError.ReadToEndAsync();
        var importOutput = importer.StandardOutput.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        try
        {
            await exporter.StandardOutput.BaseStream.CopyToAsync(importer.StandardInput.BaseStream, timeout.Token);
            importer.StandardInput.Close();
            await exporter.WaitForExitAsync(timeout.Token);
            await importer.WaitForExitAsync(timeout.Token);
            if (exporter.ExitCode != 0 || importer.ExitCode != 0)
                throw new IOException(await exportErrors + await importErrors);
            await importOutput.WaitAsync(timeout.Token);
        }
        finally
        {
            foreach (var process in new[] { exporter, importer })
                if (!process.HasExited) { process.Kill(entireProcessTree: true); await process.WaitForExitAsync(); }
        }
        await Run(0, "get", "--db", target, "--key", "a");
        await Run(2, "get", "--db", target, "--key", "b");
    }

    [Test]
    [Arguments("lsm")]
    [Arguments("mvcc")]
    public async Task MalformedImportsReportOnlyConfirmedBatches(string mode)
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        var input = Path.Combine(folder.Path, "input");
        await Run(0, "init", "--db", db, "--mode", mode);
        await File.WriteAllTextAsync(input, Rows(1030) + """{"key":"!!!","value":""}""" + "\n");
        var failed = await Run(1, "import", "--db", db, "--mode", mode, "--from", input);
        await Assert.That(failed.Error.Contains("1024 confirmed records", StringComparison.Ordinal)).IsTrue();
        await Run(0, "get", "--db", db, "--mode", mode, "--key", "000003FF", "--key-encoding", "hex", "--value-encoding", "hex");
        await Run(2, "get", "--db", db, "--mode", mode, "--key", "00000400", "--key-encoding", "hex");
        await File.WriteAllTextAsync(input, """{"value":"","key":"\u0059Q=="}""" + "\r\n");
        await Run(0, "import", "--db", db, "--mode", mode, "--from", input);
        await Run(0, "get", "--db", db, "--mode", mode, "--key", "a");
    }

    [Test]
    [Arguments("checksum")]
    [Arguments("truncated")]
    [Arguments("footer")]
    [Arguments("length")]
    [Arguments("trailing")]
    [Arguments("version")]
    public async Task BinaryCorruptionIsRejectedBeforePendingRecordsCommit(string corruption)
    {
        using var folder = new TestFolder();
        var source = Path.Combine(folder.Path, "source");
        var target = Path.Combine(folder.Path, "target");
        var dump = Path.Combine(folder.Path, "dump");
        await Run(0, "init", "--db", source);
        await Run(0, "put", "--db", source, "--key", "a", "--value", "b");
        await Run(0, "export", "--db", source, "--to", dump, "--format", "binary");
        var bytes = await File.ReadAllBytesAsync(dump);
        switch (corruption)
        {
            case "checksum": bytes[28] ^= 1; break;
            case "truncated": bytes = bytes[..^1]; break;
            case "footer": bytes[^8] = 2; break;
            case "length": BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(8), uint.MaxValue); break;
            case "trailing": bytes = [..bytes, 0]; break;
            case "version": bytes[7] = (byte)'2'; break;
        }
        await File.WriteAllBytesAsync(dump, bytes);
        await Run(0, "init", "--db", target);
        var failed = await Run(1, "import", "--db", target, "--from", dump);
        await Assert.That(failed.Error.Contains("0 confirmed records", StringComparison.Ordinal)).IsTrue();
        await Run(2, "get", "--db", target, "--key", "a");
    }

    [Test]
    [Arguments("jsonl")]
    [Arguments("binary")]
    public async Task GzipRequiresACompleteTrailer(string format)
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        var dump = Path.Combine(folder.Path, "dump");
        await Run(0, "init", "--db", db);
        await Run(0, "put", "--db", db, "--key", "a", "--value", "");
        await Run(0, "export", "--db", db, "--to", dump, "--format", format, "--gzip");
        var bytes = await File.ReadAllBytesAsync(dump);
        await File.WriteAllBytesAsync(dump, bytes[..^4]);
        var target = Path.Combine(folder.Path, "target");
        await Run(0, "init", "--db", target);
        await Run(1, "import", "--db", target, "--from", dump);
        await Run(2, "get", "--db", target, "--key", "a");
        bytes[^8] ^= 1;
        await File.WriteAllBytesAsync(dump, bytes);
        await Run(1, "import", "--db", target, "--from", dump);
    }

    [Test]
    public async Task FailedOrRacingExportsDoNotPublishOrOverwriteFiles()
    {
        using var folder = new TestFolder();
        var output = Path.Combine(folder.Path, "dump");
        await Assert.That(async () => await BulkTransfer.ExportFileAsync(output, true, async (stream, token) =>
        {
            await stream.WriteAsync("partial"u8.ToArray(), token);
            throw new IOException("Injected export failure.");
        }, CancellationToken.None)).Throws<IOException>();
        await Assert.That(Directory.GetFiles(folder.Path).Length).IsEqualTo(0);
        await Assert.That(async () => await BulkTransfer.ExportFileAsync(output, false, async (stream, token) =>
        {
            await stream.WriteAsync("ours"u8.ToArray(), token);
            await File.WriteAllTextAsync(output, "concurrent", token);
        }, CancellationToken.None)).Throws<IOException>();
        await Assert.That(await File.ReadAllTextAsync(output)).IsEqualTo("concurrent");
        await Assert.That(Directory.GetFiles(folder.Path).Length).IsEqualTo(1);
    }

    [Test]
    [Arguments("lsm")]
    [Arguments("mvcc")]
    public async Task OnlineTransfersRespectPermissionsAndReportPartialImports(string mode)
    {
        using var folder = new TestFolder();
        var state = Path.Combine(folder.Path, "state");
        var db = Path.Combine(folder.Path, "db");
        var input = Path.Combine(folder.Path, "input");
        var dump = Path.Combine(folder.Path, "dump");
        await File.WriteAllTextAsync(input, Rows(1100));
        await RunWithState(state, 0, "serve", "--db", db, "--mode", mode, "--create", "--name", "bulk", "--allow-writes", "--detach");
        try
        {
            await RunWithState(state, 0, "import", "--server", "bulk", "--from", input);
            await RunWithState(state, 0, "export", "--server", "bulk", "--to", dump, "--format", "binary", "--gzip");
            await RunWithState(state, 0, "import", "--server", "bulk", "--from", dump);
            await RunWithState(state, mode == "lsm" ? 0 : 1, "export", "--server", "bulk", "--live",
                "--to", Path.Combine(folder.Path, "live"));
            await File.WriteAllTextAsync(input, Rows(1030) + "{}\n");
            var failed = await RunWithState(state, 1, "import", "--server", "bulk", "--from", input);
            await Assert.That(failed.Error.Contains("1024 confirmed records", StringComparison.Ordinal)).IsTrue();
        }
        finally { await RunWithState(state, 0, "stop", "bulk"); }
        await RunWithState(state, 0, "serve", "--db", db, "--mode", mode, "--name", "bulk", "--detach");
        try
        {
            await RunWithState(state, 1, "import", "--server", "bulk", "--from", dump);
            await RunWithState(state, 0, "export", "--server", "bulk", "--to", Path.Combine(folder.Path, "readonly"));
        }
        finally { await RunWithState(state, 0, "stop", "bulk"); }
        var target = Path.Combine(folder.Path, "target");
        await Run(0, "init", "--db", target);
        await Run(0, "import", "--db", target, "--from", dump);
        await Run(0, "get", "--db", target, "--key", "0000044B", "--key-encoding", "hex", "--value-encoding", "hex");
    }

    [Test]
    public async Task OnlineLargeImportAndQuiescedExportCoordinateMutations()
    {
        using var folder = new TestFolder();
        var state = Path.Combine(folder.Path, "state");
        var input = Path.Combine(folder.Path, "input");
        await using (var file = File.Create(input))
        {
            using var writer = new DumpWriter(file, "binary");
            await writer.StartAsync(CancellationToken.None);
            var key = new byte[4];
            var value = new byte[16 * 1024];
            for (var i = 0; i < 5000; i++)
            {
                BinaryPrimitives.WriteInt32BigEndian(key, i);
                await writer.WriteAsync(key, value, CancellationToken.None);
            }
            await writer.CompleteAsync(CancellationToken.None);
        }
        await RunWithState(state, 0, "serve", "--db", Path.Combine(folder.Path, "db"),
            "--create", "--name", "bulk", "--allow-writes", "--detach");
        try
        {
            await RunWithState(state, 0, "import", "--server", "bulk", "--from", input);
            var record = JsonSerializer.Deserialize(await File.ReadAllTextAsync(Path.Combine(state, "bulk.json")),
                ToolJsonContext.Default.ServerRecord) ?? throw new IOException("Missing server registration.");
            using var http = new HttpClient(new SocketsHttpHandler { UseProxy = false, AllowAutoRedirect = false })
            {
                BaseAddress = new Uri(record.Endpoint), Timeout = TimeSpan.FromSeconds(30)
            };
            http.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", ServerCredentials.Read(record.Database).Token);
            using var request = new HttpRequestMessage(HttpMethod.Post, "v1/export")
            {
                Content = JsonContent.Create(new ExportRequest(), ToolJsonContext.Default.ExportRequest)
            };
            using var response = await http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
            await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.OK);
            await Assert.That(response.Headers.GetValues("X-Silex-Consistency").Single()).IsEqualTo("quiesced");
            var put = http.PostAsJsonAsync("v1/put", new PutRequest("a"u8.ToArray(), "b"u8.ToArray()), ToolJsonContext.Default.PutRequest);
            var delete = http.PostAsJsonAsync("v1/delete", new KeyRequest("missing"u8.ToArray()), ToolJsonContext.Default.KeyRequest);
            await Task.Delay(200);
            await Assert.That(put.IsCompleted).IsFalse();
            await Assert.That(delete.IsCompleted).IsFalse();
            response.Dispose();
            using var putResponse = await put.WaitAsync(TimeSpan.FromSeconds(10));
            using var deleteResponse = await delete.WaitAsync(TimeSpan.FromSeconds(10));
            await Assert.That(putResponse.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
            await Assert.That(deleteResponse.StatusCode).IsEqualTo(HttpStatusCode.NoContent);

            using var liveRequest = new HttpRequestMessage(HttpMethod.Post, "v1/export")
            {
                Content = JsonContent.Create(new ExportRequest(Live: true), ToolJsonContext.Default.ExportRequest)
            };
            using var live = await http.SendAsync(liveRequest, HttpCompletionOption.ResponseHeadersRead);
            await Assert.That(live.Headers.GetValues("X-Silex-Consistency").Single()).IsEqualTo("live");
            using var unblocked = await http.PostAsJsonAsync("v1/put", new PutRequest("a"u8.ToArray(), "c"u8.ToArray()),
                ToolJsonContext.Default.PutRequest).WaitAsync(TimeSpan.FromSeconds(5));
            await Assert.That(unblocked.StatusCode).IsEqualTo(HttpStatusCode.NoContent);
        }
        finally { await RunWithState(state, 0, "stop", "bulk"); }
    }

    [Test]
    public async Task MvccExportRetainsTheSnapshotCapturedBeforeStreaming()
    {
        using var folder = new TestFolder();
        await using var database = await DatabaseHandle.OpenAsync(Path.Combine(folder.Path, "db"), "mvcc",
            readOnly: false, create: true, wal: true, CancellationToken.None);
        database.Put("a"u8.ToArray(), "old"u8.ToArray());
        using var stream = new GatedWriteStream();
        var export = database.ExportAsync(new ExportRequest(Format: "binary"), stream, CancellationToken.None);
        try
        {
            await stream.Entered.Task.WaitAsync(TimeSpan.FromSeconds(10));
            database.Put("a"u8.ToArray(), "new"u8.ToArray());
            database.Put("b"u8.ToArray(), "added"u8.ToArray());
        }
        finally { stream.Continue.TrySetResult(); }
        await export;
        using var input = new MemoryStream(stream.ToArray());
        using var reader = await DumpReader.OpenAsync(input, "binary", CancellationToken.None);
        var first = await reader.ReadAsync(CancellationToken.None) ?? throw new IOException("Missing snapshot entry.");
        await Assert.That(Encoding.UTF8.GetString(first.Value.Span)).IsEqualTo("old");
        await Assert.That(await reader.ReadAsync(CancellationToken.None)).IsNull();
    }

    [Test]
    [Arguments("jsonl", false)]
    [Arguments("binary", false)]
    [Arguments("jsonl", true)]
    [Arguments("binary", true)]
    public async Task EmptyDumpsRoundTrip(string format, bool gzip)
    {
        using var folder = new TestFolder();
        var db = Path.Combine(folder.Path, "db");
        var dump = Path.Combine(folder.Path, "dump");
        await Run(0, "init", "--db", db);
        var args = new List<string> { "export", "--db", db, "--to", dump, "--format", format };
        if (gzip) args.Add("--gzip");
        await Run(0, args.ToArray());
        var imported = await Run(0, "import", "--db", db, "--from", dump, "--json");
        using var report = JsonDocument.Parse(imported.Output);
        await Assert.That(report.RootElement.GetProperty("records").GetInt64()).IsEqualTo(0L);
        await Assert.That(report.RootElement.GetProperty("batches").GetInt64()).IsEqualTo(0L);
    }

    [Test]
    [Arguments("jsonl")]
    [Arguments("binary")]
    public async Task RecordPayloadLimitIsExact(string format)
    {
        using var folder = new TestFolder();
        var inputPath = Path.Combine(folder.Path, "input");
        var value = new byte[ServerHost.MaxPayloadBytes - 3];
        value[^1] = 254;
        await using (var file = File.Create(inputPath))
        {
            using var writer = new DumpWriter(file, format);
            await writer.StartAsync(CancellationToken.None);
            await writer.WriteAsync("key"u8.ToArray(), value, CancellationToken.None);
            await Assert.That(async () => await writer.WriteAsync("four"u8.ToArray(), value, CancellationToken.None))
                .Throws<InvalidDataException>();
            await writer.CompleteAsync(CancellationToken.None);
        }
        var db = Path.Combine(folder.Path, "db");
        await Run(0, "init", "--db", db);
        var imported = await Run(0, "import", "--db", db, "--from", inputPath, "--json");
        using (var report = JsonDocument.Parse(imported.Output))
            await Assert.That(report.RootElement.GetProperty("records").GetInt64()).IsEqualTo(1L);
        await using (var storage = await LsmStorage.OpenReadOnlyAsync(db, new StorageOptions()))
        {
            var length = new System.Runtime.CompilerServices.StrongBox<int>();
            var found = await storage.TryReadRawAsync("key"u8.ToArray(), length, static (result, bytes) => result.Value = bytes.Length);
            await Assert.That(found).IsTrue();
            await Assert.That(length.Value).IsEqualTo(value.Length);
            using var cursor = storage.CreateOrderedReadCursor();
            using var batch = await cursor.ReadBatchAsync(128, ServerHost.MaxPayloadBytes);
            await Assert.That(batch.Count).IsEqualTo(1);
        }
        var outputPath = Path.Combine(folder.Path, "output");
        await Run(0, "export", "--db", db, "--format", format, "--to", outputPath);
        await using var input = File.OpenRead(outputPath);
        using var reader = await DumpReader.OpenAsync(input, "auto", CancellationToken.None);
        var entry = await reader.ReadAsync(CancellationToken.None) ?? throw new IOException("Missing maximum-size record.");
        await Assert.That(entry.Value.Length + entry.Key.Length).IsEqualTo(ServerHost.MaxPayloadBytes);
        await Assert.That(entry.Value.Span.SequenceEqual(value)).IsTrue();
        await Assert.That(await reader.ReadAsync(CancellationToken.None)).IsNull();
    }

    [Test]
    public async Task LostImportAcknowledgmentDoesNotClaimConfirmedProgress()
    {
        using var folder = new TestFolder();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            var response = RespondAsync();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            var instanceId = Guid.NewGuid().ToString("N");
            ServerCredentials.Write(folder.Path, new(instanceId, new string('0', 64)));
            using var client = new ServerClient(new ServerRecord("test", instanceId,
                $"http://127.0.0.1:{port}", 1, folder.Path, "lsm", "unused"));
            using var input = new MemoryStream(Encoding.UTF8.GetBytes(Rows(1)));
            IOException? failure = null;
            try { await client.ImportAsync(input, "auto", timeout.Token); }
            catch (IOException exception) { failure = exception; }
            await response.WaitAsync(timeout.Token);
            if (failure is null) throw new IOException("The truncated import acknowledgment was accepted.");
            await Assert.That(failure.Message.Contains("committed progress could not be confirmed", StringComparison.Ordinal)).IsTrue();

            async Task RespondAsync()
            {
                using var socket = await listener.AcceptTcpClientAsync(timeout.Token);
                using var stream = socket.GetStream();
                using var reader = new StreamReader(stream, Encoding.ASCII, leaveOpen: true);
                while (await reader.ReadLineAsync(timeout.Token) is { Length: > 0 }) { }
                await stream.WriteAsync(Encoding.ASCII.GetBytes(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{\"records\":1"), timeout.Token);
            }
        }
        finally { listener.Stop(); }
    }

    private sealed class GatedWriteStream : MemoryStream
    {
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Continue { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            if (Entered.TrySetResult()) await Continue.Task.WaitAsync(cancellationToken);
            await base.WriteAsync(buffer, cancellationToken);
        }
    }

    internal static string Rows(int count)
    {
        var text = new StringBuilder();
        var key = new byte[4];
        var value = new byte[5];
        value[0] = 255;
        for (var i = 0; i < count; i++)
        {
            BinaryPrimitives.WriteInt32BigEndian(key, i);
            key.CopyTo(value, 1);
            text.Append("{\"key\":\"").Append(Convert.ToBase64String(key))
                .Append("\",\"value\":\"").Append(Convert.ToBase64String(value)).Append("\"}\n");
        }
        return text.ToString();
    }
}
