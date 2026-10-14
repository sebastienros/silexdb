using System.CommandLine;
using System.CommandLine.Parsing;
using System.Text.Json;

namespace Silex.Tool;

internal static class Cli
{
    public static async Task<int> RunAsync(string[] args)
    {
        var root = new RootCommand("Silex database administration. Offline access requires exclusive directory ownership.");
        var db = new Option<string?>("--db") { Description = "Offline database directory.", Recursive = true };
        var server = new Option<string?>("--server") { Description = "Registered local server name.", Recursive = true };
        var mode = new Option<string?>("--mode") { Description = "Storage mode: lsm (default) or mvcc.", Recursive = true };
        mode.AcceptOnlyFromAmong("lsm", "mvcc");
        var noWal = new Option<bool>("--no-wal") { Description = "Match a database created without WAL.", Recursive = true };
        var json = new Option<bool>("--json") { Description = "Compact machine-readable reports.", Recursive = true };
        root.Add(db); root.Add(server); root.Add(mode); root.Add(noWal); root.Add(json);

        var init = new Command("init", "Create a database in an empty directory.");
        init.SetAction(async (result, cancellationToken) =>
        {
            await using var database = await OpenAsync(result, writable: true, create: true, cancellationToken);
            Console.Error.WriteLine($"Created {database.Mode} database '{database.Path}'.");
            return 0;
        });
        root.Add(init);

        foreach (var name in new[] { "info", "stats" })
        {
            var command = new Command(name, name == "info" ? "Inspect database files and physical structure." : "Report physical structure and since-open counters.");
            command.SetAction(async (result, cancellationToken) =>
            {
                using var client = Client(result);
                if (client is not null)
                {
                    if (name == "info") Output.Report(await client.GetAsync("info", ToolJsonContext.Default.DatabaseInfo, cancellationToken),
                        ToolJsonContext.Default.DatabaseInfo, result.GetValue(json));
                    else Output.Report(await client.GetAsync("stats", ToolJsonContext.Default.DatabaseStatistics, cancellationToken),
                        ToolJsonContext.Default.DatabaseStatistics, result.GetValue(json));
                }
                else
                {
                    await using var database = await OpenAsync(result, writable: false, create: false, cancellationToken);
                    if (name == "info") Output.Report(await database.InfoAsync(cancellationToken), ToolJsonContext.Default.DatabaseInfo, result.GetValue(json));
                    else Output.Report(await database.StatisticsAsync(cancellationToken), ToolJsonContext.Default.DatabaseStatistics, result.GetValue(json));
                }
                return 0;
            });
            root.Add(command);
        }

        foreach (var name in new[] { "get", "put", "delete" })
        {
            var command = new Command(name, $"{name} a byte-oriented key.");
            var key = new Option<string>("--key") { Required = true };
            var keyEncoding = EncodingOption("--key-encoding", "utf8");
            var valueEncoding = EncodingOption("--value-encoding", "utf8", allowRaw: name == "get");
            var value = new Option<string?>("--value");
            var file = new Option<string?>("--value-file") { Description = "Read raw value bytes from a file, or '-' for stdin." };
            command.Add(key); command.Add(keyEncoding);
            if (name is "get" or "put") command.Add(valueEncoding);
            if (name == "put") { command.Add(value); command.Add(file); }
            command.SetAction(async (result, cancellationToken) =>
            {
                var bytes = Output.Decode(result.GetValue(key)!, result.GetValue(keyEncoding)!);
                DatabaseHandle.ValidateKey(bytes);
                var encoding = result.GetValue(valueEncoding) ?? "utf8";
                var input = name == "put" ? await ValueAsync(result.GetValue(value), result.GetValue(file), encoding, cancellationToken) : null;
                using var client = Client(result);
                if (client is not null)
                {
                    if (name == "put") await client.PutAsync(bytes, input!, cancellationToken);
                    else if (name == "delete") await client.DeleteAsync(bytes, cancellationToken);
                    else
                    {
                        if (encoding == "raw")
                            return await client.ReadAsync(bytes, Console.OpenStandardOutput(), cancellationToken) ? 0 : Missing();
                        using var memory = new MemoryStream();
                        if (!await client.ReadAsync(bytes, memory, cancellationToken)) return Missing();
                        Console.WriteLine(Output.Encode(memory.GetBuffer().AsSpan(0, checked((int)memory.Length)), encoding));
                    }
                }
                else
                {
                    await using var database = await OpenAsync(result, writable: name != "get", create: false, cancellationToken);
                    if (name == "put") database.Put(bytes, input!);
                    else if (name == "delete") database.Delete(bytes);
                    else
                    {
                        using var buffer = new LimitedBuffer(ServerHost.MaxPayloadBytes);
                        if (!await database.ReadAsync(bytes, buffer, cancellationToken)) return Missing();
                        if (encoding == "raw") await Console.OpenStandardOutput().WriteAsync(buffer.WrittenMemory, cancellationToken);
                        else Console.WriteLine(Output.Encode(buffer.WrittenMemory.Span, encoding));
                    }
                }
                return 0;
            });
            root.Add(command);
        }

        var scan = new Command("scan", "Stream ordered entries as JSONL with base64 keys and values. Upper bounds are exclusive.");
        var prefix = new Option<string?>("--prefix");
        var from = new Option<string?>("--from");
        var to = new Option<string?>("--to");
        var scanEncoding = EncodingOption("--key-encoding", "utf8");
        var limit = new Option<int>("--limit") { DefaultValueFactory = _ => 100 };
        var reverse = new Option<bool>("--reverse");
        scan.Add(prefix); scan.Add(from); scan.Add(to); scan.Add(scanEncoding); scan.Add(limit); scan.Add(reverse);
        scan.SetAction(async (result, cancellationToken) =>
        {
            byte[]? Bound(Option<string?> option) => result.GetValue(option) is { } bound
                ? Output.Decode(bound, result.GetValue(scanEncoding)!) : null;
            var request = new ScanRequest(Bound(prefix), Bound(from), Bound(to), result.GetValue(limit), result.GetValue(reverse));
            using var client = Client(result);
            if (client is not null) await client.ScanAsync(request, Console.OpenStandardOutput(), cancellationToken);
            else
            {
                await using var database = await OpenAsync(result, writable: false, create: false, cancellationToken);
                await database.ScanAsync(request, Console.OpenStandardOutput(), cancellationToken);
            }
            return 0;
        });
        root.Add(scan);

        var export = new Command("export", "Stream all logical key/value records as JSONL or a checksummed binary dump.");
        var exportTo = new Option<string>("--to") { Required = true, Description = "New output file, or '-' for stdout." };
        var exportFormat = new Option<string>("--format") { DefaultValueFactory = _ => "jsonl" };
        exportFormat.AcceptOnlyFromAmong("jsonl", "binary");
        var gzip = new Option<bool>("--gzip") { Description = "Compress the output using gzip." };
        var exportPrefix = new Option<string?>("--prefix");
        var exportFrom = new Option<string?>("--from");
        var exportUpper = new Option<string?>("--to-key") { Description = "Exclusive upper key bound." };
        var exportEncoding = EncodingOption("--key-encoding", "utf8");
        var exportReverse = new Option<bool>("--reverse");
        var live = new Option<bool>("--live") { Description = "Allow a live, non-snapshot export from an online plain-LSM server." };
        export.Add(exportTo); export.Add(exportFormat); export.Add(gzip); export.Add(exportPrefix);
        export.Add(exportFrom); export.Add(exportUpper); export.Add(exportEncoding); export.Add(exportReverse); export.Add(live);
        export.SetAction(async (result, cancellationToken) =>
        {
            byte[]? Bound(Option<string?> option) => result.GetValue(option) is { } bound
                ? Output.Decode(bound, result.GetValue(exportEncoding)!) : null;
            var request = new ExportRequest(Bound(exportPrefix), Bound(exportFrom), Bound(exportUpper),
                result.GetValue(exportReverse), result.GetValue(exportFormat)!, result.GetValue(live));
            if (request.Prefix is not null && (request.From is not null || request.To is not null))
                throw new ArgumentException("Use either a prefix or range bounds, not both.");
            using var client = Client(result);
            if (client is not null)
                await BulkTransfer.ExportFileAsync(result.GetValue(exportTo)!, result.GetValue(gzip),
                    (output, token) => client.ExportAsync(request, output, token), cancellationToken);
            else
            {
                if (request.Live) throw new ArgumentException("--live is only valid for online plain-LSM exports.");
                await using var database = await OpenAsync(result, writable: false, create: false, cancellationToken);
                await BulkTransfer.ExportFileAsync(result.GetValue(exportTo)!, result.GetValue(gzip),
                    async (output, token) => { await database.ExportAsync(request, output, token); }, cancellationToken);
            }
            Console.Error.WriteLine("Export completed.");
            return 0;
        });
        root.Add(export);

        var import = new Command("import", "Upsert a logical dump in bounded batches; earlier committed batches survive an error.");
        var importFrom = new Option<string>("--from") { Required = true, Description = "Input file, or '-' for stdin." };
        var importFormat = new Option<string>("--format") { DefaultValueFactory = _ => "auto" };
        importFormat.AcceptOnlyFromAmong("auto", "jsonl", "binary");
        import.Add(importFrom); import.Add(importFormat);
        import.SetAction(async (result, cancellationToken) =>
        {
            await using var input = result.GetValue(importFrom) == "-" ? Console.OpenStandardInput()
                : new FileStream(result.GetValue(importFrom)!, FileMode.Open, FileAccess.Read, FileShare.Read,
                    64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan);
            using var client = Client(result);
            ImportResult imported;
            if (client is not null)
                imported = await client.ImportAsync(input, result.GetValue(importFormat)!, cancellationToken);
            else
            {
                await using var database = await OpenAsync(result, writable: true, create: false, cancellationToken);
                imported = await BulkTransfer.ImportAsync(database, input, result.GetValue(importFormat)!, cancellationToken);
            }
            if (result.GetValue(json)) Output.Report(imported, ToolJsonContext.Default.ImportResult, compact: true);
            else Console.Error.WriteLine($"Imported {imported.Records} records in {imported.Batches} committed batches.");
            return 0;
        });
        root.Add(import);

        var backup = new Command("backup", "Create a physical checkpoint. Online destinations are names under the server's backup root.");
        var backupTo = new Option<string>("--to") { Required = true };
        backup.Add(backupTo);
        backup.SetAction(async (result, cancellationToken) =>
        {
            using var client = Client(result);
            if (client is not null) await client.BackupAsync(result.GetValue(backupTo)!, cancellationToken);
            else
            {
                await using var database = await OpenAsync(result, writable: true, create: false, cancellationToken);
                await database.BackupAsync(result.GetValue(backupTo)!, cancellationToken);
                Console.Error.WriteLine("Backup completed.");
            }
            return 0;
        });
        root.Add(backup);

        var restore = new Command("restore", "Restore a physical backup to an empty, offline directory.");
        var restoreFrom = new Option<string>("--from") { Required = true };
        var restoreTo = new Option<string>("--to") { Required = true };
        restore.Add(restoreFrom); restore.Add(restoreTo);
        restore.SetAction(async (result, cancellationToken) =>
        {
            RequireOffline(result);
            var storageMode = result.GetValue(mode) ?? "lsm";
            DatabaseHandle.ValidateMode(storageMode);
            var options = new StorageOptions { UseWriteAheadLog = !result.GetValue(noWal) };
            if (storageMode == "mvcc")
                await MvccStorage.RestoreBackupAsync(result.GetValue(restoreFrom)!, result.GetValue(restoreTo)!, options, cancellationToken);
            else await LsmStorage.RestoreBackupAsync(result.GetValue(restoreFrom)!, result.GetValue(restoreTo)!, options, cancellationToken);
            Console.Error.WriteLine("Restore completed.");
            return 0;
        });
        root.Add(restore);

        var verify = new Command("verify", "Verify offline recovery files without modifying them.");
        verify.SetAction(async (result, cancellationToken) =>
        {
            RequireOffline(result);
            var report = await LsmStorage.VerifyIntegrityAsync(RequireDb(result),
                new StorageOptions { UseWriteAheadLog = !result.GetValue(noWal) }, cancellationToken);
            if (report.IsRecoveryComplete && result.GetValue(mode) == "mvcc")
            {
                await using var database = await OpenAsync(result, writable: false, create: false, cancellationToken);
            }
            Output.Report(new IntegrityResult(report.IsRecoveryComplete, report.Issues.ToArray()),
                ToolJsonContext.Default.IntegrityResult, result.GetValue(json));
            return report.IsRecoveryComplete ? 0 : 1;
        });
        root.Add(verify);

        var serve = new Command("serve", "Host one database for development on authenticated IPv4/IPv6 loopback HTTP. Foreground by default.");
        var config = new Option<string?>("--config");
        var serverName = new Option<string?>("--name");
        var port = new Option<int?>("--port");
        var create = new Option<bool>("--create");
        var writes = new Option<bool>("--allow-writes");
        var backupRoot = new Option<string?>("--backup-root");
        var detach = new Option<bool>("--detach");
        var background = new Option<bool>("--background") { Hidden = true };
        serve.Add(config); serve.Add(serverName); serve.Add(port); serve.Add(create); serve.Add(writes);
        serve.Add(backupRoot); serve.Add(detach); serve.Add(background);
        serve.SetAction(async (result, cancellationToken) =>
        {
            RequireOffline(result);
            var configuration = new ServerConfiguration();
            var basePath = Environment.CurrentDirectory;
            if (result.GetValue(config) is { } configPath)
            {
                configPath = Path.GetFullPath(configPath);
                basePath = Path.GetDirectoryName(configPath)!;
                await using var stream = File.OpenRead(configPath);
                configuration = await JsonSerializer.DeserializeAsync(stream, ToolJsonContext.Default.ServerConfiguration, cancellationToken)
                    ?? throw new ArgumentException("The configuration file is empty.");
            }
            var databasePath = result.GetValue(db) is { } cliPath ? Path.GetFullPath(cliPath)
                : configuration.Database is { } configuredPath ? Path.GetFullPath(configuredPath, basePath) : null;
            var backupsPath = result.GetValue(backupRoot) is { } cliBackups ? Path.GetFullPath(cliBackups)
                : configuration.BackupRoot is { } configuredBackups ? Path.GetFullPath(configuredBackups, basePath) : null;
            configuration = configuration with
            {
                Name = result.GetValue(serverName) ?? configuration.Name,
                Database = databasePath,
                BackupRoot = backupsPath,
                Mode = result.GetValue(mode) ?? configuration.Mode,
                Port = result.GetValue(port) ?? configuration.Port,
                Create = Explicit(create) ? result.GetValue(create) : configuration.Create,
                AllowWrites = Explicit(writes) ? result.GetValue(writes) : configuration.AllowWrites,
                UseWriteAheadLog = Explicit(noWal) ? !result.GetValue(noWal) : configuration.UseWriteAheadLog
            };
            configuration.Validate();
            if (result.GetValue(detach))
            {
                if (result.GetValue(background)) throw new ArgumentException("Cannot detach a detached child.");
                await new Registry().StartDetachedAsync(configuration, cancellationToken);
                return 0;
            }
            return await ServerHost.RunAsync(configuration, result.GetValue(background), cancellationToken);
            bool Explicit(Option option) => result.GetResult(option) is { Implicit: false };
        });
        root.Add(serve);

        var ps = new Command("ps", "List registered servers without exposing their authentication tokens.");
        ps.SetAction(async (result, cancellationToken) =>
        {
            Output.Report(await new Registry().ListAsync(cancellationToken), ToolJsonContext.Default.ServerStatusArray, result.GetValue(json));
            return 0;
        });
        root.Add(ps);
        var stop = new Command("stop", "Gracefully stop an identity-checked, registered server.");
        var stopName = new Argument<string>("name");
        stop.Add(stopName);
        stop.SetAction(async (result, cancellationToken) =>
        {
            using var client = RegisteredClient(result.GetValue(stopName)!);
            await client.StopAsync(cancellationToken);
            Console.Error.WriteLine("Server stopped.");
            return 0;
        });
        root.Add(stop);

        var ui = new Command("ui", "Start or attach to a local database UI. Newly started hosts remain running until stop.");
        var uiName = new Option<string>("--name") { DefaultValueFactory = _ => "local" };
        var uiWrites = new Option<bool>("--allow-writes");
        var uiBackup = new Option<string?>("--backup-root");
        var uiNoOpen = new Option<bool>("--no-open");
        ui.Add(uiName); ui.Add(uiWrites); ui.Add(uiBackup); ui.Add(uiNoOpen);
        ui.SetAction(async (result, cancellationToken) =>
        {
            var registry = new Registry();
            var name = result.GetValue(server);
            if (name is not null)
            {
                if (result.GetValue(db) is not null || result.GetValue(mode) is not null || result.GetValue(noWal) ||
                    result.GetValue(uiWrites) || result.GetValue(uiBackup) is not null)
                    throw new ArgumentException("--server cannot be combined with database or host configuration options.");
            }
            else
            {
                name = result.GetValue(uiName)!;
                await registry.StartDetachedAsync(new ServerConfiguration
                {
                    Name = name, Database = Path.GetFullPath(RequireDb(result)), Mode = result.GetValue(mode) ?? "lsm",
                    AllowWrites = result.GetValue(uiWrites), UseWriteAheadLog = !result.GetValue(noWal),
                    BackupRoot = result.GetValue(uiBackup) is { } path ? Path.GetFullPath(path) : null
                }, cancellationToken);
            }
            var record = registry.Read(name) ?? throw new IOException("Missing server registration.");
            using var client = new ServerClient(record);
            if (!await client.IsRunningAsync(cancellationToken)) throw new IOException("The server is unreachable or its identity changed.");
            var credentials = ServerCredentials.Read(record.Database);
            if (credentials.InstanceId != record.InstanceId) throw new IOException("Server instance changed. Retry the UI launch.");
            var url = record.Endpoint.TrimEnd('/') + "/ui/#token=" + credentials.Token;
            if (result.GetValue(uiNoOpen)) Console.WriteLine(url);
            else
            {
                using var browser = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo(url) { UseShellExecute = true });
                Console.Error.WriteLine($"UI opened for '{name}'. Use 'silex stop {name}' to stop the server.");
            }
            return 0;
        });
        root.Add(ui);

        try
        {
            return await root.Parse(args).InvokeAsync(new InvocationConfiguration
            {
                EnableDefaultExceptionHandler = false,
                ProcessTerminationTimeout = null
            });
        }
        catch (OperationCanceledException)
        {
            Console.Error.WriteLine("Operation canceled or timed out. A submitted mutation may have committed; it was not retried.");
            return 1;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine($"error: {exception.Message}");
            if (exception is StorageOperationException operation)
                Console.Error.WriteLine($"Commit outcome: {operation.Outcome}");
            return 1;
        }

        void RequireOffline(ParseResult result)
        {
            if (result.GetValue(server) is not null) throw new ArgumentException("This command requires offline access.");
        }
        string RequireDb(ParseResult result) => result.GetValue(db)
            ?? throw new ArgumentException("Specify --db for offline access or --server for online operations.");
        ServerClient? Client(ParseResult result)
        {
            if (result.GetValue(server) is not { } name) return null;
            if (result.GetValue(db) is not null || result.GetValue(mode) is not null || result.GetValue(noWal))
                throw new ArgumentException("--server cannot be combined with --db, --mode, or --no-wal.");
            return RegisteredClient(name);
        }
        Task<DatabaseHandle> OpenAsync(ParseResult result, bool writable, bool create, CancellationToken cancellationToken)
        {
            RequireOffline(result);
            return DatabaseHandle.OpenAsync(RequireDb(result), result.GetValue(mode) ?? "lsm",
                !writable, create, !result.GetValue(noWal), cancellationToken);
        }
    }

    private static ServerClient RegisteredClient(string name)
    {
        var record = new Registry().Read(name) ?? throw new IOException($"No registered server named '{name}'.");
        return new ServerClient(record);
    }

    private static Option<string> EncodingOption(string name, string defaultValue, bool allowRaw = false)
    {
        var option = new Option<string>(name)
        {
            DefaultValueFactory = _ => defaultValue, Description = allowRaw ? "utf8, hex, base64, or raw." : "utf8, hex, or base64."
        };
        option.AcceptOnlyFromAmong(allowRaw ? ["utf8", "hex", "base64", "raw"] : ["utf8", "hex", "base64"]);
        return option;
    }

    private static int Missing()
    {
        Console.Error.WriteLine("Key not found.");
        return 2;
    }

    private static async Task<byte[]> ValueAsync(string? value, string? path, string encoding, CancellationToken cancellationToken)
    {
        if ((value is null) == (path is null))
            throw new ArgumentException("Specify exactly one of --value and --value-file.");
        if (value is not null)
        {
            var bytes = Output.Decode(value, encoding);
            if (bytes.Length > ServerHost.MaxPayloadBytes) throw new ArgumentException("Value exceeds 16 MiB.");
            return bytes;
        }
        await using var input = path == "-" ? Console.OpenStandardInput() : File.OpenRead(path!);
        using var buffer = new LimitedBuffer(ServerHost.MaxPayloadBytes + 1);
        while (true)
        {
            var read = await input.ReadAsync(buffer.GetMemory(1), cancellationToken);
            if (read == 0) break;
            buffer.Advance(read);
            if (buffer.WrittenMemory.Length > ServerHost.MaxPayloadBytes) throw new ArgumentException("Value exceeds 16 MiB.");
        }
        return buffer.WrittenMemory.ToArray();
    }
}
