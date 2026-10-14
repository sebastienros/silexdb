using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Silex.Tool;

internal sealed record ServerConfiguration
{
    public string Name { get; init; } = "local";
    public string? Database { get; init; }
    public string Mode { get; init; } = "lsm";
    public int Port { get; init; }
    public bool Create { get; init; }
    public bool AllowWrites { get; init; }
    public string? BackupRoot { get; init; }
    public bool UseWriteAheadLog { get; init; } = true;
    public string? InstanceId { get; init; }

    public void Validate()
    {
        Registry.ValidateName(Name);
        ArgumentException.ThrowIfNullOrWhiteSpace(Database);
        DatabaseHandle.ValidateMode(Mode);
        if (Port is < 0 or > 65535)
            throw new ArgumentException("Port must be between 0 and 65535.");
    }
}

internal sealed record ServerRecord(string Name, string InstanceId, string Endpoint,
    int ProcessId, string Database, string Mode, string Log);
internal sealed record ServerIdentity(string Name, string InstanceId, int ProcessId, string Database, string Mode, bool AllowWrites);
internal sealed record ServerStatus(string Name, string State, int ProcessId, string Endpoint, string Database, string Mode, string Log);
internal sealed record KeyRequest(byte[] Key);
internal sealed record PutRequest(byte[] Key, byte[] Value);
internal sealed record ScanRequest(byte[]? Prefix = null, byte[]? From = null, byte[]? To = null, int Limit = 100, bool Reverse = false);
internal sealed record ExportRequest(byte[]? Prefix = null, byte[]? From = null, byte[]? To = null,
    bool Reverse = false, string Format = "jsonl", bool Live = false);
internal sealed record ImportResult(long Records, long Batches);
internal sealed record BackupRequest(string Name);
internal sealed record OperationStatus(string Id, string State, string? Error = null);
internal sealed record ErrorResponse(string Error, string? CommitOutcome = null, long? CommittedRecords = null);
internal sealed record IntegrityResult(bool IsRecoveryComplete, StorageIntegrityIssue[] Issues);
internal sealed record DatabaseStatistics(string Database, string Mode, string Health, string? Fault, string Pressure,
    StorageLevelStatistics[] Levels, long ActiveMemTableBytes, int ImmutableMemTableCount,
    long ImmutableMemTableBytes, long WriteOperations, long UserBytesWritten, long WalBytesWritten,
    long CacheHits, long CacheMisses, double? CacheHitRatio, double? WriteAmplification,
    long? CompactionBacklogBytes, long? PublishedSequence);
internal sealed record DatabaseInfo(DatabaseStatistics Statistics, long DiskBytes, int SstFiles, int WalFiles);

[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow, UseStringEnumConverter = true)]
[JsonSerializable(typeof(ServerConfiguration))]
[JsonSerializable(typeof(ServerRecord))]
[JsonSerializable(typeof(ServerCredentials))]
[JsonSerializable(typeof(ServerIdentity))]
[JsonSerializable(typeof(ServerStatus[]))]
[JsonSerializable(typeof(ServerStatus))]
[JsonSerializable(typeof(KeyRequest))]
[JsonSerializable(typeof(PutRequest))]
[JsonSerializable(typeof(ScanRequest))]
[JsonSerializable(typeof(ExportRequest))]
[JsonSerializable(typeof(ImportResult))]
[JsonSerializable(typeof(BackupRequest))]
[JsonSerializable(typeof(OperationStatus))]
[JsonSerializable(typeof(ErrorResponse))]
[JsonSerializable(typeof(IntegrityResult))]
[JsonSerializable(typeof(DatabaseStatistics))]
[JsonSerializable(typeof(DatabaseInfo))]
[JsonSerializable(typeof(StorageTableStatistics[]))]
internal partial class ToolJsonContext : JsonSerializerContext;

internal static class Output
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    public static byte[] Decode(string text, string encoding) => encoding switch
    {
        "utf8" => StrictUtf8.GetBytes(text),
        "hex" => Convert.FromHexString(text),
        "base64" => Convert.FromBase64String(text),
        _ => throw new ArgumentException("Input encoding must be utf8, hex, or base64.")
    };

    public static string Encode(ReadOnlySpan<byte> bytes, string encoding) => encoding switch
    {
        "utf8" => StrictUtf8.GetString(bytes),
        "hex" => Convert.ToHexString(bytes),
        "base64" => Convert.ToBase64String(bytes),
        _ => throw new ArgumentException("Output encoding must be utf8, hex, base64, or raw (get only).")
    };

    public static void Report<T>(T value, JsonTypeInfo<T> type, bool compact)
    {
        using var writer = new Utf8JsonWriter(Console.OpenStandardOutput(), new JsonWriterOptions { Indented = !compact });
        JsonSerializer.Serialize(writer, value, type);
        writer.Flush();
        Console.WriteLine();
    }
}
