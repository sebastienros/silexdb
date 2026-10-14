using System.Buffers.Binary;
using System.IO.Hashing;
using System.Text.Json;

namespace Silex;

/// <summary>
/// Records the on-disk LSM structure: which SST files are live and which level each belongs to. SST ids
/// alone cannot encode this once compaction rewrites data into fresh files, so the manifest is the
/// authoritative source for recovery under every compaction strategy. The manifest is rewritten
/// atomically (temp file + rename) after every structural change and is the single commit point that makes
/// flush and compaction process-crash-safe: a crash before the rewrite leaves the previous structure intact, and
/// any SST not referenced by the committed manifest is an orphan that recovery deletes.
/// </summary>
/// <remarks>
/// All ids stored here are <em>filename</em> ids (parsed from <c>{id}.sst</c>), never the in-memory
/// <see cref="Tables.SsTable{TKey, TValue}.Id"/>, which is a transient runtime value unrelated to the file.
/// </remarks>
internal sealed class Manifest
{
    private const string FileName = "manifest";
    private const string TempFileName = "manifest.tmp";

    /// <summary>
    /// The manifest schema version.
    /// </summary>
    public int Version { get; set; } = 1;

    /// <summary>
    /// Identifies this snapshot generation. Flush commits are appended to the sidecar log
    /// <c>manifest.{Epoch}.log</c> instead of rewriting the snapshot; every full snapshot write advances the
    /// epoch, which atomically retires the previous log (a log is only replayed onto the snapshot of its own epoch).
    /// </summary>
    public long Epoch { get; set; }

    /// <summary>
    /// The L0 SST filename ids, oldest-first (the order in which they were flushed). The newest L0 SST is
    /// last, matching the in-memory <c>LevelZeroTables</c> ordering.
    /// </summary>
    public List<long> L0 { get; set; } = [];

    /// <summary>
    /// The SST filename ids for each level below L0. Index 0 is L1, index 1 is L2, and so on. Within a
    /// level the ids are in ascending key order and the SSTs have non-overlapping key ranges.
    /// </summary>
    public List<List<long>> Levels { get; set; } = [];

    public static string GetPath(string storagePath)
    {
        return Path.Combine(storagePath, FileName);
    }

    /// <summary>
    /// Reads the manifest from <paramref name="storagePath"/>, or returns <see langword="null"/> when no
    /// manifest exists (the store was created before every strategy wrote a manifest).
    /// </summary>
    public static Manifest? TryRead(string storagePath)
    {
        var path = GetPath(storagePath);

        if (!File.Exists(path))
        {
            return null;
        }

        var json = File.ReadAllText(path);
        var manifest = JsonSerializer.Deserialize(json, StorageJsonContext.Default.Manifest);

        if (manifest == null || manifest.Version != 1 || manifest.L0 == null
            || manifest.Levels == null || manifest.Levels.Any(level => level == null))
        {
            throw new InvalidDataException($"The manifest at '{path}' has an invalid or unsupported schema.");
        }

        ManifestLog.Replay(storagePath, manifest);

        var ids = new HashSet<long>();
        foreach (var id in manifest.AllSstIds())
        {
            if (id < 0 || !ids.Add(id))
            {
                throw new InvalidDataException($"The manifest at '{path}' contains an invalid or duplicate SST id {id}.");
            }
        }

        return manifest;
    }

    /// <summary>
    /// Atomically replaces the manifest on disk with this instance: the content is written to a temporary
    /// file and renamed over the previous manifest, so a reader never observes a partially written file.
    /// Advances <see cref="Epoch"/>, which retires the previous incremental log.
    /// </summary>
    public void Write(string storagePath, Action<StorageFaultPoint>? faultInjector = null)
    {
        Epoch++;
        var tempPath = Path.Combine(storagePath, TempFileName);
        var path = GetPath(storagePath);

        using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1, FileOptions.None))
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteNumber("Version", Version);
            writer.WriteNumber("Epoch", Epoch);
            WriteIds(writer, "L0", L0);
            writer.WriteStartArray("Levels");

            foreach (var level in Levels)
            {
                writer.WriteStartArray();
                WriteIdValues(writer, level);
                writer.WriteEndArray();
            }

            writer.WriteEndArray();
            writer.WriteEndObject();
            writer.Flush();
            faultInjector?.Invoke(StorageFaultPoint.ManifestBeforeSync);
            stream.Flush(flushToDisk: true);
            faultInjector?.Invoke(StorageFaultPoint.ManifestAfterSync);
        }

        // Synchronizes contents before atomic publication; directory-entry persistence is OS/filesystem-dependent.
        File.Move(tempPath, path, overwrite: true);

        ManifestLog.DeleteStaleLogs(storagePath);
    }

    private static void WriteIds(Utf8JsonWriter writer, string name, List<long> ids)
    {
        writer.WriteStartArray(name);
        WriteIdValues(writer, ids);
        writer.WriteEndArray();
    }

    private static void WriteIdValues(Utf8JsonWriter writer, List<long> ids)
    {
        foreach (var id in ids)
        {
            writer.WriteNumberValue(id);
        }
    }

    /// <summary>
    /// Returns every SST filename id referenced by this manifest (L0 plus all levels).
    /// </summary>
    public IEnumerable<long> AllSstIds()
    {
        foreach (var id in L0)
        {
            yield return id;
        }

        foreach (var level in Levels)
        {
            foreach (var id in level)
            {
                yield return id;
            }
        }
    }
}

/// <summary>
/// Append-only sidecar to the manifest snapshot that records L0 flush commits as fixed-size, checksummed
/// records, so a flush costs one small append to an already-open file instead of a temp-file create plus
/// an overwriting rename (the dominant per-flush cost on NTFS). A record is the commit point: a torn final
/// record is ignored on replay (the flush is treated as uncommitted and its SST as an orphan), while a bad
/// record anywhere else is corruption.
/// </summary>
internal sealed class ManifestLog : IDisposable
{
    private const int RecordSize = 13;
    private const byte AppendL0Record = 1;

    private readonly string _path;
    private FileStream? _stream;

    public ManifestLog(string storagePath, long epoch)
    {
        _path = GetPath(storagePath, epoch);
    }

    private static string GetPath(string storagePath, long epoch)
    {
        return Path.Combine(storagePath, $"manifest.{epoch}.log");
    }

    /// <summary>
    /// Records, and synchronizes to disk like the snapshot write, that <paramref name="id"/> was appended to L0.
    /// </summary>
    public void AppendL0(long id, Action<StorageFaultPoint>? faultInjector = null)
    {
        _stream ??= new FileStream(_path, FileMode.Append, FileAccess.Write, FileShare.Read | FileShare.Delete, bufferSize: 1);

        Span<byte> record = stackalloc byte[RecordSize];
        record[0] = AppendL0Record;
        BinaryPrimitives.WriteInt64LittleEndian(record[1..], id);
        BinaryPrimitives.WriteUInt32LittleEndian(record[9..], Crc32.HashToUInt32(record[..9]));
        _stream.Write(record);
        faultInjector?.Invoke(StorageFaultPoint.ManifestBeforeSync);
        _stream.Flush(flushToDisk: true);
        faultInjector?.Invoke(StorageFaultPoint.ManifestAfterSync);
    }

    public void Dispose()
    {
        _stream?.Dispose();
        _stream = null;
    }

    public static void Replay(string storagePath, Manifest manifest)
    {
        var path = GetPath(storagePath, manifest.Epoch);

        if (!File.Exists(path))
        {
            return;
        }

        var bytes = File.ReadAllBytes(path);
        var count = bytes.Length / RecordSize;

        for (var i = 0; i < count; i++)
        {
            var record = bytes.AsSpan(i * RecordSize, RecordSize);
            var valid = record[0] == AppendL0Record
                && BinaryPrimitives.ReadUInt32LittleEndian(record[9..]) == Crc32.HashToUInt32(record[..9]);

            if (!valid)
            {
                if (i == count - 1 && bytes.Length == count * RecordSize)
                {
                    return;
                }

                throw new InvalidDataException($"The manifest log '{path}' is corrupt at record {i}.");
            }

            manifest.L0.Add(BinaryPrimitives.ReadInt64LittleEndian(record[1..]));
        }
    }

    public static void DeleteStaleLogs(string storagePath)
    {
        foreach (var file in Directory.EnumerateFiles(storagePath, "manifest.*.log"))
        {
            try
            {
                File.Delete(file);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }
}
