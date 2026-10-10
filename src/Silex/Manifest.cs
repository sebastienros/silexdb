using System.Text.Json;

namespace Silex;

/// <summary>
/// Records the on-disk LSM structure: which SST files are live and which level each belongs to. SST ids
/// alone cannot encode this once compaction rewrites data into fresh files, so the manifest is the
/// authoritative source for recovery under every compaction strategy. The manifest is rewritten
/// atomically (temp file + rename) after every structural change and is the single commit point that makes
/// flush and compaction crash-safe: a crash before the rewrite leaves the previous structure intact, and
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

    private static readonly JsonSerializerOptions _jsonOptions = new() { WriteIndented = false };

    /// <summary>
    /// The manifest schema version.
    /// </summary>
    public int Version { get; set; } = 1;

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
        var manifest = JsonSerializer.Deserialize<Manifest>(json, _jsonOptions);

        if (manifest == null || manifest.Version != 1 || manifest.L0 == null
            || manifest.Levels == null || manifest.Levels.Any(level => level == null))
        {
            throw new InvalidDataException($"The manifest at '{path}' has an invalid or unsupported schema.");
        }

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
    /// </summary>
    public void Write(string storagePath)
    {
        var tempPath = Path.Combine(storagePath, TempFileName);
        var path = GetPath(storagePath);

        using (var stream = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: 1, FileOptions.None))
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteNumber("Version", Version);
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
        }

        // Atomic publish: the rename is the commit point for the structural change.
        File.Move(tempPath, path, overwrite: true);
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
