using System.Text.Json;

namespace Silex;

/// <summary>
/// Records the on-disk LSM structure for <see cref="CompactionStrategy.Leveled"/>: which SST files are
/// live and which level each belongs to. SST ids alone cannot encode this because a deeper level's SST is
/// older data yet receives a fresh (higher) id every time it is rewritten by compaction, so id ordering no
/// longer reflects recency. The manifest is rewritten atomically (temp file + rename) after every
/// structural change and is the single commit point that makes flush and compaction crash-safe: a crash
/// before the rewrite leaves the previous structure intact, and any SST not referenced by the committed
/// manifest is an orphan that recovery deletes.
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
    /// manifest exists (the store was never written with leveled compaction).
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

        return manifest ?? throw new InvalidDataException($"The manifest at '{path}' could not be parsed.");
    }

    /// <summary>
    /// Atomically replaces the manifest on disk with this instance: the content is written to a temporary
    /// file and renamed over the previous manifest, so a reader never observes a partially written file.
    /// </summary>
    public void Write(string storagePath)
    {
        var tempPath = Path.Combine(storagePath, TempFileName);
        var path = GetPath(storagePath);

        var json = JsonSerializer.Serialize(this, _jsonOptions);
        File.WriteAllText(tempPath, json);

        // Atomic publish: the rename is the commit point for the structural change.
        File.Move(tempPath, path, overwrite: true);
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
