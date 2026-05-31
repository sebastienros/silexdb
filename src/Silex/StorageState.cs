using Silex.MemTables;
using Silex.Tables;
using System.Collections.Immutable;

namespace Silex;

/// <summary>
/// Stores the current structure of the LSM storage engine. All the logic is maintained in <see cref="LsmStorageInner"/>.
/// </summary>
/// <remarks>
/// This structure is mostly immutable (minus CurrentMemTable) such that we can duplicate it
/// to get a snapshot for read-only operation.
/// </remarks>
internal struct StorageState<TKey> where TKey : notnull
{
    public StorageState()
    {
    }

    /// <summary>
    /// The list of immutable MemTables.
    /// </summary>
    public ImmutableQueue<IMemTable<TKey>> ImmutableMemTables { get; set; } = [];

    public required IMemTable<TKey> CurrentMemTable { get; set; }

    /// <summary>
    /// The list of level-0 <see cref="SsTable"/>.
    /// </summary>
    /// <remarks>
    /// Level-0 SSTs are the set of SSTs files directly created as a result of MemTable flush.
    /// They are also treated differently when iterated as tables are not ordered is relation
    /// to others in this level, so a Merge Iterator is required.
    /// </remarks>
    public List<SsTable<TKey>> LevelZeroTables = [];

    /// <summary>
    /// The list of <see cref="SsTable"/> for each level but 0.
    /// </summary>
    /// <remarks>
    /// These levels are the result of compaction (either tiered or leveled).
    /// Each level has ordered tables related to each other so a Concat Iterator can be used.
    /// </remarks>
    public List<List<SsTable<TKey>>> LeveledSsTables { get; set; } = [];
}
