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
internal struct StorageState
{
    public StorageState(StorageOptions _)
    {
    }

    /// <summary>
    /// The list of immutable MemTables.
    /// </summary>
    public ImmutableQueue<IMemTable> ImmutableMemTables { get; set; } = [];

    public required IMemTable CurrentMemTable { get; set; }

    /// <summary>
    /// The list of <see cref="SsTable"/> for each level.
    /// </summary>
    /// <remarks>
    /// Level-0 SSTs are the set of SSTs files 
    /// directly created as a result of MemTable flush. Other levels are the result of compaction,
    /// (either tiered or leveled).
    /// </remarks>
    public List<List<SsTable>> SsTables { get; set; }

    public StorageState Clone()
    {
        return new StorageState
        {
            CurrentMemTable = CurrentMemTable,
            ImmutableMemTables = ImmutableMemTables,
            SsTables = SsTables
        };
    }
}
