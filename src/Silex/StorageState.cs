using Silex.MemTables;
using Silex.Tables;
using System.Collections.Immutable;

namespace Silex;

/// <summary>
/// Stores the current structure of the LSM storage engine. All the logic is maintained in <see cref="LsmStorageInner"/>.
/// </summary>
internal struct StorageState
{
    public StorageState(StorageOptions _)
    {
    }

    /// <summary>
    /// The list of immutable MemTables. The last MemTable is the mutable one.
    /// </summary>
    public ImmutableStack<IMemTable> ImmutableMemTables { get; set; } = [];

    public required IMemTable CurrentMemTable { get; set; }

    /// <summary>
    /// The list of <see cref="SsTable"/> for each level.
    /// </summary>
    /// <remarks>
    /// This table is initialized for level-0.
    /// </remarks>
    public List<List<SsTable>> SsTables { get; set; } = [[]];

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
