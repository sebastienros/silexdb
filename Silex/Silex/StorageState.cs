using System.Collections.Immutable;

namespace Silex;

/// <summary>
/// Stores the current structure of the LSM storage engine. All the logic is maintained in <see cref="LsmStorageInner"/>.
/// </summary>
internal class StorageState
{
    public StorageState(StorageOptions _)
    {
    }

    /// <summary>
    /// The list of immutable MemTables. The last MemTable is the mutable one.
    /// </summary>
    public ImmutableStack<IMemTable> ImmutableMemTables { get; set; } = [];

    public required IMemTable CurrentMemTable { get; set; }
}
