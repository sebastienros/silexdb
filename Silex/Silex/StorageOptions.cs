namespace Silex;

public class StorageOptions
{
    /// <summary>64MiB</summary>
    private static readonly long _defaultTableSizeLimit = 64.MiB();

    /// <summary>
    /// Gets or sets the maximum size of a <see cref="MemTable">. When the size is reached it is made immutable and 
    /// can be stored as an SST.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultTableSizeLimit"/>.
    /// </value>
    public long MemTableSizeLimit { get; set; } = _defaultTableSizeLimit;
}
