namespace Silex;

using Silex.Blocks;
using Silex.MemTables;
using Silex.Tables;

public class StorageOptions
{
    /// <summary>64MiB</summary>
    private static readonly long _defaultTableSizeLimit = 64.MiB();

    /// <summary>4KiB</summary>
    private static readonly ushort _defaultBlockSize = (ushort)4.KiB();

    /// <summary><see cref="DefaultBlockEncoder"></summary>
    private static readonly IBlockEncoder _defaultBlockEncoder = new DefaultBlockEncoder();

    /// <summary><see cref="DefaultSsTableEncoder"></summary>
    private static readonly ISsTableEncoder _defaultSsTableEncoder = new DefaultSsTableEncoder();

    /// <summary>
    /// Gets or sets the maximum size of a <see cref="MemTable">. When the size is reached it is made immutable and 
    /// can be stored as a level-0 SST.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultTableSizeLimit"/>.
    /// </value>
    public long MemTableSizeLimit { get; set; } = _defaultTableSizeLimit;

    /// <summary>
    /// Gets or sets the size of a block.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockSize"/>.
    /// </value>
    public ushort BlockSize { get; set; } = _defaultBlockSize;

    /// <summary>
    /// Gets or set the <see cref="IBlockEncoder"> to use.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultBlockEncoder"/>.
    /// </value>
    public IBlockEncoder BlockEncoder { get; set; } = _defaultBlockEncoder;

    /// <summary>
    /// Gets or set the <see cref="ISsTableEncoder"> to use.
    /// </summary>
    /// <value>
    /// The default value is <inheritdoc cref="_defaultSsTableEncoder"/>.
    /// </value>
    public ISsTableEncoder SsTableEncoder { get; set; } = _defaultSsTableEncoder;
}
