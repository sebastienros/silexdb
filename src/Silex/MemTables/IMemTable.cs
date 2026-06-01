using Silex.Tables;
using System.Diagnostics.CodeAnalysis;

namespace Silex.MemTables;

internal interface IMemTable : IDisposable
{
    long Id { get; }

    /// <summary>
    /// Gets the number of entries in the <see cref="IMemTable">.
    /// </summary>
    int Count { get; }

    /// <summary>
    /// Gets the size of the <see cref="IMemTable"> in bytes.
    /// </summary>
    long Size { get; }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns><c>true</c> if the key was found, <c>false</c> otherwise.</returns>
    bool TryGet(ByteSlice key, [MaybeNullWhen(false)] out ByteSlice result);

    void Put(ByteSlice key, ByteSlice value);

    IStorageIterator CreateIterator();

    /// <summary>
    /// Adds all entries to an SST Builder.
    /// </summary>
    /// <param name="builder">The builder.</param>
    Task FlushAsync(ISsTableBuilder builder, CancellationToken cancellationToken = default);
}
