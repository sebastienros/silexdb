using Silex.Tables;
using System.Diagnostics.CodeAnalysis;

namespace Silex.MemTables;

public interface IMemTable<TKey> : IDisposable where TKey : notnull
{
    long Id { get; }

    /// <summary>
    /// Gets the number of entries in the <see cref="IMemTable{TKey}">.
    /// </summary>
    int Count { get; }

    /// <summary>
    /// Gets the size of the <see cref="IMemTable{TKey}"> in bytes.
    /// </summary>
    long Size { get; }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns><c>true</c> if the key was found, <c>false</c> otherwise.</returns>
    bool TryGet(TKey key, [MaybeNullWhen(false)] out ValueBuffer result);

    void Put(TKey key, ValueBuffer value);

    IStorageIterator<TKey, ValueBuffer> CreateIterator();

    /// <summary>
    /// Adds all entries to an SST Builder.
    /// </summary>
    /// <param name="builder">The builder.</param>
    Task FlushAsync(ISsTableBuilder<TKey> builder, CancellationToken cancellationToken = default);
}
