using Silex.Tables;
using System.Diagnostics.CodeAnalysis;

namespace Silex.MemTables;

public interface IMemTable<TKey, TValue> : IDisposable where TKey : notnull
{
    long Id { get; }

    /// <summary>
    /// Gets the number of entries in the <see cref="IMemTable{TKey, TValue}">.
    /// </summary>
    int Count { get; }

    /// <summary>
    /// Gets the size of the <see cref="IMemTable{TKey, TValue}"> in bytes.
    /// </summary>
    long Size { get; }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns><c>true</c> if the key was found, <c>false</c> otherwise.</returns>
    bool TryGet(TKey key, [MaybeNullWhen(false)] out TValue result);

    void Put(TKey key, TValue value);

    IStorageIterator<TKey, TValue> CreateIterator();

    void Flush(SsTableBuilder<TKey, TValue> builder);
}
