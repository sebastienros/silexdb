namespace Silex.MemTables;

using Silex.Tables;
using System.Diagnostics.CodeAnalysis;

public interface IMemTable<TKey, TValue> : IDisposable where TKey : notnull
{
    long Id { get; }

    /// <summary>
    /// Gets the size of the <see cref="MemTable"/>.
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
