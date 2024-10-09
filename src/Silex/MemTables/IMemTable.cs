using System.Buffers;

namespace Silex.MemTables;

public interface IMemTable
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
    bool TryGet(Bytes key, out Bytes result);

    void Put(Bytes key, Bytes value);

    IStorageIterator CreateIterator();
}
