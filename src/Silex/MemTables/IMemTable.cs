using System.Buffers;

namespace Silex.MemTables;

public interface IMemTable
{
    /// <summary>
    /// Gets the size of the <see cref="MemTable"/>.
    /// </summary>
    long Size { get; }

    /// <summary>
    /// Gets the value associated with the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <returns><c>true</c> if the key was found, <c>false</c> otherwise.</returns>
    bool TryGet(ReadOnlyMemory<byte> key, out ReadOnlyMemory<byte> result);

    /// <summary>
    /// Puts a value with the specified key. If one already exists it is replaced.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="memoryOwner"></param>
    void Put(ReadOnlyMemory<byte> key, IMemoryOwner<byte> memoryOwner, int bufferSize);

    IStorageIterator CreateIterator();
}
