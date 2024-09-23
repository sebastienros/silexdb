namespace Silex;

public interface IIterator
{
    IEnumerable<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> Scan();
}