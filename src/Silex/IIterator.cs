namespace Silex;

public interface IIterator
{
    IEnumerable<KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>> Scan(ReadOnlyMemory<byte>? minValue = null, ReadOnlyMemory<byte>? maxValue = null);
}
