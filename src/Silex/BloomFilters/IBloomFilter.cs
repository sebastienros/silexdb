namespace Silex.BloomFilters;

public interface IBloomFilter
{
    void Add(ReadOnlySpan<byte> value);
    bool Probe(ReadOnlySpan<byte> item);
    Span<byte> GetBytes();

    int K { get; }
}
