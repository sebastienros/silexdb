namespace Silex.BloomFilters;

public interface IBloomFilter
{
    void Add(ReadOnlySpan<byte> value);
    bool Probe(ReadOnlySpan<byte> item);
    ReadOnlySpan<byte> GetBytes();

    int K { get; }

    int AlgorithmVersion => 0;
}
