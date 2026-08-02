namespace Silex.BloomFilters;

public interface IBloomFilterFactory
{
    IBloomFilter CreateBloomFilter(int n, double p);

    IBloomFilter CreateBloomFilter(ReadOnlySpan<byte> bytes, int k);

    IBloomFilter CreateBloomFilterFromOwnedBytes(byte[] bytes, int k) => CreateBloomFilter(bytes, k);

    IBloomFilter CreateBloomFilterFromOwnedBytes(byte[] bytes, int k, int algorithmVersion);
}
