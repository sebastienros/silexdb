namespace Silex.BloomFilters;

public class DefaultBloomFilterFactory : IBloomFilterFactory
{
    public IBloomFilter CreateBloomFilter(int n, double p)
    {
        return new BloomFilter(n, p);
    }

    public IBloomFilter CreateBloomFilter(ReadOnlySpan<byte> bytes, int k)
    {
        var bloomFilter = new BloomFilter(bytes.ToArray(), k);
        return bloomFilter;
    }

    public IBloomFilter CreateBloomFilterFromOwnedBytes(byte[] bytes, int k)
    {
        var bloomFilter = new BloomFilter(bytes, k);
        return bloomFilter;
    }

    public IBloomFilter CreateBloomFilterFromOwnedBytes(byte[] bytes, int k, int algorithmVersion)
    {
        var bloomFilter = new BloomFilter(bytes, k, algorithmVersion);
        return bloomFilter;
    }
}
