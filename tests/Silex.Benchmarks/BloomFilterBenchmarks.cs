using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Silex.BloomFilters;

[MemoryDiagnoser]
public class BloomFilterBenchmarks
{
    private const double P = 0.01;

    private BloomFilter _filter = null!;
    private byte[] _members = null!;
    private byte[] _misses = null!;

    [Params(10_000, 1_000_000)]
    public int Count { get; set; }

    [Params(BloomFilter.CacheLocalAlgorithmVersion, BloomFilter.CurrentAlgorithmVersion)]
    public int Algorithm { get; set; }

    [Params(4, 16)]
    public int KeyLength { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _filter = new BloomFilter(Count, P, Algorithm);

        _members = new byte[Count * KeyLength];
        _misses = new byte[Count * KeyLength];

        for (var i = 0; i < Count; i++)
        {
            var member = _members.AsSpan(i * KeyLength, KeyLength);
            BinaryPrimitives.WriteInt32LittleEndian(member, i);
            _filter.Add(member);

            var miss = _misses.AsSpan(i * KeyLength, KeyLength);
            BinaryPrimitives.WriteInt32LittleEndian(miss, Count + 1 + i);
        }
    }

    [Benchmark]
    public void Add()
    {
        var filter = new BloomFilter(Count, P, Algorithm);
        for (var i = 0; i < Count; i++)
        {
            filter.Add(_members.AsSpan(i * KeyLength, KeyLength));
        }
    }

    [Benchmark]
    public int ProbeHit()
    {
        var count = 0;
        for (var i = 0; i < Count; i++)
        {
            if (_filter.Probe(_members.AsSpan(i * KeyLength, KeyLength)))
            {
                count++;
            }
        }

        return count;
    }

    [Benchmark]
    public int ProbeMiss()
    {
        var count = 0;
        for (var i = 0; i < Count; i++)
        {
            if (_filter.Probe(_misses.AsSpan(i * KeyLength, KeyLength)))
            {
                count++;
            }
        }

        return count;
    }
}
