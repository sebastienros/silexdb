using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Silex.BloomFilters;

[MemoryDiagnoser]
public class BloomFilterBenchmarks
{
    private const int N = 10_000;
    private const double P = 0.01;

    private BloomFilter _filter = null!;
    private byte[][] _members = null!;
    private byte[][] _misses = null!;

    [GlobalSetup]
    public void Setup()
    {
        _filter = new BloomFilter(N, P);

        _members = new byte[N][];
        _misses = new byte[N][];

        for (var i = 0; i < N; i++)
        {
            var member = new byte[sizeof(int)];
            BinaryPrimitives.WriteInt32LittleEndian(member, i);
            _members[i] = member;
            _filter.Add(member);

            var miss = new byte[sizeof(int)];
            BinaryPrimitives.WriteInt32LittleEndian(miss, N + 1 + i);
            _misses[i] = miss;
        }
    }

    [Benchmark]
    public void Add()
    {
        var filter = new BloomFilter(N, P);
        for (var i = 0; i < N; i++)
        {
            filter.Add(_members[i]);
        }
    }

    [Benchmark]
    public int ProbeHit()
    {
        var count = 0;
        for (var i = 0; i < N; i++)
        {
            if (_filter.Probe(_members[i]))
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
        for (var i = 0; i < N; i++)
        {
            if (_filter.Probe(_misses[i]))
            {
                count++;
            }
        }

        return count;
    }
}
