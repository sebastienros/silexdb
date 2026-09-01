using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Silex.Blocks;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class BlockLookupBenchmarks
{
    private const int LookupCount = 1_000;

    private Block _block = null!;
    private ByteSlice[] _hitSlices = null!;
    private byte[][] _hitBytes = null!;
    private ByteSlice[] _missSlices = null!;
    private byte[][] _missBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        using var builder = new BlockBuilder(new DefaultBlockEncoder());
        var keys = new List<long>();
        var gaps = new List<long>();
        var random = new Random(42);
        long key = 0;

        while (true)
        {
            var step = 1 + random.Next(1, 10);
            if (keys.Count > 0 && step > 1)
            {
                gaps.Add(key + 1);
            }

            key += step;
            var keyBytes = ToBigEndian(key);
            var valueBytes = ToBigEndian(~key);

            if (!builder.Add(ByteSlice.FromMemory(keyBytes), ByteSlice.FromMemory(valueBytes)))
            {
                break;
            }

            keys.Add(key);
        }

        _block = builder.BuildBlock();
        _hitSlices = new ByteSlice[LookupCount];
        _hitBytes = new byte[LookupCount][];
        _missSlices = new ByteSlice[LookupCount];
        _missBytes = new byte[LookupCount][];

        for (var i = 0; i < LookupCount; i++)
        {
            var hitBytes = ToBigEndian(keys[random.Next(keys.Count)]);
            var missBytes = ToBigEndian(gaps[random.Next(gaps.Count)]);
            _hitBytes[i] = hitBytes;
            _hitSlices[i] = ByteSlice.FromMemory(hitBytes);
            _missBytes[i] = missBytes;
            _missSlices[i] = ByteSlice.FromMemory(missBytes);
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _block.Dispose();

    [Benchmark(Baseline = true), BenchmarkCategory("Hit")]
    public int ByteSliceHit()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _block.TryGetValue(_hitSlices[i], out _) ? 1 : 0;
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Hit")]
    public int EncodedSpanHit()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _block.TryGetValue(_hitBytes[i], out _) ? 1 : 0;
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Miss")]
    public int ByteSliceMiss()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _block.TryGetValue(_missSlices[i], out _) ? 1 : 0;
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Miss")]
    public int EncodedSpanMiss()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _block.TryGetValue(_missBytes[i], out _) ? 1 : 0;
        }

        return found;
    }

    private static byte[] ToBigEndian(long value)
    {
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        return bytes;
    }
}
