using BenchmarkDotNet.Attributes;
using Silex.Blocks;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

/// <summary>
/// In-block point lookups. <see cref="BlockCount"/> = 1 searches one block that stays hot in the CPU caches;
/// a large count spreads the lookups over enough blocks (about 256 MiB at 4 KiB) that every search starts on
/// a cold block, as in a block cache much larger than the CPU caches.
/// </summary>
[MemoryDiagnoser, ShortRunJob]
public class BlockLookupBenchmarks
{
    private const int LookupCount = 1_000;

    public enum KeyShape
    {
        /// <summary>8-byte big-endian integers: the first 8 bytes distinguish every key.</summary>
        Distinct8,

        /// <summary>16-byte keys sharing their first 8 bytes, then a big-endian integer.</summary>
        SharedPrefix16,
    }

    private Block[] _blocks = null!;
    private ByteSlice[] _hitSlices = null!;
    private byte[][] _hitBytes = null!;
    private int[] _hitBlocks = null!;
    private ByteSlice[] _missSlices = null!;
    private byte[][] _missBytes = null!;
    private int[] _missBlocks = null!;

    [Params(KeyShape.Distinct8, KeyShape.SharedPrefix16)]
    public KeyShape Keys { get; set; }

    [Params(false, true)]
    public bool KeyWindows { get; set; }

    [Params(1, 65_536)]
    public int BlockCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var encoder = new DefaultBlockEncoder(4096, KeyWindows);
        using var builder = new BlockBuilder(encoder);
        var random = new Random(42);
        var keys = new List<long>[BlockCount];
        var gaps = new List<long>[BlockCount];
        _blocks = new Block[BlockCount];
        long key = 0;

        for (var b = 0; b < BlockCount; b++)
        {
            builder.Clear();
            keys[b] = [];
            gaps[b] = [];

            while (true)
            {
                var step = 1 + random.Next(1, 10);
                var candidate = key + step;
                if (!builder.Add(ByteSlice.FromMemory(ToKey(candidate)), ByteSlice.FromMemory(ToBigEndian(~candidate))))
                {
                    break;
                }

                if (keys[b].Count > 0)
                {
                    gaps[b].Add(key + 1);
                }

                key = candidate;
                keys[b].Add(key);
            }

            _blocks[b] = builder.BuildBlock();
        }

        _hitSlices = new ByteSlice[LookupCount];
        _hitBytes = new byte[LookupCount][];
        _hitBlocks = new int[LookupCount];
        _missSlices = new ByteSlice[LookupCount];
        _missBytes = new byte[LookupCount][];
        _missBlocks = new int[LookupCount];

        for (var i = 0; i < LookupCount; i++)
        {
            var hitBlock = random.Next(BlockCount);
            var missBlock = random.Next(BlockCount);
            var hitBytes = ToKey(keys[hitBlock][random.Next(keys[hitBlock].Count)]);
            var missBytes = ToKey(gaps[missBlock][random.Next(gaps[missBlock].Count)]);
            _hitBlocks[i] = hitBlock;
            _hitBytes[i] = hitBytes;
            _hitSlices[i] = ByteSlice.FromMemory(hitBytes);
            _missBlocks[i] = missBlock;
            _missBytes[i] = missBytes;
            _missSlices[i] = ByteSlice.FromMemory(missBytes);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        foreach (var block in _blocks)
        {
            block.Dispose();
        }
    }

    [Benchmark(Baseline = true), BenchmarkCategory("Hit")]
    public int ByteSliceHit()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _blocks[_hitBlocks[i]].TryGetValue(_hitSlices[i], out _) ? 1 : 0;
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Hit")]
    public int EncodedSpanHit()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _blocks[_hitBlocks[i]].TryGetValue(_hitBytes[i], out _) ? 1 : 0;
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Miss")]
    public int ByteSliceMiss()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _blocks[_missBlocks[i]].TryGetValue(_missSlices[i], out _) ? 1 : 0;
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Miss")]
    public int EncodedSpanMiss()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _blocks[_missBlocks[i]].TryGetValue(_missBytes[i], out _) ? 1 : 0;
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Seek")]
    public int EncodedSpanLowerBound()
    {
        var sum = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            sum += _blocks[_missBlocks[i]].LowerBound(_missBytes[i]);
        }

        return sum;
    }

    private byte[] ToKey(long value)
    {
        if (Keys == KeyShape.Distinct8)
        {
            return ToBigEndian(value);
        }

        var bytes = new byte[16];
        "tenant42"u8.CopyTo(bytes);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(8), value);
        return bytes;
    }

    private static byte[] ToBigEndian(long value)
    {
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        return bytes;
    }
}
