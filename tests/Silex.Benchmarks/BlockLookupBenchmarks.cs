using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Silex.Blocks;
using Silex.Buffers;

namespace Silex.Benchmarks;

/// <summary>
/// Prototype comparing three ways to do a point lookup inside a single 4 KiB block:
///
///   1. <see cref="TypedLong_Hit"/>      - the current typed path (<c>Block&lt;long,long&gt;</c>): each
///      visited entry's key is decoded to a <see cref="long"/> (allocation-free) and compared numerically.
///   2. <see cref="BytesArray_Hit"/>     - the current byte[] path (<c>Block&lt;byte[],byte[]&gt;</c>): each
///      visited entry's key is decoded via <c>ToArray()</c> (one heap allocation per comparison) and
///      compared with <c>SequenceCompareTo</c>.
///   3. <see cref="SpanCore_Hit"/>       - the proposed span-based byte core: binary search runs directly
///      over the block bytes, reading each entry's key as a <see cref="ReadOnlySpan{T}"/> and comparing
///      with <c>SequenceCompareTo</c>. No per-entry decode, no allocation, no TKey materialization.
///
/// Paths 2 and 3 run against the *same* <c>Block&lt;byte[],byte[]&gt;</c>, so path 3 is a drop-in
/// replacement for path 2. Path 1 is the typed primitive baseline (8-byte numeric keys).
/// </summary>
[MemoryDiagnoser, ShortRunJob]
public class BlockLookupBenchmarks
{
    private const int LookupCount = 1000;

    private DefaultBlockEncoder<long, long> _typedEncoder = null!;
    private DefaultBlockEncoder<byte[], byte[]> _bytesEncoder = null!;
    private Block<long, long> _typedBlock = null!;
    private Block<byte[], byte[]> _bytesBlock = null!;

    private long[] _hitLongs = null!;
    private byte[][] _hitBytes = null!;
    private long[] _missLongs = null!;
    private byte[][] _missBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        _typedEncoder = new DefaultBlockEncoder<long, long>();
        _bytesEncoder = new DefaultBlockEncoder<byte[], byte[]>();

        using var typedBuilder = new BlockBuilder<long, long>(_typedEncoder);
        using var bytesBuilder = new BlockBuilder<byte[], byte[]>(_bytesEncoder);

        var keys = new List<long>();
        var gaps = new List<long>(); // keys that are absent but lie inside the populated range

        var rnd = new Random(42);
        long k = 0;

        while (true)
        {
            var step = 1 + rnd.Next(1, 10);

            // Remember an in-range missing key whenever there is a gap before this key.
            if (keys.Count > 0 && step > 1)
            {
                gaps.Add(k + 1);
            }

            k += step;

            var keyBytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(keyBytes, k);
            var valueBytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(valueBytes, ~k);

            // Both encodings have identical on-disk sizes, so both builders fill at the same rate.
            var okTyped = typedBuilder.Add(k, k);
            var okBytes = bytesBuilder.Add(keyBytes, valueBytes);

            if (!okTyped || !okBytes)
            {
                break;
            }

            keys.Add(k);
        }

        _typedBlock = typedBuilder.BuildBlock();
        _bytesBlock = bytesBuilder.BuildBlock();

        if (_typedBlock.Offsets.Count != _bytesBlock.Offsets.Count)
        {
            throw new InvalidOperationException(
                $"Block entry counts diverged: typed={_typedBlock.Offsets.Count}, bytes={_bytesBlock.Offsets.Count}");
        }

        // Build LookupCount hit queries (keys that exist) and miss queries (in-range absent keys).
        _hitLongs = new long[LookupCount];
        _hitBytes = new byte[LookupCount][];
        _missLongs = new long[LookupCount];
        _missBytes = new byte[LookupCount][];

        for (var i = 0; i < LookupCount; i++)
        {
            var hit = keys[rnd.Next(keys.Count)];
            _hitLongs[i] = hit;
            _hitBytes[i] = ToBigEndian(hit);

            var miss = gaps[rnd.Next(gaps.Count)];
            _missLongs[i] = miss;
            _missBytes[i] = ToBigEndian(miss);
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _typedBlock.Dispose();
        _bytesBlock.Dispose();
    }

    // ---- Hits -------------------------------------------------------------------------------------

    [Benchmark(Baseline = true), BenchmarkCategory("Hit")]
    public int TypedLong_Hit()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            if (_typedBlock.TryGetValue(_hitLongs[i], out _))
            {
                found++;
            }
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Hit")]
    public int BytesArray_Hit()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            if (_bytesBlock.TryGetValue(_hitBytes[i], out _))
            {
                found++;
            }
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Hit")]
    public int SpanCore_Hit()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            if (SpanLookup(_bytesBlock, _hitBytes[i], out _))
            {
                found++;
            }
        }

        return found;
    }

    // ---- Misses (in-range, full-depth search) -----------------------------------------------------

    [Benchmark, BenchmarkCategory("Miss")]
    public int TypedLong_Miss()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            if (_typedBlock.TryGetValue(_missLongs[i], out _))
            {
                found++;
            }
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Miss")]
    public int BytesArray_Miss()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            if (_bytesBlock.TryGetValue(_missBytes[i], out _))
            {
                found++;
            }
        }

        return found;
    }

    [Benchmark, BenchmarkCategory("Miss")]
    public int SpanCore_Miss()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            if (SpanLookup(_bytesBlock, _missBytes[i], out _))
            {
                found++;
            }
        }

        return found;
    }

    /// <summary>
    /// Binary search over the raw block bytes. Mirrors <c>Block&lt;TKey,TValue&gt;.TryGetValue</c> exactly,
    /// but reads each entry's key as a span and compares it directly instead of materializing a TKey.
    /// </summary>
    private static bool SpanLookup(Block<byte[], byte[]> block, ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        var memory = block.Memory;
        var offsets = block.Offsets;

        var start = 0;
        var end = offsets.Count - 1;

        while (start <= end)
        {
            var m = start + (end - start) / 2;

            var reader = new EncoderBinaryReader(memory, offsets[m]);
            var keyLength = reader.Read7BitEncodedInt();
            var entryKey = reader.ReadBytesSpan(keyLength);

            var cmp = key.SequenceCompareTo(entryKey);

            if (cmp == 0)
            {
                var valueLength = reader.Read7BitEncodedInt();
                value = reader.ReadBytesSpan(valueLength);
                return true;
            }

            if (cmp > 0)
            {
                start = m + 1;
            }
            else
            {
                end = m - 1;
            }
        }

        value = default;
        return false;
    }

    private static byte[] ToBigEndian(long value)
    {
        var bytes = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(bytes, value);
        return bytes;
    }
}
