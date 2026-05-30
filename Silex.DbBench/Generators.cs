using System.Buffers.Binary;

namespace Silex.DbBench;

/// <summary>
/// Produces fixed-width byte keys from an integer, mirroring RocksDB <c>db_bench</c>'s
/// <c>GenerateKeyFromInt</c>: the first <c>min(8, keySize)</c> bytes hold the value big-endian (so
/// lexicographic byte order matches numeric order, which keeps sequential scans meaningful), and any
/// remaining bytes are padded with the ASCII character '0' (0x30).
/// </summary>
internal sealed class KeyGenerator
{
    private readonly int _keySize;
    private readonly int _prefixBytes;

    public KeyGenerator(int keySize)
    {
        _keySize = keySize;
        _prefixBytes = Math.Min(8, keySize);
    }

    /// <summary>
    /// Allocates a fresh key array for <paramref name="value"/>. A new array is required on every call
    /// because Silex takes ownership of the key (zero-copy); a shared/reused buffer would be mutated under
    /// the engine.
    /// </summary>
    public byte[] Generate(long value)
    {
        var key = new byte[_keySize];
        GenerateInto(value, key);
        return key;
    }

    /// <summary>
    /// Writes the key for <paramref name="value"/> into <paramref name="destination"/> without allocating.
    /// Use this only for reads/lookups, where Silex does not take ownership of the key buffer, so a single
    /// per-thread scratch array can be reused across operations.
    /// </summary>
    public void GenerateInto(long value, Span<byte> destination)
    {
        Span<byte> scratch = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(scratch, (ulong)value);
        scratch[(8 - _prefixBytes)..].CopyTo(destination);

        for (var i = _prefixBytes; i < _keySize; i++)
        {
            destination[i] = (byte)'0';
        }
    }
}

/// <summary>
/// Generates value payloads of a fixed size. Like <c>db_bench</c>'s <c>RandomGenerator</c> it precomputes
/// one backing buffer of random bytes and serves successive slices from a rolling offset, but it copies
/// each slice into its own array because Silex takes ownership of stored values.
/// </summary>
internal sealed class ValueGenerator
{
    private readonly byte[] _data;
    private int _offset;

    public ValueGenerator(int seed, int valueSize)
    {
        // A buffer comfortably larger than a single value so successive slices differ.
        var size = Math.Max(valueSize * 16, 1 << 20);
        _data = new byte[size];
        new Random(seed).NextBytes(_data);
    }

    public byte[] Generate(int valueSize)
    {
        if (_offset + valueSize > _data.Length)
        {
            _offset = 0;
        }

        var value = new byte[valueSize];
        _data.AsSpan(_offset, valueSize).CopyTo(value);
        _offset += valueSize;

        return value;
    }
}

/// <summary>
/// Creates well-distributed independent RNG streams. Streams are identified by a <c>stream</c> id so that,
/// for example, a <c>readrandom</c> after a <c>fillrandom</c> draws a *different* key sequence than the
/// fill did (otherwise every read would trivially hit). The seed is mixed with a deterministic finalizer
/// (FNV-1a + SplitMix64) so the same <c>--seed</c> reproduces the same workload across processes — unlike
/// <see cref="HashCode"/>, which is randomized per process — while also avoiding the correlation .NET's
/// legacy <see cref="Random"/> exhibits between nearby integer seeds.
/// </summary>
internal static class RngStreams
{
    public const int Write = 1;
    public const int Read = 2;
    public const int Seek = 3;

    public static Random Create(int seed, int threadId, int stream) =>
        new(DeterministicSeed(seed, threadId, stream));

    private static int DeterministicSeed(int seed, int threadId, int stream)
    {
        // FNV-1a over the three inputs.
        var hash = 0xcbf29ce484222325UL;
        foreach (var value in stackalloc[] { seed, threadId, stream })
        {
            hash = (hash ^ (uint)value) * 0x100000001b3UL;
        }

        // SplitMix64 finalizer to spread the bits.
        hash ^= hash >> 30;
        hash *= 0xbf58476d1ce4e5b9UL;
        hash ^= hash >> 27;
        hash *= 0x94d049bb133111ebUL;
        hash ^= hash >> 31;

        return (int)hash;
    }
}

/// <summary>Collects per-operation latencies (microseconds) and reports percentiles, db_bench style.</summary>
internal sealed class Histogram
{
    private readonly List<double> _micros = new();

    public void Add(double micros) => _micros.Add(micros);

    public void Merge(Histogram other) => _micros.AddRange(other._micros);

    public string Summary()
    {
        if (_micros.Count == 0)
        {
            return string.Empty;
        }

        _micros.Sort();

        return $"  Microseconds per op: avg {Average():F3}  p50 {Percentile(50):F3}  " +
               $"p95 {Percentile(95):F3}  p99 {Percentile(99):F3}  max {_micros[^1]:F3}";
    }

    private double Average()
    {
        double sum = 0;
        foreach (var value in _micros)
        {
            sum += value;
        }

        return sum / _micros.Count;
    }

    private double Percentile(double percentile)
    {
        var rank = (int)Math.Ceiling(percentile / 100.0 * _micros.Count) - 1;
        return _micros[Math.Clamp(rank, 0, _micros.Count - 1)];
    }
}
