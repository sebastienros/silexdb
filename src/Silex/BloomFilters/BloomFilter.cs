using System.IO.Hashing;

namespace Silex.BloomFilters;

/// <summary>
/// A Bloom filter is a probabilistic data structure which provides an efficient way to query whether an element is a member of a set.
/// When <see cref="Probe"/> returns false, the value is deemed not to be in the filter. If it returns true it may or may not be in the collection.
/// </summary>
public class BloomFilter : IBloomFilter
{
    private readonly int _k; // Number of hashing iterations
    private readonly int _m; // Size of the bloom filter in bits

    // The bits are stored LSB-first per byte (bit i lives in _bytes[i >> 3] at mask 1 << (i & 7)).
    // This matches the legacy BitArray byte layout so persisted filters remain readable.
    private readonly byte[] _bytes;

    public BloomFilter(int length, double p)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(length);

        if (p is <= 0 or >= 1)
        {
            throw new ArgumentOutOfRangeException(nameof(p), p, "The false positive probability must be in the range (0, 1).");
        }

        var n = length;
        _m = (int)CalculateM(n, p);

        // Round up to a multiple of 8 (bytes) since the filter is stored as a byte array.
        _m = (int)Math.Ceiling((double)_m / 8) * 8;

        // Clamp to at least one hash function; CalculateK can return 0 for degenerate ratios.
        _k = Math.Max(1, CalculateK(n, _m));
        _bytes = new byte[_m / 8];
    }

    public BloomFilter(byte[] span, int k)
    {
        _bytes = span;
        _m = _bytes.Length * 8;
        _k = k;
    }

    public ReadOnlySpan<byte> GetBytes() => _bytes;

    public int K => _k;

    public void Add(ReadOnlySpan<byte> item)
    {
        if (_k == 0) return;

        Span<int> positions = stackalloc int[_k];
        ComputeHashPositions(item, positions);

        // Not thread-safe for concurrent writers: Bloom filters are built single-threaded.
        foreach (var b in positions)
        {
            _bytes[b >> 3] |= (byte)(1 << (b & 7));
        }
    }

    public bool Probe(ReadOnlySpan<byte> item)
    {
        if (_k == 0) return true;

        // A per-call buffer keeps Probe thread-safe, as multiple reads can run concurrently.
        Span<int> positions = stackalloc int[_k];
        ComputeHashPositions(item, positions);
        foreach (var b in positions)
        {
            if ((_bytes[b >> 3] & (1 << (b & 7))) == 0) return false;
        }

        return true;
    }

    private void ComputeHashPositions(ReadOnlySpan<byte> item, Span<int> positions)
    {
        // A single 128-bit hash pass yields both base hashes used for double hashing.
        var hash128 = XxHash128.HashToUInt128(item);
        var a = (ulong)hash128;
        var delta = (ulong)(hash128 >> 64);

        var m = (ulong)_m;

        for (var i = 0; i < _k; i++)
        {
            // Lemire multiply-shift reduction maps the hash uniformly into [0, _m) without a division.
            positions[i] = (int)Math.BigMul(a, m, out _);

            // Enhanced double hashing (Dillinger & Manolios): the quadratic delta breaks up
            // degenerate strides that plain h1 + i*h2 produces when h2 shares a factor with _m.
            a += delta;
            delta += (ulong)i;
        }
    }

    public override string ToString()
    {
        char[] result = new char[_m];

        for (var i = 0; i < _m; i++)
        {
            result[i] = (_bytes[i >> 3] & (1 << (i & 7))) != 0 ? '1' : '0';
        }

        return String.Concat($"m: {_m}, k: {_k} bloom: ", new string(result));
    }

    /// <summary>
    /// Calculates the optimal size of the bloom filter in bits given the number of expected elements
    /// and the probability of the tolerable false positive probability.
    /// </summary>
    /// <param name="n">Expected number of elements inserted.</param>
    /// <param name="p">Tolerable false positive rate.</param>
    /// <returns>The optimal size of the bloom filter in bits</returns>
    internal static long CalculateM(long n, double p)
    {
        return (long)Math.Ceiling(-1 * (n * Math.Log(p)) / Math.Pow(Math.Log(2), 2));
    }

    /// <summary>
    /// Calculates the optimal number of hash function hash given the number of expected elements 
    /// and the size of the bloom filter in bits.
    /// </summary>
    /// <param name="n">Expected number of elements inserted in the bloom filter.</param>
    /// <param name="m">The size of the bloom filter in bits.</param>
    /// <returns>The optimal number of hash functions hashes.</returns>
    internal static int CalculateK(long n, long m)
    {
        return (int)Math.Ceiling((Math.Log(2) * m) / n);
    }

    /// <summary>
    /// Calculates the amount of elements a Bloom filter for which the given configuration of size and hashes is optimal.
    /// </summary>
    /// <param name="k">Number of hashes.</param>
    /// <param name="m">The size of the bloom filter in bits.</param>
    /// <returns>Number of elements for which the given configuration of size and hashes is optimal.</returns>
    internal static long CalculateN(int k, long m)
    {
        return (long)Math.Ceiling((Math.Log(2) * m) / k);
    }

    /// <summary>
    /// Calculates the best-case (uniform hash function) false positive probability.
    /// </summary>
    /// <param name="k">The number of hashes.</param>
    /// <param name="m">The size of the bloom filter in bits.</param>
    /// <param name="n">The number of elements inserted in the filter.</param>
    /// <returns>The calculated false positive probability.</returns>
    internal static double CalculateP(int k, long m, double n)
    {
        return Math.Pow((1 - Math.Exp(-k * n / m)), k);
    }

}
