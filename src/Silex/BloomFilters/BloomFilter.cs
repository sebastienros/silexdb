using System.Collections;
using System.IO.Hashing;

namespace Silex.BloomFilters;

/// <summary>
/// A Bloom filter is a probabilistic data structure which provides an efficient way to query whether an element is a member of a set.
/// When <see cref="Probe"/> returns false, the value is deemed not to be in the filter. If it returns true it may or may not be in the collection.
/// </summary>
public class BloomFilter : IBloomFilter
{
    private readonly int _k = 5; // Number of hashing iterations
    private readonly int _m = 2048; // Size of the bloom filter in bits

    private readonly BitArray _bits;

    public BloomFilter(int length, double p)
    {
        var n = length;
        _m = (int)CalculateM(n, p);

        // Round to a multiple of 8 (bytes) because BitArray will load these
        _m = (int)Math.Ceiling((double)_m / 8) * 8;

        _k = CalculateK(n, _m);
        _bits = new BitArray(_m);
    }

    public BloomFilter(byte[] span, int k)
    {
        _bits = new BitArray(span);
        _m = _bits.Length;
        _k = k;
    }

    public Span<byte> GetBytes()
    {
        var bytes = new byte[(int)Math.Ceiling((double)_bits.Count / 8)];
        _bits.CopyTo(bytes, 0);
        return bytes;
    }

    public int K => _k;

    public void Add(ReadOnlySpan<byte> item)
    {
        if (_k == 0) return;

        Span<int> positions = stackalloc int[_k];
        ComputeHashPositions(item, positions);
        foreach (var b in positions)
        {
            _bits[b] = true;
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
            if (!_bits[b]) return false;
        }

        return true;
    }

    private void ComputeHashPositions(ReadOnlySpan<byte> item, Span<int> positions)
    {
        // Read-only bloom filter?
        if (_k == 0) return;

        var hash1 = XxHash3.HashToUInt64(item);
        var hash2 = XxHash64.HashToUInt64(item);

        ulong hash = hash1;

        for (var i = 0; i < _k; i++)
        {
            if (i != 0) hash += hash2;
            
            var bit_pos = (int)(hash % (ulong)_m);
            positions[i] = bit_pos;
        }
    }

    public override string ToString()
    {
        char[] result = new char[_m];

        for (var i = 0; i < _m; i++)
        {
            result[i] = _bits[i] ? '1' : '0';
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
