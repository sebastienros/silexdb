using System.IO.Hashing;
using System.Runtime.InteropServices;

namespace Silex.BloomFilters;

/// <summary>
/// A Bloom filter is a probabilistic data structure which provides an efficient way to query whether an element is a member of a set.
/// When <see cref="Probe"/> returns false, the value is deemed not to be in the filter. If it returns true it may or may not be in the collection.
/// </summary>
public class BloomFilter : IBloomFilter
{
    private const int CacheLineBytes = 64;
    private const int CacheLineBits = CacheLineBytes * 8;
    private const int MaxHashFunctions = 2_048;
    private const int MaxLocalHashFunctions = 64;
    private const int MaxByteLength = int.MaxValue / 8;

    public const int GlobalAlgorithmVersion = 1;
    public const int CurrentAlgorithmVersion = 2;

    private readonly int _k; // Number of hashing iterations
    private readonly int _m; // Size of the bloom filter in bits
    private readonly Algorithm _algorithm;
    private readonly int _offset;
    private readonly int _length;

    // The bits are stored LSB-first per byte (bit i lives in _bytes[i >> 3] at mask 1 << (i & 7)).
    // This matches the legacy BitArray byte layout so persisted filters remain readable.
    private readonly byte[] _bytes;

    public BloomFilter(int length, double p)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(length);

        if (!double.IsFinite(p) || p is <= 0 or >= 1)
        {
            throw new ArgumentOutOfRangeException(nameof(p), p, "The false positive probability must be in the range (0, 1).");
        }

        var (byteLength, hashFunctions) = CalculateLocalConfiguration(length, p);
        _bytes = AllocateCacheLineAligned(byteLength, out _offset);
        _length = byteLength;
        _m = checked(byteLength * 8);
        _k = hashFunctions;
        _algorithm = Algorithm.CacheLocal;
    }

    public BloomFilter(byte[] bytes, int k)
        : this(bytes, k, algorithmVersion: 0)
    {
    }

    public BloomFilter(byte[] bytes, int k, int algorithmVersion)
    {
        ArgumentNullException.ThrowIfNull(bytes);

        if (bytes.Length == 0 || bytes.Length > MaxByteLength)
        {
            throw new ArgumentOutOfRangeException(nameof(bytes), bytes.Length, "The Bloom filter must contain a supported, non-zero number of bytes.");
        }

        if (k is <= 0 or > MaxHashFunctions)
        {
            throw new ArgumentOutOfRangeException(nameof(k), k, $"The hash count must be in the range [1, {MaxHashFunctions}].");
        }

        if (algorithmVersion is < 0 or > CurrentAlgorithmVersion)
        {
            throw new ArgumentOutOfRangeException(nameof(algorithmVersion), algorithmVersion, "The Bloom filter algorithm version is not supported.");
        }

        if (algorithmVersion == CurrentAlgorithmVersion && bytes.Length % CacheLineBytes != 0)
        {
            throw new ArgumentException($"Version {CurrentAlgorithmVersion} Bloom filters must contain whole cache-line blocks.", nameof(bytes));
        }

        if (algorithmVersion == CurrentAlgorithmVersion)
        {
            _bytes = AllocateCacheLineAligned(bytes.Length, out _offset);
            bytes.CopyTo(_bytes.AsSpan(_offset, bytes.Length));
        }
        else
        {
            _bytes = bytes;
        }

        _length = bytes.Length;
        _m = checked(_length * 8);
        _k = k;
        _algorithm = (Algorithm)algorithmVersion;
    }

    public ReadOnlySpan<byte> GetBytes() => _bytes.AsSpan(_offset, _length);

    public int K => _k;

    public int AlgorithmVersion => (int)_algorithm;

    public void Add(ReadOnlySpan<byte> item)
    {
        switch (_algorithm)
        {
            case Algorithm.CacheLocal:
                AddCacheLocal(item);
                break;
            case Algorithm.Global:
                AddGlobal(item);
                break;
            default:
                // Unversioned filters can have either historical layout. Updating all compatible
                // layouts preserves Add/Probe semantics without guessing which one was persisted.
                if (SupportsCacheLocal)
                {
                    AddCacheLocal(item);
                }

                AddGlobal(item);
                AddLegacy(item);
                break;
        }
    }

    public bool Probe(ReadOnlySpan<byte> item)
    {
        return _algorithm switch
        {
            Algorithm.CacheLocal => ProbeCacheLocal(item),
            Algorithm.Global => ProbeGlobal(item),
            _ => (SupportsCacheLocal && ProbeCacheLocal(item)) || ProbeGlobal(item) || ProbeLegacy(item)
        };
    }

    private bool SupportsCacheLocal => _length % CacheLineBytes == 0;

    private void AddCacheLocal(ReadOnlySpan<byte> item)
    {
        var hash = XxHash3.HashToUInt64(item);
        var blockHash = (uint)hash;
        var probeHash = (uint)(hash >> 32);
        var block = _offset + (int)(((ulong)blockHash * (uint)(_length / CacheLineBytes)) >> 32) * CacheLineBytes;

        for (var i = 0; i < _k; i++)
        {
            var bit = (int)(probeHash >> 23);
            _bytes[block + (bit >> 3)] |= (byte)(1 << (bit & 7));
            probeHash *= 0x9e3779b9;
        }
    }

    private bool ProbeCacheLocal(ReadOnlySpan<byte> item)
    {
        var hash = XxHash3.HashToUInt64(item);
        var blockHash = (uint)hash;
        var probeHash = (uint)(hash >> 32);
        var block = _offset + (int)(((ulong)blockHash * (uint)(_length / CacheLineBytes)) >> 32) * CacheLineBytes;

        for (var i = 0; i < _k; i++)
        {
            var bit = (int)(probeHash >> 23);
            if ((_bytes[block + (bit >> 3)] & (1 << (bit & 7))) == 0)
            {
                return false;
            }

            probeHash *= 0x9e3779b9;
        }

        return true;
    }

    private void AddGlobal(ReadOnlySpan<byte> item)
    {
        var hash128 = XxHash128.HashToUInt128(item);
        var a = (ulong)hash128;
        var delta = (ulong)(hash128 >> 64);
        var m = (ulong)_m;

        for (var i = 0; i < _k; i++)
        {
            var bit = (int)Math.BigMul(a, m, out _);
            _bytes[_offset + (bit >> 3)] |= (byte)(1 << (bit & 7));
            a += delta;
            delta += (ulong)i;
        }
    }

    private bool ProbeGlobal(ReadOnlySpan<byte> item)
    {
        var hash128 = XxHash128.HashToUInt128(item);
        var a = (ulong)hash128;
        var delta = (ulong)(hash128 >> 64);
        var m = (ulong)_m;

        for (var i = 0; i < _k; i++)
        {
            var bit = (int)Math.BigMul(a, m, out _);
            if ((_bytes[_offset + (bit >> 3)] & (1 << (bit & 7))) == 0)
            {
                return false;
            }

            a += delta;
            delta += (ulong)i;
        }

        return true;
    }

    private void AddLegacy(ReadOnlySpan<byte> item)
    {
        var hash = XxHash3.HashToUInt64(item);
        var delta = XxHash64.HashToUInt64(item);

        for (var i = 0; i < _k; i++)
        {
            var bit = (int)(hash % (ulong)_m);
            _bytes[_offset + (bit >> 3)] |= (byte)(1 << (bit & 7));
            hash += delta;
        }
    }

    private bool ProbeLegacy(ReadOnlySpan<byte> item)
    {
        var hash = XxHash3.HashToUInt64(item);
        var delta = XxHash64.HashToUInt64(item);

        for (var i = 0; i < _k; i++)
        {
            var bit = (int)(hash % (ulong)_m);
            if ((_bytes[_offset + (bit >> 3)] & (1 << (bit & 7))) == 0)
            {
                return false;
            }

            hash += delta;
        }

        return true;
    }

    public override string ToString()
    {
        char[] result = new char[_m];

        for (var i = 0; i < _m; i++)
        {
            result[i] = (_bytes[_offset + (i >> 3)] & (1 << (i & 7))) != 0 ? '1' : '0';
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
        return checked((long)Math.Ceiling(-(n * Math.Log(p)) / (Math.Log(2) * Math.Log(2))));
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
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(n);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(m);

        var exact = Math.Log(2) * m / n;
        var lower = Math.Max(1, (int)Math.Floor(exact));
        var upper = Math.Max(1, (int)Math.Ceiling(exact));
        return CalculateP(lower, m, n) <= CalculateP(upper, m, n) ? lower : upper;
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

    private static (int ByteLength, int HashFunctions) CalculateLocalConfiguration(int n, double p)
    {
        var classicBits = CalculateM(n, p);
        if (Math.Log(2) * classicBits / n < 1)
        {
            // Integer Bloom filters cannot use a fractional probe. For high requested
            // probabilities, size directly for the one-probe case instead.
            classicBits = Math.Max(classicBits, checked((long)Math.Ceiling(n / -Math.Log(1 - p))));
        }

        var blocks = Math.Max(1L, checked((classicBits + CacheLineBits - 1) / CacheLineBits));
        var maxBlocks = MaxByteLength / CacheLineBytes;
        var target = p - Math.Min(p * 0.05, 0.001);

        for (var attempt = 0; attempt < 64 && blocks <= maxBlocks; attempt++)
        {
            var lambda = n / (double)blocks;
            var bestProbability = double.PositiveInfinity;
            var bestK = 1;

            for (var k = 1; k <= MaxLocalHashFunctions; k++)
            {
                var probability = EstimateLocalFalsePositive(n, blocks, lambda, k);
                if (probability < bestProbability)
                {
                    bestProbability = probability;
                    bestK = k;
                }

                if (probability <= target)
                {
                    return (checked((int)blocks * CacheLineBytes), k);
                }
            }

            var growth = Math.Clamp(Math.Pow(bestProbability / target, 1d / bestK) * 1.05, 1.05, 2);
            var nextBlocks = Math.Max(blocks + 1, checked((long)Math.Ceiling(blocks * growth)));
            blocks = nextBlocks;
        }

        throw new ArgumentOutOfRangeException(nameof(p), p, "The requested false positive probability requires a Bloom filter larger than the supported maximum.");
    }

    private static double EstimateLocalFalsePositive(int n, long blocks, double lambda, int k)
    {
        double localProbability;
        if (blocks == 1)
        {
            localProbability = ConditionalLocalFalsePositive(n, k);
        }
        else
        {
            var mode = (int)Math.Floor(lambda);
            var probabilitySum = ConditionalLocalFalsePositive(mode, k);
            var weightSum = 1d;
            var weight = 1d;

            for (var count = mode - 1; count >= 0; count--)
            {
                weight *= (count + 1) / lambda;
                probabilitySum += weight * ConditionalLocalFalsePositive(count, k);
                weightSum += weight;

                if (weight < weightSum * 1e-15)
                {
                    break;
                }
            }

            weight = 1d;
            var upperLimit = mode + Math.Max(32, (int)Math.Ceiling(16 * Math.Sqrt(lambda + 1)));
            for (var count = mode + 1; count <= upperLimit; count++)
            {
                weight *= lambda / count;
                probabilitySum += weight * ConditionalLocalFalsePositive(count, k);
                weightSum += weight;

                if (count > mode + 8 && weight < weightSum * 1e-15)
                {
                    break;
                }
            }

            localProbability = probabilitySum / weightSum;
        }

        var fingerprintRatio = n / (blocks * 4_294_967_296d);
        var fingerprintProbability = fingerprintRatio < 1e-5
            ? fingerprintRatio - fingerprintRatio * fingerprintRatio / 2
            : 1 - Math.Exp(-fingerprintRatio);

        return localProbability + fingerprintProbability - localProbability * fingerprintProbability;
    }

    private static double ConditionalLocalFalsePositive(int entriesInBlock, int k)
    {
        if (entriesInBlock == 0)
        {
            return 0;
        }

        var occupied = 1 - Math.Exp(-(double)k * entriesInBlock / CacheLineBits);
        return Math.Pow(occupied, k);
    }

    private static byte[] AllocateCacheLineAligned(int byteLength, out int offset)
    {
        var bytes = GC.AllocateArray<byte>(checked(byteLength + CacheLineBytes - 1), pinned: true);
        var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
        try
        {
            var address = handle.AddrOfPinnedObject().ToInt64();
            offset = (int)((CacheLineBytes - (address & (CacheLineBytes - 1))) & (CacheLineBytes - 1));
        }
        finally
        {
            handle.Free();
        }

        return bytes;
    }

    private enum Algorithm
    {
        Compatibility = 0,
        Global = GlobalAlgorithmVersion,
        CacheLocal = CurrentAlgorithmVersion
    }
}
