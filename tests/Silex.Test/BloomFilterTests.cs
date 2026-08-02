using Silex.BloomFilters;
using System.Buffers.Binary;
using System.IO.Hashing;

namespace Silex.Test;

public class BloomFilterTests
{
    [Test]
    [Arguments(1_000, 0.01)]
    [Arguments(1_000, 0.05)]
    [Arguments(1_000, 0.10)]
    [Arguments(1_000, 0.50)]
    [Arguments(10_000, 0.01)]
    [Arguments(10_000, 0.10)]
    public async Task ShouldApproximateProbability(int n, double p)
    {
        var bloom = new BloomFilter(n, p);
        const int sampleCount = 1_000_000;
        var falseNegativeCount = 0;
        var falsePositiveCount = 0;

        {
            Span<byte> buffer = stackalloc byte[sizeof(int)];

            for (var value = 0; value < n; value++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
                bloom.Add(buffer);
            }

            for (var value = 0; value < n; value++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
                if (!bloom.Probe(buffer))
                {
                    falseNegativeCount++;
                }
            }

            // Negative values are disjoint from the inserted [0, n) range.
            for (var value = -sampleCount; value < 0; value++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
                if (bloom.Probe(buffer))
                {
                    falsePositiveCount++;
                }
            }
        }

        await Assert.That(falseNegativeCount).IsEqualTo(0);

        var standardDeviation = Math.Sqrt(p * (1 - p) / sampleCount);
        var observedRate = (double)falsePositiveCount / sampleCount;

        await Assert.That(bloom.AlgorithmVersion).IsEqualTo(BloomFilter.CurrentAlgorithmVersion);
        await Assert.That(bloom.GetBytes().Length % 64).IsEqualTo(0);
        await Assert.That(observedRate).IsLessThanOrEqualTo(p + 6 * standardDeviation);
    }

    [Test]
    public async Task ShouldProbeAddedValuesAfterRoundTrip()
    {
        var bloom = new BloomFilter(1_000, 0.01);

        {
            Span<byte> buffer = stackalloc byte[sizeof(int)];

            for (var value = 0; value < 1_000; value++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
                bloom.Add(buffer);
            }
        }

        var restored = new BloomFilter(bloom.GetBytes().ToArray(), bloom.K, bloom.AlgorithmVersion);
        var falseNegativeCount = 0;

        {
            Span<byte> buffer = stackalloc byte[sizeof(int)];

            for (var value = 0; value < 1_000; value++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
                if (!restored.Probe(buffer))
                {
                    falseNegativeCount++;
                }
            }
        }

        await Assert.That(falseNegativeCount).IsEqualTo(0);
        await Assert.That(restored.AlgorithmVersion).IsEqualTo(BloomFilter.CurrentAlgorithmVersion);
    }

    [Test]
    public async Task ShouldProbeLegacyFilters()
    {
        const int n = 1_000;
        const double p = 0.01;
        var bitCount = (int)BloomFilter.CalculateM(n, p);
        bitCount = (bitCount + 7) & ~7;
        var k = (int)Math.Ceiling(Math.Log(2) * bitCount / n);
        var bytes = new byte[bitCount / 8];

        var falseNegativeCount = 0;
        {
            Span<byte> buffer = stackalloc byte[sizeof(int)];
            for (var value = 0; value < n; value++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
                AddLegacy(bytes, k, buffer);
            }

            var restored = new BloomFilter(bytes, k);
            for (var value = 0; value < n; value++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
                if (!restored.Probe(buffer))
                {
                    falseNegativeCount++;
                }
            }
        }

        await Assert.That(falseNegativeCount).IsEqualTo(0);
    }

    [Test]
    public async Task ShouldProbeUnversionedGlobalFilters()
    {
        const int n = 1_000;
        const double p = 0.01;
        var bitCount = (int)BloomFilter.CalculateM(n, p);
        bitCount = (bitCount + 7) & ~7;
        var k = BloomFilter.CalculateK(n, bitCount);
        var bytes = new byte[bitCount / 8];

        var falseNegativeCount = 0;
        {
            Span<byte> buffer = stackalloc byte[sizeof(int)];
            for (var value = 0; value < n; value++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
                AddGlobal(bytes, k, buffer);
            }

            var restored = new BloomFilter(bytes, k);
            for (var value = 0; value < n; value++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(buffer, value);
                if (!restored.Probe(buffer))
                {
                    falseNegativeCount++;
                }
            }
        }

        await Assert.That(falseNegativeCount).IsEqualTo(0);
    }

    [Test]
    public async Task ShouldKeepEachProbeWithinOneCacheLine()
    {
        var bloom = new BloomFilter(10_000, 0.01);
        bloom.Add("cache-local"u8);

        var firstSetByte = -1;
        var lastSetByte = -1;
        var bytes = bloom.GetBytes();
        for (var i = 0; i < bytes.Length; i++)
        {
            if (bytes[i] == 0)
            {
                continue;
            }

            firstSetByte = firstSetByte < 0 ? i : firstSetByte;
            lastSetByte = i;
        }

        await Assert.That(firstSetByte >= 0).IsTrue();
        await Assert.That(firstSetByte / 64).IsEqualTo(lastSetByte / 64);
    }

    [Test]
    public async Task ProbeShouldNotAllocate()
    {
        var bloom = new BloomFilter(10_000, 0.01);
        bloom.Add("member"u8);
        _ = bloom.Probe("member"u8);
        _ = bloom.Probe("missing"u8);

        bool result;
        long allocated;
        {
            var before = GC.GetAllocatedBytesForCurrentThread();
            result = false;
            for (var i = 0; i < 10_000; i++)
            {
                result ^= bloom.Probe("missing"u8);
            }

            allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        }

        GC.KeepAlive(result);
        await Assert.That(allocated).IsEqualTo(0);
    }

    [Test]
    public async Task ShouldRejectInvalidConstructionParameters()
    {
        await Assert.That(() => new BloomFilter(1_000, double.NaN)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new BloomFilter(500_000_000, 0.01)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new BloomFilter(10_000, 1e-15)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new BloomFilter([], 1)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new BloomFilter(new byte[8], 0)).Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new BloomFilter(new byte[8], 2_049)).Throws<ArgumentOutOfRangeException>();
    }

    [Test]
    public async Task ShouldChooseTheBestIntegerHashCount()
    {
        var bitCount = (int)BloomFilter.CalculateM(1_000, 0.5);
        bitCount = (bitCount + 7) & ~7;

        await Assert.That(BloomFilter.CalculateK(1_000, bitCount)).IsEqualTo(1);
    }

    private static void AddLegacy(byte[] bytes, int k, ReadOnlySpan<byte> item)
    {
        var bitCount = (ulong)bytes.Length * 8;
        var hash = XxHash3.HashToUInt64(item);
        var delta = XxHash64.HashToUInt64(item);

        for (var i = 0; i < k; i++)
        {
            var bit = (int)(hash % bitCount);
            bytes[bit >> 3] |= (byte)(1 << (bit & 7));
            hash += delta;
        }
    }

    private static void AddGlobal(byte[] bytes, int k, ReadOnlySpan<byte> item)
    {
        var bitCount = (ulong)bytes.Length * 8;
        var hash128 = XxHash128.HashToUInt128(item);
        var hash = (ulong)hash128;
        var delta = (ulong)(hash128 >> 64);

        for (var i = 0; i < k; i++)
        {
            var bit = (int)Math.BigMul(hash, bitCount, out _);
            bytes[bit >> 3] |= (byte)(1 << (bit & 7));
            hash += delta;
            delta += (ulong)i;
        }
    }
}
