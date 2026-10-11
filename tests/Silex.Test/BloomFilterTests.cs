using Silex.BloomFilters;
using System.Buffers.Binary;
using System.IO.Hashing;

namespace Silex.Test;

public class BloomFilterTests
{
    [Test]
    [Arguments(1, 0.01)]
    [Arguments(17, 0.01)]
    [Arguments(8192, 0.01)]
    [Arguments(10_000, 0.0001)]
    [Arguments(10_000, 0.5)]
    public async Task SplitBlockVectorAndScalarMatchAndMeetTheRequestedProbability(int n, double p)
    {
        var bloom = new BloomFilter(n, p);
        var key = new byte[16];
        for (var i = 0; i < n; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            bloom.Add(key);
        }
        var restored = new BloomFilter(bloom.GetBytes().ToArray(), bloom.K, bloom.AlgorithmVersion);
        var positives = 0;
        const int samples = 1_000_000;
        for (var i = 0; i < n + samples; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            var vector = restored.Probe(key);
            var scalar = restored.ProbeScalar(key);
            if (vector != scalar || i < n && !vector)
                throw new InvalidOperationException("Scalar, SIMD or restored filter disagreed.");
            if (i >= n && vector) positives++;
        }
        var threshold = p + 6 * Math.Sqrt(p * (1 - p) / samples);
        await Assert.That((double)positives / samples).IsLessThanOrEqualTo(threshold);
    }

    [Test]
    public async Task SplitBlockBytesMatchScalarLittleEndianEncoding()
    {
        var bloom = new BloomFilter(1000, 0.01);
        var expected = new byte[bloom.GetBytes().Length];
        uint[] salts = [0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d,
            0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31];
        var key = new byte[16];
        for (var i = 0; i < 1000; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            bloom.Add(key);
            var hash = XxHash3.HashToUInt64(key);
            var offset = (int)(((ulong)(uint)hash * (uint)(expected.Length / 32)) >> 32) * 32;
            for (var lane = 0; lane < salts.Length; lane++)
            {
                var word = expected.AsSpan(offset + lane * sizeof(uint), sizeof(uint));
                var bit = 1u << (int)(unchecked((uint)(hash >> 32) * salts[lane]) >> 27);
                BinaryPrimitives.WriteUInt32LittleEndian(word, BinaryPrimitives.ReadUInt32LittleEndian(word) | bit);
            }
        }
        await Assert.That(bloom.GetBytes().SequenceEqual(expected)).IsTrue();
    }

    [Test]
    public async Task PersistedCacheLocalFiltersRetainTheirOriginalAlgorithm()
    {
        var previous = new BloomFilter(1000, 0.01, BloomFilter.CacheLocalAlgorithmVersion);
        previous.Add("member"u8);
        var restored = new BloomFilter(previous.GetBytes().ToArray(), previous.K, previous.AlgorithmVersion);
        restored.Add("second"u8);
        await Assert.That(restored.AlgorithmVersion).IsEqualTo(BloomFilter.CacheLocalAlgorithmVersion);
        await Assert.That(restored.Probe("member"u8)).IsTrue();
        await Assert.That(restored.Probe("second"u8)).IsTrue();
    }

    [Test]
    public async Task UnversionedSplitBlockRoundTripDoesNotIntroduceFalseNegatives()
    {
        var filter = new BloomFilter(1000, 0.01);
        var key = new byte[16];
        for (var i = 0; i < 1000; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            filter.Add(key);
        }
        var restored = new BloomFilter(filter.GetBytes().ToArray(), filter.K);
        restored.Add("later"u8);
        for (var i = 0; i < 1000; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(key, i);
            if (!restored.Probe(key)) throw new InvalidOperationException("Unversioned filter rejected a member.");
        }
        await Assert.That(restored.Probe("later"u8)).IsTrue();
    }

    [Test]
    public async Task WarmSplitBlockAddsAndProbesDoNotAllocate()
    {
        var bloom = new BloomFilter(1000, 0.01);
        for (var i = 0; i < 32; i++)
        {
            bloom.Add("member"u8);
            bloom.Probe("member"u8);
            bloom.Probe("missing"u8);
        }
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            bloom.Add("member"u8);
            bloom.Probe("member"u8);
            bloom.Probe("missing"u8);
        }
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);
    }

    [Test]
    public async Task SplitBlockRejectsIncorrectWordCountsAndProbeCounts()
    {
        await Assert.That(() => new BloomFilter(new byte[31], 8, BloomFilter.CurrentAlgorithmVersion))
            .Throws<ArgumentException>();
        await Assert.That(() => new BloomFilter(new byte[32], 7, BloomFilter.CurrentAlgorithmVersion))
            .Throws<ArgumentOutOfRangeException>();
        await Assert.That(() => new BloomFilter(new byte[32], 8, BloomFilter.CurrentAlgorithmVersion + 1))
            .Throws<ArgumentOutOfRangeException>();
    }

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
        await Assert.That(bloom.GetBytes().Length % 32).IsEqualTo(0);
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
