using Silex.BloomFilters;
using System.Buffers.Binary;

namespace Silex.Test;

public class BloomFilterTests
{
    [Test]
    [Arguments(1_000, 0.01)]
    [Arguments(1_000, 0.05)]
    [Arguments(1_000, 0.10)]
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

        var setBitCount = 0;
        foreach (var value in bloom.GetBytes())
        {
            setBitCount += System.Numerics.BitOperations.PopCount(value);
        }

        var bitCount = (double)bloom.GetBytes().Length * 8;
        var occupancy = (double)setBitCount / bitCount;
        var hashCount = (double)bloom.K * n;
        var configuredRate = BloomFilter.CalculateP(bloom.K, (long)bitCount, n);
        var emptyProbability = Math.Pow(1 - 1d / bitCount, hashCount);
        var expectedOccupancy = 1 - emptyProbability;
        var bothOccupiedProbability = 1 - 2 * emptyProbability + Math.Pow(1 - 2d / bitCount, hashCount);
        var occupancyVariance = (bitCount * expectedOccupancy * (1 - expectedOccupancy)
            + bitCount * (bitCount - 1) * (bothOccupiedProbability - expectedOccupancy * expectedOccupancy))
            / (bitCount * bitCount);
        var occupancyStandardDeviation = Math.Sqrt(Math.Max(0, occupancyVariance));

        // Integer hash counts can make the theoretical rate slightly exceed the
        // continuous optimum requested by p, but the sizing error stays below 3%.
        await Assert.That(configuredRate).IsLessThanOrEqualTo(p * 1.03);
        await Assert.That(Math.Abs(occupancy - expectedOccupancy))
            .IsLessThanOrEqualTo(6 * occupancyStandardDeviation);

        var expectedRate = Math.Pow(occupancy, bloom.K);
        var standardDeviation = Math.Sqrt(expectedRate * (1 - expectedRate) / sampleCount);
        var observedRate = (double)falsePositiveCount / sampleCount;

        // The fixed sample is deterministic; the statistical bound tolerates the small
        // model error from double hashing while detecting probe-distribution regressions.
        await Assert.That(observedRate).IsLessThanOrEqualTo(expectedRate + 6 * standardDeviation);
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

        var restored = new BloomFilter(bloom.GetBytes().ToArray(), bloom.K);
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
    }
}
