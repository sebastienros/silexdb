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
        // n: distinct values
        // p: tolerable false positive rate 

        var s = 1_000 * n; // Cardinality (number of values in the set)
        var attempts = 1_000_000; // Attempts to build stats

        var elements = new HashSet<int>();

        while (elements.Count < n) elements.Add(Random.Shared.Next(1, s));

        var bloom = new BloomFilter(n, p);

        var buffer = new byte[sizeof(int) / sizeof(byte)];

        foreach (int element in elements)
        {
            BinaryPrimitives.WriteInt32LittleEndian(buffer, element);
            bloom.Add(buffer);
        }

        int positive = 0;
        int negative = 0;
        int falsePositive = 0;
        int falseNegative = 0;

        for (var i = 0; i < attempts; i++)
        {
            var v = Random.Shared.Next(1, s);

            BinaryPrimitives.WriteInt32LittleEndian(buffer, v);
            var result = bloom.Probe(buffer);

            if (result)
            {
                positive++;
            }
            else
            {
                negative++;
            }

            if (result && !elements.Contains(v)) falsePositive++;
            if (!result && elements.Contains(v)) falseNegative++;
        }

        var pResult = positive == 0 ? 1 : (double)falsePositive / positive;
        var nResult = negative == 0 ? 1 : (double)falseNegative / negative;
        var fnRate = (double)negative / attempts;
        var fpRate = (double)positive / attempts;

        Console.WriteLine($"May be set: {positive} ({fpRate * 100}%), Tolerable (p) was {p}");
        Console.WriteLine($"False positive: {pResult} ({pResult * 100}%)");
        Console.WriteLine($"Is not in set: {negative} ({fnRate * 100}%)");

        // A bloom filter should not return false negatives
        await Assert.That(falseNegative).IsEqualTo(0);

        // Ensure the resulting rate is within 20% of the expected probability
        await Assert.That((fpRate - p) / p < 0.20).IsTrue();
    }

    [Test]
    public async Task BloomFilterShouldNotReturnFalsePositive()
    {
        var buffer = new byte[sizeof(int) / sizeof(byte)];
        var bloom = new BloomFilter(1000, 0.1);
        
        BinaryPrimitives.WriteInt32LittleEndian(buffer, 123);
        bloom.Add(buffer);

        BinaryPrimitives.WriteInt32LittleEndian(buffer, 456);
        bloom.Add(buffer);

        var result = bloom.Probe(buffer);

        BinaryPrimitives.WriteInt32LittleEndian(buffer, 111);
        await Assert.That(bloom.Probe(buffer)).IsFalse();

        BinaryPrimitives.WriteInt32LittleEndian(buffer, 222);
        await Assert.That(bloom.Probe(buffer)).IsFalse();
    }
}
