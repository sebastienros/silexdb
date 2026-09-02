using System.Text;
using Silex.Wal;

namespace Silex.Test;

public class Crc32CTests
{
    [Test]
    public async Task Crc32CMatchesStandardVectors()
    {
        var vectors = new (byte[] Data, uint Checksum)[]
        {
            ([], 0x00000000),
            (Encoding.ASCII.GetBytes("123456789"), 0xE3069283),
            (new byte[32], 0x8A9136AA),
            (Enumerable.Repeat((byte)0xFF, 32).ToArray(), 0x62A8AB43),
            (Enumerable.Range(0, 32).Select(static value => (byte)value).ToArray(), 0x46DD794E),
            (Enumerable.Range(0, 32).Select(static value => (byte)(31 - value)).ToArray(), 0x113FDB5C),
        };

        foreach (var (data, checksum) in vectors)
        {
            await Assert.That(Crc32C.ComputeSoftware(data)).IsEqualTo(checksum);
            await Assert.That(Crc32C.Compute(data)).IsEqualTo(checksum);
        }
    }

    [Test]
    public void HardwareAndSoftwareCrc32CAgreeAcrossLengthsAndAlignments()
    {
        var random = new Random(42);
        var data = new byte[4096 + 16];
        random.NextBytes(data);

        for (var offset = 0; offset < 16; offset++)
        {
            for (var length = 0; length <= 4096; length++)
            {
                var input = data.AsSpan(offset, length);
                var expected = Crc32C.ComputeSoftware(input);
                var actual = Crc32C.Compute(input);
                if (actual != expected)
                {
                    throw new InvalidOperationException(
                        $"CRC32C mismatch at offset {offset}, length {length}: expected {expected:X8}, actual {actual:X8}.");
                }
            }
        }
    }
}
