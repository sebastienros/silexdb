namespace Silex.Test;

using Silex.Blocks;
using System.Text;

public class BlockTests
{
    [Fact]
    public void ShouldEncodeBlock()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder(), (ushort)4.KiB());

        var key = BitConverter.GetBytes((ushort)7);
        var value = Encoding.UTF8.GetBytes($"hello");

        blockBuilder.AddEntry(new BlockEntry { Key = key, Value = value });

        var block = blockBuilder.BuildBlock();

        var expectedDataSize = 
            1 + // 1 byte to store the 7-bits encoded key size length (should be 02)
            key.Length + // 2 bytes to store the key (should be 07:00)
            1 + // 1 byte to store the 7-bits encoded value size length (should be 05)
            value.Length + // 5 bytes to store the key (should 68:65:6c:6c:6f)
            2 + // 2 bytes for offset of 1st entry (should be 00:00)
            2 + // 2 bytes for number of elements (should be 01:00)

        Assert.Single(block.Offsets);
        Assert.Equal(expectedDataSize, block.Data.Length);
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block.Data);
    }

    [Fact]
    public void ShouldDecodeBlock()
    {
        var raw = new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 };
        var key = BitConverter.GetBytes((ushort)7);
        var value = Encoding.UTF8.GetBytes($"hello");

        var encoder = new DefaultBlockEncoder();

        var block = encoder.Decode(raw);

        var entry = encoder.DecodeEntry(block.Data, block.Offsets[0]);
        
        Assert.Single(block.Offsets);
        Assert.Equal(0, block.Offsets[0]);
        Assert.Equal(key, entry.Key);
        Assert.Equal(value, entry.Value);
    }
}
