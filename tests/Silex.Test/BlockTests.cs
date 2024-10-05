namespace Silex.Test;

using Silex.Blocks;
using System.Text;

public class BlockTests
{
    [Fact]
    public void ShouldEncodeBlock()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var key = BitConverter.GetBytes((ushort)7);
        var value = Encoding.UTF8.GetBytes($"hello");

        blockBuilder.AddEntry(key, value);

        var block = blockBuilder.BuildBlock();

        var expectedDataSize =
            1 + // 1 byte to store the 7-bits encoded key size length (should be 02)
            key.Length + // 2 bytes to store the key (should be 07:00)
            1 + // 1 byte to store the 7-bits encoded value size length (should be 05)
            value.Length + // 5 bytes to store the key (should 68:65:6c:6c:6f)
            2 + // 2 bytes for offset of 1st entry (should be 00:00)
            2;  // 2 bytes for number of elements (should be 01:00)

        Assert.Single(block.Offsets);
        Assert.Equal(expectedDataSize, block.Memory.Length);
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block.Memory);
    }

    [Fact]
    public void ShouldDecodeBlock()
    {
        var raw = new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 };
        var key = BitConverter.GetBytes((ushort)7);

        var encoder = new DefaultBlockEncoder();

        using var block = encoder.Decode(raw);

        var entry = encoder.DecodeEntry(block.Memory, block.Offsets[0]);
        
        Assert.Single(block.Offsets);
        Assert.Equal(0, block.Offsets[0]);
        Assert.Equal(key, entry.Key);
        Assert.Equal(4, entry.BlockOffset);
        Assert.Equal(5, entry.Length);
    }

    [Fact]
    public void ShouldIterateAllEntries()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var value = "hello"u8.ToArray();
        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.AddEntry(BitConverter.GetBytes(i), value);
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator(block);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Equivalent(allKeys, result);
    }

    [Fact]
    public void ShouldIterateFromKey()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var value = "hello"u8.ToArray();
        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.AddEntry(BitConverter.GetBytes(i), value);
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator(block);

        var result = iterator.EnumerateAsync(BitConverter.GetBytes(2)).ToBlockingEnumerable().Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Equivalent(allKeys.Skip(1), result);
    }

    [Fact]
    public void ShouldIterateFromUnknownKey()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var value = "hello"u8.ToArray();
        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.AddEntry(BitConverter.GetBytes(i), value);
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator(block);

        var result = iterator.EnumerateAsync(BitConverter.GetBytes(8)).ToBlockingEnumerable().Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Empty(result);
    }
}
