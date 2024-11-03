using Silex.Blocks;

namespace Silex.Test;

public class BlockTests
{
    [Fact]
    public void ShouldEncodeBlock()
    {
        var blockBuilder = new BlockBuilder<ushort, string> (new DefaultBlockEncoder<ushort, string>());

        ushort key = 7;
        var value = "hello";

        blockBuilder.Add(key, value);

        var block = blockBuilder.BuildBlock();

        var expectedDataSize =
            1 + // 1 byte to store the 7-bits encoded key size length (should be 02)
            sizeof(ushort) + // 2 bytes to store the key (should be 07:00)
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
        var key = new Bytes((ushort)7);

        var encoder = new DefaultBlockEncoder<ushort, byte[]>();

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
        var blockBuilder = new BlockBuilder<int, string>(new DefaultBlockEncoder<int, string>());

        var value = "hello";
        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, value);
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator<int, string>(block);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => x.Key).ToArray();

        Assert.Equivalent(allKeys, result);
    }

    [Fact]
    public void ShouldIterateFromKey()
    {
        var blockBuilder = new BlockBuilder<int, string>(new DefaultBlockEncoder<int, string>());

        var value = "hello";
        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, value);
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator<int, string>(block);

        var result = iterator.EnumerateAsync(2).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        Assert.Equivalent(allKeys.Skip(1), result);
    }

    [Fact]
    public void ShouldIterateFromUnknownKey()
    {
        var blockBuilder = new BlockBuilder<int, string>(new DefaultBlockEncoder<int, string>());

        var value = "hello";
        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, value);
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator<int, string>(block);

        var result = iterator.EnumerateAsync(8).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        Assert.Empty(result);
    }

    [Fact]
    public void TryGetValueShouldReturnCorrectValues()
    {
        var blockBuilder = new BlockBuilder<int, int>(new DefaultBlockEncoder<int, int>());

        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, i*i);
        }

        var block = blockBuilder.BuildBlock();

        // Assert

        foreach (var i in allKeys)
        {
            var result = block.GetValue(i);
            Assert.False(result.IsEmpty);
            Assert.Equal(BitConverter.GetBytes(i * i), new Bytes(result));
        }
    }

    [Fact]
    public void ShouldNotAcceptEntriesWhenFull()
    {
        var blockBuilder = new BlockBuilder<int, byte[]>(new DefaultBlockEncoder<int, byte[]>());

        var value = new byte[1024];
        var r1 = blockBuilder.Add(1, value);
        var r2 = blockBuilder.Add(2, value);
        var r3 = blockBuilder.Add(3, value);
        var r4 = blockBuilder.Add(4, value);

        Assert.True(r1);
        Assert.True(r2);
        Assert.True(r3);
        Assert.False(r4);
    }

    [Fact]
    public void NewBlocksShouldAcceptFirstEntryBiggerThanBlockSize()
    {
        var blockBuilder = new BlockBuilder<int, byte[]>(new DefaultBlockEncoder<int, byte[]>());

        var value = new byte[8.KiB()];
        var r1 = blockBuilder.Add(1, value);
        var r2 = blockBuilder.Add(1, [1] );

        Assert.True(r1);
        Assert.False(r2);
    }

    [Fact(Skip = "Requires values to be serialized in the MemTable buffer")]
    public void EntryBuffersShouldBeCopied()
    {
        var blockBuilder = new BlockBuilder<int, byte[]>(new DefaultBlockEncoder<int, byte[]>());

        byte[] bytes = [111];
        var value = bytes;
        blockBuilder.Add(1, value);

        // The value should be copied, so 222 should not update the value stored with key '1'
        bytes[0] = 222;

        blockBuilder.Add(2, value);

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator<int, byte[]>(block);

        var locations = iterator.EnumerateAsync().ToBlockingEnumerable().ToArray();
        var v1 = locations[0].Value;
        var v2 = locations[1].Value;

        Assert.Equal(111, v1[0]);
        Assert.Equal(222, v2[0]);
    }
}
