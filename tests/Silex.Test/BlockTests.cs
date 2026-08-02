using Silex.Blocks;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class BlockTests
{
    private static int DecodeInt32(ByteSlice value) => new Silex.Serialization.Int32Encoder().Decode(value.Span);

    [Test]
    public async Task ShouldEncodeBlock()
    {
        var blockBuilder = new BlockBuilder (new DefaultBlockEncoder());

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

        await Assert.That(block.Offsets).HasSingleItem();
        await Assert.That(block.Memory.Length).IsEqualTo(expectedDataSize);
        await Assert.That(block.Memory).IsEquivalentTo(new byte[] { 2, 0, 7, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
    }

    [Test]
    public async Task ShouldDecodeBlock()
    {
        var raw = new byte[] { 2, 0, 7, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 };
        var key = ByteSliceTestExtensions.Slice((ushort)7);

        var encoder = new DefaultBlockEncoder();

        using var block = encoder.Decode(raw);

        var entry = encoder.DecodeEntry(block.Memory, block.Offsets[0]);
        
        await Assert.That(block.Offsets).HasSingleItem();
        await Assert.That((int)block.Offsets[0]).IsEqualTo(0);
        await Assert.That(entry.Key).IsEqualTo(key);
        await Assert.That((int)entry.BlockOffset).IsEqualTo(4);
        await Assert.That((int)entry.Length).IsEqualTo(5);
    }

    [Test]
    public async Task ShouldDistinguishEmptyValueFromTombstone()
    {
        using var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());
        blockBuilder.Add(ByteSliceTestExtensions.Slice(1), ByteSlice.Empty);
        blockBuilder.Add(ByteSliceTestExtensions.Slice(2), ByteSlice.Tombstone);

        using var block = blockBuilder.BuildBlock();
        using var key1 = OwnedByteSlice.CopyFrom(ByteSliceTestExtensions.Slice(1).Span);
        using var key2 = OwnedByteSlice.CopyFrom(ByteSliceTestExtensions.Slice(2).Span);

        var empty = Lookup(block, key1.Span);
        await Assert.That(empty.Found).IsTrue();
        await Assert.That(empty.Length).IsEqualTo(0);
        await Assert.That(empty.IsTombstone).IsFalse();

        var deleted = Lookup(block, key2.Span);
        await Assert.That(deleted.Found).IsTrue();
        await Assert.That(deleted.Length).IsEqualTo(0);
        await Assert.That(deleted.IsTombstone).IsTrue();

        var entries = new BlockIterator(block).EnumerateAsync().ToBlockingEnumerable().ToArray();
        await Assert.That(entries[0].Value.IsEmpty).IsTrue();
        await Assert.That(entries[0].Value.IsTombstone).IsFalse();
        await Assert.That(entries[1].Value.IsTombstone).IsTrue();

        static (bool Found, int Length, bool IsTombstone) Lookup(Block block, ReadOnlySpan<byte> key)
        {
            var found = block.TryGetValue(key, out var value, out var isTombstone);
            return (found, value.Length, isTombstone);
        }
    }

    [Test]
    public async Task ShouldIterateAllEntries()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var value = "hello";
        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, value);
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator(block);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => DecodeInt32(x.Key)).ToArray();

        await Assert.That(result).IsEquivalentTo(allKeys);
    }

    [Test]
    public async Task ShouldIterateFromKey()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var value = "hello";
        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, value);
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator(block);

        var result = iterator.EnumerateAsync(2).ToBlockingEnumerable().Select(x => DecodeInt32(x.Key)).ToArray();

        await Assert.That(result).IsEquivalentTo(allKeys.Skip(1));
    }

    [Test]
    public async Task ShouldIterateFromUnknownKey()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var value = "hello";
        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, value);
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator(block);

        var result = iterator.EnumerateAsync(8).ToBlockingEnumerable().Select(x => DecodeInt32(x.Key)).ToArray();

        await Assert.That(result).IsEmpty();
    }

    [Test]
    public async Task ShouldIterateAllEntriesBackwards()
    {
        using var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());
        var allKeys = new[] { 1, 3, 5, 6, 7 };

        foreach (var key in allKeys)
        {
            blockBuilder.Add(key, "hello");
        }

        using var block = blockBuilder.BuildBlock();
        var result = new BlockIterator(block).EnumerateBackwardsAsync().ToBlockingEnumerable().Select(x => DecodeInt32(x.Key)).ToArray();

        await Assert.That(result).IsEquivalentTo(allKeys.AsEnumerable().Reverse(), CollectionOrdering.Matching);
    }

    [Test]
    [Arguments(6, new[] { 6, 5, 3, 1 })]
    [Arguments(4, new[] { 3, 1 })]
    [Arguments(0, new int[0])]
    [Arguments(8, new[] { 7, 6, 5, 3, 1 })]
    public async Task ShouldIterateBackwardsFromKey(int from, int[] expected)
    {
        using var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        foreach (var key in new[] { 1, 3, 5, 6, 7 })
        {
            blockBuilder.Add(key, "hello");
        }

        using var block = blockBuilder.BuildBlock();
        var result = new BlockIterator(block).EnumerateBackwardsAsync(from).ToBlockingEnumerable().Select(x => DecodeInt32(x.Key)).ToArray();

        await Assert.That(result).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    [Test]
    public async Task TryGetValueShouldReturnCorrectValues()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var allKeys = new int[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, i*i);
        }

        var block = blockBuilder.BuildBlock();

        // Assert

        foreach (var i in allKeys)
        {
            var resultIsEmpty = block.GetValue(i).IsEmpty;
            var decoded = new Silex.Serialization.Int32Encoder().Decode(block.GetValue(i));
            await Assert.That(resultIsEmpty).IsFalse();
            await Assert.That(decoded).IsEqualTo(i * i);
        }
    }

    [Test]
    public async Task TryGetValueByEncodedKeyShouldMatchTypedLookup()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var allKeys = new int[] { -7, -1, 0, 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, i * i);
        }

        var block = blockBuilder.BuildBlock();
        var encoder = new Silex.Serialization.Int32Encoder();

        foreach (var i in allKeys)
        {
            var bufferWriter = new Silex.Buffers.PooledArrayBufferWriter<byte>();
            var writer = new Silex.Buffers.EncoderBinaryWriter(bufferWriter);
            encoder.Encode(i, ref writer);
            writer.Flush();

            bool found;
            int decodedValue;
            {
                found = block.TryGetValue((ReadOnlySpan<byte>)bufferWriter.WrittenMemory.Span, out var value);
                decodedValue = found ? encoder.Decode(value) : 0;
            }

            await Assert.That(found).IsTrue();
            await Assert.That(decodedValue).IsEqualTo(i * i);
        }

        // An absent in-range key must report a miss.
        var missWriter = new Silex.Buffers.PooledArrayBufferWriter<byte>();
        var w = new Silex.Buffers.EncoderBinaryWriter(missWriter);
        encoder.Encode(4, ref w);
        w.Flush();
        var missFound = block.TryGetValue((ReadOnlySpan<byte>)missWriter.WrittenMemory.Span, out _);
        await Assert.That(missFound).IsFalse();
    }

    [Test]
    public async Task ShouldNotAcceptEntriesWhenFull()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var value = new byte[1024];
        var r1 = blockBuilder.Add(1, value);
        var r2 = blockBuilder.Add(2, value);
        var r3 = blockBuilder.Add(3, value);
        var r4 = blockBuilder.Add(4, value);

        await Assert.That(r1).IsTrue();
        await Assert.That(r2).IsTrue();
        await Assert.That(r3).IsTrue();
        await Assert.That(r4).IsFalse();
    }

    [Test]
    public async Task NewBlocksShouldAcceptFirstEntryBiggerThanBlockSize()
    {
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var value = new byte[8.KiB()];
        var r1 = blockBuilder.Add(1, value);
        var r2 = blockBuilder.Add(1, [1] );

        await Assert.That(r1).IsTrue();
        await Assert.That(r2).IsFalse();
    }
}
