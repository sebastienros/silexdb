using System.Buffers.Binary;
using Silex.Blocks;
using Silex.Buffers;
using Silex.Serialization;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class BlockTests
{
    private static byte[] BE(uint value)
    {
        var bytes = new byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, value);
        return bytes;
    }

    [Test]
    public async Task ShouldEncodeBlock()
    {
        var blockBuilder = new BlockBuilder<byte[]>(new DefaultBlockEncoder<byte[]>());

        // A 2-byte key and the 5-byte ASCII value "hello".
        var key = new byte[] { 7, 0 };
        var value = new byte[] { 104, 101, 108, 108, 111 };

        blockBuilder.Add(key, new ValueBuffer(value));

        var block = blockBuilder.BuildBlock();

        var expectedDataSize =
            1 + // 1 byte to store the 7-bits encoded key size length (should be 02)
            key.Length + // 2 bytes to store the key (should be 07:00)
            1 + // 1 byte to store the 7-bits encoded value size length (should be 05)
            value.Length + // 5 bytes to store the value (should 68:65:6c:6c:6f)
            2 + // 2 bytes for offset of 1st entry (should be 00:00)
            2;  // 2 bytes for number of elements (should be 01:00)

        await Assert.That(block.Offsets).HasSingleItem();
        await Assert.That(block.Memory.Length).IsEqualTo(expectedDataSize);
        await Assert.That(block.Memory).IsEquivalentTo(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, CollectionOrdering.Matching);
    }

    [Test]
    public async Task ShouldDecodeBlock()
    {
        var raw = new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 };
        var key = new Bytes(new byte[] { 7, 0 });

        var encoder = new DefaultBlockEncoder<byte[]>();

        using var block = encoder.Decode(raw);

        var entry = encoder.DecodeEntry(block.Memory, block.Offsets[0]);

        await Assert.That(block.Offsets).HasSingleItem();
        await Assert.That((int)block.Offsets[0]).IsEqualTo(0);
        await Assert.That(new Bytes(entry.Key)).IsEqualTo(key);
        await Assert.That((int)entry.BlockOffset).IsEqualTo(4);
        await Assert.That((int)entry.Length).IsEqualTo(5);
    }

    [Test]
    public async Task ShouldIterateAllEntries()
    {
        var blockBuilder = new BlockBuilder<uint>(new DefaultBlockEncoder<uint>());

        var value = "hello"u8.ToArray();
        var allKeys = new uint[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, new ValueBuffer(value));
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator<uint>(block);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEquivalentTo(allKeys);
    }

    [Test]
    public async Task ShouldIterateFromKey()
    {
        var blockBuilder = new BlockBuilder<uint>(new DefaultBlockEncoder<uint>());

        var value = "hello"u8.ToArray();
        var allKeys = new uint[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, new ValueBuffer(value));
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator<uint>(block);

        var result = iterator.EnumerateAsync(2).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEquivalentTo(allKeys.Skip(1));
    }

    [Test]
    public async Task ShouldIterateFromUnknownKey()
    {
        var blockBuilder = new BlockBuilder<uint>(new DefaultBlockEncoder<uint>());

        var value = "hello"u8.ToArray();
        var allKeys = new uint[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, new ValueBuffer(value));
        }

        var block = blockBuilder.BuildBlock();
        var iterator = new BlockIterator<uint>(block);

        var result = iterator.EnumerateAsync(8).ToBlockingEnumerable().Select(x => x.Key).ToArray();

        await Assert.That(result).IsEmpty();
    }

    [Test]
    public async Task TryGetValueShouldReturnCorrectValues()
    {
        var blockBuilder = new BlockBuilder<uint>(new DefaultBlockEncoder<uint>());

        var allKeys = new uint[] { 1, 3, 5, 6, 7 };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, new ValueBuffer(BE(i * i)));
        }

        var block = blockBuilder.BuildBlock();

        // Assert

        foreach (var i in allKeys)
        {
            var value = block.GetValue(i);
            var isEmpty = value.IsEmpty;
            var decoded = isEmpty ? 0u : BinaryPrimitives.ReadUInt32BigEndian(value);
            await Assert.That(isEmpty).IsFalse();
            await Assert.That(decoded).IsEqualTo(i * i);
        }
    }

    [Test]
    public async Task TryGetValueByEncodedKeyShouldMatchTypedLookup()
    {
        var blockBuilder = new BlockBuilder<uint>(new DefaultBlockEncoder<uint>());

        // Unsigned edge-order keys: the byte core must agree with the typed comparer across the whole
        // 32-bit range, including the high half (>= 0x8000_0000) that a signed encoding would mis-order.
        var allKeys = new uint[] { 0, 1, 3, 5, 6, 7, 0x7FFF_FFFF, 0x8000_0000, uint.MaxValue };
        foreach (var i in allKeys)
        {
            blockBuilder.Add(i, new ValueBuffer(BE(i)));
        }

        var block = blockBuilder.BuildBlock();
        var encoder = new UInt32Encoder();

        foreach (var i in allKeys)
        {
            var bufferWriter = new PooledArrayBufferWriter<byte>();
            var writer = new EncoderBinaryWriter(bufferWriter);
            encoder.Encode(i, ref writer);
            writer.Flush();

            var found = block.TryGetValue((ReadOnlySpan<byte>)bufferWriter.WrittenMemory.Span, out var value);
            var decodedValue = found ? BinaryPrimitives.ReadUInt32BigEndian(value) : 0u;

            await Assert.That(found).IsTrue();
            await Assert.That(decodedValue).IsEqualTo(i);
        }

        // An absent in-range key must report a miss.
        var missWriter = new PooledArrayBufferWriter<byte>();
        var w = new EncoderBinaryWriter(missWriter);
        encoder.Encode(4, ref w);
        w.Flush();
        var missFound = block.TryGetValue((ReadOnlySpan<byte>)missWriter.WrittenMemory.Span, out _);
        await Assert.That(missFound).IsFalse();
    }

    [Test]
    public async Task ShouldNotAcceptEntriesWhenFull()
    {
        var blockBuilder = new BlockBuilder<uint>(new DefaultBlockEncoder<uint>());

        var value = new byte[1024];
        var r1 = blockBuilder.Add(1, new ValueBuffer(value));
        var r2 = blockBuilder.Add(2, new ValueBuffer(value));
        var r3 = blockBuilder.Add(3, new ValueBuffer(value));
        var r4 = blockBuilder.Add(4, new ValueBuffer(value));

        await Assert.That(r1).IsTrue();
        await Assert.That(r2).IsTrue();
        await Assert.That(r3).IsTrue();
        await Assert.That(r4).IsFalse();
    }

    [Test]
    public async Task NewBlocksShouldAcceptFirstEntryBiggerThanBlockSize()
    {
        var blockBuilder = new BlockBuilder<uint>(new DefaultBlockEncoder<uint>());

        var value = new byte[8.KiB()];
        var r1 = blockBuilder.Add(1, new ValueBuffer(value));
        var r2 = blockBuilder.Add(1, new ValueBuffer([1]));

        await Assert.That(r1).IsTrue();
        await Assert.That(r2).IsFalse();
    }
}
