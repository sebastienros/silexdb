using Silex.Buffers;
using Silex.Serialization;
using Silex.Tables;
using Silex.Wal;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class BinaryFormatTests
{
    [Test]
    public async Task StorageIntegersHaveCanonicalLittleEndianEncoding()
    {
        using var buffer = new PooledArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(buffer);

        writer.WriteUInt16(0x0102);
        writer.WriteUInt32(0x01020304);
        writer.WriteUInt64(0x0102030405060708);
        writer.Flush();

        var bytes = buffer.WrittenMemory.ToArray();
        var expected = new byte[]
        {
            0x02, 0x01,
            0x04, 0x03, 0x02, 0x01,
            0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01
        };

        var reader = new EncoderBinaryReader(expected, 0);
        var uint16 = reader.ReadUInt16();
        var uint32 = reader.ReadUInt32();
        var uint64 = reader.ReadUInt64();
        var isEof = reader.IsEOF;

        await Assert.That(bytes).IsEquivalentTo(expected, CollectionOrdering.Matching);
        await Assert.That(uint16).IsEqualTo((ushort)0x0102);
        await Assert.That(uint32).IsEqualTo(0x01020304u);
        await Assert.That(uint64).IsEqualTo(0x0102030405060708ul);
        await Assert.That(isEof).IsTrue();
    }

    [Test]
    public async Task OrderedNumericKeysHaveCanonicalBigEndianEncoding()
    {
        await AssertCanonicalEncoding(new UInt16Serializer(), (ushort)0x0102, [0x01, 0x02]);
        await AssertCanonicalEncoding(new UInt32Encoder(), 0x01020304u, [0x01, 0x02, 0x03, 0x04]);
        await AssertCanonicalEncoding(new UInt64Encoder(), 0x0102030405060708ul, [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
        await AssertCanonicalEncoding(new Int32Encoder(), 0x01020304, [0x81, 0x02, 0x03, 0x04]);
        await AssertCanonicalEncoding(new Int64Encoder(), 0x0102030405060708L, [0x81, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08]);
    }

    [Test]
    public async Task SsTableMetadataHasCanonicalEncoding()
    {
        using var firstKey = OwnedByteSlice.CopyFrom([0x01, 0x02, 0x03, 0x04]);
        using var lastKey = OwnedByteSlice.CopyFrom([0xFE, 0xFF]);
        using var metadata = new BlockMetadata
        {
            Index = 0,
            Offset = 0x01020304,
            FirstKeyOwner = OwnedByteSlice.CopyFrom(firstKey.Span),
            LastKeyOwner = OwnedByteSlice.CopyFrom(lastKey.Span)
        };
        using var buffer = new PooledArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(buffer);

        new DefaultSsTableEncoder().EncodeMetadata(ref writer, [metadata], 0x01020304, SsTableFormat.LegacyVersion);
        writer.Flush();

        var bytes = buffer.WrittenMemory.ToArray();
        var expected = new byte[]
        {
            0x01,
            0x84, 0x86, 0x88, 0x08,
            0x04, 0x01, 0x02, 0x03, 0x04,
            0x02, 0xFE, 0xFF,
            0x04, 0x03, 0x02, 0x01
        };

        await Assert.That(bytes).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    [Test]
    public async Task WriteAheadLogHasCanonicalEncoding()
    {
        using var tempFolder = TempFolder.Create();
        var path = tempFolder.GetRandomFileName();
        var key = Enumerable.Repeat((byte)0xA5, 130).ToArray();
        var value = new byte[] { 0xFE, 0xFF };

        using (var log = new WriteAheadLog(path, syncToDisk: false))
        {
            log.AppendRaw(key, value);
        }

        var bytes = await File.ReadAllBytesAsync(path);
        var expected = new byte[2 + key.Length + 1 + value.Length];
        expected[0] = 0x82;
        expected[1] = 0x01;
        key.CopyTo(expected, 2);
        expected[2 + key.Length] = 0x02;
        value.CopyTo(expected, 3 + key.Length);

        await Assert.That(bytes).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    private static async Task AssertCanonicalEncoding<T>(IBinaryEncoder<T> encoder, T value, byte[] expected)
    {
        using var buffer = new PooledArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(buffer);
        encoder.Encode(value, ref writer);
        writer.Flush();

        var bytes = buffer.WrittenMemory.ToArray();
        var decoded = encoder.Decode(expected);

        await Assert.That(bytes).IsEquivalentTo(expected, CollectionOrdering.Matching);
        await Assert.That(decoded).IsEqualTo(value);
    }
}
