namespace Silex.Test;

using Silex.Buffers;
using Silex.Serialization;
using TUnit.Assertions.Enums;

public class EncoderTests
{
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(64)]
    [Arguments(UTF8StringEncoder.StackAllocThreshold - 1)]
    [Arguments(UTF8StringEncoder.StackAllocThreshold)]
    [Arguments(UTF8StringEncoder.StackAllocThreshold + 1)]
    [Arguments(500)]
    [Arguments(64000)]
    public async Task UTF8StringEncoderShouldEncodeAsciiStrings(int length)
    {
        var array = Random.Shared.GetItems<char>("abcdefghijklmnopqrstuvw", length);
        var value = new string(array);

        var encoder = new UTF8StringEncoder();

        await Assert.That(value.Length).IsEqualTo(length);
        await Assert.That(encoder.GetLength(value)).IsEqualTo(length);

        var _bufferWriter = new PooledArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(_bufferWriter);
        encoder.Encode(value, ref writer);
        writer.Flush();
        var memory = _bufferWriter.WrittenMemory;
        var decoded = encoder.Decode(memory.Span);

        await Assert.That(memory.Length).IsEqualTo(length);
        await Assert.That(decoded).IsEqualTo(value);
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(64)]
    [Arguments(UTF8StringEncoder.StackAllocThreshold - 1)]
    [Arguments(UTF8StringEncoder.StackAllocThreshold)]
    [Arguments(UTF8StringEncoder.StackAllocThreshold + 1)]
    [Arguments(500)]
    [Arguments(64000)]
    public async Task UTF8StringEncoderShouldEncodeNonAsciiStrings(int length)
    {
        var array = Random.Shared.GetItems<char>("ちこそしいはきくにまのりもみらせたすとかなひてさんつ", length);
        var value = new string(array);

        var encoder = new UTF8StringEncoder();

        await Assert.That(value.Length).IsEqualTo(length);

        if (length != 0)
        {
            await Assert.That(length < encoder.GetLength(value)).IsTrue();
        }

        var _bufferWriter = new PooledArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(_bufferWriter);
        encoder.Encode(value, ref writer);
        writer.Flush();
        var memory = _bufferWriter.WrittenMemory;
        var decoded = encoder.Decode(memory.Span);

        await Assert.That(decoded).IsEqualTo(value);
    }

    [Test]
    public async Task ByteArrayEncoderEqualityComparerShouldUseContent()
    {
        IEqualityComparer<byte[]> comparer = ((IBinaryEncoder<byte[]>)new ByteArrayEncoder()).EqualityComparer;

        var a = new byte[] { 1, 2, 3 };
        var b = new byte[] { 1, 2, 3 };
        var c = new byte[] { 1, 2, 4 };

        await Assert.That(comparer.Equals(a, b)).IsTrue();
        await Assert.That(comparer.Equals(a, c)).IsFalse();
        await Assert.That(comparer.GetHashCode(b)).IsEqualTo(comparer.GetHashCode(a));
    }

    [Test]
    public async Task ByteArrayEncoderTreatsAnyEmptyArrayAsTombstone()
    {
        var encoder = new ByteArrayEncoder();

        // The tombstone is content-based (length 0), not a specific shared instance.
        await Assert.That(encoder.IsTombstoneValue(Array.Empty<byte>())).IsTrue();
        await Assert.That(encoder.IsTombstoneValue(new byte[0])).IsTrue();
        await Assert.That(encoder.IsTombstoneValue(new byte[] { 0 })).IsFalse();
        await Assert.That(encoder.UsesEmptyTombstone).IsTrue();
    }

    private static byte[] Encode<T>(IBinaryEncoder<T> encoder, T value)
    {
        var bufferWriter = new PooledArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(bufferWriter);
        encoder.Encode(value, ref writer);
        writer.Flush();
        return bufferWriter.WrittenMemory.ToArray();
    }

    // The byte core compares encoded keys as raw bytes, which is only correct if the encoding is
    // order-preserving: a bytewise (lexicographic) comparison of the encoded bytes must match the
    // typed comparison of the values, with no two distinct values colliding to the same bytes.
    private static async Task AssertOrderPreserving<T>(IBinaryEncoder<T> encoder, IReadOnlyList<T> values)
    {
        var encoded = values.Select(v => (Value: v, Bytes: Encode(encoder, v))).ToList();

        // No two distinct values may encode to the same bytes.
        for (var i = 0; i < encoded.Count; i++)
        {
            for (var j = i + 1; j < encoded.Count; j++)
            {
                var collides = encoded[i].Bytes.AsSpan().SequenceEqual(encoded[j].Bytes);
                await Assert.That(collides).IsFalse();
            }
        }

        var byBytes = encoded.OrderBy(e => e.Bytes, Comparer<byte[]>.Create((a, b) => a.AsSpan().SequenceCompareTo(b))).Select(e => e.Value).ToList();
        var byTyped = encoded.Select(e => e.Value).OrderBy(v => v, encoder.Comparer).ToList();

        await Assert.That(byBytes).IsEquivalentTo(byTyped, CollectionOrdering.Matching);

        // Round-trip must hold for every value.
        foreach (var (value, bytes) in encoded)
        {
            await Assert.That(encoder.Decode(bytes)).IsEqualTo(value);
        }
    }

    [Test]
    public async Task UInt32EncoderIsOrderPreserving()
    {
        var values = new uint[] { 0, 1, 2, 255, 256, 65535, 65536, 1000000, uint.MaxValue - 1, uint.MaxValue };
        await AssertOrderPreserving(new UInt32Encoder(), values);
    }

    [Test]
    public async Task UInt64EncoderIsOrderPreserving()
    {
        var values = new ulong[]
        {
            0, 1, 2, 255, 256, 65535, 65536, uint.MaxValue, (ulong)uint.MaxValue + 1,
            long.MaxValue, 1UL << 63, ulong.MaxValue - 1, ulong.MaxValue
        };
        await AssertOrderPreserving(new UInt64Encoder(), values);
    }

    [Test]
    public async Task StringEncoderIsOrderPreserving()
    {
        // Includes a supplementary character (U+10000, a surrogate pair) which sorts differently under
        // UTF-16 ordinal vs UTF-8/code-point order: this is the case the code-point comparer must get right.
        var values = new[] { "", "a", "aa", "ab", "b", "ba", "z", "\uE000", "ち", "ちこ", "\uFFFF", "\U00010000" };
        await AssertOrderPreserving(new UTF8StringEncoder(), values);
    }
}
