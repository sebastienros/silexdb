namespace Silex.Test;

using Silex.Buffers;
using Silex.Serialization;

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
}
