namespace Silex.Test;

using Silex.Buffers;
using Silex.Serialization;

public class EncoderTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(64)]
    [InlineData(UTF8StringEncoder.StackAllocThreshold - 1)]
    [InlineData(UTF8StringEncoder.StackAllocThreshold)]
    [InlineData(UTF8StringEncoder.StackAllocThreshold + 1)]
    [InlineData(500)]
    [InlineData(64000)]
    public void UTF8StringEncoderShouldEncodeAsciiStrings(int length)
    {
        var array = Random.Shared.GetItems<char>("abcdefghijklmnopqrstuvw", length);
        var value = new string(array);

        var encoder = new UTF8StringEncoder();

        Assert.Equal(length, value.Length);
        Assert.Equal(length, encoder.GetLength(value));

        var _bufferWriter = new PooledArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(_bufferWriter);
        encoder.Encode(value, ref writer);
        writer.Flush();
        var memory = _bufferWriter.WrittenMemory;
        var decoded = encoder.Decode(memory.Span);

        Assert.Equal(length, memory.Length);
        Assert.Equal(value, decoded);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(64)]
    [InlineData(UTF8StringEncoder.StackAllocThreshold - 1)]
    [InlineData(UTF8StringEncoder.StackAllocThreshold)]
    [InlineData(UTF8StringEncoder.StackAllocThreshold + 1)]
    [InlineData(500)]
    [InlineData(64000)]
    public void UTF8StringEncoderShouldEncodeNonAsciiStrings(int length)
    {
        var array = Random.Shared.GetItems<char>("ちこそしいはきくにまのりもみらせたすとかなひてさんつ", length);
        var value = new string(array);

        var encoder = new UTF8StringEncoder();

        Assert.Equal(length, value.Length);

        if (length != 0)
        {
            Assert.True(length < encoder.GetLength(value));
        }

        var _bufferWriter = new PooledArrayBufferWriter<byte>();
        var writer = new EncoderBinaryWriter(_bufferWriter);
        encoder.Encode(value, ref writer);
        writer.Flush();
        var memory = _bufferWriter.WrittenMemory;
        var decoded = encoder.Decode(memory.Span);

        Assert.Equal(value, decoded);
    }

    [Fact]
    public void ByteArrayEncoderEqualityComparerShouldUseContent()
    {
        IEqualityComparer<byte[]> comparer = ((IBinaryEncoder<byte[]>)new ByteArrayEncoder()).EqualityComparer;

        var a = new byte[] { 1, 2, 3 };
        var b = new byte[] { 1, 2, 3 };
        var c = new byte[] { 1, 2, 4 };

        Assert.True(comparer.Equals(a, b));
        Assert.False(comparer.Equals(a, c));
        Assert.Equal(comparer.GetHashCode(a), comparer.GetHashCode(b));
    }
}
