using Silex.Buffers;
using System.Text;

namespace Silex.Serialization;

public sealed class UTF8CharEncoder : IBinaryEncoder<char>
{
    public char Decode(ReadOnlySpan<byte> data)
    {
        Span<char> span = stackalloc char[1];
        Encoding.UTF8.TryGetChars(data, span, out int charsWritten);
        return span[0];
    }

    public int GetLength(char value) 
    { 
        if (value < 0x80) return 1;

        Span<char> span = [value];
        return Encoding.UTF8.GetByteCount(span);
    }

    public char GetTombstoneValue() => '\0';

    public bool IsTombstoneValue(char value) => value == '\0';

    public int Encode(char value, ref EncoderBinaryWriter writer)
    {
        Span<char> span = [value];
        Span<byte> bytes = [0, 0, 0, 0]; // 4 bytes, max char size in UTF-8
        Encoding.UTF8.TryGetBytes(span, bytes, out int bytesWritten);
        writer.WriteRaw(bytes.Slice(0, bytesWritten));
        return bytesWritten;
    }
}
