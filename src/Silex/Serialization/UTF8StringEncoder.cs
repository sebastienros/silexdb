using Silex.Buffers;
using System.Text;

namespace Silex.Serialization;

public sealed class UTF8StringEncoder : IBinaryEncoder<string>
{
    internal const int StackAllocThreshold = 100;

    public string Decode(ReadOnlySpan<byte> data)
    {
        return Encoding.UTF8.GetString(data);
    }

    public int GetLength(string value)
    {
        return Encoding.UTF8.GetByteCount(value);
    }

    public string GetTombstoneValue()
    {
        return null!;
    }

    public bool IsTombstoneValue(string value)
    {
        return value is null;
    }

    public int Encode(string value, ref EncoderBinaryWriter writer)
    {
        if (string.IsNullOrEmpty(value))
        {
            return 0;
        }

        var length = Encoding.UTF8.GetByteCount(value);

        var span = value.AsSpan();
        Span<byte> bytes = stackalloc byte[StackAllocThreshold];

        if (length <= StackAllocThreshold)
        {
            var written = Encoding.UTF8.GetBytes(span, bytes);
            writer.WriteRaw(bytes.Slice(0, written));
            return written;
        }

        int current = 0;
        var remaining = span.Length;

        while (remaining > 0)
        {            
            // Process a max number of chars of 128 / 4 which is the worst case
            var slice = span.Slice(current, Math.Min(128 / 4, remaining));
            var written = Encoding.UTF8.GetBytes(slice, bytes);
            writer.WriteRaw(bytes.Slice(0, written));
            current += slice.Length;
            remaining -= slice.Length;
        }

        return current;
    }
}
