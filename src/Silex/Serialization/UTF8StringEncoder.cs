using Silex.Buffers;
using System.Text;

namespace Silex.Serialization;

public sealed class UTF8StringEncoder : IBinaryEncoder<string>
{
    internal const int StackAllocThreshold = 100;

    // String keys are encoded as UTF-8. UTF-8 byte order equals Unicode code-point (scalar) order, which is
    // NOT the same as StringComparer.Ordinal: ordinal compares UTF-16 code units, so a supplementary
    // character (encoded as a surrogate pair starting at U+D800) would sort before BMP characters such as
    // U+E000 under ordinal, but after them by code point / UTF-8 bytes. To keep the on-disk byte order
    // consistent with the typed comparison (required by the span-based byte core), keys are compared by
    // code point. Equality only needs to agree for round-trippable strings, where ordinal equality matches.
    IComparer<string> IBinaryEncoder<string>.Comparer => CodePointComparer.Instance;

    IEqualityComparer<string> IBinaryEncoder<string>.EqualityComparer => StringComparer.Ordinal;

    private sealed class CodePointComparer : IComparer<string>
    {
        public static readonly CodePointComparer Instance = new();

        public int Compare(string? x, string? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x is null) return -1;
            if (y is null) return 1;

            var ex = x.EnumerateRunes();
            var ey = y.EnumerateRunes();

            while (true)
            {
                var hasX = ex.MoveNext();
                var hasY = ey.MoveNext();

                if (!hasX) return hasY ? -1 : 0;
                if (!hasY) return 1;

                var diff = ex.Current.Value - ey.Current.Value;
                if (diff != 0) return diff;
            }
        }
    }

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
