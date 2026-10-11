using System.Text;

namespace Silex.Keys;

/// <summary>
/// Shared constants and exact-size calculators for the ordered composite key format.
/// </summary>
/// <remarks>
/// <para>
/// A key is the concatenation of self-delimiting components. Every component starts with a type tag
/// in <c>0x01..0x7F</c> (ascending) or its bitwise complement in <c>0x80..0xFE</c> (descending); <c>0x00</c> and
/// <c>0xFF</c> are never tags. Because no component encoding is a prefix of another, comparing two keys with
/// unsigned byte-wise lexicographic order is identical to comparing their component sequences in order, and a
/// shorter tuple that is a prefix of a longer one sorts first.
/// </para>
/// <list type="bullet">
/// <item><description>Boolean: tag <c>0x04</c> (false) or <c>0x05</c> (true), 1 byte.</description></item>
/// <item><description>Bytes <c>0x10</c> and String <c>0x11</c>: tag, payload with every <c>0x00</c> written as
/// <c>00 FF</c>, then terminator <c>00 01</c>. Strings are strict UTF-8; order is by Unicode code point.</description></item>
/// <item><description>Int64 <c>0x20</c>: tag + big-endian value with the sign bit flipped (9 bytes).</description></item>
/// <item><description>UInt64 <c>0x21</c>: tag + big-endian value (9 bytes).</description></item>
/// <item><description>Double <c>0x22</c>: tag + big-endian IEEE-754 bits transformed to a total order (9 bytes):
/// -Infinity &lt; negative finite &lt; -0.0 &lt; +0.0 &lt; positive finite &lt; +Infinity &lt; NaN. All NaNs encode as
/// one canonical NaN, so non-canonical NaN payloads are not preserved.</description></item>
/// <item><description>Descending: the whole component (tag, payload, terminator) is bitwise complemented, which
/// exactly reverses the order of that component because encodings are prefix-free.</description></item>
/// </list>
/// <para>
/// Different kinds are separated by tag order (Boolean &lt; Bytes &lt; String &lt; Int64 &lt; UInt64 &lt; Double, then
/// all descending kinds in reverse). Integers and doubles are never interleaved numerically. The format defines
/// no NULL, collation, case folding, decimal, date or JSON ordering; those semantics belong to the layer using it.
/// </para>
/// </remarks>
public static class OrderedKey
{
    /// <summary>Encoded size of a boolean component.</summary>
    public const int BooleanLength = 1;

    /// <summary>Encoded size of an <see cref="long"/> component.</summary>
    public const int Int64Length = 9;

    /// <summary>Encoded size of a <see cref="ulong"/> component.</summary>
    public const int UInt64Length = 9;

    /// <summary>Encoded size of a <see cref="double"/> component.</summary>
    public const int DoubleLength = 9;

    internal const ulong CanonicalNaNBits = 0x7FF8000000000000UL;

    private static readonly UTF8Encoding StrictUtf8 = new(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);

    /// <summary>Exact encoded size of a bytes component (identical for both orders).</summary>
    /// <exception cref="OverflowException">The size does not fit in <see cref="int"/>.</exception>
    public static int GetBytesLength(ReadOnlySpan<byte> value)
    {
        return checked(3 + value.Length + value.Count((byte)0));
    }

    /// <summary>Exact encoded size of a string component (identical for both orders).</summary>
    /// <exception cref="ArgumentException">The string contains an unpaired surrogate.</exception>
    /// <exception cref="OverflowException">The size does not fit in <see cref="int"/>.</exception>
    public static int GetStringLength(ReadOnlySpan<char> value)
    {
        return checked(3 + StrictUtf8.GetByteCount(value) + value.Count('\0'));
    }

    internal static byte Tag(OrderedKeyComponent kind, bool boolValue, OrderedKeyOrder order)
    {
        var tag = (byte)kind;
        if (kind == OrderedKeyComponent.Boolean && boolValue) tag++;
        return order == OrderedKeyOrder.Descending ? (byte)~tag : tag;
    }

    internal static byte Mask(OrderedKeyOrder order) => order == OrderedKeyOrder.Descending ? (byte)0xFF : (byte)0;

    internal static ulong ToOrderedDoubleBits(double value)
    {
        var bits = double.IsNaN(value) ? CanonicalNaNBits : (ulong)BitConverter.DoubleToInt64Bits(value);
        return (bits & (1UL << 63)) != 0 ? ~bits : bits | (1UL << 63);
    }

    internal static bool TryFromOrderedDoubleBits(ulong ordered, out double value)
    {
        var bits = (ordered & (1UL << 63)) != 0 ? ordered & ~(1UL << 63) : ~ordered;
        value = BitConverter.Int64BitsToDouble((long)bits);
        return !double.IsNaN(value) || bits == CanonicalNaNBits;
    }
}
