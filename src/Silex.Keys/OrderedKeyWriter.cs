using System.Buffers.Binary;
using System.Text.Unicode;

namespace Silex.Keys;

/// <summary>
/// Appends ordered components to a caller-owned buffer without allocating.
/// </summary>
/// <remarks>
/// Every <c>TryWrite*</c> method either appends the complete component and returns <see langword="true"/>, or
/// returns <see langword="false"/> because the remaining capacity is smaller than the exact encoded size; in
/// the latter case <see cref="Written"/> is unchanged and the unwritten tail's contents are unspecified. Invalid
/// input (an unpaired surrogate) throws <see cref="ArgumentException"/> before anything is written. Inputs must not
/// overlap the destination. Default-constructed writers have zero capacity.
/// </remarks>
public ref struct OrderedKeyWriter
{
    private readonly Span<byte> _destination;
    private int _written;

    /// <summary>Creates a writer over <paramref name="destination"/>, starting at its first byte.</summary>
    public OrderedKeyWriter(Span<byte> destination)
    {
        _destination = destination;
        _written = 0;
    }

    /// <summary>The number of bytes written so far.</summary>
    public readonly int Written => _written;

    /// <summary>The unused capacity.</summary>
    public readonly int Remaining => _destination.Length - _written;

    /// <summary>The encoded key so far (a valid tuple prefix of any key that continues it).</summary>
    public readonly ReadOnlySpan<byte> WrittenSpan => _destination.Slice(0, _written);

    /// <summary>Discards all written components.</summary>
    public void Reset() => _written = 0;

    /// <summary>Appends a boolean component.</summary>
    public bool TryWriteBoolean(bool value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        if (!TryBegin(OrderedKey.BooleanLength, out var s)) return false;
        s[0] = OrderedKey.Tag(OrderedKeyComponent.Boolean, value, order);
        return true;
    }

    /// <summary>Appends a signed 64-bit integer component.</summary>
    public bool TryWriteInt64(long value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        return TryWriteFixed(OrderedKeyComponent.Int64, (ulong)value ^ (1UL << 63), order);
    }

    /// <summary>Appends an unsigned 64-bit integer component.</summary>
    public bool TryWriteUInt64(ulong value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        return TryWriteFixed(OrderedKeyComponent.UInt64, value, order);
    }

    /// <summary>Appends a double component using the total order documented on <see cref="OrderedKey"/>.</summary>
    public bool TryWriteDouble(double value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        return TryWriteFixed(OrderedKeyComponent.Double, OrderedKey.ToOrderedDoubleBits(value), order);
    }

    /// <summary>Appends an exact decimal number; decimal scale and signed zero are normalized.</summary>
    public bool TryWriteDecimal(decimal value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        if (!TryBegin(OrderedKey.DecimalLength, out var s)) return false;
        s[0] = OrderedKey.Tag(OrderedKeyComponent.Decimal, false, order);
        OrderedDecimal.Encode(value, s.Slice(1));
        Complement(s, order);
        return true;
    }

    /// <summary>Appends a bytes component; its size is <see cref="OrderedKey.GetBytesLength"/>.</summary>
    public bool TryWriteBytes(ReadOnlySpan<byte> value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        if (!TryBegin(OrderedKey.GetBytesLength(value), out var s)) return false;
        s[0] = OrderedKey.Tag(OrderedKeyComponent.Bytes, false, order);
        var p = 1;
        while (true)
        {
            var i = value.IndexOf((byte)0);
            if (i < 0)
            {
                value.CopyTo(s.Slice(p));
                p += value.Length;
                break;
            }
            value.Slice(0, i).CopyTo(s.Slice(p));
            p += i;
            s[p++] = 0;
            s[p++] = 0xFF;
            value = value.Slice(i + 1);
        }
        s[p++] = 0;
        s[p] = 1;
        Complement(s, order);
        return true;
    }

    /// <summary>Appends a string component as strict UTF-8; its size is <see cref="OrderedKey.GetStringLength"/>.</summary>
    /// <exception cref="ArgumentException">The string contains an unpaired surrogate.</exception>
    public bool TryWriteString(ReadOnlySpan<char> value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        if (!TryBegin(OrderedKey.GetStringLength(value), out var s)) return false;
        s[0] = OrderedKey.Tag(OrderedKeyComponent.String, false, order);
        var p = 1;
        while (true)
        {
            var i = value.IndexOf('\0');
            var segment = i < 0 ? value : value.Slice(0, i);
            Utf8.FromUtf16(segment, s.Slice(p), out _, out var w, replaceInvalidSequences: false, isFinalBlock: true);
            p += w;
            if (i < 0) break;
            s[p++] = 0;
            s[p++] = 0xFF;
            value = value.Slice(i + 1);
        }
        s[p++] = 0;
        s[p] = 1;
        Complement(s, order);
        return true;
    }

    private bool TryWriteFixed(OrderedKeyComponent kind, ulong ordered, OrderedKeyOrder order)
    {
        if (!TryBegin(9, out var s)) return false;
        s[0] = OrderedKey.Tag(kind, false, order);
        BinaryPrimitives.WriteUInt64BigEndian(s.Slice(1), ordered);
        Complement(s, order);
        return true;
    }

    private bool TryBegin(int length, out Span<byte> component)
    {
        if (length > _destination.Length - _written)
        {
            component = default;
            return false;
        }
        component = _destination.Slice(_written, length);
        _written += length;
        return true;
    }

    private static void Complement(Span<byte> component, OrderedKeyOrder order)
    {
        if (order == OrderedKeyOrder.Ascending) return;
        // The tag was already complemented; complementing it again would undo that.
        for (var i = 1; i < component.Length; i++) component[i] = (byte)~component[i];
    }
}
