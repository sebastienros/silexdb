using System.Buffers;
using System.Buffers.Binary;
using System.Text.Unicode;

namespace Silex.Keys;

/// <summary>
/// Reads ordered components from a key without allocating.
/// </summary>
/// <remarks>
/// Decoding is strict: only canonical output of <see cref="OrderedKeyWriter"/> is accepted, so each key has
/// exactly one decoding. A read that does not return <see cref="OrderedKeyDecodeStatus.Success"/> leaves the
/// reader position unchanged. For <see cref="OrderedKeyDecodeStatus.DestinationTooSmall"/> the
/// out length is the exact capacity required, and destination contents are unspecified.
/// </remarks>
public ref struct OrderedKeyReader
{
    private readonly ReadOnlySpan<byte> _source;
    private int _position;

    /// <summary>Creates a reader over <paramref name="source"/>.</summary>
    public OrderedKeyReader(ReadOnlySpan<byte> source)
    {
        _source = source;
        _position = 0;
    }

    /// <summary>The number of bytes consumed.</summary>
    public readonly int Position => _position;

    /// <summary>The unread tail.</summary>
    public readonly ReadOnlySpan<byte> Remaining => _source.Slice(_position);

    /// <summary>Whether all input was consumed.</summary>
    public readonly bool IsEnd => _position == _source.Length;

    /// <summary>Reports the next component's kind and order after validating its complete encoding.</summary>
    public readonly OrderedKeyDecodeStatus TryPeek(out OrderedKeyComponent kind, out OrderedKeyOrder order)
    {
        var status = Parse(Remaining, out var c);
        kind = c.Kind;
        order = c.Order;
        return status;
    }

    /// <summary>Reports the encoded size of the next component and validates it, without decoding its value.</summary>
    public readonly OrderedKeyDecodeStatus TryPeekLength(out int encodedLength)
    {
        var status = Parse(Remaining, out var c);
        encodedLength = c.Total;
        return status;
    }

    /// <summary>Reports the decoded payload size of the next bytes or string component (UTF-8 bytes for strings).</summary>
    public readonly OrderedKeyDecodeStatus TryPeekPayloadLength(out int payloadLength)
    {
        var status = Parse(Remaining, out var c);
        payloadLength = c.Payload;
        if (status == OrderedKeyDecodeStatus.Success
            && c.Kind is not (OrderedKeyComponent.Bytes or OrderedKeyComponent.String))
        {
            return OrderedKeyDecodeStatus.UnexpectedComponent;
        }
        return status;
    }

    /// <summary>Skips one validated component of any kind.</summary>
    public OrderedKeyDecodeStatus TrySkip()
    {
        var status = Parse(Remaining, out var c);
        if (status == OrderedKeyDecodeStatus.Success) _position += c.Total;
        return status;
    }

    /// <summary>Reads a boolean component.</summary>
    public OrderedKeyDecodeStatus TryReadBoolean(out bool value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        value = false;
        var status = Expect(OrderedKeyComponent.Boolean, order, out var c);
        if (status != OrderedKeyDecodeStatus.Success) return status;
        value = c.BoolValue;
        _position += c.Total;
        return status;
    }

    /// <summary>Reads a signed 64-bit integer component.</summary>
    public OrderedKeyDecodeStatus TryReadInt64(out long value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        value = 0;
        var status = ReadFixed(OrderedKeyComponent.Int64, order, out var bits);
        if (status == OrderedKeyDecodeStatus.Success) value = (long)(bits ^ (1UL << 63));
        return status;
    }

    /// <summary>Reads an unsigned 64-bit integer component.</summary>
    public OrderedKeyDecodeStatus TryReadUInt64(out ulong value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        return ReadFixed(OrderedKeyComponent.UInt64, order, out value);
    }

    /// <summary>Reads a double component; non-canonical NaN encodings are <see cref="OrderedKeyDecodeStatus.Malformed"/>.</summary>
    public OrderedKeyDecodeStatus TryReadDouble(out double value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        value = 0;
        var status = ReadFixed(OrderedKeyComponent.Double, order, out var bits, keepPosition: true);
        if (status != OrderedKeyDecodeStatus.Success) return status;
        if (!OrderedKey.TryFromOrderedDoubleBits(bits, out value)) return OrderedKeyDecodeStatus.Malformed;
        _position += OrderedKey.DoubleLength;
        return status;
    }

    /// <summary>Reads an exact decimal component with the minimum possible scale and positive scale-zero zero.</summary>
    /// <remarks>Payloads not exactly representable as decimal are malformed; failed reads do not advance.</remarks>
    public OrderedKeyDecodeStatus TryReadDecimal(out decimal value, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        value = 0;
        var status = Expect(OrderedKeyComponent.Decimal, order, out var c);
        if (status != OrderedKeyDecodeStatus.Success) return status;
        value = c.DecimalValue;
        _position += c.Total;
        return status;
    }

    /// <summary>Reads a bytes component into <paramref name="destination"/>.</summary>
    /// <param name="destination">Receives the unescaped payload.</param>
    /// <param name="length">The payload length written, or the required length if the destination is too small.</param>
    /// <param name="order">The expected order.</param>
    public OrderedKeyDecodeStatus TryReadBytes(Span<byte> destination, out int length, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        length = 0;
        var status = Expect(OrderedKeyComponent.Bytes, order, out var c);
        if (status != OrderedKeyDecodeStatus.Success) return status;
        length = c.Payload;
        if (destination.Length < c.Payload) return OrderedKeyDecodeStatus.DestinationTooSmall;
        Unescape(Remaining.Slice(1, c.Total - 3), c.Mask, destination);
        _position += c.Total;
        return status;
    }

    /// <summary>Reads a string component into <paramref name="destination"/>, validating strict UTF-8.</summary>
    /// <param name="destination">Receives the UTF-16 characters.</param>
    /// <param name="length">The chars written, or the exact chars required if the destination is too small.</param>
    /// <param name="order">The expected order.</param>
    public OrderedKeyDecodeStatus TryReadString(Span<char> destination, out int length, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        length = 0;
        var status = Expect(OrderedKeyComponent.String, order, out var c);
        if (status != OrderedKeyDecodeStatus.Success) return status;
        status = DecodeUtf8(Remaining.Slice(1, c.Total - 3), c.Mask, destination, out length);
        if (status == OrderedKeyDecodeStatus.Success) _position += c.Total;
        return status;
    }

    /// <summary>Reads a string component as strict UTF-8 bytes.</summary>
    /// <param name="destination">Receives the decoded string content as unescaped UTF-8 bytes, not a JSON token.</param>
    /// <param name="length">The bytes written, or the exact bytes required if the destination is too small; zero for other failures.</param>
    /// <param name="order">The expected order.</param>
    /// <remarks>Malformed UTF-8 is rejected even when the destination is too small. Failed reads do not advance;
    /// destination contents are unspecified on failure.</remarks>
    public OrderedKeyDecodeStatus TryReadUtf8String(Span<byte> destination, out int length, OrderedKeyOrder order = OrderedKeyOrder.Ascending)
    {
        length = 0;
        var status = Expect(OrderedKeyComponent.String, order, out var c);
        if (status != OrderedKeyDecodeStatus.Success) return status;
        var body = Remaining.Slice(1, c.Total - 3);
        if (destination.Length < c.Payload)
        {
            // Validate into the existing decoder's scratch before reporting a required byte count.
            if (DecodeUtf8(body, c.Mask, Span<char>.Empty, out _) == OrderedKeyDecodeStatus.Malformed)
                return OrderedKeyDecodeStatus.Malformed;
            length = c.Payload;
            return OrderedKeyDecodeStatus.DestinationTooSmall;
        }
        Unescape(body, c.Mask, destination);
        if (!Utf8.IsValid(destination.Slice(0, c.Payload))) return OrderedKeyDecodeStatus.Malformed;
        length = c.Payload;
        _position += c.Total;
        return OrderedKeyDecodeStatus.Success;
    }

    private OrderedKeyDecodeStatus ReadFixed(OrderedKeyComponent kind, OrderedKeyOrder order, out ulong bits, bool keepPosition = false)
    {
        bits = 0;
        var status = Expect(kind, order, out var c);
        if (status != OrderedKeyDecodeStatus.Success) return status;
        bits = BinaryPrimitives.ReadUInt64BigEndian(Remaining.Slice(1)) ^ (c.Mask == 0 ? 0UL : ulong.MaxValue);
        if (!keepPosition) _position += c.Total;
        return status;
    }

    private readonly OrderedKeyDecodeStatus Expect(OrderedKeyComponent kind, OrderedKeyOrder order, out Parsed c)
    {
        var status = Parse(Remaining, out c);
        if (status != OrderedKeyDecodeStatus.Success) return status;
        return c.Kind == kind && c.Order == order ? status : OrderedKeyDecodeStatus.UnexpectedComponent;
    }

    private struct Parsed
    {
        public OrderedKeyComponent Kind;
        public OrderedKeyOrder Order;
        public byte Mask;
        public bool BoolValue;
        public int Total;
        public int Payload;
        public decimal DecimalValue;
    }

    private static OrderedKeyDecodeStatus Parse(ReadOnlySpan<byte> src, out Parsed c)
    {
        c = default;
        if (src.IsEmpty) return OrderedKeyDecodeStatus.EndOfData;
        var tagByte = src[0];
        c.Mask = tagByte >= 0x80 ? (byte)0xFF : (byte)0;
        c.Order = c.Mask == 0 ? OrderedKeyOrder.Ascending : OrderedKeyOrder.Descending;
        var tag = (byte)(tagByte ^ c.Mask);
        switch (tag)
        {
            case 0x04:
            case 0x05:
                c.Kind = OrderedKeyComponent.Boolean;
                c.BoolValue = tag == 0x05;
                c.Total = 1;
                return OrderedKeyDecodeStatus.Success;
            case 0x20:
            case 0x21:
            case 0x22:
                c.Kind = (OrderedKeyComponent)tag;
                c.Total = 9;
                return src.Length >= 9 ? OrderedKeyDecodeStatus.Success : OrderedKeyDecodeStatus.Malformed;
            case 0x23:
                c.Kind = OrderedKeyComponent.Decimal;
                c.Total = OrderedKey.DecimalLength;
                if (src.Length < c.Total) return OrderedKeyDecodeStatus.Malformed;
                return OrderedDecimal.TryDecode(src.Slice(1, c.Total - 1), c.Mask, out c.DecimalValue)
                    ? OrderedKeyDecodeStatus.Success : OrderedKeyDecodeStatus.Malformed;
            case 0x10:
            case 0x11:
                c.Kind = (OrderedKeyComponent)tag;
                break;
            default:
                return OrderedKeyDecodeStatus.Malformed;
        }

        // Variable-length: a zero byte (equal to the mask) is followed by ~mask (escaped payload zero)
        // or by 01^mask (terminator).
        var zero = c.Mask;
        var escape = (byte)~c.Mask;
        var terminator = (byte)(1 ^ c.Mask);
        var i = 1;
        var payload = 0;
        while (true)
        {
            var j = src.Slice(i).IndexOf(zero);
            if (j < 0) return OrderedKeyDecodeStatus.Malformed;
            i += j;
            payload += j;
            if (i + 1 >= src.Length) return OrderedKeyDecodeStatus.Malformed;
            if (src[i + 1] == escape)
            {
                payload++;
                i += 2;
                continue;
            }
            if (src[i + 1] != terminator) return OrderedKeyDecodeStatus.Malformed;
            c.Total = i + 2;
            c.Payload = payload;
            return OrderedKeyDecodeStatus.Success;
        }
    }

    // body excludes the tag and the two-byte terminator.
    private static void Unescape(ReadOnlySpan<byte> body, byte mask, Span<byte> destination)
    {
        var p = 0;
        while (true)
        {
            var j = body.IndexOf(mask);
            var segment = j < 0 ? body : body.Slice(0, j);
            if (mask == 0)
            {
                segment.CopyTo(destination.Slice(p));
            }
            else
            {
                for (var k = 0; k < segment.Length; k++) destination[p + k] = (byte)~segment[k];
            }
            p += segment.Length;
            if (j < 0) return;
            destination[p++] = 0;
            body = body.Slice(j + 2);
        }
    }

    private static OrderedKeyDecodeStatus DecodeUtf8(ReadOnlySpan<byte> body, byte mask, Span<char> destination, out int length)
    {
        var sink = new Utf8Sink(stackalloc byte[256], stackalloc char[256], destination);
        while (true)
        {
            var j = body.IndexOf(mask);
            if (!sink.Append(j < 0 ? body : body.Slice(0, j), mask)) break;
            if (j < 0)
            {
                if (sink.Finish()) goto done;
                break;
            }
            if (!sink.AppendNul())
            {
                break;
            }
            body = body.Slice(j + 2);
        }
        length = 0;
        return OrderedKeyDecodeStatus.Malformed;

    done:
        length = sink.Total;
        return sink.Overflow ? OrderedKeyDecodeStatus.DestinationTooSmall : OrderedKeyDecodeStatus.Success;
    }

    // Chunked strict UTF-8 decoder; once the destination fills it keeps counting into scratch.
    private ref struct Utf8Sink
    {
        private readonly Span<byte> _chunk;
        private readonly Span<char> _scratch;
        private readonly Span<char> _destination;
        private int _filled;
        private int _outPos;

        public int Total;
        public bool Overflow;

        public Utf8Sink(Span<byte> chunk, Span<char> scratch, Span<char> destination)
        {
            _chunk = chunk;
            _scratch = scratch;
            _destination = destination;
            _filled = 0;
            _outPos = 0;
            Total = 0;
            Overflow = false;
        }

        public bool Append(ReadOnlySpan<byte> segment, byte mask)
        {
            while (!segment.IsEmpty)
            {
                var n = Math.Min(_chunk.Length - _filled, segment.Length);
                for (var k = 0; k < n; k++) _chunk[_filled + k] = (byte)(segment[k] ^ mask);
                _filled += n;
                segment = segment.Slice(n);
                if (_filled == _chunk.Length && !Flush(false)) return false;
            }
            return true;
        }

        public bool AppendNul()
        {
            if (_filled == _chunk.Length && !Flush(false)) return false;
            _chunk[_filled++] = 0;
            return true;
        }

        public bool Finish() => Flush(true);

        private bool Flush(bool final)
        {
            var input = _chunk.Slice(0, _filled);
            while (true)
            {
                var target = Overflow ? _scratch : _destination.Slice(_outPos);
                var status = Utf8.ToUtf16(input, target, out var read, out var written,
                    replaceInvalidSequences: false, isFinalBlock: final);
                Total += written;
                if (!Overflow) _outPos += written;
                input = input.Slice(read);
                if (status == OperationStatus.Done || status == OperationStatus.NeedMoreData) break;
                if (status != OperationStatus.DestinationTooSmall) return false;
                Overflow = true;
            }
            input.CopyTo(_chunk);
            _filled = input.Length;
            return true;
        }
    }
}
