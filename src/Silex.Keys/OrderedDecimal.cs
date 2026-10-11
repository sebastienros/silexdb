using System.Buffers.Binary;

namespace Silex.Keys;

internal static class OrderedDecimal
{
    private static readonly UInt192 Maximum = new UInt192(ulong.MaxValue, uint.MaxValue, 0).Scale(28);

    internal static void Encode(decimal value, Span<byte> destination)
    {
        Span<int> bits = stackalloc int[4];
        decimal.GetBits(value, bits);
        var magnitude = new UInt192((uint)bits[0] | (ulong)(uint)bits[1] << 32, (uint)bits[2], 0);
        magnitude = magnitude.Scale(28 - ((bits[3] >> 16) & 0xFF));
        if (bits[3] < 0) magnitude = magnitude.Negate();
        BinaryPrimitives.WriteUInt64BigEndian(destination, magnitude.High ^ (1UL << 63));
        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(8), magnitude.Middle);
        BinaryPrimitives.WriteUInt64BigEndian(destination.Slice(16), magnitude.Low);
    }

    internal static bool TryDecode(ReadOnlySpan<byte> payload, byte mask, out decimal value)
    {
        value = 0;
        var wordMask = mask == 0 ? 0UL : ulong.MaxValue;
        var magnitude = new UInt192(
            BinaryPrimitives.ReadUInt64BigEndian(payload.Slice(16)) ^ wordMask,
            BinaryPrimitives.ReadUInt64BigEndian(payload.Slice(8)) ^ wordMask,
            BinaryPrimitives.ReadUInt64BigEndian(payload) ^ wordMask ^ (1UL << 63));
        var negative = (magnitude.High & (1UL << 63)) != 0;
        // Unsigned negation also handles the magnitude of the minimum signed-192 payload safely.
        if (negative) magnitude = magnitude.Negate();
        if (magnitude.IsGreaterThan(Maximum)) return false;
        if ((magnitude.Low | magnitude.Middle | magnitude.High) == 0) return true;

        byte scale = 28;
        while (scale > 0)
        {
            var quotient = magnitude.Divide10(out var remainder);
            if (remainder != 0) break;
            magnitude = quotient;
            scale--;
        }
        if (magnitude.High != 0 || magnitude.Middle > uint.MaxValue) return false;
        value = new decimal(unchecked((int)magnitude.Low), unchecked((int)(magnitude.Low >> 32)),
            unchecked((int)magnitude.Middle), negative, scale);
        return true;
    }

    private readonly struct UInt192(ulong low, ulong middle, ulong high)
    {
        internal readonly ulong Low = low;
        internal readonly ulong Middle = middle;
        internal readonly ulong High = high;

        internal UInt192 Scale(int power)
        {
            var result = this;
            while (power >= 9)
            {
                result = result.Multiply(1_000_000_000);
                power -= 9;
            }
            uint factor = 1;
            for (var i = 0; i < power; i++) factor *= 10;
            return result.Multiply(factor);
        }

        private UInt192 Multiply(uint factor)
        {
            var low = (UInt128)Low * factor;
            var middle = (UInt128)Middle * factor + (low >> 64);
            var high = (UInt128)High * factor + (middle >> 64);
            return new UInt192((ulong)low, (ulong)middle, (ulong)high);
        }

        internal UInt192 Negate()
        {
            var low = unchecked(~Low + 1);
            var middle = unchecked(~Middle + (low == 0 ? 1UL : 0));
            var high = unchecked(~High + (low == 0 && middle == 0 ? 1UL : 0));
            return new UInt192(low, middle, high);
        }

        internal UInt192 Divide10(out uint remainder)
        {
            var high = High / 10;
            var middleDividend = (UInt128)(High % 10) << 64 | Middle;
            var middle = (ulong)(middleDividend / 10);
            var lowDividend = (middleDividend % 10) << 64 | Low;
            remainder = (uint)(lowDividend % 10);
            return new UInt192((ulong)(lowDividend / 10), middle, high);
        }

        internal bool IsGreaterThan(UInt192 other) =>
            High != other.High ? High > other.High :
            Middle != other.Middle ? Middle > other.Middle : Low > other.Low;
    }
}
