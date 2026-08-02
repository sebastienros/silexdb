using System.Runtime.CompilerServices;

namespace Silex;

internal static class RecordValueEncoding
{
    // Zero remains the legacy tombstone code. A rarely written five-byte sentinel represents a live empty
    // value, keeping every existing non-empty value and tombstone byte-compatible on disk.
    internal const int EmptyValueLengthCode = -1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int EncodeLength(int length, bool isTombstone)
    {
        return isTombstone ? 0 : length == 0 ? EmptyValueLengthCode : length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int DecodeLength(int encodedLength, out bool isTombstone)
    {
        isTombstone = encodedLength == 0;
        return encodedLength == EmptyValueLengthCode ? 0 : encodedLength;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetEncodedLengthSize(int length, bool isTombstone)
    {
        var value = (uint)EncodeLength(length, isTombstone);
        var size = 1;

        while (value > 0x7F)
        {
            size++;
            value >>= 7;
        }

        return size;
    }
}
