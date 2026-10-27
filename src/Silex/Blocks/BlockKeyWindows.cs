using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Silex.Blocks;

/// <summary>
/// Helpers for the optional key-window index stored in a block (see <see cref="DefaultBlockEncoder"/>).
/// </summary>
/// <remarks>
/// <para>
/// The index strips the prefix shared by every key of the block and keeps, for each entry, the next eight key
/// bytes as a big-endian number zero-padded on the right (the entry's "window"). Windows are non-decreasing in
/// key order, so a search can binary-search the dense window array without decoding the varint-prefixed
/// entries, and only touches an entry when two windows tie or to confirm a hit.
/// </para>
/// <para>
/// When the windows of two keys differ, their order is the order of the keys: at the first differing byte the
/// greater window holds a real key byte, and the smaller one either holds a smaller byte or the key ended there.
/// Equal windows say nothing about the order (the keys may differ past the window, or differ only by trailing
/// zero bytes), so ties are resolved by comparing the full keys.
/// </para>
/// </remarks>
internal static class BlockKeyWindows
{
    /// <summary>The flag set in a block's trailing entry count when the block carries a key-window index.</summary>
    public const ushort CountFlag = 0x8000;

    /// <summary>The largest shared prefix stored for a block; longer shared prefixes are truncated.</summary>
    public const int MaxPrefixLength = byte.MaxValue;

    public const int WindowLength = sizeof(ulong);

    /// <summary>The bytes the index adds to a block besides the per-entry windows: the shared prefix and its length.</summary>
    public static int GetFixedOverhead(int prefixLength) => prefixLength + 1;

    public static int GetPrefixLength(ReadOnlySpan<byte> firstKey, ReadOnlySpan<byte> lastKey)
    {
        return Math.Min(firstKey.CommonPrefixLength(lastKey), MaxPrefixLength);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong GetWindow(ReadOnlySpan<byte> key, int prefixLength)
    {
        var remaining = key.Length - prefixLength;
        if (remaining >= WindowLength)
        {
            return BinaryPrimitives.ReadUInt64BigEndian(key.Slice(prefixLength, WindowLength));
        }

        return GetShortWindow(key[prefixLength..]);
    }

    private static ulong GetShortWindow(ReadOnlySpan<byte> tail)
    {
        ulong window = 0;
        for (var i = 0; i < tail.Length; i++)
        {
            window |= (ulong)tail[i] << (56 - (i * 8));
        }

        return window;
    }
}
