using System.Buffers.Binary;
using Silex.Wal;

namespace Silex.Tables;

internal static class VersionPrefixBloom
{
    internal const int TrailerLength = 24;
    internal const uint Magic = 0x58465056;

    internal static bool IsPrefix(ReadOnlySpan<byte> prefix) =>
        prefix.Length >= 3 && prefix[0] == 1 && prefix[^2] == 0 && prefix[^1] == 0;

    internal static bool IsVersionKey(ReadOnlySpan<byte> key) =>
        key.Length >= 11 && IsPrefix(key[..^sizeof(long)]);

    internal static uint Checksum(ReadOnlySpan<byte> bytes, ReadOnlySpan<byte> descriptor) =>
        Crc32C.Compute(bytes) ^ Crc32C.Compute(descriptor);

    internal static void WriteDescriptor(Span<byte> descriptor, int hashes, int algorithm, long offset)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(descriptor, (uint)hashes);
        BinaryPrimitives.WriteUInt32LittleEndian(descriptor[4..], (uint)algorithm);
        BinaryPrimitives.WriteUInt64LittleEndian(descriptor[8..], (ulong)offset);
    }
}
