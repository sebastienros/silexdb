using Silex.Buffers;
using System.Buffers.Binary;

namespace Silex.Tables;

internal static class SsTableFormat
{
    public const int LegacyVersion = 0;
    public const int CurrentVersion = 1;
    public const int FooterLength = sizeof(ulong) + sizeof(uint);
    public const int MaxCompressedBlockUncompressedLength = 64 * 1024 * 1024;

    private const ulong Magic = 0x54535358454C4953;

    public static void WriteFooter(ref EncoderBinaryWriter writer)
    {
        writer.WriteUInt64(Magic);
        writer.WriteUInt32(CurrentVersion);
    }

    public static int TryReadVersion(ReadOnlySpan<byte> footer)
    {
        if (footer.Length != FooterLength
            || BinaryPrimitives.ReadUInt64LittleEndian(footer) != Magic)
        {
            return LegacyVersion;
        }

        var version = BinaryPrimitives.ReadUInt32LittleEndian(footer[sizeof(ulong)..]);
        if (version is 0 or > CurrentVersion)
        {
            throw new InvalidDataException($"Unsupported SST format version: {version}.");
        }

        return (int)version;
    }
}
