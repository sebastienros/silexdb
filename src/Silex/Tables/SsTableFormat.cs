using Silex.Buffers;
using System.Buffers.Binary;

namespace Silex.Tables;

internal static class SsTableFormat
{
    public const int LegacyVersion = 0;

    /// <summary>The newest table layout a builder can be asked for (2: with a version-prefix filter trailer).</summary>
    public const int CurrentVersion = 2;

    /// <summary>
    /// The newest footer version this reader accepts. Versions 3 and 4 are versions 1 and 2 whose blocks may carry
    /// a key-window index (see <see cref="Blocks.BlockKeyWindows"/>); the blocks flag it themselves, the footer
    /// version only keeps readers that do not understand the index from opening the table.
    /// </summary>
    public const int MaxReadableVersion = 4;

    private const int KeyWindowVersionDelta = 2;
    public const int FooterLength = sizeof(ulong) + sizeof(uint);
    public const int MaxCompressedBlockUncompressedLength = 64 * 1024 * 1024;

    private const ulong Magic = 0x54535358454C4953;

    public static int GetFooterVersion(bool hasVersionPrefixBloom, bool hasKeyWindows) =>
        (hasVersionPrefixBloom ? 2 : 1) + (hasKeyWindows ? KeyWindowVersionDelta : 0);

    public static bool HasVersionPrefixBloom(int version) => version is 2 or 4;

    public static void WriteFooter(ref EncoderBinaryWriter writer, int version = CurrentVersion)
    {
        writer.WriteUInt64(Magic);
        writer.WriteUInt32((uint)version);
    }

    public static int TryReadVersion(ReadOnlySpan<byte> footer)
    {
        if (footer.Length != FooterLength
            || BinaryPrimitives.ReadUInt64LittleEndian(footer) != Magic)
        {
            return LegacyVersion;
        }

        var version = BinaryPrimitives.ReadUInt32LittleEndian(footer[sizeof(ulong)..]);
        if (version is 0 or > MaxReadableVersion)
        {
            throw new InvalidDataException($"Unsupported SST format version: {version}.");
        }

        return (int)version;
    }
}
