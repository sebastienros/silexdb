namespace Silex.BloomFilters;

internal static class BloomFilterPersistence
{
    private const uint MarkerPrefix = 0x584C4200; // "BLX" followed by the algorithm version.
    private const uint MarkerMask = 0xFFFFFF00;

    public const uint VersionedSentinel = 0;
    public const int LegacyFooterLength = 2 * sizeof(uint);
    public const int VersionedFooterLength = 4 * sizeof(uint);

    public static uint EncodeMarker(int algorithmVersion)
    {
        if ((uint)(algorithmVersion - 1) >= byte.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(algorithmVersion));
        }

        return MarkerPrefix | (uint)algorithmVersion;
    }

    public static bool TryDecodeMarker(uint marker, out int algorithmVersion)
    {
        algorithmVersion = (int)(marker & byte.MaxValue);
        return (marker & MarkerMask) == MarkerPrefix && algorithmVersion != 0;
    }
}
