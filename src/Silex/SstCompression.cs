namespace Silex;

/// <summary>
/// Compression applied independently to data blocks written to SST files.
/// </summary>
public enum SstCompression : byte
{
    None = 0,
    Lz4 = 1,
    Zstandard = 2,
}
