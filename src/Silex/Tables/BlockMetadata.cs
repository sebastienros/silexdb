namespace Silex.Tables;

internal sealed class BlockMetadata : IDisposable
{
    public int Index { get; set; }
    public long Offset { get; set; }
    public int UncompressedLength { get; set; }
    public SstCompression Compression { get; set; }
    public uint Checksum { get; set; }
    public required OwnedByteSlice FirstKeyOwner { get; set; }
    public required OwnedByteSlice LastKeyOwner { get; set; }
    public ByteSlice FirstKey => FirstKeyOwner.Slice;
    public ByteSlice LastKey => LastKeyOwner.Slice;

    public void Dispose()
    {
        FirstKeyOwner.Dispose();
        LastKeyOwner.Dispose();
    }
}
