namespace Silex.Blocks;

/// <summary>
/// Describes a single block entry whose key and value bytes have already been copied into shared buffers.
/// </summary>
/// <param name="KeyOffset">The offset of the encoded key in the shared key buffer.</param>
/// <param name="KeyLength">The length, in bytes, of the encoded key.</param>
/// <param name="ValueOffset">The offset of the value in the shared value buffer.</param>
/// <param name="ValueLength">The value length in bytes, or <c>-1</c> for a tombstone.</param>
internal readonly record struct BlockEntry(int KeyOffset, int KeyLength, int ValueOffset, int ValueLength)
{
    public bool IsTombstone => ValueLength < 0;
    public int StoredValueLength => IsTombstone ? 0 : ValueLength;
}
