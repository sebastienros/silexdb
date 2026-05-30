namespace Silex.Blocks;

/// <summary>
/// Describes a single block entry whose key has already been encoded into a shared buffer.
/// </summary>
/// <param name="KeyOffset">The offset of the encoded key in the shared key buffer.</param>
/// <param name="KeyLength">The length, in bytes, of the encoded key.</param>
/// <param name="Value">The (not yet encoded) value associated with the key.</param>
public readonly record struct BlockEntry<TValue>(int KeyOffset, int KeyLength, TValue Value);
