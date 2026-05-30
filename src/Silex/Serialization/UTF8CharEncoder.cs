using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;

namespace Silex.Serialization;

/// <summary>
/// Encodes a <see cref="char"/> as a fixed-width, big-endian 16-bit value. A bytewise comparison of the
/// encoded bytes therefore matches <see cref="Comparer{T}.Default"/> for <see cref="char"/> (which orders
/// by UTF-16 code unit). A variable-length UTF-8 encoding is deliberately not used: a lone surrogate is
/// not a valid scalar value and would otherwise be replaced (collapsing distinct chars to identical
/// bytes), which would break the order-preserving, byte-comparable key contract.
/// </summary>
public sealed class UTF8CharEncoder : IBinaryEncoder<char>
{
    public char Decode(ReadOnlySpan<byte> data)
    {
        Debug.Assert(data.Length == sizeof(char));
        return (char)BinaryPrimitives.ReadUInt16BigEndian(data);
    }

    public int GetLength(char value) => sizeof(char);

    public char GetTombstoneValue() => '\0';

    public bool IsTombstoneValue(char value) => value == '\0';

    public int Encode(char value, ref EncoderBinaryWriter writer)
    {
        var length = sizeof(char);
        Span<byte> span = stackalloc byte[length];
        BinaryPrimitives.WriteUInt16BigEndian(span, value);
        writer.WriteRaw(span);

        return length;
    }
}
