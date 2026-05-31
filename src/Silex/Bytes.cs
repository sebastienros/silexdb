using Silex.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Silex;

/// <summary>
/// Represents a set of bytes.
/// </summary>
[DebuggerDisplay($"{{{nameof(GetDebuggerDisplay)}(),nq}}")]
public readonly struct Bytes : IEquatable<Bytes>, IComparable<Bytes>
{
    private readonly MemoryOwner<byte> _data;

    public static readonly IComparer<Bytes> Comparer = BytesComparer.Instance;
    public static readonly IEqualityComparer<Bytes> EqualityComparer = BytesComparer.Instance;

    public Bytes(Memory<byte> value)
    {
        _data = MemoryOwner<byte>.RentCopy(value.Span);
    }

    public Bytes(ReadOnlyMemory<byte> value)
    {
        _data = MemoryOwner<byte>.RentCopy(value.Span);
    }

    public Bytes(byte value)
    {
        _data = MemoryOwner<byte>.Rent(1);
        _data.Span[0] = value;
    }

    public Bytes(params byte[] bytes)
    {
        _data = MemoryOwner<byte>.RentCopy(bytes);
    }

    public Bytes(byte[] value, int start, int length)
    {
        _data = MemoryOwner<byte>.RentCopy(value.AsSpan(start, length));
    }

    /// <summary>
    /// Create a new <see cref="Bytes"/> instance using the UTF-8 bytes from a <see cref="string"/>.
    /// </summary>
    /// <remarks>
    /// This allocates a new <see cref="byte[]"/> instance.
    /// </remarks>
    public Bytes(string value)
    {
        _data = MemoryOwner<byte>.Rent(Encoding.UTF8.GetByteCount(value));
        Encoding.UTF8.GetBytes(value, _data.Span);
    }

    /// <summary>
    /// Create a new <see cref="Bytes"/> instance using the UTF-8 bytes from a <see cref="ReadOnlySpan<byte>"/>.
    /// </summary>
    public Bytes(ReadOnlySpan<byte> value)
    {
        _data = MemoryOwner<byte>.RentCopy(value);
    }

    /// <summary>
    /// Create a new <see cref="Bytes"/> instance from an <see cref="short">.
    /// </summary>
    public Bytes(short value)
    {
        _data = MemoryOwner<byte>.Rent(sizeof(short));
        BinaryPrimitives.WriteInt16LittleEndian(_data.Span, value);
    }

    /// <summary>
    /// Create a new <see cref="Bytes"/> instance from an <see cref="ushort">.
    /// </summary>
    public Bytes(ushort value)
    {
        _data = MemoryOwner<byte>.Rent(sizeof(ushort));
        BinaryPrimitives.WriteUInt16LittleEndian(_data.Span, value);
    }

    /// <summary>
    /// Create a new <see cref="Bytes"/> instance from an <see cref="int">.
    /// </summary>
    public Bytes(int value)
    {
        _data = MemoryOwner<byte>.Rent(sizeof(int));
        BinaryPrimitives.WriteInt32LittleEndian(_data.Span, value);
    }

    /// <summary>
    /// Create a new <see cref="Bytes"/> instance from an <see cref="uint">.
    /// </summary>
    public Bytes(uint value)
    {
        _data = MemoryOwner<byte>.Rent(sizeof(uint));
        BinaryPrimitives.WriteUInt32LittleEndian(_data.Span, value);
    }

    /// <summary>
    /// Create a new <see cref="Bytes"/> instance from an <see cref="long">.
    /// </summary>
    public Bytes(long value)
    {
        _data = MemoryOwner<byte>.Rent(sizeof(long));
        BinaryPrimitives.WriteInt64LittleEndian(_data.Span, value);
    }

    /// <summary>
    /// Create a new <see cref="Bytes"/> instance from an <see cref="ulong">.
    /// </summary>
    public Bytes(ulong value)
    {
        _data = MemoryOwner<byte>.Rent(sizeof(ulong));
        BinaryPrimitives.WriteUInt64LittleEndian(_data.Span, value);
    }

    public readonly ReadOnlySpan<byte> Span => _data == null ? default : _data.Span;

    public readonly ReadOnlyMemory<byte> Memory => _data == null ? default : _data.Memory;

    /// <summary>
    /// Returns an empty <see cref="Bytes"/>.
    /// </summary>
    public static Bytes Empty => default;

    /// <summary>
    /// The number of bytes.
    /// </summary>
    public readonly int Length => _data == null ? 0 : _data.Length;

    /// <summary>
    /// Returns <see langword="true"> if <see cref="Length"> is 0.
    /// </summary>
    public readonly bool IsEmpty => Length == 0;

    public void Dispose()
    {
        _data?.Dispose();
    }

    public static implicit operator Bytes(byte b) => new(b);
    public static implicit operator Bytes(byte[] bytes) => new(bytes);
    public static implicit operator Bytes(string s) => new(s);
    public static implicit operator Bytes(short value) => new(value);
    public static implicit operator Bytes(ushort value) => new(value);
    public static implicit operator Bytes(int value) => new(value);
    public static implicit operator Bytes(uint value) => new(value);
    public static implicit operator Bytes(long value) => new(value);
    public static implicit operator Bytes(ulong value) => new(value);
    public static implicit operator Bytes(ReadOnlyMemory<byte> value) => new(value);
    public static implicit operator Bytes(Memory<byte> value) => new(value);

    public static bool operator <(Bytes left, Bytes right)
    {
        return left.CompareTo(right) < 0;
    }

    public static bool operator <=(Bytes left, Bytes right)
    {
        return left.Equals(right) || left.CompareTo(right) <= 0;
    }

    public static bool operator >(Bytes left, Bytes right)
    {
        return left.CompareTo(right) > 0;
    }

    public static bool operator >=(Bytes left, Bytes right)
    {
        return left.Equals(right) || left.CompareTo(right) > 0;
    }

    public override readonly bool Equals([NotNullWhen(true)] object? obj)
    {
        return obj is Bytes b && ((Bytes)this).Equals(b);
    }

    public readonly bool Equals(Bytes other)
    {
        return Span.SequenceEqual(other.Span);
    }

    public readonly int CompareTo(Bytes other)
    {
        return Span.SequenceCompareTo(other.Span);
    }

    public override readonly int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.AddBytes(Span);
        return hashCode.ToHashCode();
    }

    public override readonly string ToString()
    {
        return $"[{Length}] {Convert.ToHexString(Span)} (\"{JsonEncodedText.Encode(Span, JavaScriptEncoder.UnsafeRelaxedJsonEscaping).Value}\")";
    }

    private string GetDebuggerDisplay()
    {
        return ToString();
    }

    public static bool operator ==(Bytes left, Bytes right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(Bytes left, Bytes right)
    {
        return !(left == right);
    }

    private class BytesComparer : EqualityComparer<Bytes>, IComparer<Bytes>
    {
        public static readonly BytesComparer Instance = new();

        public int Compare(Bytes x, Bytes y) => x.CompareTo(y);

        public override bool Equals(Bytes x, Bytes y) => x.Equals(y);

        public override int GetHashCode(Bytes obj) => obj.GetHashCode();
    }
}
