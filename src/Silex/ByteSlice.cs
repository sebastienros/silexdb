using System.Buffers;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Silex;

/// <summary>
/// Represents comparable bytes backed by engine-owned memory.
/// </summary>
[DebuggerDisplay($"{{{nameof(GetDebuggerDisplay)}(),nq}}")]
internal sealed class ByteSlice : IEquatable<ByteSlice>, IComparable<ByteSlice>
{
    private readonly IMemoryOwner<byte>? _owner;
    private readonly ReadOnlyMemory<byte> _memory;
    private readonly int _offset;
    private readonly int _length;

    public static readonly IComparer<ByteSlice> Comparer = ByteSliceComparer.Instance;
    public static readonly IEqualityComparer<ByteSlice> EqualityComparer = ByteSliceComparer.Instance;
    public static readonly ByteSlice Empty = new(ReadOnlyMemory<byte>.Empty);

    private ByteSlice(ReadOnlyMemory<byte> memory)
    {
        _memory = memory;
        _offset = 0;
        _length = memory.Length;
    }

    private ByteSlice(IMemoryOwner<byte> owner, int offset, int length)
    {
        ArgumentNullException.ThrowIfNull(owner);
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        if (offset > owner.Memory.Length || length > owner.Memory.Length - offset)
        {
            throw new ArgumentOutOfRangeException(nameof(length), length, "The requested slice is outside the owner memory.");
        }

        _owner = owner;
        _offset = offset;
        _length = length;
    }

    public static ByteSlice FromMemory(ReadOnlyMemory<byte> memory) => memory.IsEmpty ? Empty : new ByteSlice(memory);

    internal static ByteSlice CreateView(IMemoryOwner<byte> owner, int offset, int length) => length == 0 ? Empty : new ByteSlice(owner, offset, length);

    public ReadOnlySpan<byte> Span => _owner is null ? _memory.Span : _owner.Memory.Span.Slice(_offset, _length);

    public ReadOnlySpan<byte> AsSpan() => Span;

    public ReadOnlyMemory<byte> Memory => _owner is null ? _memory : _owner.Memory.Slice(_offset, _length);

    public int Length => _length;

    public bool IsEmpty => _length == 0;

    public override bool Equals([NotNullWhen(true)] object? obj)
    {
        return obj is ByteSlice b && Equals(b);
    }

    public bool Equals(ByteSlice? other)
    {
        return other is not null && Span.SequenceEqual(other.Span);
    }

    public int CompareTo(ByteSlice? other)
    {
        if (other is null)
        {
            return 1;
        }

        return Span.SequenceCompareTo(other.Span);
    }

    public override int GetHashCode()
    {
        var hashCode = new HashCode();
        hashCode.AddBytes(Span);
        return hashCode.ToHashCode();
    }

    public override string ToString()
    {
        return $"[{Length}] {Convert.ToHexString(Span)} (\"{JsonEncodedText.Encode(Span, JavaScriptEncoder.UnsafeRelaxedJsonEscaping).Value}\")";
    }

    public string ToString(IFormatProvider? provider) => ToString();

    private string GetDebuggerDisplay() => ToString();

    public static bool operator ==(ByteSlice? left, ByteSlice? right)
    {
        if (ReferenceEquals(left, right))
        {
            return true;
        }

        return left is not null && left.Equals(right);
    }

    public static bool operator !=(ByteSlice? left, ByteSlice? right) => !(left == right);

    public static bool operator <(ByteSlice left, ByteSlice right) => left.CompareTo(right) < 0;
    public static bool operator <=(ByteSlice left, ByteSlice right) => left.Equals(right) || left.CompareTo(right) <= 0;
    public static bool operator >(ByteSlice left, ByteSlice right) => left.CompareTo(right) > 0;
    public static bool operator >=(ByteSlice left, ByteSlice right) => left.Equals(right) || left.CompareTo(right) > 0;

    private sealed class ByteSliceComparer : EqualityComparer<ByteSlice>, IComparer<ByteSlice>
    {
        public static readonly ByteSliceComparer Instance = new();

        public int Compare(ByteSlice? x, ByteSlice? y)
        {
            if (ReferenceEquals(x, y))
            {
                return 0;
            }

            if (x is null)
            {
                return -1;
            }

            if (y is null)
            {
                return 1;
            }

            return x.CompareTo(y);
        }

        public override bool Equals(ByteSlice? x, ByteSlice? y)
        {
            if (ReferenceEquals(x, y))
            {
                return true;
            }

            return x is not null && x.Equals(y);
        }

        public override int GetHashCode(ByteSlice obj) => obj.GetHashCode();
    }
}
