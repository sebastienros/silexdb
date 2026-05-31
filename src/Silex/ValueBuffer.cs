using System.Buffers.Binary;

namespace Silex;

/// <summary>
/// The representation of a stored value. Values in Silex are opaque byte sequences with no ordering, so a
/// single <see cref="ValueBuffer"/> replaces what used to be an open <c>TValue</c> generic parameter on
/// the store and every internal type that carried a value. It surfaces on the pluggable encoder/builder
/// extension points (<see cref="Silex.Blocks.IBlockEncoder{TKey}"/>,
/// <see cref="Silex.Tables.ISsTableBuilder{TKey}"/>); the high-level <see cref="LsmStorage{TKey}"/> API
/// works in terms of <see cref="byte"/>[] and <see cref="ReadOnlySpan{T}"/> instead.
/// </summary>
/// <remarks>
/// A <see cref="ValueBuffer"/> is an immutable, garbage-collected wrapper over a single
/// <see cref="byte"/>[]. It is intentionally <em>not</em> pooled and not disposable: instances flow
/// freely through the memtable, flush, compaction and iteration paths, are shallow-copied into iterator
/// snapshots, and outlive the locks they were read under. Backing the value with a plain array keeps all
/// of that safe without any ownership or disposal bookkeeping.
///
/// An empty buffer (<see cref="Empty"/>, a <see langword="null"/> array, or a zero-length array) is the
/// canonical tombstone: it is how deletions are represented on disk and in memory. Empty values are
/// therefore indistinguishable from deletions and cannot be stored as live data.
///
/// The array reference is taken as-is by <see cref="ValueBuffer(byte[])"/> (zero-copy); callers that
/// transfer an array must not mutate it afterwards. <see cref="FromSpan"/> copies, since a span does not
/// own its memory.
/// </remarks>
public readonly struct ValueBuffer
{
    private readonly byte[]? _array;

    /// <summary>
    /// Wraps <paramref name="array"/> directly without copying. The buffer takes ownership of the array;
    /// the caller must not mutate it afterwards. A <see langword="null"/> array is a tombstone.
    /// </summary>
    public ValueBuffer(byte[]? array)
    {
        _array = array;
    }

    /// <summary>
    /// The empty buffer, which is the canonical tombstone.
    /// </summary>
    public static ValueBuffer Empty => default;

    /// <summary>
    /// Copies <paramref name="value"/> into a freshly allocated array. An empty span yields
    /// <see cref="Empty"/> without allocating.
    /// </summary>
    public static ValueBuffer FromSpan(ReadOnlySpan<byte> value)
    {
        return value.IsEmpty ? default : new ValueBuffer(value.ToArray());
    }

    /// <summary>
    /// The raw value bytes. Returns an empty span for a tombstone.
    /// </summary>
    public ReadOnlySpan<byte> Span => _array;

    /// <summary>
    /// Encodes <paramref name="value"/> as four little-endian bytes. The fixed little-endian layout keeps
    /// the persisted form deterministic across architectures.
    /// </summary>
    public static ValueBuffer FromInt32(int value)
    {
        var array = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(array, value);
        return new ValueBuffer(array);
    }

    /// <summary>
    /// Encodes <paramref name="value"/> as four little-endian bytes. The fixed little-endian layout keeps
    /// the persisted form deterministic across architectures.
    /// </summary>
    public static ValueBuffer FromUInt32(uint value)
    {
        var array = new byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32LittleEndian(array, value);
        return new ValueBuffer(array);
    }

    /// <summary>
    /// Encodes <paramref name="value"/> as eight little-endian bytes. The fixed little-endian layout keeps
    /// the persisted form deterministic across architectures.
    /// </summary>
    public static ValueBuffer FromInt64(long value)
    {
        var array = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(array, value);
        return new ValueBuffer(array);
    }

    /// <summary>
    /// Encodes <paramref name="value"/> as eight little-endian bytes. The fixed little-endian layout keeps
    /// the persisted form deterministic across architectures.
    /// </summary>
    public static ValueBuffer FromUInt64(ulong value)
    {
        var array = new byte[sizeof(ulong)];
        BinaryPrimitives.WriteUInt64LittleEndian(array, value);
        return new ValueBuffer(array);
    }

    /// <summary>
    /// The number of value bytes; zero for a tombstone.
    /// </summary>
    public int Length => _array?.Length ?? 0;

    /// <summary>
    /// <see langword="true"/> when this buffer is a tombstone (no live value bytes).
    /// </summary>
    public bool IsEmpty => Length == 0;

    /// <summary>
    /// Returns the underlying array for a live value, or <see langword="null"/> for a tombstone. The
    /// array is returned without copying, so callers must not mutate it.
    /// </summary>
    public byte[]? ToNullableArray() => IsEmpty ? null : _array;
}
