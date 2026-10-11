namespace Silex.Keys;

/// <summary>Sort direction of one encoded component.</summary>
public enum OrderedKeyOrder : byte
{
    /// <summary>Byte order of the encoded component follows the natural order of its values.</summary>
    Ascending = 0,

    /// <summary>Byte order of the encoded component is the exact reverse of the natural order.</summary>
    Descending = 1,
}

/// <summary>The component kinds of the ordered key format, in their encoded type-tag order.</summary>
public enum OrderedKeyComponent : byte
{
    /// <summary>A boolean; <see langword="false"/> sorts before <see langword="true"/>.</summary>
    Boolean = 0x04,

    /// <summary>An arbitrary byte sequence, ordered lexicographically as unsigned bytes.</summary>
    Bytes = 0x10,

    /// <summary>A string stored as strict UTF-8, ordered by Unicode code point, not UTF-16 ordinal order.</summary>
    String = 0x11,

    /// <summary>A signed 64-bit integer.</summary>
    Int64 = 0x20,

    /// <summary>An unsigned 64-bit integer.</summary>
    UInt64 = 0x21,

    /// <summary>A 64-bit IEEE-754 value with a total order (see <see cref="OrderedKey"/>).</summary>
    Double = 0x22,

    /// <summary>An exact decimal value; scale and signed zero are normalized (see <see cref="OrderedKey"/>).</summary>
    Decimal = 0x23,
}

/// <summary>Outcome of a read from <see cref="OrderedKeyReader"/>.</summary>
public enum OrderedKeyDecodeStatus
{
    /// <summary>The component was decoded and the reader advanced.</summary>
    Success = 0,

    /// <summary>No input remains.</summary>
    EndOfData,

    /// <summary>The next component has a different kind or order than requested.</summary>
    UnexpectedComponent,

    /// <summary>The input is truncated or not canonical output of this format.</summary>
    Malformed,

    /// <summary>The destination is too small; the out length reports the required size.</summary>
    DestinationTooSmall,
}
