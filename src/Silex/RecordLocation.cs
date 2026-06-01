using Silex.Serialization;

namespace Silex;

/// <summary>
/// Represents the information stored in a Block for a specific record.
/// It doesn't contain the value itself, but all the information to be able
/// to read the value from the block it is saved in.
/// </summary>
internal readonly struct RecordLocation : IComparable<RecordLocation>
{
    private static readonly IComparer<ByteSlice> _keyComparer = BinaryEncoderFactory<ByteSlice>.BinarySerializer.Comparer;

    public ByteSlice Key { get; init; }
    public int BlockOffset { get; init; }
    public int Length { get; init; }
    public int CompareTo(RecordLocation other)
    {
        return _keyComparer.Compare(Key, other.Key);
    }
}
