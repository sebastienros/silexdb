using System.Buffers.Binary;
using System.Collections;

namespace Silex.Blocks;

/// <summary>
/// Zero-allocation view over the offset section stored at the tail of an encoded block. Each offset is a
/// little-endian <see cref="ushort"/> pointing at an entry in the data section. Reading offsets straight from
/// the block bytes avoids materializing a <c>ushort[]</c> on every block decode, which is the dominant
/// per-miss allocation on the read-miss hot path.
/// </summary>
public readonly struct BlockOffsets : IReadOnlyList<ushort>
{
    private readonly ReadOnlyMemory<byte> _block;
    private readonly int _start;
    private readonly int _count;

    /// <param name="block">The decoded block bytes (data section followed by the offset section).</param>
    /// <param name="start">Byte position of the first offset within <paramref name="block"/>.</param>
    /// <param name="count">Number of entries (offsets) in the block.</param>
    public BlockOffsets(ReadOnlyMemory<byte> block, int start, int count)
    {
        _block = block;
        _start = start;
        _count = count;
    }

    public int Count => _count;

    public ushort this[int index]
    {
        get
        {
            if ((uint)index >= (uint)_count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            return BinaryPrimitives.ReadUInt16LittleEndian(_block.Span.Slice(_start + index * sizeof(ushort), sizeof(ushort)));
        }
    }

    public Enumerator GetEnumerator() => new(this);

    IEnumerator<ushort> IEnumerable<ushort>.GetEnumerator() => GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public struct Enumerator : IEnumerator<ushort>
    {
        private readonly BlockOffsets _offsets;
        private int _index;

        internal Enumerator(BlockOffsets offsets)
        {
            _offsets = offsets;
            _index = -1;
        }

        public readonly ushort Current => _offsets[_index];

        readonly object IEnumerator.Current => Current;

        public bool MoveNext() => ++_index < _offsets._count;

        public void Reset() => _index = -1;

        public readonly void Dispose()
        {
        }
    }
}
