using System.Buffers;

namespace Silex.MemTables;

internal sealed class MemTableArena : IDisposable
{
    private readonly int _blockSize;
    // An array rather than a List so optimistic readers can index it while a writer appends: a slab is stored
    // before the array is republished, and a reader that sees a slice also sees the slab it points into.
    private SlabOwner?[] _slabs = new SlabOwner?[4];
    private int _slabCount;
    private SlabOwner? _currentSlab;
    private int _currentSlabIndex = -1;
    private int _position;
    private bool _disposed;

    public MemTableArena(int blockSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockSize);
        _blockSize = blockSize;
    }

    public ArenaSlice Copy(ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (value.IsEmpty)
        {
            return ArenaSlice.Empty;
        }

        var slab = GetWritableSlab(value.Length);
        var slabIndex = ReferenceEquals(slab, _currentSlab) ? _currentSlabIndex : _slabCount - 1;
        var offset = ReferenceEquals(slab, _currentSlab) ? _position : 0;
        value.CopyTo(slab.Memory.Span.Slice(offset, value.Length));

        if (ReferenceEquals(slab, _currentSlab))
        {
            _position += value.Length;
        }

        return new ArenaSlice(slabIndex, offset, value.Length);
    }

    public ReadOnlySpan<byte> GetSpan(ArenaSlice slice)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return slice.Length == 0
            ? ReadOnlySpan<byte>.Empty
            : _slabs[slice.SlabIndex]!.Memory.Span.Slice(slice.Offset, slice.Length);
    }

    /// <summary>
    /// Resolves <paramref name="slice"/> for an optimistic reader, which may hold a torn or stale slice: every
    /// bound is checked and a failure is reported instead of thrown, so the caller can validate and retry.
    /// Slab bytes are never rewritten while the arena is alive, so a slice that validates reads stable bytes.
    /// </summary>
    public bool TryGetSpanOptimistic(ArenaSlice slice, out ReadOnlySpan<byte> span)
    {
        if (slice.Length == 0)
        {
            span = ReadOnlySpan<byte>.Empty;
            return true;
        }
        var slabs = Volatile.Read(ref _slabs);
        if ((uint)slice.SlabIndex < (uint)slabs.Length && slabs[slice.SlabIndex]?.ArrayOrNull is { } array &&
            (uint)slice.Offset <= (uint)array.Length && (uint)slice.Length <= (uint)(array.Length - slice.Offset))
        {
            span = new ReadOnlySpan<byte>(array, slice.Offset, slice.Length);
            return true;
        }
        span = default;
        return false;
    }

    public ReadOnlyMemory<byte> GetMemory(ArenaSlice slice)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return slice.Length == 0
            ? ReadOnlyMemory<byte>.Empty
            : _slabs[slice.SlabIndex]!.Memory.Slice(slice.Offset, slice.Length);
    }

    public ByteSlice GetByteSlice(ArenaSlice slice)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return slice.Length == 0
            ? ByteSlice.Empty
            : ByteSlice.CreateView(_slabs[slice.SlabIndex]!, slice.Offset, slice.Length);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        for (var i = 0; i < _slabCount; i++)
        {
            _slabs[i]!.Dispose();
        }

        _slabs = [];
        _slabCount = 0;
        _currentSlab = null;
        _currentSlabIndex = -1;
        _position = 0;
    }

    private SlabOwner GetWritableSlab(int length)
    {
        if (length > _blockSize)
        {
            return AddSlab(length);
        }

        if (_currentSlab is null || _currentSlab.Memory.Length - _position < length)
        {
            _currentSlab = AddSlab(_blockSize);
            _currentSlabIndex = _slabCount - 1;
            _position = 0;
        }

        return _currentSlab;
    }

    private SlabOwner AddSlab(int length)
    {
        var slab = new SlabOwner(length);
        if (_slabCount == _slabs.Length)
        {
            var grown = new SlabOwner?[_slabs.Length * 2];
            _slabs.AsSpan(0, _slabCount).CopyTo(grown);
            Volatile.Write(ref _slabs, grown);
        }
        _slabs[_slabCount++] = slab;
        return slab;
    }

    internal readonly record struct ArenaSlice(int SlabIndex, int Offset, int Length)
    {
        public static readonly ArenaSlice Empty = new(-1, 0, 0);
    }

    private sealed class SlabOwner : IMemoryOwner<byte>
    {
        private byte[]? _array;

        public SlabOwner(int length)
        {
            _array = GC.AllocateUninitializedArray<byte>(length);
        }

        public byte[]? ArrayOrNull => Volatile.Read(ref _array);

        public Memory<byte> Memory
        {
            get
            {
                var array = _array;
                if (array is null)
                {
                    throw new ObjectDisposedException(nameof(MemTableArena), "The arena block has already been disposed.");
                }

                return array;
            }
        }

        public void Dispose()
        {
            _array = null;
        }
    }
}
