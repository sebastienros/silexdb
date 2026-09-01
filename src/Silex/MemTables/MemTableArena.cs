using System.Buffers;

namespace Silex.MemTables;

internal sealed class MemTableArena : IDisposable
{
    private readonly int _blockSize;
    private readonly List<SlabOwner> _slabs = [];
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
        var slabIndex = ReferenceEquals(slab, _currentSlab) ? _currentSlabIndex : _slabs.Count - 1;
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
            : _slabs[slice.SlabIndex].Memory.Span.Slice(slice.Offset, slice.Length);
    }

    public ReadOnlyMemory<byte> GetMemory(ArenaSlice slice)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return slice.Length == 0
            ? ReadOnlyMemory<byte>.Empty
            : _slabs[slice.SlabIndex].Memory.Slice(slice.Offset, slice.Length);
    }

    public ByteSlice GetByteSlice(ArenaSlice slice)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return slice.Length == 0
            ? ByteSlice.Empty
            : ByteSlice.CreateView(_slabs[slice.SlabIndex], slice.Offset, slice.Length);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        foreach (var slab in _slabs)
        {
            slab.Dispose();
        }

        _slabs.Clear();
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
            _currentSlabIndex = _slabs.Count - 1;
            _position = 0;
        }

        return _currentSlab;
    }

    private SlabOwner AddSlab(int length)
    {
        var slab = new SlabOwner(length);
        _slabs.Add(slab);
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
