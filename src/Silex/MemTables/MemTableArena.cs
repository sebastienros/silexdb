using System.Buffers;

namespace Silex.MemTables;

internal sealed class MemTableArena : IDisposable
{
    private readonly int _blockSize;
    private readonly List<SlabOwner> _slabs = [];
    private SlabOwner? _currentSlab;
    private int _position;
    private bool _disposed;

    public MemTableArena(int blockSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(blockSize);
        _blockSize = blockSize;
    }

    public ByteSlice Copy(ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (value.IsEmpty)
        {
            return ByteSlice.Empty;
        }

        var slab = GetWritableSlab(value.Length);
        var offset = ReferenceEquals(slab, _currentSlab) ? _position : 0;
        value.CopyTo(slab.Memory.Span.Slice(offset, value.Length));

        if (ReferenceEquals(slab, _currentSlab))
        {
            _position += value.Length;
        }

        return ByteSlice.CreateView(slab, offset, value.Length);
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
