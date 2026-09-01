using System.Collections;
using System.Runtime.InteropServices;
using Silex.Buffers;
using Silex.Ownership;

namespace Silex.Tables;

internal readonly struct BlockMetadata
{
    private readonly ReadOnlyMemory<byte> _firstKey;
    private readonly ReadOnlyMemory<byte> _lastKey;

    public BlockMetadata(
        int index,
        long offset,
        int uncompressedLength,
        SstCompression compression,
        uint checksum,
        ReadOnlyMemory<byte> firstKey,
        ReadOnlyMemory<byte> lastKey)
    {
        Index = index;
        Offset = offset;
        UncompressedLength = uncompressedLength;
        Compression = compression;
        Checksum = checksum;
        _firstKey = firstKey;
        _lastKey = lastKey;
    }

    public int Index { get; }
    public long Offset { get; }
    public int UncompressedLength { get; }
    public SstCompression Compression { get; }
    public uint Checksum { get; }
    public ByteSlice FirstKey => ByteSlice.FromMemory(_firstKey);
    public ByteSlice LastKey => ByteSlice.FromMemory(_lastKey);
    internal ReadOnlySpan<byte> FirstKeySpan => _firstKey.Span;
    internal ReadOnlySpan<byte> LastKeySpan => _lastKey.Span;
}

/// <summary>
/// Stores fixed-size metadata descriptors densely and all boundary keys in one pooled byte buffer.
/// </summary>
internal sealed class BlockMetadataStore : IReadOnlyList<BlockMetadata>, IDisposable
{
    private readonly BlockMetadataDescriptor[] _descriptors;
    [OwnedResource]
    private MemoryOwner<byte>? _boundaryKeys;

    private BlockMetadataStore(BlockMetadataDescriptor[] descriptors, MemoryOwner<byte> boundaryKeys)
    {
        _descriptors = descriptors;
        _boundaryKeys = boundaryKeys;
    }

    public int Count => _descriptors.Length;
    public int Length => _descriptors.Length;

    public BlockMetadata this[int index]
    {
        get
        {
            ref readonly var descriptor = ref _descriptors[index];
            var keys = GetBoundaryKeyMemory();

            return new BlockMetadata(
                index,
                descriptor.Offset,
                descriptor.UncompressedLength,
                descriptor.Compression,
                descriptor.Checksum,
                keys.Slice(descriptor.FirstKeyOffset, descriptor.FirstKeyLength),
                keys.Slice(descriptor.LastKeyOffset, descriptor.LastKeyLength));
        }
    }

    public static BlockMetadataStore Create(IReadOnlyList<BlockMetadata> metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);

        var descriptors = new BlockMetadataDescriptor[metadata.Count];
        var boundaryKeyLength = 0;

        for (var i = 0; i < metadata.Count; i++)
        {
            boundaryKeyLength = checked(boundaryKeyLength + metadata[i].FirstKeySpan.Length + metadata[i].LastKeySpan.Length);
        }

        var boundaryKeys = MemoryOwner<byte>.Rent(boundaryKeyLength);

        try
        {
            var destination = boundaryKeys.Span;
            var offset = 0;

            for (var i = 0; i < metadata.Count; i++)
            {
                var source = metadata[i];
                var firstKey = source.FirstKeySpan;
                var firstKeyOffset = offset;
                firstKey.CopyTo(destination[offset..]);
                offset += firstKey.Length;

                var lastKey = source.LastKeySpan;
                var lastKeyOffset = offset;
                lastKey.CopyTo(destination[offset..]);
                offset += lastKey.Length;

                descriptors[i] = new BlockMetadataDescriptor(
                    source.Offset,
                    source.UncompressedLength,
                    source.Compression,
                    source.Checksum,
                    firstKeyOffset,
                    firstKey.Length,
                    lastKeyOffset,
                    lastKey.Length);
            }

            return new BlockMetadataStore(descriptors, boundaryKeys);
        }
        catch
        {
            boundaryKeys.Dispose();
            throw;
        }
    }

    internal ReadOnlySpan<byte> GetFirstKeySpan(int index)
    {
        ref readonly var descriptor = ref _descriptors[index];
        return GetBoundaryKeySpan().Slice(descriptor.FirstKeyOffset, descriptor.FirstKeyLength);
    }

    internal ReadOnlySpan<byte> GetLastKeySpan(int index)
    {
        ref readonly var descriptor = ref _descriptors[index];
        return GetBoundaryKeySpan().Slice(descriptor.LastKeyOffset, descriptor.LastKeyLength);
    }

    internal ReadOnlyMemory<byte> GetFirstKeyMemory(int index)
    {
        ref readonly var descriptor = ref _descriptors[index];
        return GetBoundaryKeyMemory().Slice(descriptor.FirstKeyOffset, descriptor.FirstKeyLength);
    }

    internal ReadOnlyMemory<byte> GetLastKeyMemory(int index)
    {
        ref readonly var descriptor = ref _descriptors[index];
        return GetBoundaryKeyMemory().Slice(descriptor.LastKeyOffset, descriptor.LastKeyLength);
    }

    internal int FindMatchingBlockIndex(ReadOnlySpan<byte> key)
    {
        var start = 0;
        var end = _descriptors.Length - 1;
        var boundaryKeys = GetBoundaryKeySpan();

        while (start <= end)
        {
            var middle = start + (end - start) / 2;
            ref readonly var descriptor = ref _descriptors[middle];
            var lastKey = boundaryKeys.Slice(descriptor.LastKeyOffset, descriptor.LastKeyLength);

            if (key.SequenceCompareTo(lastKey) > 0)
            {
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
        }

        if ((uint)start >= (uint)_descriptors.Length)
        {
            return -1;
        }

        ref readonly var candidate = ref _descriptors[start];
        var firstKey = boundaryKeys.Slice(candidate.FirstKeyOffset, candidate.FirstKeyLength);
        return key.SequenceCompareTo(firstKey) >= 0 ? start : -1;
    }

    internal int FindStartBlockIndex(ReadOnlySpan<byte> key)
    {
        var start = 0;
        var end = _descriptors.Length - 1;
        var boundaryKeys = GetBoundaryKeySpan();

        while (start <= end)
        {
            var middle = start + (end - start) / 2;
            ref readonly var descriptor = ref _descriptors[middle];
            var firstKey = boundaryKeys.Slice(descriptor.FirstKeyOffset, descriptor.FirstKeyLength);
            var comparison = firstKey.SequenceCompareTo(key);

            if (comparison == 0)
            {
                return Math.Max(0, middle - 1);
            }

            if (comparison < 0)
            {
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
        }

        return Math.Max(0, start - 1);
    }

    public Enumerator GetEnumerator() => new(this);
    IEnumerator<BlockMetadata> IEnumerable<BlockMetadata>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public void Dispose()
    {
        _boundaryKeys?.Dispose();
        _boundaryKeys = null;
    }

    private ReadOnlySpan<byte> GetBoundaryKeySpan()
    {
        return (_boundaryKeys ?? throw new ObjectDisposedException(nameof(BlockMetadataStore))).Span;
    }

    private ReadOnlyMemory<byte> GetBoundaryKeyMemory()
    {
        return (_boundaryKeys ?? throw new ObjectDisposedException(nameof(BlockMetadataStore))).Memory;
    }

    public struct Enumerator : IEnumerator<BlockMetadata>
    {
        private readonly BlockMetadataStore _store;
        private int _index;

        internal Enumerator(BlockMetadataStore store)
        {
            _store = store;
            _index = -1;
        }

        public readonly BlockMetadata Current => _store[_index];
        readonly object IEnumerator.Current => Current;

        public bool MoveNext() => ++_index < _store.Count;
        public void Reset() => _index = -1;
        public readonly void Dispose()
        {
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    private readonly struct BlockMetadataDescriptor
    {
        public BlockMetadataDescriptor(
            long offset,
            int uncompressedLength,
            SstCompression compression,
            uint checksum,
            int firstKeyOffset,
            int firstKeyLength,
            int lastKeyOffset,
            int lastKeyLength)
        {
            Offset = offset;
            UncompressedLength = uncompressedLength;
            Compression = compression;
            Checksum = checksum;
            FirstKeyOffset = firstKeyOffset;
            FirstKeyLength = firstKeyLength;
            LastKeyOffset = lastKeyOffset;
            LastKeyLength = lastKeyLength;
        }

        public readonly long Offset;
        public readonly int UncompressedLength;
        public readonly SstCompression Compression;
        public readonly uint Checksum;
        public readonly int FirstKeyOffset;
        public readonly int FirstKeyLength;
        public readonly int LastKeyOffset;
        public readonly int LastKeyLength;
    }
}
