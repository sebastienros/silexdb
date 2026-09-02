using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using Silex.Tables;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class BlockMetadataBenchmarks
{
    private const int BlockCount = 1_024;
    private const int LookupCount = 1_000;

    private LegacyMetadata[] _legacy = null!;
    private BlockMetadataStore _packed = null!;
    private byte[][] _keys = null!;

    [GlobalSetup]
    public void Setup()
    {
        _legacy = new LegacyMetadata[BlockCount];
        var metadata = new BlockMetadata[BlockCount];

        for (var i = 0; i < BlockCount; i++)
        {
            var firstKey = EncodeKey(i * 10L);
            var lastKey = EncodeKey(i * 10L + 9);
            _legacy[i] = new LegacyMetadata(i, firstKey, lastKey);
            metadata[i] = new BlockMetadata(i, i * 4096L, 4096, SstCompression.None, 0, firstKey, lastKey);
        }

        _packed = BlockMetadataStore.Create(metadata);
        _keys = new byte[LookupCount][];
        var random = new Random(42);

        for (var i = 0; i < _keys.Length; i++)
        {
            _keys[i] = EncodeKey(random.NextInt64(BlockCount * 10L));
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _packed.Dispose();
        foreach (var metadata in _legacy)
        {
            metadata.Dispose();
        }
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = LookupCount)]
    public int ObjectArrayLookup()
    {
        var result = 0;
        for (var i = 0; i < _keys.Length; i++)
        {
            result += FindLegacy(_keys[i]);
        }

        return result;
    }

    [Benchmark(OperationsPerInvoke = LookupCount)]
    public int PackedLookup()
    {
        var result = 0;
        for (var i = 0; i < _keys.Length; i++)
        {
            result += _packed.FindMatchingBlockIndex(_keys[i]);
        }

        return result;
    }

    private int FindLegacy(ReadOnlySpan<byte> key)
    {
        var start = 0;
        var end = _legacy.Length - 1;

        while (start <= end)
        {
            var middle = start + (end - start) / 2;
            var metadata = _legacy[middle];

            if (key.SequenceCompareTo(metadata.LastKey.Span) > 0)
            {
                start = middle + 1;
            }
            else
            {
                end = middle - 1;
            }
        }

        if ((uint)start >= (uint)_legacy.Length)
        {
            return -1;
        }

        var candidate = _legacy[start];
        return key.SequenceCompareTo(candidate.FirstKey.Span) >= 0 ? candidate.Index : -1;
    }

    private static byte[] EncodeKey(long value)
    {
        var bytes = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(8), value);
        return bytes;
    }

    private sealed class LegacyMetadata : IDisposable
    {
        private readonly OwnedByteSlice _firstKeyOwner;
        private readonly OwnedByteSlice _lastKeyOwner;

        public LegacyMetadata(int index, byte[] firstKey, byte[] lastKey)
        {
            Index = index;
            _firstKeyOwner = OwnedByteSlice.CopyFrom(firstKey);
            _lastKeyOwner = OwnedByteSlice.CopyFrom(lastKey);
        }

        public int Index { get; }
        public ByteSlice FirstKey => _firstKeyOwner.Slice;
        public ByteSlice LastKey => _lastKeyOwner.Slice;

        public void Dispose()
        {
            _firstKeyOwner.Dispose();
            _lastKeyOwner.Dispose();
        }
    }
}
