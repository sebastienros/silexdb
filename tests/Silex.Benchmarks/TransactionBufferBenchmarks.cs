using BenchmarkDotNet.Attributes;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class TransactionBufferBenchmarks
{
    private MvccWriteBuffer _buffer = null!;
    private byte[][] _keys = null!;
    private byte[][] _probes = null!;
    private readonly byte[] _value = new byte[16];

    [Params(1, 4, 8, 16, 64, 4096)]
    public int Entries { get; set; }

    // Forced indexing and the previous linear algorithm bracket the production hybrid.
    [Params(0, MvccWriteBuffer.DefaultIndexThreshold, int.MaxValue)]
    public int IndexThreshold { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _buffer = new MvccWriteBuffer(IndexThreshold);
        _keys = Enumerable.Range(0, Entries).Select(Key).ToArray();
        _probes = Enumerable.Range(0, 64).Select(i => Key((i * 7919) % (Entries * 2))).ToArray();
        foreach (var key in _keys)
        {
            _buffer.AddMutation(key, _value, false);
            _buffer.TrackRead(key);
        }
    }

    [Benchmark]
    public int PointLookup64()
    {
        var sum = 0;
        foreach (var key in _probes) sum += _buffer.FindMutation(key);
        return sum;
    }

    [Benchmark]
    public int DuplicateTrackedReads()
    {
        foreach (var key in _keys) _buffer.TrackRead(key);
        return _buffer.TrackedReadCount;
    }

    [Benchmark]
    public int StageDistinctWrites()
    {
        using var buffer = new MvccWriteBuffer(IndexThreshold);
        foreach (var key in _keys)
        {
            if (buffer.FindMutation(key) != -1) throw new InvalidOperationException();
            buffer.AddMutation(key, _value, false);
        }
        return buffer.MutationCount;
    }

    [GlobalCleanup]
    public void Cleanup() => _buffer.Dispose();

    private static byte[] Key(int value)
    {
        var key = new byte[8];
        BinaryPrimitives.WriteInt64BigEndian(key, value);
        return key;
    }
}
