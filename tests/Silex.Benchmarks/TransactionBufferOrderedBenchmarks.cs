using BenchmarkDotNet.Attributes;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class TransactionBufferOrderedBenchmarks
{
    private MvccWriteBuffer _buffer = null!;
    private OrderedReadRange _range = null!;
    private readonly byte[] _value = new byte[16];

    [Params(64, 4096, 65536)]
    public int Entries { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _buffer = new MvccWriteBuffer();
        for (var i = Entries - 1; i >= 0; i--)
        {
            _buffer.AddMutation(Key(i), _value, false);
        }
        _range = new OrderedReadRange(Key(Entries / 4), true, Key(Entries / 4 + 32), false);
        using var warmup = OrderedWriteSnapshot.Create(_buffer, _range);
        if (warmup?.Count != 32) throw new InvalidOperationException("Expected 32 overlay entries.");
    }

    // Reference algorithm: visit every pending key, copy the matching range, sort the copy.
    // It omits the reference snapshot wrapper, conservatively favoring the old algorithm.
    [Benchmark(Baseline = true)]
    public int FullScanAndSortRange32()
    {
        using var copy = new MvccWriteBuffer(indexThreshold: int.MaxValue);
        for (var i = 0; i < _buffer.MutationCount; i++)
        {
            var mutation = _buffer.GetMutation(i);
            var key = _buffer.GetKey(mutation).Span;
            if (_range.Before(key) || _range.After(key)) continue;
            copy.AddMutation(key, _buffer.GetValue(mutation).Span, mutation.IsTombstone);
        }
        return copy.OrderedMutations().Length;
    }

    [Benchmark]
    public int CachedOrderRange32()
    {
        using var snapshot = OrderedWriteSnapshot.Create(_buffer, _range);
        return snapshot?.Count ?? 0;
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
