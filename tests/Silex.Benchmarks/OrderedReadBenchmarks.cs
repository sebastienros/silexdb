using BenchmarkDotNet.Attributes;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class OrderedReadBenchmarks
{
    private string _directory = null!;
    private LsmStorage _storage = null!;
    private byte[] _lower = null!;
    private OrderedReadRange _range = null!;

    [Params(2048, 65536)]
    public int Entries { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"silex-ordered-{Guid.NewGuid():N}");
        _storage = await LsmStorage.OpenAsync(_directory, new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
            MemTableSizeLimit = 64 * 1024 * 1024,
        });
        for (var i = 0; i < Entries; i++) _storage.Put(Key(i), new byte[16]);
        _lower = Key(Entries / 2);
        _range = new OrderedReadRange(_lower, true, Key(Entries / 2 + 32), false);
        using var warmup = _storage.CreateOrderedReadCursor(_range);
        using var batch = await warmup.ReadBatchAsync(32, 640);
        if (batch.Count != 32 || batch.PayloadBytes != 640)
        {
            throw new InvalidOperationException("Unexpected bounded-read shape.");
        }
    }

    [Benchmark(Baseline = true)]
    public ValueTask<long> CallbackSeek32() =>
        _storage.SeekRawAsync(_lower, 0, static (_, _, _) => true, 32);

    [Benchmark]
    public async ValueTask<int> OwnedForward32()
    {
        using var cursor = _storage.CreateOrderedReadCursor(_range);
        using var batch = await cursor.ReadBatchAsync(32, 640);
        return batch.Count;
    }

    [Benchmark]
    public async ValueTask<int> OwnedReverse32()
    {
        using var cursor = _storage.CreateOrderedReadCursor(_range, OrderedReadDirection.Reverse);
        using var batch = await cursor.ReadBatchAsync(32, 640);
        return batch.Count;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _storage.DisposeAsync();
        Directory.Delete(_directory, recursive: true);
    }

    private static byte[] Key(int value)
    {
        var bytes = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        return bytes;
    }
}
