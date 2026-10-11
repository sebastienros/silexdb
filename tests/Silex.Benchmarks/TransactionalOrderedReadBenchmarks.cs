using BenchmarkDotNet.Attributes;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class TransactionalOrderedReadBenchmarks
{
    private string _directory = null!;
    private MvccStorage _storage = null!;
    private MvccTransaction _transaction = null!;
    private OrderedReadRange _range = null!;

    [Params(2048, 65536)]
    public int Entries { get; set; }

    [Params(0, 8, 128)]
    public int PendingWrites { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"silex-transaction-ordered-{Guid.NewGuid():N}");
        _storage = await MvccStorage.OpenAsync(_directory, new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
            MemTableSizeLimit = 64 * 1024 * 1024,
        });
        for (var i = 0; i < Entries; i++) _storage.Put(Key(i), new byte[16]);
        _range = new OrderedReadRange(Key(Entries / 2), true, Key(Entries / 2 + 32), false);
        _transaction = _storage.BeginTransaction();
        for (var i = PendingWrites - 1; i >= 0; i--)
        {
            _transaction.Put(Key(Entries / 2 + i), new byte[16]);
        }
        using var warmup = _transaction.CreateOrderedReadCursor(_range);
        using var batch = await warmup.ReadBatchAsync(32, 640);
        if (batch.Count != 32 || batch.PayloadBytes != 640)
        {
            throw new InvalidOperationException("Unexpected transactional bounded-read shape.");
        }
    }

    [Benchmark(Baseline = true)]
    public async ValueTask<int> SnapshotForward32()
    {
        using var cursor = _storage.CreateOrderedReadCursor(_range);
        using var batch = await cursor.ReadBatchAsync(32, 640);
        return batch.Count;
    }

    [Benchmark]
    public async ValueTask<int> TransactionForward32()
    {
        using var cursor = _transaction.CreateOrderedReadCursor(_range);
        using var batch = await cursor.ReadBatchAsync(32, 640);
        return batch.Count;
    }

    [Benchmark]
    public async ValueTask<int> TransactionReverse32()
    {
        using var cursor = _transaction.CreateOrderedReadCursor(_range, OrderedReadDirection.Reverse);
        using var batch = await cursor.ReadBatchAsync(32, 640);
        return batch.Count;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _transaction.Dispose();
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
