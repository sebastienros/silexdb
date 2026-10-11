using BenchmarkDotNet.Attributes;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class PointReadBatchBenchmarks
{
    private const int Entries = 65536;
    private string _directory = null!;
    private MvccStorage _storage = null!;
    private MvccSnapshot _snapshot = null!;
    private ReadOnlyMemory<byte>[] _keys = null!;
    private readonly byte[] _buffer = new byte[256];

    [Params(1, 8, 64, 512)]
    public int Keys { get; set; }

    [Params(false, true)]
    public bool Sst { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"silex-point-batch-{Guid.NewGuid():N}");
        _storage = await MvccStorage.OpenAsync(_directory, new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
            MemTableSizeLimit = 64 * 1024 * 1024,
        });
        for (var i = 0; i < Entries; i++) _storage.Put(Key(i), new byte[100]);
        if (Sst) await _storage.FlushAndCompactAsync();
        _snapshot = _storage.CreateSnapshot();
        var random = new Random(42);
        _keys = Enumerable.Range(0, Keys).Select(_ => (ReadOnlyMemory<byte>)Key(random.Next(Entries))).ToArray();
    }

    [Benchmark(Baseline = true)]
    public async ValueTask<int> IndividualGets()
    {
        var found = 0;
        foreach (var key in _keys)
        {
            if (await _snapshot.GetRawAsync(key.Span, _buffer) >= 0) found++;
        }
        return found;
    }

    [Benchmark]
    public async ValueTask<int> BatchedSnapshot()
    {
        using var batch = await _snapshot.ReadManyRawAsync(_keys, 1 << 20);
        return batch.Count;
    }

    [Benchmark]
    public async ValueTask<int> BatchedStorage()
    {
        using var batch = await _storage.ReadManyRawAsync(_keys, 1 << 20);
        return batch.Count;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _snapshot.Dispose();
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
