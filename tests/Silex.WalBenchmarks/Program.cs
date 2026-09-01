using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Silex;

BenchmarkRunner.Run<WriteAheadLogBenchmarks>();

[MemoryDiagnoser, ShortRunJob]
public class WriteAheadLogBenchmarks
{
    private const int MutationCount = 1_000;

    private readonly LsmWriteBatch _batch = new();
    private readonly byte[][] _keys = new byte[MutationCount][];
    private readonly byte[][] _values = new byte[MutationCount][];
    private LsmStorage _storage = null!;
    private string _path = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        for (var i = 0; i < MutationCount; i++)
        {
            _keys[i] = BitConverter.GetBytes(i);
            _values[i] = BitConverter.GetBytes(i + 1);
            _batch.Put(_keys[i], _values[i]);
        }
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _path = Path.Combine(Path.GetTempPath(), $"silex-wal-benchmark-{Guid.NewGuid():N}");
        _storage = LsmStorage.OpenAsync(
                _path,
                new StorageOptions
                {
                    FlushPeriod = TimeSpan.Zero,
                    MemTableSizeLimit = long.MaxValue,
                })
            .GetAwaiter()
            .GetResult();
    }

    [IterationCleanup]
    public void IterationCleanup()
    {
        _storage.CloseAsync().GetAwaiter().GetResult();
        Directory.Delete(_path, recursive: true);
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = MutationCount)]
    public void IndividualFrames()
    {
        for (var i = 0; i < _batch.Count; i++)
        {
            _storage.Put(_keys[i], _values[i]);
        }
    }

    [Benchmark(OperationsPerInvoke = MutationCount)]
    public void OneBatchFrame()
    {
        _storage.Write(_batch);
    }
}
