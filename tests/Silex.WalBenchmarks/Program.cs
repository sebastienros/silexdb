using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Silex;
using System.Diagnostics;

if (args.Length != 0 && args[0] == "--mvcc")
{
    if (args.Length is < 3 or > 4)
    {
        throw new ArgumentException("Usage: --mvcc <clients> <transactions-per-client> [group-size]");
    }
    var clients = int.Parse(args[1]);
    var count = int.Parse(args[2]);
    ArgumentOutOfRangeException.ThrowIfNegativeOrZero(clients);
    ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
    var latencies = new double[checked(clients * count)];
    var path = Path.Combine(Path.GetTempPath(), $"silex-group-benchmark-{Guid.NewGuid():N}");
    var options = new StorageOptions
    {
        FlushPeriod = TimeSpan.Zero,
        MemTableSizeLimit = long.MaxValue,
        SyncWriteAheadLogToDisk = true
    };
    if (args.Length > 3)
    {
        options.MvccCommitGroupSize = int.Parse(args[3]);
    }
    try
    {
        await using var storage = await MvccStorage.OpenAsync(path, options);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = Enumerable.Range(0, clients).Select(client => Task.Run(async () =>
        {
            var key = new byte[16];
            var value = new byte[100];
            BitConverter.TryWriteBytes(key, client);
            await start.Task;
            for (var i = 0; i < count; i++)
            {
                var before = Stopwatch.GetTimestamp();
                using var transaction = storage.BeginTransaction();
                transaction.Put(key, value);
                await transaction.CommitAsync();
                latencies[client * count + i] = Stopwatch.GetElapsedTime(before).TotalMilliseconds;
            }
        })).ToArray();
        var clock = Stopwatch.StartNew();
        start.SetResult();
        await Task.WhenAll(workers);
        clock.Stop();
        Console.WriteLine($"clients={clients} group={options.MvccCommitGroupSize} transactions={clients * count} seconds={clock.Elapsed.TotalSeconds:F3} tx/sec={clients * count / clock.Elapsed.TotalSeconds:F1}");
        Array.Sort(latencies);
        Console.WriteLine($"latency ms: p50={latencies[(latencies.Length - 1) / 2]:F3} p95={latencies[(latencies.Length - 1) * 95 / 100]:F3} p99={latencies[(latencies.Length - 1) * 99 / 100]:F3} max={latencies[^1]:F3}");
    }
    finally
    {
        Directory.Delete(path, recursive: true);
    }
}
else
{
    BenchmarkRunner.Run<WriteAheadLogBenchmarks>();
}

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
