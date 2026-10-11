using BenchmarkDotNet.Attributes;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class ConditionalMutationBenchmarks
{
    private const int Entries = 8192;
    private const int OperationCount = 1024;
    private string _directory = null!;
    private LsmStorage? _plain;
    private MvccStorage? _mvcc;
    private byte[][] _keys = null!;
    private readonly byte[] _expected = new byte[100];
    private readonly byte[] _replacement = Enumerable.Repeat((byte)1, 100).ToArray();
    private readonly byte[] _scratch = new byte[100];
    private readonly LsmWriteBatch _batch = new();

    [Params(false, true)]
    public bool Sync { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _keys = Enumerable.Range(0, Entries).Select(i =>
        {
            var key = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(key, i);
            return key;
        }).ToArray();
    }

    [IterationSetup(Target = nameof(UncheckedPlainReadBatch))]
    public void SetupPlain() => InitializeAsync(mvcc: false).GetAwaiter().GetResult();

    [IterationSetup(Targets = new[] { nameof(MvccUnconditionalBatch), nameof(MvccConditionalBatch) })]
    public void SetupMvcc() => InitializeAsync(mvcc: true).GetAwaiter().GetResult();

    private async Task InitializeAsync(bool mvcc)
    {
        _directory = Path.Combine(Path.GetTempPath(), $"silex-conditional-{Guid.NewGuid():N}");
        var options = new StorageOptions
        {
            UseWriteAheadLog = true,
            SyncWriteAheadLogToDisk = Sync,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
            MemTableSizeLimit = 64 * 1024 * 1024
        };
        if (mvcc)
        {
            _mvcc = await MvccStorage.OpenAsync(_directory, options);
            using var transaction = _mvcc.BeginTransaction();
            foreach (var key in _keys) transaction.Put(key, _expected);
            await transaction.CommitAsync();
            await _mvcc.FlushAndCompactAsync();
        }
        else
        {
            _plain = await LsmStorage.OpenAsync(_directory, options);
            _batch.Clear();
            foreach (var key in _keys) _batch.Put(key, _expected);
            _plain.Write(_batch);
            await _plain.FlushAndCompactAsync();
        }
    }

    // This is an unchecked cost baseline, not an atomic read/check/write implementation.
    [Benchmark(Baseline = true, OperationsPerInvoke = OperationCount)]
    public async Task UncheckedPlainReadBatch()
    {
        for (var i = 0; i < OperationCount; i++)
        {
            var length = await _plain!.GetRawAsync(_keys[i * 2], _scratch);
            if (length != _expected.Length || !_scratch.AsSpan().SequenceEqual(_expected))
                throw new InvalidOperationException("Unexpected guard value.");
            _batch.Clear();
            _batch.Put(_keys[i * 2], _replacement);
            _batch.Put(_keys[i * 2 + 1], _replacement);
            _plain.Write(_batch);
        }
    }

    [Benchmark(OperationsPerInvoke = OperationCount)]
    public async Task MvccUnconditionalBatch()
    {
        for (var i = 0; i < OperationCount; i++)
        {
            using var transaction = _mvcc!.BeginTransaction();
            transaction.Put(_keys[i * 2], _replacement);
            transaction.Put(_keys[i * 2 + 1], _replacement);
            await transaction.CommitAsync();
        }
    }

    [Benchmark(OperationsPerInvoke = OperationCount)]
    public async Task MvccConditionalBatch()
    {
        for (var i = 0; i < OperationCount; i++)
        {
            using var transaction = _mvcc!.BeginTransaction();
            var length = await transaction.GetForUpdateRawAsync(_keys[i * 2], _scratch);
            if (length != _expected.Length || !_scratch.AsSpan().SequenceEqual(_expected))
                throw new InvalidOperationException("Unexpected guard value.");
            transaction.Put(_keys[i * 2], _replacement);
            transaction.Put(_keys[i * 2 + 1], _replacement);
            await transaction.CommitAsync();
        }
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _plain?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _mvcc?.DisposeAsync().AsTask().GetAwaiter().GetResult();
        _plain = null;
        _mvcc = null;
        Directory.Delete(_directory, recursive: true);
    }
}
