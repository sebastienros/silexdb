using BenchmarkDotNet.Attributes;
using System.Text;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class MvccDocumentTransactionBenchmarks
{
    private const int Documents = 8192;
    private const int OperationCount = 16;
    private string _directory = null!;
    private MvccStorage _storage = null!;
    private byte[][] _documents = null!;
    private byte[][] _oldCategory = null!;
    private byte[][] _newCategory = null!;
    private byte[][] _oldStatus = null!;
    private byte[][] _newStatus = null!;
    private readonly byte[] _guardKey = "tenant/settings"u8.ToArray();
    private readonly byte[] _original = new byte[512];
    private readonly byte[] _replacement = Enumerable.Repeat((byte)1, 512).ToArray();
    private readonly byte[] _buffer = new byte[512];

    [Params(false, true)]
    public bool Sst { get; set; }

    [GlobalSetup]
    public void SetupKeys()
    {
        _documents = new byte[Documents][];
        _oldCategory = new byte[Documents][];
        _newCategory = new byte[Documents][];
        _oldStatus = new byte[Documents][];
        _newStatus = new byte[Documents][];
        for (var i = 0; i < Documents; i++)
        {
            _documents[i] = Encoding.UTF8.GetBytes($"document/{i:D8}");
            _oldCategory[i] = Encoding.UTF8.GetBytes($"index/category/old/{i:D8}");
            _newCategory[i] = Encoding.UTF8.GetBytes($"index/category/new/{i:D8}");
            _oldStatus[i] = Encoding.UTF8.GetBytes($"index/status/old/{i:D8}");
            _newStatus[i] = Encoding.UTF8.GetBytes($"index/status/new/{i:D8}");
        }
    }

    [IterationSetup]
    public void Initialize() => InitializeAsync().GetAwaiter().GetResult();

    private async Task InitializeAsync()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"silex-document-tx-{Guid.NewGuid():N}");
        _storage = await MvccStorage.OpenAsync(_directory, new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
            MemTableSizeLimit = 64 * 1024 * 1024,
        });
        for (var i = 0; i < Documents; i++)
        {
            _storage.Put(_documents[i], _original);
            _storage.Put(_oldCategory[i], _documents[i]);
            _storage.Put(_oldStatus[i], _documents[i]);
        }
        _storage.Put(_guardKey, [1]);
        if (Sst) await _storage.FlushAndCompactAsync();
        await _storage.GetRawAsync(_documents[0], _buffer);
    }

    [Benchmark(OperationsPerInvoke = OperationCount)]
    public async Task UpdateDocumentAndTwoIndexes()
    {
        for (var i = 0; i < OperationCount; i++)
        {
            var index = i * (Documents / OperationCount);
            using var transaction = _storage.BeginTransaction();
            if (await transaction.GetForUpdateRawAsync(_documents[index], _buffer) != _original.Length ||
                !_buffer.AsSpan().SequenceEqual(_original))
            {
                throw new InvalidOperationException("Unexpected document value.");
            }
            if (await transaction.GetForUpdateRawAsync(_guardKey, _buffer) != 1 || _buffer[0] != 1)
            {
                throw new InvalidOperationException("Unexpected tenant guard.");
            }
            transaction.Put(_documents[index], _replacement);
            transaction.Delete(_oldCategory[index]);
            transaction.Put(_newCategory[index], _documents[index]);
            transaction.Delete(_oldStatus[index]);
            transaction.Put(_newStatus[index], _documents[index]);
            await transaction.CommitAsync();
        }
    }

    [IterationCleanup]
    public void Cleanup()
    {
        _storage.DisposeAsync().AsTask().GetAwaiter().GetResult();
        Directory.Delete(_directory, recursive: true);
    }
}
