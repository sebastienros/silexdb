using BenchmarkDotNet.Attributes;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class MvccPointLookupBenchmarks
{
    private const int Entries = 8192;
    private const int OperationCount = 64;
    private string _directory = null!;
    private MvccStorage _storage = null!;
    private MvccSnapshot _snapshot = null!;
    private byte[][] _keys = null!;
    private byte[][] _missingKeys = null!;
    private MvccWriteBuffer _tracked = null!;
    private MvccWriteBuffer _written = null!;
    private long _validationSequence;
    private readonly byte[] _buffer = new byte[100];

    [Params(false, true)]
    public bool Sst { get; set; }

    [Params(1, 8)]
    public int Versions { get; set; }

    [GlobalSetup]
    public async Task Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"silex-mvcc-lookup-{Guid.NewGuid():N}");
        _storage = await MvccStorage.OpenAsync(_directory, new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
            MemTableSizeLimit = 64 * 1024 * 1024,
        });
        for (var version = 0; version < Versions; version++)
        {
            for (var i = 0; i < Entries; i++) _storage.Put(Key(i * 2), _buffer);
        }

        var random = new Random(42);
        _keys = Enumerable.Range(0, OperationCount)
            .Select(_ => Key(random.Next(Entries - 1) * 2)).ToArray();
        _missingKeys = _keys.Select(key =>
            Key(BinaryPrimitives.ReadInt32BigEndian(key) + 1)).ToArray();
        _tracked = new MvccWriteBuffer();
        _written = new MvccWriteBuffer();
        foreach (var key in _keys)
        {
            _tracked.TrackRead(key);
            _written.AddMutation(key, _buffer, isTombstone: false);
        }
        _validationSequence = _storage.PublishedSequence;
        var conflictingKey = Key((Entries - 1) * 2);
        _written.AddMutation(conflictingKey, _buffer, isTombstone: false);
        _storage.Delete(conflictingKey);
        if (Sst) await _storage.FlushAndCompactAsync();
        _snapshot = _storage.CreateSnapshot();
    }

    [Benchmark(OperationsPerInvoke = OperationCount)]
    public async ValueTask<int> SnapshotHits()
    {
        var found = 0;
        foreach (var key in _keys)
        {
            if (await _snapshot.GetRawAsync(key, _buffer) >= 0) found++;
        }
        return found;
    }

    [Benchmark(OperationsPerInvoke = OperationCount)]
    public async ValueTask<int> SnapshotMisses()
    {
        var found = 0;
        foreach (var key in _missingKeys)
        {
            if (await _snapshot.GetRawAsync(key, _buffer) >= 0) found++;
        }
        return found;
    }

    [Benchmark]
    public ValueTask<MvccConflict?> TrackedReadValidation() =>
        _storage.TryCommitAsync(_validationSequence, _tracked, default);

    // The final written key conflicts, so repeated invocations validate without growing the database.
    [Benchmark]
    public ValueTask<MvccConflict?> WrittenKeyValidation() =>
        _storage.TryCommitAsync(_validationSequence, _written, default);

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _snapshot.Dispose();
        _tracked.Dispose();
        _written.Dispose();
        await _storage.DisposeAsync();
        Directory.Delete(_directory, recursive: true);
    }

    private static byte[] Key(int value)
    {
        var key = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(key, value);
        return key;
    }
}
