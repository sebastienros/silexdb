using BenchmarkDotNet.Attributes;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class RawMergeScanBenchmarks
{
    private const int EntryCount = 2048;
    private const int TableCount = 8;

    private string _directory = null!;
    private LsmStorageInner _storage = null!;
    private byte[] _seekKey = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"silex-raw-merge-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_directory);
        _storage = new LsmStorageInner(_directory, new StorageOptions
        {
            UseWriteAheadLog = false,
            BlockSize = 4096,
            MaxCompactionTiers = int.MaxValue,
        });

        for (var table = 0; table < TableCount; table++)
        {
            for (var entry = 0; entry < EntryCount; entry++)
            {
                _storage.Put(
                    ByteSlice.FromMemory(Encode(entry)),
                    ByteSlice.FromMemory(Encode((table << 20) | entry)));
            }

            _storage.ForceFreezeMemTable();
            await _storage.ForceFlushNextImmutableMemTableAsync();
        }

        _seekKey = Encode(EntryCount / 2);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _storage.Dispose();
        Directory.Delete(_directory, recursive: true);
    }

    [Benchmark]
    public ValueTask<long> ScanRaw_Overlapping()
    {
        return _storage.ScanRawAsync(0L, static (_, key, value) =>
        {
            return key.Length == sizeof(int) && value.Length == sizeof(int);
        });
    }

    [Benchmark]
    public ValueTask<long> SeekRaw_Overlapping()
    {
        return _storage.SeekRawAsync(_seekKey, 0L, static (_, key, value) =>
        {
            return key.Length == sizeof(int) && value.Length == sizeof(int);
        });
    }

    private static byte[] Encode(int value)
    {
        var bytes = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        return bytes;
    }
}
