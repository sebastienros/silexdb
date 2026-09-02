using BenchmarkDotNet.Attributes;
using Silex.MemTables;
using System.Buffers;
using System.Buffers.Binary;

namespace Silex.Benchmarks;

[MemoryDiagnoser, ShortRunJob]
public class DataOrientedWriteBenchmarks
{
    private const int OperationCount = 1_000;

    private byte[][] _keys = null!;
    private byte[][] _values = null!;

    [GlobalSetup]
    public void Setup()
    {
        _keys = new byte[OperationCount][];
        _values = new byte[OperationCount][];
        for (var i = 0; i < OperationCount; i++)
        {
            _keys[i] = BitConverter.GetBytes(i);
            _values[i] = BitConverter.GetBytes(~i);
        }
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = OperationCount)]
    [BenchmarkCategory("MemTable")]
    public int LegacyMemTableWrites()
    {
        using var table = new LegacyMemTable();
        for (var i = 0; i < OperationCount; i++)
        {
            table.Put(_keys[i], _values[i]);
        }

        return table.Count;
    }

    [Benchmark(OperationsPerInvoke = OperationCount)]
    [BenchmarkCategory("MemTable")]
    public int PackedMemTableWrites()
    {
        using var table = new MemTable(1);
        for (var i = 0; i < OperationCount; i++)
        {
            table.PutRaw(_keys[i], _values[i]);
        }

        return table.Count;
    }

    private sealed class LegacyMemTable : IDisposable
    {
        private readonly Dictionary<ByteSlice, ByteSlice> _entries = new(ByteSlice.EqualityComparer);
        private readonly SlabOwner _slab = new(OperationCount * 16);
        private int _position;

        public int Count => _entries.Count;

        public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            var storedKey = Copy(key);
            var storedValue = Copy(value);
            _entries[storedKey] = storedValue;
        }

        public void Dispose() => _slab.Dispose();

        private ByteSlice Copy(ReadOnlySpan<byte> value)
        {
            var offset = _position;
            value.CopyTo(_slab.Memory.Span[offset..]);
            _position += value.Length;
            return ByteSlice.CreateView(_slab, offset, value.Length);
        }
    }

    private sealed class SlabOwner(int length) : IMemoryOwner<byte>
    {
        private byte[]? _bytes = GC.AllocateUninitializedArray<byte>(length);

        public Memory<byte> Memory => _bytes ?? throw new ObjectDisposedException(nameof(SlabOwner));

        public void Dispose() => _bytes = null;
    }
}

[MemoryDiagnoser, ShortRunJob]
public class MvccMutationBenchmarks
{
    private const int OperationCount = 1_000;

    private byte[][] _keys = null!;
    private byte[][] _values = null!;

    [GlobalSetup]
    public void Setup()
    {
        _keys = new byte[OperationCount][];
        _values = new byte[OperationCount][];
        for (var i = 0; i < OperationCount; i++)
        {
            _keys[i] = BitConverter.GetBytes(i);
            _values[i] = BitConverter.GetBytes(~i);
        }
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = OperationCount)]
    public int LegacyMvccMutations()
    {
        var mutations = new List<LegacyMutation>();
        for (var i = 0; i < OperationCount; i++)
        {
            mutations.Add(new LegacyMutation(_keys[i].ToArray(), _values[i].ToArray()));
        }

        return mutations.Count;
    }

    [Benchmark(OperationsPerInvoke = OperationCount)]
    public int PackedMvccMutations()
    {
        using var writes = new MvccWriteBuffer();
        for (var i = 0; i < OperationCount; i++)
        {
            writes.AddMutation(_keys[i], _values[i], isTombstone: false);
        }

        return writes.MutationCount;
    }

    private sealed class LegacyMutation(byte[] key, byte[] value)
    {
        public byte[] Key { get; } = key;
        public byte[] Value { get; } = value;
    }
}

[MemoryDiagnoser, ShortRunJob]
public class TypedEncodingBenchmarks
{
    private const int OperationCount = 1_000;

    [Benchmark(Baseline = true, OperationsPerInvoke = OperationCount)]
    public int ArrayPoolInt32Encoding()
    {
        var checksum = 0;
        for (var i = 0; i < OperationCount; i++)
        {
            var rented = ArrayPool<byte>.Shared.Rent(sizeof(int));
            BitConverter.TryWriteBytes(rented, i);
            checksum ^= rented[0];
            ArrayPool<byte>.Shared.Return(rented);
        }

        return checksum;
    }

    [Benchmark(OperationsPerInvoke = OperationCount)]
    public int StackallocInt32Encoding()
    {
        var checksum = 0;
        Span<byte> buffer = stackalloc byte[sizeof(int)];
        for (var i = 0; i < OperationCount; i++)
        {
            BitConverter.TryWriteBytes(buffer, i);
            checksum ^= buffer[0];
        }

        return checksum;
    }
}

[MemoryDiagnoser, ShortRunJob]
public class MemTableLookupBenchmarks
{
    private const int EntryCount = 4_096;
    private const int LookupCount = 16_384;

    private MemTable _table = null!;
    private byte[][] _hitKeys = null!;
    private byte[][] _missKeys = null!;
    private int[] _lookupIndices = null!;

    [Params(16, 1024, 16 * 1024)]
    public int ValueSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _table = new MemTable(1);
        _hitKeys = new byte[EntryCount][];
        _missKeys = new byte[EntryCount][];
        _lookupIndices = new int[LookupCount];
        var value = new byte[ValueSize];
        var random = new Random(42);

        for (var i = 0; i < EntryCount; i++)
        {
            _hitKeys[i] = EncodeKey(i);
            _missKeys[i] = EncodeKey(EntryCount + i);
            _table.PutRaw(_hitKeys[i], value);
        }

        for (var i = 0; i < LookupCount; i++)
        {
            _lookupIndices[i] = random.Next(EntryCount);
        }
    }

    [GlobalCleanup]
    public void Cleanup() => _table.Dispose();

    [Benchmark(OperationsPerInvoke = LookupCount)]
    public int RandomHit()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _table.TryGetRaw(_hitKeys[_lookupIndices[i]], out _, out _) ? 1 : 0;
        }

        return found;
    }

    [Benchmark(OperationsPerInvoke = LookupCount)]
    public int RandomMiss()
    {
        var found = 0;
        for (var i = 0; i < LookupCount; i++)
        {
            found += _table.TryGetRaw(_missKeys[_lookupIndices[i]], out _, out _) ? 1 : 0;
        }

        return found;
    }

    private static byte[] EncodeKey(int value)
    {
        var key = new byte[16];
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(12), value);
        return key;
    }
}

[MemoryDiagnoser, ShortRunJob]
public class ImmutableMemTableLookupBenchmarks
{
    private const int ImmutableTableCount = 8;
    private const int OperationsPerInvoke = 1_000;

    private string _directory = null!;
    private LsmStorageInner _storage = null!;
    private byte[] _key = null!;
    private byte[] _destination = null!;

    [GlobalSetup]
    public void Setup()
    {
        _directory = Path.Combine(Path.GetTempPath(), $"silex-immutable-lookup-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_directory);
        _storage = new LsmStorageInner(_directory, new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
        });
        _key = new byte[16];
        _destination = new byte[sizeof(int)];

        for (var i = 0; i < ImmutableTableCount; i++)
        {
            BinaryPrimitives.WriteInt32BigEndian(_destination, i);
            _storage.Put(ByteSlice.FromMemory(_key), ByteSlice.FromMemory(_destination));
            _storage.ForceFreezeMemTable();
        }
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _storage.Dispose();
        Directory.Delete(_directory, recursive: true);
    }

    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public int NewestImmutableHit()
    {
        var length = 0;
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            length += _storage.GetRawAsync(_key, _destination).GetAwaiter().GetResult();
        }

        return length;
    }
}
