using System.Buffers;
using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Silex.BloomFilters;
using Silex.Blocks;
using Silex.Serialization;
using Silex.MemTables;
using Silex.Wal;
using TUnit.Assertions.Enums;

namespace Silex.Test;

public class StorageTests
{
    private static int DecodeInt32(ByteSlice? value) => value is null || value.IsEmpty ? default : new Int32Encoder().Decode(value.Span);
    private static int DecodeInt32(OwnedByteSlice? value)
    {
        using (value)
        {
            return value is null || value.IsEmpty ? default : new Int32Encoder().Decode(value.Span);
        }
    }

    private static int DecodeInt32(int value) => value;
    private static byte[] ToArray(ByteSlice value) => value.Span.ToArray();
    private static byte[] ToArray(OwnedByteSlice? value)
    {
        using (value)
        {
            return value is null ? [] : value.Span.ToArray();
        }
    }

    // These in-memory unit tests construct LsmStorageInner directly against the shared system temp
    // folder, so the write-ahead log is disabled to avoid littering it (and the per-append flush).
    private readonly StorageOptions _defaultStorageOptions = new() { UseWriteAheadLog = false };
    private readonly TextWriter? _output = null;

    [Test]
    public async Task CanPutArray()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6];

        storage.Put(key, value);
        var result = await storage.GetAsync(key);

        await Assert.That(ToArray(result)).IsEquivalentTo(value, CollectionOrdering.Matching);
        await Assert.That(storage._state.CurrentMemTable.Size).IsEqualTo(10);
    }

    [Test]
    public async Task GetReturnsZeroCopyBorrowFromMemTable()
    {
        // The byte-only memtable copies incoming spans into its arena, so reads should match by content.
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6];

        storage.Put(key, value);

        var result = await storage.GetAsync(key);
        await Assert.That(ToArray(result)).IsEquivalentTo(value, CollectionOrdering.Matching);

        var scanned = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList().Single();
        await Assert.That(ToArray(scanned.Key)).IsEquivalentTo(key, CollectionOrdering.Matching);
        await Assert.That(ToArray(scanned.Value)).IsEquivalentTo(value, CollectionOrdering.Matching);
    }

    [Test]
    public async Task PutValueIsCopied()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key1 = [1];
        byte[] key2 = [2];
        byte[] value = [4, 5, 6];

        storage.Put(key1, value);
        storage.Put(key2, value);

        var result1 = await storage.GetAsync(key1);
        var result2 = await storage.GetAsync(key2);

        await Assert.That(ToArray(result1)).IsEquivalentTo(value, CollectionOrdering.Matching);
        await Assert.That(ToArray(result2)).IsEquivalentTo(value, CollectionOrdering.Matching);
        await Assert.That(storage._state.CurrentMemTable.Size).IsEqualTo(16);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PutUpdatesExistingMemTableEntry(bool sortBeforeUpdate)
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);
        byte[] key = [1];
        byte[] value = [10];

        storage.Put(key, value);

        if (sortBeforeUpdate)
        {
            _ = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();
        }

        key[0] = 2;
        value[0] = 11;
        storage.Put([1], [20]);

        await Assert.That(storage._state.CurrentMemTable.Count).IsEqualTo(1);
        await Assert.That(ToArray(await storage.GetAsync([1]))).IsEquivalentTo(new byte[] { 20 }, CollectionOrdering.Matching);
        await Assert.That(await storage.GetAsync([2])).IsNull();
    }

    [Test]
    public async Task DeleteShouldStoreTombStone()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];

        storage.Delete(key);
        using var ownedKey = OwnedByteSlice.CopyFrom(key);

        await Assert.That(storage._state.CurrentMemTable.TryGet(ownedKey.Slice, out var result)).IsTrue();
        await Assert.That(result!.Length).IsEqualTo(0);
        await Assert.That(storage._state.CurrentMemTable.Size).IsEqualTo(7);
    }

    [Test]
    [Arguments(1, 1, 0)]
    [Arguments(7, 10, 1)]
    [Arguments(100, 100, 100)]
    [Arguments(10, 500, 83)]
    public async Task ShouldFreezeMemTablesWhenSizeIsOverLimit(int valueSize, int entries, int expectedImmutableTables)
    {
        var memTableSizeLimit = 100;

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit, UseWriteAheadLog = false };
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = i; // 4 bytes
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);

            storage.Put(key, value);
        }

        await Assert.That(storage._state.ImmutableMemTables.Count()).IsEqualTo(expectedImmutableTables);
    }

    [Test]
    public async Task GetFromImmutableMemTables()
    {
        var memTableSizeLimit = 100;
        int valueSize = 10;
        int entries = 100;

        var dictionary = new Dictionary<int, byte[]>();

        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit, UseWriteAheadLog = false };
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = i;
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);

            dictionary[i] = value;
            storage.Put(key, value);
        }

        for (var i = 1; i <= entries; i++)
        {
            var expectedValue = dictionary[i];
            var actualValue = await storage.GetAsync(i);

            await Assert.That(ToArray(actualValue)).IsEquivalentTo(expectedValue, CollectionOrdering.Matching);
        }
    }

    [Test]
    public async Task DeletedEntriesShouldAppearAfterPuts()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = FillImmutableMemTables(tempFolder);

        int key = 10;

        using (var result = await storage.GetAsync(key))
        {
            await Assert.That(result?.Length).IsEqualTo(10);
        }

        storage.Delete(key);
        using var deleted = await storage.GetAsync(key);

        await Assert.That(deleted).IsNull();
    }

    [Test]
    public async Task ScanListsAllMemTables()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        // table1: 2->del, 3->4, 4->5
        // table2: 1->1, 2->2, 3->3
        // table3: 5->4

        storage.Put(5, [4]);
        storage.ForceFreezeMemTable();

        storage.Put(1, [1]);
        storage.Put(2, [2]);
        storage.Put(3, [3]);
        storage.ForceFreezeMemTable();

        storage.Delete(2);
        storage.Put(3, [4]);
        storage.Put(4, [5]);

        var iterator = storage.CreateIterator();
        var list = iterator.EnumerateAsync().ToBlockingEnumerable().SnapshotList();

        // 1->1, 3->4, 4->5, 5->4 and 2->del should be discarded

        await Assert.That(list.Count).IsEqualTo(4);

        await Assert.That(DecodeInt32(list[0].Key)).IsEqualTo(1);
        await Assert.That(DecodeInt32(list[1].Key)).IsEqualTo(3);
        await Assert.That(DecodeInt32(list[2].Key)).IsEqualTo(4);
        await Assert.That(DecodeInt32(list[3].Key)).IsEqualTo(5);
    }

    [Test]
    public async Task BackwardsScanListsAllMemTables()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        storage.Put(5, [4]);
        storage.ForceFreezeMemTable();

        storage.Put(1, [1]);
        storage.Put(2, [2]);
        storage.Put(3, [3]);
        storage.ForceFreezeMemTable();

        storage.Delete(2);
        storage.Put(3, [4]);
        storage.Put(4, [5]);

        var iterator = storage.CreateIterator();
        var all = iterator.EnumerateBackwardsAsync().ToBlockingEnumerable().SnapshotList();
        var bounded = iterator.EnumerateBackwardsAsync(4).ToBlockingEnumerable().SnapshotList();

        await Assert.That(all.Select(entry => DecodeInt32(entry.Key))).IsEquivalentTo(new[] { 5, 4, 3, 1 }, CollectionOrdering.Matching);
        await Assert.That(bounded.Select(entry => DecodeInt32(entry.Key))).IsEquivalentTo(new[] { 4, 3, 1 }, CollectionOrdering.Matching);
    }

    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(5)]
    [Arguments(10)]
    public async Task ShouldHandleConcurrentClients(int levelOfConcurrency)
    {
        var maxKeysValue = 50; // Limit the number of unique ids to generate collisions
        var iterations = 50;
        var storageOptions = new StorageOptions { MemTableSizeLimit = 100, UseWriteAheadLog = false };
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, storageOptions);
        var iterator = storage.CreateIterator();

        var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5)).Token;

        await Parallel.ForAsync(0, levelOfConcurrency, timeout, (i, cancellationToken) =>
        {
            return Work(storage);
        });

        var allEntries = iterator.EnumerateAsync().ToBlockingEnumerable().SnapshotList();

        _output?.WriteLine($"Entries: {allEntries.Count}");
        _output?.WriteLine($"Immutable MemTables: {storage._state.ImmutableMemTables.Count()}");

        _output?.WriteLine($"Current MemTable: {storage._state.CurrentMemTable.Size}");

        foreach (var table in storage._state.ImmutableMemTables)
        {
            _output?.WriteLine($"ImmutableMemTable: {table.Size}");
        }

        _output?.WriteLine($"Entries:");
        foreach (var entry in allEntries)
        {
            var key = entry.Key.ToString(CultureInfo.InvariantCulture);
            var entryValue = await storage.GetAsync(entry.Key);
            var value = entryValue is null || entryValue.Length == 0 ? "del" : BinaryPrimitives.ReadUInt64LittleEndian(entryValue.AsSpan()).ToString(CultureInfo.InvariantCulture);

            _output?.WriteLine($"{key} -> {value}");
        }

        await Assert.That(allEntries.Count <= maxKeysValue).IsTrue();

        async ValueTask Work(LsmStorageInner storage)
        {
            for (var i = 0; i < iterations; i++)
            {
                var id = Random.Shared.NextInt64(maxKeysValue);
                var value = Random.Shared.NextInt64();
                storage.Put(id, BitConverter.GetBytes(value));
            }

            for (var i = 0; i < iterations; i++)
            {
                var operation = Random.Shared.NextInt64(10);
                var id = Random.Shared.NextInt64(maxKeysValue);

                switch (operation)
                {
                    case 0: // Put
                        var value = Random.Shared.NextInt64();
                        storage.Put(id, BitConverter.GetBytes(value));
                        break;

                    case 1: // Get
                        var actualValue = await storage.GetAsync(id);
                        break;

                    case 2: // Delete
                        storage.Delete(id);
                        break;

                    case 3: // Scan
                        _ = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();
                        break;
                }
            }
        }
    }

    [Test]
    [Arguments(null)]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(5)]
    public async Task ScanWithBoundsShouldFilterResults(int? lowerBound)
    {
        var count = 10;

        using var tempFolder = TempFolder.Create();
        using var storage = FillImmutableMemTables(tempFolder, entries: count, memTableSizeLimit: 1.KiB());
        int lowerBytes = lowerBound == null ? -1 : lowerBound.Value;

        var expectedKeys = Enumerable.Range(1, count);

        var entries = storage.CreateIterator().EnumerateAsync(lowerBytes).ToBlockingEnumerable().SnapshotList();

        if (lowerBound.HasValue)
        {
            expectedKeys = expectedKeys.Where(x => x >= lowerBound);

            foreach (var e in entries)
            {
                await Assert.That(lowerBytes <= DecodeInt32(e.Key)).IsTrue();
            }
        }

        var actualKeys = storage.CreateIterator().EnumerateAsync(lowerBytes).ToBlockingEnumerable().Select(x => DecodeInt32(x.Key)).ToArray();

        await Assert.That(actualKeys).IsEquivalentTo(expectedKeys, CollectionOrdering.Matching);
    }

    [Test]
    public async Task OpenAsyncShouldCreateFolder()
    {
        using var tempFolder = TempFolder.Create();

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, _defaultStorageOptions);

        await Assert.That(Directory.Exists(tempFolder)).IsTrue();

        await storage.CloseAsync();
    }

    [Test]
    public async Task ForceFlushShouldNotFailWithNoImmutableMemTable()
    {
        using var tempFolder = TempFolder.Create();

        var storage = await LsmStorage.OpenAsync<int, byte[]>(tempFolder, new StorageOptions());

        storage.Put(5, [4]);

        // Don't freeze current MemTable

        // Nothing to flush
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        await Assert.That(Directory.Exists(tempFolder)).IsTrue();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).IsEmpty();

        await storage.CloseAsync();
    }

    [Test]
    public async Task ForceFlushShouldFlushMemTable()
    {
        using var tempFolder = TempFolder.Create();

        var storage = await LsmStorage.OpenAsync<int, byte[]>(tempFolder, new());

        storage.Put(5, [4]);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        await Assert.That(Directory.Exists(tempFolder)).IsTrue();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        await storage.CloseAsync();
    }

    [Test]
    public async Task GetShouldReadFromFlushedSsTable()
    {
        using var tempFolder = TempFolder.Create();

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, new());

        const int count = 200;

        // Values are offset by 1 so a missing key (default 0) is distinguishable from a stored value.
        for (var i = 0; i < count; i++)
        {
            storage.Put(i, i + 1);
        }

        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        // Everything has been flushed: a single SST, no immutable mem tables, and an empty current mem table.
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();
        await Assert.That(storage._inner._state.ImmutableMemTables).IsEmpty();
        await Assert.That(storage._inner._state.CurrentMemTable.Size).IsEqualTo(0L);

        // Reads must go through the bloom filter and block decoding of the SST.
        for (var i = 0; i < count; i++)
        {
            await Assert.That(DecodeInt32(await storage.GetAsync(i))).IsEqualTo(i + 1);
        }

        // A key that was never inserted returns the default value.
        await Assert.That(DecodeInt32(await storage.GetAsync(10000))).IsEqualTo(0);

        await storage.CloseAsync();
    }

    [Test]
    public async Task GetRawShouldKeepRandomReadsCorrectWhenBlockCacheChurns()
    {
        const int count = 50_000;
        const int keySize = 16;
        const int valueSize = 100;

        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            MemTableSizeLimit = 1.KiB(),
            BlockCacheSizeLimit = 1,
        };

        using var storage = new LsmStorageInner(tempFolder, options);
        var value = new byte[valueSize];

        for (var i = 0; i < value.Length; i++)
        {
            value[i] = (byte)i;
        }

        for (var i = 0; i < count; i++)
        {
            storage.Put(CreateBenchmarkKey(i), value.ToArray());
        }

        await storage.FlushAndCompactAsync();

        var keyBuffer = new byte[keySize];
        var valueBuffer = new byte[valueSize];
        var rng = CreateDeterministicRandom(seed: 1000, threadId: 0, stream: 2);
        var misses = 0;

        for (var i = 0; i < count; i++)
        {
            WriteBenchmarkKey(rng.NextInt64(count), keyBuffer);

            if (await storage.GetRawAsync(keyBuffer, valueBuffer) < 0)
            {
                misses++;
            }
        }

        await Assert.That(misses).IsEqualTo(0);

        static byte[] CreateBenchmarkKey(long value)
        {
            var key = new byte[keySize];
            WriteBenchmarkKey(value, key);
            return key;
        }

        static void WriteBenchmarkKey(long value, Span<byte> destination)
        {
            BinaryPrimitives.WriteUInt64BigEndian(destination[..8], (ulong)value);

            for (var i = 8; i < destination.Length; i++)
            {
                destination[i] = (byte)'0';
            }
        }

        static Random CreateDeterministicRandom(int seed, int threadId, int stream)
        {
            var hash = 0xcbf29ce484222325UL;

            foreach (var value in stackalloc[] { seed, threadId, stream })
            {
                hash = (hash ^ (uint)value) * 0x100000001b3UL;
            }

            hash ^= hash >> 30;
            hash *= 0xbf58476d1ce4e5b9UL;
            hash ^= hash >> 27;
            hash *= 0x94d049bb133111ebUL;
            hash ^= hash >> 31;

            return new Random((int)hash);
        }
    }

    [Test]
    public async Task CompacterShouldCreateSst()
    {
        // When the number of mem tables is higher than MemTableMaxCount it should
        // flush the oldest mem table to disk

        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { MemTableMaxCount = 2, FlushPeriod = TimeSpan.Zero };
        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        storage.Put(1, 1);
        storage._inner.ForceFreezeMemTable();

        await storage._compacter.RunMaintenanceAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).IsEmpty();
        await Assert.That(storage._inner._state.ImmutableMemTables).HasSingleItem();
        await Assert.That(storage._inner._state.ImmutableMemTables.Peek().TryGet(1, out _)).IsTrue();
        await Assert.That(storage._inner._state.ImmutableMemTables.Peek().TryGet(2, out _)).IsFalse();

        storage.Put(2, 2);
        storage._inner.ForceFreezeMemTable();

        await storage._compacter.RunMaintenanceAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();
        await Assert.That(storage._inner._state.ImmutableMemTables).HasSingleItem();
        await Assert.That(storage._inner._state.ImmutableMemTables.Peek().TryGet(1, out _)).IsFalse();
        await Assert.That(storage._inner._state.ImmutableMemTables.Peek().TryGet(2, out _)).IsTrue();

        await storage.CloseAsync();
    }

    [Test]
    public async Task CloseAsyncShouldFlushToDisk()
    {
        using var tempFolder = TempFolder.Create();
        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, new StorageOptions());

        storage.Put(1, 1);
        await storage.CloseAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        await storage.CloseAsync();
    }

    [Test]
    public async Task CloseAsyncCanBeInvokedMultipleTimes()
    {
        using var tempFolder = TempFolder.Create();
        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, new StorageOptions());

        storage.Put(1, 1);
        await storage.CloseAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        await storage.CloseAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        storage = await LsmStorage.OpenAsync<int, int>(tempFolder, new StorageOptions());
        storage.Put(1, 2);
        await storage.CloseAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst").Count()).IsEqualTo(2);

        await storage.CloseAsync();
    }

    [Test]
    public async Task FinalizerShouldNotFlushOrCrashWhenStorageIsNotClosed()
    {
        // A storage that is never closed must not flush to disk during finalization (no WAL exists,
        // durability is only guaranteed by CloseAsync). Even when its directory has been removed, the
        // finalizer must be a no-op and never crash the process.
        using var tempFolder = TempFolder.Create();

        await CreateUnclosedStorageAsync(tempFolder);
        tempFolder.Dispose();

        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        // Reaching this point means finalization neither flushed to the deleted folder nor crashed.
        await Assert.That(Directory.Exists(tempFolder)).IsFalse();

        // The storage was never closed, so nothing was ever persisted.
        static async Task CreateUnclosedStorageAsync(string folder)
        {
            var storage = await LsmStorage.OpenAsync<int, int>(folder, new StorageOptions { UseWriteAheadLog = false });
            storage.Put(1, 1);
            // Intentionally not closed/disposed: it becomes eligible for finalization on return.
        }
    }

    [Test]
    public async Task GetShouldReturnMostRecentImmutableMemTableValue()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        storage.Put(1, 100);
        storage.ForceFreezeMemTable();

        storage.Put(1, 200);
        storage.ForceFreezeMemTable();

        // Both immutable mem tables hold key 1; the most recently frozen value must win.
        await Assert.That(storage._state.ImmutableMemTables.Count()).IsEqualTo(2);
        await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(200);
    }

    [Test]
    public async Task ScanShouldReturnMostRecentImmutableMemTableValue()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        storage.Put(1, 100);
        storage.ForceFreezeMemTable();

        storage.Put(1, 200);
        storage.ForceFreezeMemTable();

        var list = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();

        await Assert.That(list).HasSingleItem();
        await Assert.That(DecodeInt32(list[0].Value)).IsEqualTo(200);
    }

    [Test]
    public async Task GetShouldFindByteArrayKeyByContent()
    {
        using var tempFolder = TempFolder.Create();

        using var storage = new LsmStorageInner(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        storage.Put([1, 2, 3], 42);

        // A different array instance with the same content must resolve to the stored value.
        await Assert.That(DecodeInt32(await storage.GetAsync([1, 2, 3]))).IsEqualTo(42);
    }

    [Test]
    public async Task ReopenShouldPreserveLevelZeroRecency()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        storage.Put(1, 100);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        storage.Put(1, 200);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst").Count()).IsEqualTo(2);
        await storage.CloseAsync();

        // After reopening, the SSTs must be ordered by creation so the newest value still wins.
        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        await Assert.That(DecodeInt32(await reopened.GetAsync(1))).IsEqualTo(200);

        await reopened.CloseAsync();
    }

    [Test]
    public async Task GetShouldReadBytesValueOfArbitraryLengthFromSsTable()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        var storage = await LsmStorage.OpenAsync<int, ByteSlice>(tempFolder, options);

        // A ByteSlice value whose length is not 4 bytes used to trip an incorrect decode assertion.
        byte[] expected = [1, 2, 3, 4, 5, 6, 7];
        var value = ByteSlice.FromMemory(expected);
        storage.Put(1, value);

        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();

        var result = new ArrayBufferWriter<byte>();
        await Assert.That(await storage.TryGetRawAsync(1, result)).IsTrue();
        await Assert.That(result.WrittenSpan.ToArray()).IsEquivalentTo(expected);

        await storage.CloseAsync();
    }

    [Test]
    public async Task PublicPutShouldCopyBorrowedSpans()
    {
        using var tempFolder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6];

        storage.Put(key, value);
        key[0] = 9;
        value[0] = 9;

        byte[] read = new byte[3];
        var length = await storage.GetRawAsync([1, 2, 3], read);

        await Assert.That(length).IsEqualTo(3);
        await Assert.That(read).IsEquivalentTo(new byte[] { 4, 5, 6 }, CollectionOrdering.Matching);
    }

    [Test]
    public async Task PublicPutShouldCopyBytesIntoMemTableArena()
    {
        using var tempFolder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        byte[] sourceKey = [1, 2, 3];
        byte[] sourceValue = [4, 5, 6];
        using var key = OwnedByteSlice.TakeOwnership(sourceKey, 3);
        using var value = OwnedByteSlice.TakeOwnership(sourceValue, 3);

        storage.Put(key.Span, value.Span);

        sourceKey[0] = 9;
        sourceValue[0] = 9;

        byte[] read = new byte[3];
        var length = await storage.GetRawAsync([1, 2, 3], read);

        await Assert.That(length).IsEqualTo(3);
        await Assert.That(read).IsEquivalentTo(new byte[] { 4, 5, 6 }, CollectionOrdering.Matching);
    }

    [Test]
    public async Task ByteMemTableShouldFreezeBasedOnArenaBytesWritten()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            MemTableSizeLimit = 20,
            MemTableArenaBlockSize = 16,
        };

        using var storage = new LsmStorageInner(tempFolder, options);
        using var key = OwnedByteSlice.CopyFrom([1]);

        for (var i = 0; i < 5; i++)
        {
            using var value = OwnedByteSlice.CopyFrom([(byte)i]);
            storage.Put(key.Slice, value.Slice);
        }

        await Assert.That(storage._state.ImmutableMemTables).IsNotEmpty();
    }

    [Test]
    public async Task TypedPutHelpersShouldUseOrderPreservingEncoders()
    {
        using var tempFolder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        storage.Put(-1, 10);
        storage.Put(0, 20);
        storage.Put(1, 30);

        await Assert.That(await storage.GetInt32Async(-1)).IsEqualTo(10);
        await Assert.That(await storage.GetInt32Async(0)).IsEqualTo(20);
        await Assert.That(await storage.GetInt32Async(1)).IsEqualTo(30);
    }

    [Test]
    public async Task CopyFromPutHelpersShouldStoreStreamValues()
    {
        using var tempFolder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        using var stream = new MemoryStream([4, 5, 6]);
        storage.Put("stream", stream);

        using var asyncStream = new MemoryStream([7, 8, 9]);
        await storage.PutAsync("async-stream", asyncStream);

        using var streamKey = LsmStorageTypedExtensions.EncodeKey("stream");
        using var asyncStreamKey = LsmStorageTypedExtensions.EncodeKey("async-stream");
        byte[] read = new byte[3];
        var length = await storage.GetRawAsync(streamKey.Span, read);

        await Assert.That(length).IsEqualTo(3);
        await Assert.That(read).IsEquivalentTo(new byte[] { 4, 5, 6 }, CollectionOrdering.Matching);

        length = await storage.GetRawAsync(asyncStreamKey.Span, read);

        await Assert.That(length).IsEqualTo(3);
        await Assert.That(read).IsEquivalentTo(new byte[] { 7, 8, 9 }, CollectionOrdering.Matching);
    }

    [Test]
    public async Task CopyFromPutHelpersShouldStoreReadOnlySequenceValues()
    {
        using var tempFolder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        storage.Put("sequence", new ReadOnlySequence<byte>(new byte[] { 10, 11, 12 }));

        using var sequenceKey = LsmStorageTypedExtensions.EncodeKey("sequence");
        byte[] read = new byte[3];
        var length = await storage.GetRawAsync(sequenceKey.Span, read);

        await Assert.That(length).IsEqualTo(3);
        await Assert.That(read).IsEquivalentTo(new byte[] { 10, 11, 12 }, CollectionOrdering.Matching);
    }

    [Test]
    public async Task CopyFromPutHelpersShouldStoreUtf8JsonReaderValues()
    {
        using var tempFolder = TempFolder.Create();
        await using var storage = await LsmStorage.OpenAsync(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        var json = Encoding.UTF8.GetBytes("""{"name":"caf\u00e9"}""");
        var reader = new Utf8JsonReader(json);

        reader.Read();
        reader.Read();
        reader.Read();

        storage.Put("json", in reader);

        await Assert.That(await storage.GetStringAsync("json")).IsEqualTo("café");
    }

    [Test]
    public async Task WriteAheadLogRecoversUnflushedEntriesAfterCrash()
    {
        using var tempFolder = TempFolder.Create();
        // Disable background flushing so the data stays only in the memtable + WAL (never an SST).
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        await CrashRecoveryTestProcess.WriteAndExitWithoutDisposalAsync(tempFolder, entryCount: 10);

        // Nothing was ever flushed, yet the WAL is on disk holding the unflushed writes.
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).IsEmpty();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.wal")).IsNotEmpty();

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        for (var i = 0; i < 10; i++)
        {
            await Assert.That(DecodeInt32(await reopened.GetAsync(i))).IsEqualTo(i + 1);
        }

        await reopened.CloseAsync();
    }

    [Test]
    public async Task WriteAheadLogDistinguishesEmptyValueFromDelete()
    {
        using var tempFolder = TempFolder.Create();
        var walPath = Path.Combine(tempFolder, "empty-values.wal");

        using (var wal = new WriteAheadLog(walPath, syncToDisk: false))
        {
            wal.AppendRaw([1], []);
            wal.AppendDeleteRaw([2]);
        }

        using var memTable = new MemTable(1);
        WriteAheadLog.Replay(walPath, memTable);

        using var emptyKey = OwnedByteSlice.CopyFrom([1]);
        using var deletedKey = OwnedByteSlice.CopyFrom([2]);

        await Assert.That(memTable.TryGet(emptyKey.Slice, out var empty)).IsTrue();
        await Assert.That(empty!.IsEmpty).IsTrue();
        await Assert.That(empty.IsTombstone).IsFalse();

        await Assert.That(memTable.TryGet(deletedKey.Slice, out var deleted)).IsTrue();
        await Assert.That(deleted!.IsTombstone).IsTrue();
    }

    [Test]
    public async Task WriteAheadLogReplayCanReadAnOpenWriter()
    {
        using var tempFolder = TempFolder.Create();
        var walPath = Path.Combine(tempFolder, "open-writer.wal");

        using var wal = new WriteAheadLog(walPath, syncToDisk: false);
        wal.AppendRaw([1], [2]);
        wal.Flush();

        using var memTable = new MemTable(1);
        WriteAheadLog.Replay(walPath, memTable);

        using var key = OwnedByteSlice.CopyFrom([1]);
        await Assert.That(memTable.TryGet(key.Slice, out var value)).IsTrue();
        await Assert.That(value!.Length).IsEqualTo(1);
        await Assert.That(value.Span[0]).IsEqualTo((byte)2);

        if (OperatingSystem.IsWindows())
        {
            await Assert.That(() =>
            {
                using var concurrentWriter = new WriteAheadLog(walPath, syncToDisk: false);
            }).Throws<IOException>();
        }
    }

    [Test]
    public async Task WriteAheadLogRecoveryRequiresTheWalFile()
    {
        // Sanity check that the recovery above is genuinely driven by the WAL: with the WAL removed
        // (and nothing flushed), the data is gone after a crash.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        await CrashRecoveryTestProcess.WriteAndExitWithoutDisposalAsync(tempFolder, entryCount: 1);

        foreach (var wal in Directory.EnumerateFiles(tempFolder, "*.wal"))
        {
            File.Delete(wal);
        }

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        await Assert.That(DecodeInt32(await reopened.GetAsync(0))).IsEqualTo(0);

        await reopened.CloseAsync();
    }

    [Test]
    public async Task WriteAheadLogRecoveryToleratesTornTrailingRecord()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        await CrashRecoveryTestProcess.WriteAndExitWithoutDisposalAsync(tempFolder, entryCount: 10);

        // Truncate the final byte to simulate a crash in the middle of the last append.
        var walFile = Directory.EnumerateFiles(tempFolder, "*.wal").Single();
        using (var stream = new FileStream(walFile, FileMode.Open, FileAccess.Write, FileShare.None))
        {
            stream.SetLength(stream.Length - 1);
        }

        // Recovery must not throw and the earlier, intact records must still be recovered.
        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        await Assert.That(DecodeInt32(await reopened.GetAsync(0))).IsEqualTo(1);

        await reopened.CloseAsync();
    }

    [Test]
    public async Task RecoveryDeletesStaleWalWhenSstAlreadyExists()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero };

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        storage.Put(1, 42);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        var sstFile = Directory.EnumerateFiles(tempFolder, "*.sst").Single();
        var sstId = Path.GetFileNameWithoutExtension(sstFile);
        await storage.CloseAsync();

        // Simulate a crash that wrote and durably persisted the SST but didn't get to delete the WAL.
        // The bytes are intentionally garbage: if replayed they could fail, so recovery must skip them.
        var staleWal = Path.Combine(tempFolder, sstId + ".wal");
        await File.WriteAllBytesAsync(staleWal, new byte[] { 0xFF, 0xFF, 0xFF });

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        await Assert.That(DecodeInt32(await reopened.GetAsync(1))).IsEqualTo(42);
        // The stale WAL was deleted (and never replayed) because its SST was already loaded. The
        // reopened store has its own fresh current-memtable WAL, so only assert the stale one is gone.
        await Assert.That(File.Exists(staleWal)).IsFalse();

        await reopened.CloseAsync();
    }

    [Test]
    public async Task TieredCompactionMergesAllTiersUnderSpaceAmplification()
    {
        // Three equally sized single-entry tiers hit the space-amplification trigger (sum of the newer
        // tiers reaches MaxSizeAmplificationPercent of the oldest), so all three merge into one SST.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 3 };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(3);

            var compacted = await storage.TryTieredCompactionAsync();

            await Assert.That(compacted).IsTrue();
            await Assert.That(storage._state.LevelZeroTables).HasSingleItem();
            await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();
            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(10);
            await Assert.That(DecodeInt32(await storage.GetAsync(2))).IsEqualTo(20);
            await Assert.That(DecodeInt32(await storage.GetAsync(3))).IsEqualTo(30);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionDropsTombstonesOnFullCompaction()
    {
        // A full compaction (the oldest tier participates) may drop tombstones. The delete of key 1 in a
        // newer tier shadows its value in the oldest tier and is then discarded entirely.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 2 };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 100));
            await FlushTierAsync(storage, () =>
            {
                storage.Delete(1);
                storage.Put(2, 200);
            });

            var compacted = await storage.TryTieredCompactionAsync();

            await Assert.That(compacted).IsTrue();
            await Assert.That(storage._state.LevelZeroTables).HasSingleItem();
            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(0);
            await Assert.That(DecodeInt32(await storage.GetAsync(2))).IsEqualTo(200);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionPartialMergeKeepsTombstones()
    {
        // A large oldest tier plus several tiny newest tiers avoids the space-amplification trigger and
        // instead fires the size-ratio trigger, merging only the newest tiers. Because the oldest tier is
        // excluded, a tombstone for a key it still holds must be preserved (the key stays logically gone).
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 4 };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1000; k < 1500; k++)
                {
                    storage.Put(k, k);
                }
            });
            await FlushTierAsync(storage, () => storage.Put(1, 1));
            await FlushTierAsync(storage, () => storage.Delete(1000));
            await FlushTierAsync(storage, () => storage.Put(3, 3));
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(4);

            var compacted = await storage.TryTieredCompactionAsync();

            await Assert.That(compacted).IsTrue();
            // The big oldest tier is untouched; the three newest tiers collapse into one.
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(2);
            // The tombstone was kept, so the key the oldest tier still holds remains logically deleted.
            await Assert.That(DecodeInt32(await storage.GetAsync(1000))).IsEqualTo(0);
            // Untouched keys from the oldest tier are still readable.
            await Assert.That(DecodeInt32(await storage.GetAsync(1001))).IsEqualTo(1001);
            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(1);
            await Assert.That(DecodeInt32(await storage.GetAsync(3))).IsEqualTo(3);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionBelowThresholdIsNoOp()
    {
        // With fewer tiers than MaxCompactionTiers there is nothing to do: the call is a no-op.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 8 };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            var compacted = await storage.TryTieredCompactionAsync();

            await Assert.That(compacted).IsFalse();
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(2);
            await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst").Count()).IsEqualTo(2);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionDisabledByStrategyIsNoOp()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, MaxCompactionTiers = 2, CompactionStrategy = CompactionStrategy.None };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));

            await Assert.That(await storage.TryTieredCompactionAsync()).IsFalse();
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(3);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task TieredCompactionRecencySurvivesReopen()
    {
        // After a full compaction the merged output gets the highest id, so reopen-by-id still resolves
        // the newest value for a key written across several tiers.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { FlushPeriod = TimeSpan.Zero, MaxCompactionTiers = 3 };

        var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        for (var v = 1; v <= 3; v++)
        {
            storage.Put(1, v * 100);
            storage._inner.ForceFreezeMemTable();
            await storage._inner.ForceFlushNextImmutableMemTableAsync();
        }

        await Assert.That(await storage._inner.TryTieredCompactionAsync()).IsTrue();
        await Assert.That(Directory.EnumerateFiles(tempFolder, "*.sst")).HasSingleItem();
        await storage.CloseAsync();

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
        await Assert.That(DecodeInt32(await reopened.GetAsync(1))).IsEqualTo(300);

        await reopened.CloseAsync();
    }

    [Test]
    public async Task TieredStoreWritesManifestAndCanReopenAsLeveled()
    {
        using var tempFolder = TempFolder.Create();
        var tieredOptions = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };

        {
            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, tieredOptions);
            storage.Put(1, 10);
            storage._inner.ForceFreezeMemTable();
            await storage._inner.ForceFlushNextImmutableMemTableAsync();
            storage.Put(2, 20);
            storage._inner.ForceFreezeMemTable();
            await storage._inner.ForceFlushNextImmutableMemTableAsync();

            await Assert.That(File.Exists(Path.Combine(tempFolder, "manifest"))).IsTrue();
            await storage.CloseAsync();
        }

        var leveledOptions = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
        };
        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, leveledOptions);

        try
        {
            await Assert.That(reopened._inner._state.LevelZeroTables.Count).IsEqualTo(2);
            await Assert.That(await reopened._inner.TryLeveledCompactionAsync()).IsTrue();
            await Assert.That(reopened._inner._state.LevelZeroTables.Count).IsEqualTo(0);
            await Assert.That(reopened._inner._state.LeveledSsTables[0].Count).IsEqualTo(1);
            await Assert.That(DecodeInt32(await reopened.GetAsync(1))).IsEqualTo(10);
            await Assert.That(DecodeInt32(await reopened.GetAsync(2))).IsEqualTo(20);
        }
        finally
        {
            await reopened.DisposeAsync();
        }
    }

    [Test]
    public async Task LeveledStoreCanReopenAsTieredWithoutResurrectingDeletedKeys()
    {
        using var tempFolder = TempFolder.Create();
        var leveledOptions = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 1,
        };

        {
            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, leveledOptions);
            storage.Put(1, 100);
            storage._inner.ForceFreezeMemTable();
            await storage._inner.ForceFlushNextImmutableMemTableAsync();
            await Assert.That(await storage._inner.TryLeveledCompactionAsync()).IsTrue();
            await Assert.That(storage._inner._state.LeveledSsTables[0].Count).IsEqualTo(1);
            await storage.CloseAsync();
        }

        var tieredOptions = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Tiered,
            MaxCompactionTiers = 2,
        };
        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, tieredOptions);

        try
        {
            await Assert.That(reopened._inner._state.LeveledSsTables[0].Count).IsEqualTo(1);

            reopened.Delete(1);
            reopened._inner.ForceFreezeMemTable();
            await reopened._inner.ForceFlushNextImmutableMemTableAsync();
            reopened.Put(2, 200);
            reopened._inner.ForceFreezeMemTable();
            await reopened._inner.ForceFlushNextImmutableMemTableAsync();

            await Assert.That(await reopened._inner.TryTieredCompactionAsync()).IsTrue();
            await Assert.That(reopened._inner._state.LevelZeroTables.Count).IsEqualTo(1);
            await Assert.That(DecodeInt32(await reopened.GetAsync(1))).IsEqualTo(0);
            await Assert.That(DecodeInt32(await reopened.GetAsync(2))).IsEqualTo(200);
        }
        finally
        {
            await reopened.DisposeAsync();
        }

        var reopenedAgain = await LsmStorage.OpenAsync<int, int>(tempFolder, tieredOptions);

        try
        {
            await Assert.That(DecodeInt32(await reopenedAgain.GetAsync(1))).IsEqualTo(0);
            await Assert.That(DecodeInt32(await reopenedAgain.GetAsync(2))).IsEqualTo(200);
        }
        finally
        {
            await reopenedAgain.DisposeAsync();
        }
    }

    [Test]
    public async Task OpenAsyncMigratesManifestlessStoreToManifest()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };

        {
            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
            storage.Put(1, 10);
            storage._inner.ForceFreezeMemTable();
            await storage._inner.ForceFlushNextImmutableMemTableAsync();
            await storage.CloseAsync();
        }

        var manifestPath = Path.Combine(tempFolder, "manifest");
        File.Delete(manifestPath);

        var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

        try
        {
            await Assert.That(File.Exists(manifestPath)).IsTrue();
            await Assert.That(DecodeInt32(await reopened.GetAsync(1))).IsEqualTo(10);
        }
        finally
        {
            await reopened.DisposeAsync();
        }
    }

    [Test]
    public async Task GetContinuesToOlderSsTableOnBloomFalsePositive()
    {
        // A newer SST whose key range straddles the target key but does not contain it must not mask an
        // older SST that does. Forcing the bloom filter to always report "maybe present" simulates the
        // false positive that would otherwise make the lookup stop prematurely.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            BloomFilterFactory = new AlwaysPositiveBloomFilterFactory(),
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            // Older table holds key 5.
            await FlushTierAsync(storage, () => storage.Put(5, 50));
            // Newer table spans [1, 10] (covering 5) but does not contain 5.
            await FlushTierAsync(storage, () =>
            {
                storage.Put(1, 10);
                storage.Put(10, 100);
            });

            await Assert.That(DecodeInt32(await storage.GetAsync(5))).IsEqualTo(50);
            // Keys that really are in the newer table still resolve from it.
            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(10);
            await Assert.That(DecodeInt32(await storage.GetAsync(10))).IsEqualTo(100);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task GetFindsValuesAcrossManySsTableBlocks()
    {
        const ushort blockSize = 128;
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            BlockSize = blockSize,
            BloomFilterFactory = new AlwaysPositiveBloomFilterFactory(),
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(20, 2000));
            await FlushTierAsync(storage, () =>
            {
                for (var key = 1; key <= 199; key += 2)
                {
                    storage.Put(key, key * 10);
                }
            });

            await Assert.That(storage._state.LevelZeroTables.Any(table => table.BlockMetadata.Count > 1)).IsTrue();

            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(10);
            await Assert.That(DecodeInt32(await storage.GetAsync(99))).IsEqualTo(990);
            await Assert.That(DecodeInt32(await storage.GetAsync(199))).IsEqualTo(1990);
            await Assert.That(DecodeInt32(await storage.GetAsync(20))).IsEqualTo(2000);
            await Assert.That(DecodeInt32(await storage.GetAsync(200))).IsEqualTo(0);
        }
        finally
        {
            storage.Dispose();
        }
    }

    private sealed class AlwaysPositiveBloomFilterFactory : IBloomFilterFactory
    {
        private static readonly DefaultBloomFilterFactory _inner = new();

        public IBloomFilter CreateBloomFilter(int n, double p) => new AlwaysPositiveBloomFilter(_inner.CreateBloomFilter(n, p));

        public IBloomFilter CreateBloomFilter(ReadOnlySpan<byte> bytes, int k) => new AlwaysPositiveBloomFilter(_inner.CreateBloomFilter(bytes, k));

        public IBloomFilter CreateBloomFilterFromOwnedBytes(byte[] bytes, int k) => new AlwaysPositiveBloomFilter(_inner.CreateBloomFilterFromOwnedBytes(bytes, k));

        public IBloomFilter CreateBloomFilterFromOwnedBytes(byte[] bytes, int k, int algorithmVersion)
            => new AlwaysPositiveBloomFilter(_inner.CreateBloomFilterFromOwnedBytes(bytes, k, algorithmVersion));
    }

    private sealed class AlwaysPositiveBloomFilter(IBloomFilter inner) : IBloomFilter
    {
        public int K => inner.K;

        public int AlgorithmVersion => inner.AlgorithmVersion;

        public void Add(ReadOnlySpan<byte> value) => inner.Add(value);

        public bool Probe(ReadOnlySpan<byte> item) => true;

        public ReadOnlySpan<byte> GetBytes() => inner.GetBytes();
    }

    [Test]
    public async Task ConcurrentFlushAndScanNeverMissesCommittedData()
    {
        // Regression guard for the flush/scan race: while an immutable MemTable is mid-flush (dequeued from
        // the queue but its SST not yet published), a concurrent scan must still observe its data. Flush makes
        // that transition atomic under the level0 write lock, so every previously committed key stays visible.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            const int keyCount = 80;
            var committedCount = 0;
            using var done = new CancellationTokenSource();

            var scanner = Task.Run(async () =>
            {
                while (!done.IsCancellationRequested)
                {
                    // Keys are committed in ascending order, so observing N commits means keys 1..N must
                    // all be present in a scan regardless of any in-flight flush.
                    var expected = Volatile.Read(ref committedCount);
                    var keys = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable()
                        .Select(e => DecodeInt32(e.Key)).ToHashSet();

                    for (var k = 1; k <= expected; k++)
                    {
                        await Assert.That(keys).Contains(k);
                    }
                }
            });

            for (var k = 1; k <= keyCount; k++)
            {
                storage.Put(k, k);
                storage.ForceFreezeMemTable();
                // Once frozen the key lives in the immutable queue and is committed: it must remain visible to
                // scans throughout the subsequent flush, including the brief mid-flush transition window.
                Volatile.Write(ref committedCount, k);
                await storage.ForceFlushNextImmutableMemTableAsync();
            }

            done.Cancel();
            await scanner;
        }
        finally
        {
            storage.Dispose();
        }
    }

    private static async Task FlushTierAsync(LsmStorageInner storage, Action write)
    {
        write();
        storage.ForceFreezeMemTable();
        await storage.ForceFlushNextImmutableMemTableAsync();
    }

    private static async Task FlushByteTierAsync(LsmStorageInner storage, Action write)
    {
        write();
        storage.ForceFreezeMemTable();
        await storage.ForceFlushNextImmutableMemTableAsync();
    }

    [Test]
    public async Task FullScanIncludesFlushedSsTableData()
    {
        // Data flushed to L0 SSTs (with nothing left in the MemTables) must still appear in a full scan.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();

            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 1, 2, 3 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(new[] { 10, 20, 30 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task RawScanReturnsFlushedByteEntriesWithoutTombstones()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushByteTierAsync(storage, () => storage.Put([1], [10]));
            await FlushByteTierAsync(storage, () => storage.Put([2], [20]));
            await FlushByteTierAsync(storage, () => storage.Put([3], [30]));
            await FlushByteTierAsync(storage, () => storage.Delete([4]));

            var entries = new List<(byte Key, byte Value)>();

            await storage.ScanRawAsync(entries, static (results, key, value) =>
            {
                results.Add((key[0], value[0]));
                return true;
            });

            await Assert.That(entries.Select(e => e.Key)).IsEquivalentTo(new byte[] { 1, 2, 3 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => e.Value)).IsEquivalentTo(new byte[] { 10, 20, 30 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task RawScanCanStopEarlyAndScanAgain()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, BlockSize = 64 };
        using var storage = new LsmStorageInner(tempFolder, options);

        for (byte key = 1; key <= 40; key++)
        {
            storage.Put([key], [(byte)(key + 100)]);
        }

        storage.ForceFreezeMemTable();
        await storage.ForceFlushNextImmutableMemTableAsync();

        var firstScanCount = await storage.ScanRawAsync(0, static (_, _, _) => false);
        var secondScanCount = await storage.ScanRawAsync(0, static (_, _, _) => true);

        await Assert.That(firstScanCount).IsEqualTo(1);
        await Assert.That(secondScanCount).IsEqualTo(40);
    }

    [Test]
    public async Task SeekRawAcrossTablesHonorsLowerBoundMaxEntriesAndTombstones()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            // Three single-key tables form one globally sorted run, exercising the cross-table seek path.
            await FlushByteTierAsync(storage, () => storage.Put([10], [100]));
            await FlushByteTierAsync(storage, () => storage.Put([20], [200]));
            await FlushByteTierAsync(storage, () => storage.Put([30], [44]));

            await Assert.That(await SeekRawCollectAsync(storage, [0])).IsEquivalentTo(new (byte, byte)[] { (10, 100), (20, 200), (30, 44) }, CollectionOrdering.Matching);
            await Assert.That(await SeekRawCollectAsync(storage, [20])).IsEquivalentTo(new (byte, byte)[] { (20, 200), (30, 44) }, CollectionOrdering.Matching);
            await Assert.That(await SeekRawCollectAsync(storage, [15])).IsEquivalentTo(new (byte, byte)[] { (20, 200), (30, 44) }, CollectionOrdering.Matching);
            await Assert.That(await SeekRawCollectAsync(storage, [99])).IsEmpty();
            await Assert.That(await SeekRawCollectAsync(storage, [0], maxEntries: 1)).IsEquivalentTo(new (byte, byte)[] { (10, 100) }, CollectionOrdering.Matching);

            // A tombstone at the lower bound must be skipped without counting against maxEntries.
            await FlushByteTierAsync(storage, () => storage.Delete([20]));

            await Assert.That(await SeekRawCollectAsync(storage, [20], maxEntries: 1)).IsEquivalentTo(new (byte, byte)[] { (30, 44) }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task SeekRawLandsOnLowerBoundAcrossBlockBoundaries()
    {
        using var tempFolder = TempFolder.Create();
        // A tiny block size forces the single flushed table into many blocks so the in-block lower-bound search,
        // the stepped-back-block correction, and cross-block continuation are all exercised by the seek sweep.
        var options = new StorageOptions { UseWriteAheadLog = false, BlockSize = 64 };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            for (byte k = 1; k <= 40; k++)
            {
                storage.Put([k], [(byte)(k + 100)]);
            }

            storage.ForceFreezeMemTable();
            await storage.ForceFlushNextImmutableMemTableAsync();

            for (byte from = 1; from <= 40; from++)
            {
                var expected = new List<(byte, byte)>();
                for (byte k = from; k <= 40; k++)
                {
                    expected.Add((k, (byte)(k + 100)));
                }

                await Assert.That(await SeekRawCollectAsync(storage, [from])).IsEquivalentTo(expected, CollectionOrdering.Matching);
            }

            await Assert.That((await SeekRawCollectAsync(storage, [0])).Count).IsEqualTo(40);
            await Assert.That(await SeekRawCollectAsync(storage, [41])).IsEmpty();
            await Assert.That(await SeekRawCollectAsync(storage, [25], maxEntries: 3)).IsEquivalentTo(new (byte, byte)[] { (25, 125), (26, 126), (27, 127) }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    private static async Task<List<(byte Key, byte Value)>> SeekRawCollectAsync(LsmStorageInner storage, byte[] from, long maxEntries = long.MaxValue)
    {
        var entries = new List<(byte Key, byte Value)>();

        await storage.SeekRawAsync(from, entries, static (results, key, value) =>
        {
            results.Add((key[0], value[0]));
            return true;
        }, maxEntries);

        return entries;
    }

    [Test]
    public async Task RawScanFallsBackForOverlappingTablesAndKeepsNewestValue()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushByteTierAsync(storage, () => storage.Put([1], [10]));
            await FlushByteTierAsync(storage, () => storage.Put([1], [20]));

            var entries = new List<(byte Key, byte Value)>();

            await storage.ScanRawAsync(entries, static (results, key, value) =>
            {
                results.Add((key[0], value[0]));
                return true;
            });

            await Assert.That(entries).HasSingleItem();
            await Assert.That(entries[0].Key).IsEqualTo((byte)1);
            await Assert.That(entries[0].Value).IsEqualTo((byte)20);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task FlushAndCompactAsyncFlushesPendingWritesForRawScan()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };
        var storage = await LsmStorage.OpenAsync<byte[], byte[]>(tempFolder, options);

        try
        {
            storage.Put([1], [10]);
            storage.Put([2], [20]);

            await storage.FlushAndCompactAsync();

            await Assert.That(storage._inner._state.CurrentMemTable.Count).IsEqualTo(0);
            await Assert.That(storage._inner._state.ImmutableMemTables.IsEmpty).IsTrue();

            var entries = new List<byte>();

            await storage.ScanRawAsync(entries, static (results, key, value) =>
            {
                results.Add(value[0]);
                return true;
            });

            await Assert.That(entries).IsEquivalentTo(new byte[] { 10, 20 }, CollectionOrdering.Matching);
        }
        finally
        {
            await storage.DisposeAsync();
        }
    }

    [Test]
    public async Task ScanReturnsNewestValueAcrossMemTableAndSsTable()
    {
        // A key written to an SST and later overwritten in the current MemTable must scan as the newer value.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            // Overwrite key 1 in the live MemTable; this value is newer than the flushed one.
            storage.Put(1, 999);

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();

            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 1, 2 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(new[] { 999, 20 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task ScanHidesKeysDeletedAfterFlush()
    {
        // A key present in an SST but deleted afterwards (tombstone in the MemTable) must be absent from scans.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            storage.Delete(1);

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();

            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 2 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(new[] { 20 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task RangeScanIncludesSsTableData()
    {
        // A bounded scan (keys >= from) must include matching entries that live only in SSTs.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));
            storage.Put(4, 40);

            var entries = storage.CreateIterator().EnumerateAsync(2).ToBlockingEnumerable().SnapshotList();

            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 2, 3, 4 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(new[] { 20, 30, 40 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task BackwardsRangeScanIncludesSsTableData()
    {
        using var tempFolder = TempFolder.Create();
        var storage = new LsmStorageInner(tempFolder, new StorageOptions { UseWriteAheadLog = false });

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));
            storage.Put(2, 200);
            storage.Put(4, 40);

            var entries = storage.CreateIterator().EnumerateBackwardsAsync(3).ToBlockingEnumerable().SnapshotList();

            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 3, 2, 1 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(new[] { 30, 200, 10 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task ScanAfterReopenIncludesSsTableData()
    {
        // After reopening, all live data is in SSTs (MemTables are empty), so the scan exercises the SST path.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };

        {
            var storage = new LsmStorageInner(tempFolder, options);
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            storage.Dispose();

            var reopened = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                var iterator = reopened._inner.CreateIterator();
                var entries = iterator.EnumerateAsync().ToBlockingEnumerable().SnapshotList();
                var backwards = iterator.EnumerateBackwardsAsync().ToBlockingEnumerable().SnapshotList();
                var boundedBackwards = iterator.EnumerateBackwardsAsync(1).ToBlockingEnumerable().SnapshotList();

                await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 1, 2 }, CollectionOrdering.Matching);
                await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(new[] { 10, 20 }, CollectionOrdering.Matching);
                await Assert.That(backwards.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 2, 1 }, CollectionOrdering.Matching);
                await Assert.That(boundedBackwards.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 1 }, CollectionOrdering.Matching);
            }
            finally
            {
                reopened.Dispose();
            }
        }
    }


    [Test]
    public async Task GetReturnsDefaultForSentinelTombstoneStoredInSsTable()
    {
        // Sentinel-based encoders (here int) persist a deletion as a fixed non-empty value. Reading that
        // key back from an SST must resolve to the default (deleted), not the raw sentinel.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 100));
            await FlushTierAsync(storage, () => storage.Delete(1));

            // The delete lives in the newest SST as a sentinel value; it must shadow the older value.
            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(0);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionFlushesL0IntoL1()
    {
        // Once Level0CompactionThreshold L0 SSTs accumulate, leveled compaction merges them all into a
        // single L1 sorted run and empties L0.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 4,
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));
            await FlushTierAsync(storage, () => storage.Put(3, 30));
            await FlushTierAsync(storage, () => storage.Put(4, 40));
            await Assert.That(storage._state.LevelZeroTables.Count).IsEqualTo(4);

            var compacted = await storage.TryLeveledCompactionAsync();

            await Assert.That(compacted).IsTrue();
            await Assert.That(storage._state.LevelZeroTables).IsEmpty();
            await Assert.That(storage._state.LeveledSsTables).HasSingleItem();
            await Assert.That(storage._state.LeveledSsTables[0]).HasSingleItem();
            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(10);
            await Assert.That(DecodeInt32(await storage.GetAsync(2))).IsEqualTo(20);
            await Assert.That(DecodeInt32(await storage.GetAsync(3))).IsEqualTo(30);
            await Assert.That(DecodeInt32(await storage.GetAsync(4))).IsEqualTo(40);

            // The merged run is also visible to a full scan in key order.
            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();
            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 1, 2, 3, 4 }, CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(new[] { 10, 20, 30, 40 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionPushesOverSizedLevelDown()
    {
        // With a tiny base target every level is over budget, so after L0 flushes into L1 a second
        // compaction pushes L1 down into L2.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
            BaseLevelTargetBytes = 1,
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () => storage.Put(1, 10));
            await FlushTierAsync(storage, () => storage.Put(2, 20));

            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();
            await Assert.That(storage._state.LeveledSsTables[0]).HasSingleItem(); // L1 populated

            // Second action: L1 is over its (tiny) target, so it is pushed down into L2.
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();
            await Assert.That(storage._state.LeveledSsTables.Count).IsEqualTo(2);
            await Assert.That(storage._state.LeveledSsTables[0]).IsEmpty(); // L1 now empty
            await Assert.That(storage._state.LeveledSsTables[1]).HasSingleItem(); // L2 populated

            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(10);
            await Assert.That(DecodeInt32(await storage.GetAsync(2))).IsEqualTo(20);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionDropsTombstonesAtBottomLevel()
    {
        // When the destination is the last non-empty level, a delete may be discarded entirely because no
        // older value survives below it.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () =>
            {
                storage.Put(1, 100);
                storage.Put(2, 200);
            });
            await FlushTierAsync(storage, () => storage.Delete(1));

            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(0);
            await Assert.That(DecodeInt32(await storage.GetAsync(2))).IsEqualTo(200);

            // The tombstone for key 1 was dropped, so only key 2 survives in L1.
            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();
            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 2 }, CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionKeepsTombstoneWhenLowerLevelHoldsKey()
    {
        // A tombstone must be preserved when a deeper level still holds the key it shadows, otherwise the
        // older value would be resurrected.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
            BaseLevelTargetBytes = 1,
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            // Seed L2 with key 1 = 100.
            await FlushTierAsync(storage, () => storage.Put(1, 100));
            await FlushTierAsync(storage, () => storage.Put(2, 200));
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue(); // L0 -> L1
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue(); // L1 -> L2

            // Now delete key 1 in a fresh L0 batch and compact into L1 (which sits above the L2 that holds 1).
            await FlushTierAsync(storage, () => storage.Delete(1));
            await FlushTierAsync(storage, () => storage.Put(3, 300));
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue(); // L0 -> L1, must keep the tombstone

            // The delete is preserved: key 1 reads as deleted even though L2 still physically holds 100.
            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(0);
            await Assert.That(DecodeInt32(await storage.GetAsync(2))).IsEqualTo(200);
            await Assert.That(DecodeInt32(await storage.GetAsync(3))).IsEqualTo(300);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledReopenRestoresLevelsViaManifest()
    {
        // The manifest persists the L0/level structure so a reopen restores the exact levels (and recency)
        // even though leveled SST ids no longer encode it.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
            BaseLevelTargetBytes = 1,
        };

        {
            var building = new LsmStorageInner(tempFolder, options);
            await FlushTierAsync(building, () => building.Put(1, 10));
            await FlushTierAsync(building, () => building.Put(2, 20));
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue(); // L0 -> L1
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue(); // L1 -> L2
            await FlushTierAsync(building, () => building.Put(3, 30));
            await FlushTierAsync(building, () => building.Put(4, 40));
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue(); // L0 -> L1
            building.Dispose();

            await Assert.That(File.Exists(Path.Combine(tempFolder, "manifest"))).IsTrue();

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                // L1 holds {3,4}; L2 holds {1,2}.
                await Assert.That(storage._inner._state.LeveledSsTables.Count).IsEqualTo(2);
                await Assert.That(storage._inner._state.LeveledSsTables[0]).HasSingleItem();
                await Assert.That(storage._inner._state.LeveledSsTables[1]).HasSingleItem();
                await Assert.That(storage._inner._state.LevelZeroTables).IsEmpty();

                var entries = storage._inner.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();
                await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(new[] { 1, 2, 3, 4 }, CollectionOrdering.Matching);
                await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(new[] { 10, 20, 30, 40 }, CollectionOrdering.Matching);
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task LeveledReopenDeletesOrphanSstNotInManifest()
    {
        // An SST left behind by a flush/compaction that crashed before the manifest commit is not
        // referenced by the manifest and must be deleted on reopen.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 2,
        };

        {
            var building = new LsmStorageInner(tempFolder, options);
            await FlushTierAsync(building, () => building.Put(1, 10));
            await FlushTierAsync(building, () => building.Put(2, 20));
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue();
            building.Dispose();

            // Simulate an orphan output (id far in the future, not referenced by the manifest).
            var orphan = Path.Combine(tempFolder, "999999.sst");
            File.WriteAllText(orphan, "not a real sst");

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                await Assert.That(File.Exists(orphan)).IsFalse();
                await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(10);
                await Assert.That(DecodeInt32(await storage.GetAsync(2))).IsEqualTo(20);
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task LeveledCompactionSplitsOutputIntoMultipleSsTables()
    {
        // With a small per-SST target, a level holds several size-bounded, non-overlapping runs instead of
        // one giant SST.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 1,
            BlockSize = 128,
            TargetSstSizeBytes = 128,
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1; k <= 400; k++)
                {
                    storage.Put(k, k * 10);
                }
            });

            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            // The single L1 run was split into several SSTs.
            var l1 = storage._state.LeveledSsTables[0];
            await Assert.That(l1.Count > 1).IsTrue();

            // They are sorted and non-overlapping.
            for (var i = 1; i < l1.Count; i++)
            {
                await Assert.That(l1[i - 1].LastKey < l1[i].FirstKey).IsTrue();
            }

            // Every key is still readable and a full scan returns them all in order.
            await Assert.That(DecodeInt32(await storage.GetAsync(250))).IsEqualTo(2500);
            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();
            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(Enumerable.Range(1, 400), CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(Enumerable.Range(1, 400).Select(k => k * 10), CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledCompactionRewritesOnlyOverlappingTargetSsTables()
    {
        // Partial selection: an L0 batch that overlaps only the low key range rewrites just the overlapping
        // L1 SSTs; the non-overlapping L1 SSTs keep their identity (same file) and data.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 1,
            BlockSize = 128,
            TargetSstSizeBytes = 128,
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            // Seed L1 with keys 1..400 split across several SSTs (default base target is large, so no
            // size-triggered cascade fires).
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1; k <= 400; k++)
                {
                    storage.Put(k, k);
                }
            });
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            // Record the L1 SSTs that do not overlap the low range [1,5]; these must survive untouched.
            var untouchedBefore = storage._state.LeveledSsTables[0]
                .Where(t => t.FirstKey > ByteSliceTestExtensions.Slice(5))
                .Select(t => t.Filename)
                .ToHashSet();
            await Assert.That(untouchedBefore).IsNotEmpty();

            // A small batch overwriting only keys 1..5 overlaps just the first L1 SST.
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1; k <= 5; k++)
                {
                    storage.Put(k, k * 1000);
                }
            });
            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            // The non-overlapping L1 SSTs were carried over by reference (same files), not rewritten.
            var l1After = storage._state.LeveledSsTables[0].Select(t => t.Filename).ToHashSet();
            await Assert.That(l1After.IsSupersetOf(untouchedBefore)).IsTrue();

            // Updated keys reflect the new values; everything else is unchanged.
            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(1000);
            await Assert.That(DecodeInt32(await storage.GetAsync(5))).IsEqualTo(5000);
            await Assert.That(DecodeInt32(await storage.GetAsync(6))).IsEqualTo(6);
            await Assert.That(DecodeInt32(await storage.GetAsync(400))).IsEqualTo(400);

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();
            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(Enumerable.Range(1, 400), CollectionOrdering.Matching);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task LeveledReopenRestoresSplitLevelsViaManifest()
    {
        // A reopen restores a level made of multiple split SSTs from the manifest, with all data intact.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 1,
            BlockSize = 128,
            TargetSstSizeBytes = 128,
        };

        {
            var building = new LsmStorageInner(tempFolder, options);
            await FlushTierAsync(building, () =>
            {
                for (var k = 1; k <= 400; k++)
                {
                    building.Put(k, k * 10);
                }
            });
            await Assert.That(await building.TryLeveledCompactionAsync()).IsTrue();
            var splitCount = building._state.LeveledSsTables[0].Count;
            await Assert.That(splitCount > 1).IsTrue();
            building.Dispose();

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);

            try
            {
                await Assert.That(storage._inner._state.LeveledSsTables[0].Count).IsEqualTo(splitCount);

                var entries = storage._inner.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();
                await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(Enumerable.Range(1, 400), CollectionOrdering.Matching);
                await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(Enumerable.Range(1, 400).Select(k => k * 10), CollectionOrdering.Matching);
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task ParallelSubcompactionProducesCompleteSortedData()
    {
        // With parallelism enabled and an input large enough to split across several key-range partitions,
        // the parallel leveled subcompaction must produce sorted, non-overlapping SSTs holding every key.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.Leveled,
            Level0CompactionThreshold = 1,
            BlockSize = 128,
            TargetSstSizeBytes = 128,
            MaxCompactionParallelism = 8,
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            await FlushTierAsync(storage, () =>
            {
                for (var k = 1; k <= 800; k++)
                {
                    storage.Put(k, k * 7);
                }
            });

            await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();

            var l1 = storage._state.LeveledSsTables[0];
            await Assert.That(l1.Count > 1).IsTrue();
            for (var i = 1; i < l1.Count; i++)
            {
                await Assert.That(l1[i - 1].LastKey < l1[i].FirstKey).IsTrue();
            }

            var entries = storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable().SnapshotList();
            await Assert.That(entries.Select(e => DecodeInt32(e.Key))).IsEquivalentTo(Enumerable.Range(1, 800), CollectionOrdering.Matching);
            await Assert.That(entries.Select(e => DecodeInt32(e.Value))).IsEquivalentTo(Enumerable.Range(1, 800).Select(k => k * 7), CollectionOrdering.Matching);
            await Assert.That(DecodeInt32(await storage.GetAsync(700))).IsEqualTo(700 * 7);
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task SequentialAndParallelCompactionProduceSameData()
    {
        // DOP=1 and DOP>1 must yield identical logical contents (same keys/values on a full scan).
        static async Task<List<KeyValuePair<int, int>>> RunAsync(int dop)
        {
            using var tempFolder = TempFolder.Create();
            var options = new StorageOptions
            {
                UseWriteAheadLog = false,
                CompactionStrategy = CompactionStrategy.Leveled,
                Level0CompactionThreshold = 1,
                BlockSize = 128,
                TargetSstSizeBytes = 128,
                MaxCompactionParallelism = dop,
            };
            var storage = new LsmStorageInner(tempFolder, options);
            try
            {
                await FlushTierAsync(storage, () =>
                {
                    for (var k = 1; k <= 600; k++)
                    {
                        storage.Put(k, k * 3);
                    }
                });
                await Assert.That(await storage.TryLeveledCompactionAsync()).IsTrue();
                return storage.CreateIterator().EnumerateAsync().ToBlockingEnumerable()
                    .Select(e => new KeyValuePair<int, int>(DecodeInt32(e.Key), DecodeInt32(e.Value)))
                    .ToList();
            }
            finally
            {
                storage.Dispose();
            }
        }

        var sequential = await RunAsync(1);
        var parallel = await RunAsync(8);

        await Assert.That(parallel.Select(e => e.Key)).IsEquivalentTo(sequential.Select(e => e.Key), CollectionOrdering.Matching);
        await Assert.That(parallel.Select(e => e.Value)).IsEquivalentTo(sequential.Select(e => e.Value), CollectionOrdering.Matching);
        await Assert.That(parallel.Select(e => e.Key)).IsEquivalentTo(Enumerable.Range(1, 600), CollectionOrdering.Matching);
    }

    [Test]
    public async Task ParallelL0ProbeReturnsNewestValueAndHandlesTombstones()
    {
        // With read parallelism on and enough accumulated L0 tables to cross the probe threshold, point reads
        // must still honor recency (newest table wins), tombstones, and absent keys.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            CompactionStrategy = CompactionStrategy.None,
            MaxReadParallelism = 8,
        };
        var storage = new LsmStorageInner(tempFolder, options);

        try
        {
            // 12 L0 tables (> the parallel probe threshold of 8). Key 1 is overwritten in successive tables;
            // key 2 is later deleted; key 3 only ever exists in one table; key 99 never exists.
            for (var t = 0; t < 12; t++)
            {
                var snapshot = t;
                await FlushTierAsync(storage, () =>
                {
                    storage.Put(1, snapshot * 100);
                    if (snapshot == 0)
                    {
                        storage.Put(3, 333);
                    }
                    if (snapshot == 5)
                    {
                        storage.Put(2, 222);
                    }
                    if (snapshot == 9)
                    {
                        storage.Delete(2);
                    }
                });
            }

            await Assert.That(storage._state.LevelZeroTables.Count >= 8).IsTrue();
            await Assert.That(DecodeInt32(await storage.GetAsync(1))).IsEqualTo(1100); // newest write wins
            await Assert.That(DecodeInt32(await storage.GetAsync(3))).IsEqualTo(333);
            await Assert.That(DecodeInt32(await storage.GetAsync(2))).IsEqualTo(0); // tombstoned -> default
            await Assert.That(DecodeInt32(await storage.GetAsync(99))).IsEqualTo(0); // never written -> default
        }
        finally
        {
            storage.Dispose();
        }
    }

    [Test]
    public async Task ParallelReopenRestoresData()
    {
        // OpenAsync loads SSTs in parallel when read parallelism is enabled; a reopen must restore all data.
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
            MaxReadParallelism = 8,
        };

        {
            var building = new LsmStorageInner(tempFolder, options);
            for (var t = 0; t < 10; t++)
            {
                var snapshot = t;
                await FlushTierAsync(building, () =>
                {
                    for (var k = snapshot * 10 + 1; k <= snapshot * 10 + 10; k++)
                    {
                        building.Put(k, k * 2);
                    }
                });
            }
            building.Dispose();

            var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, options);
            try
            {
                for (var k = 1; k <= 100; k++)
                {
                    await Assert.That(DecodeInt32(await storage._inner.GetAsync(k))).IsEqualTo(k * 2);
                }
            }
            finally
            {
                await storage.DisposeAsync();
            }
        }
    }

    [Test]
    public async Task TryGetRawAsyncWritesValueFromMemTable()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6, 7];
        storage.Put(key, value);

        var destination = new ArrayBufferWriter<byte>();
        var found = await storage.TryGetRawAsync(key, destination);

        await Assert.That(found).IsTrue();
        await Assert.That(destination.WrittenSpan.ToArray()).IsEquivalentTo(value, CollectionOrdering.Matching);
    }

    [Test]
    public async Task RawReadsReturnPresentEmptyValueFromMemTable()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        storage.Put(key, []);

        var destination = new ArrayBufferWriter<byte>();
        await Assert.That(await storage.TryGetRawAsync(key, destination)).IsTrue();
        await Assert.That(destination.WrittenCount).IsEqualTo(0);
        await Assert.That(await storage.GetRawAsync(key, Memory<byte>.Empty)).IsEqualTo(0);

        var lengths = new List<int>();
        await Assert.That(await storage.TryReadRawAsync(key, lengths, static (state, value) => state.Add(value.Length))).IsTrue();
        await Assert.That(lengths).IsEquivalentTo(new[] { 0 }, CollectionOrdering.Matching);

        using var value = await storage.GetAsync(key);
        await Assert.That(value).IsNotNull();
        await Assert.That(value!.IsEmpty).IsTrue();

        storage.Delete(key);
        await Assert.That(await storage.TryGetRawAsync(key, destination)).IsFalse();

        storage.Put(key, []);
        await Assert.That(await storage.TryGetRawAsync(key, destination)).IsTrue();
    }

    [Test]
    public async Task EmptyByteArraySurvivesFlushScanAndReopen()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions { UseWriteAheadLog = false, FlushPeriod = TimeSpan.Zero };

        var storage = await LsmStorage.OpenAsync<byte[], byte[]>(tempFolder, options);
        storage.Put([1], []);
        storage.Put([2], [20]);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        var entries = new List<(byte Key, int ValueLength)>();
        await storage.ScanRawAsync(entries, static (state, key, value) =>
        {
            state.Add((key[0], value.Length));
            return true;
        });

        await Assert.That(entries).IsEquivalentTo(new[] { ((byte)1, 0), ((byte)2, 1) }, CollectionOrdering.Matching);
        await storage.CloseAsync();

        storage = await LsmStorage.OpenAsync<byte[], byte[]>(tempFolder, options);
        var empty = await storage.GetAsync([1]);
        await Assert.That(empty).IsNotNull();
        await Assert.That(empty!).IsEmpty();
        await Assert.That(await storage.GetRawAsync([1], Memory<byte>.Empty)).IsEqualTo(0);

        var destination = new ArrayBufferWriter<byte>();
        await Assert.That(await storage.TryGetRawAsync([1], destination)).IsTrue();
        await storage.CloseAsync();
    }

    [Test]
    public async Task CompactionKeepsEmptyValueThatShadowsOlderData()
    {
        using var tempFolder = TempFolder.Create();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            MaxCompactionTiers = 2,
        };
        using var storage = new LsmStorageInner(tempFolder, options);

        await FlushByteTierAsync(storage, () => storage.Put([1], [10]));
        await FlushByteTierAsync(storage, () => storage.Put([1], []));

        await Assert.That(await storage.TryTieredCompactionAsync()).IsTrue();
        await Assert.That(await storage.GetRawAsync([1], Memory<byte>.Empty)).IsEqualTo(0);

        var entries = new List<int>();
        await storage.ScanRawAsync(entries, static (state, _, value) =>
        {
            state.Add(value.Length);
            return true;
        });
        await Assert.That(entries).IsEquivalentTo(new[] { 0 }, CollectionOrdering.Matching);
    }

    [Test]
    public async Task TryGetRawAsyncReturnsFalseForMissingKey()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        var destination = new ArrayBufferWriter<byte>();
        var found = await storage.TryGetRawAsync([9, 9], destination);

        await Assert.That(found).IsFalse();
        await Assert.That(destination.WrittenCount).IsEqualTo(0);
    }

    [Test]
    public async Task TryGetRawAsyncReturnsFalseForDeletedKeyInMemTable()
    {
        // A delete is distinct from a live empty value and is reported as not found.
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        storage.Put(key, [4, 5, 6]);
        storage.Delete(key);

        var destination = new ArrayBufferWriter<byte>();
        var found = await storage.TryGetRawAsync(key, destination);

        await Assert.That(found).IsFalse();
        await Assert.That(destination.WrittenCount).IsEqualTo(0);
    }

    [Test]
    public async Task GetRawAsyncCopiesIntoDestinationAndReturnsLength()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6, 7, 8];
        storage.Put(key, value);

        var destination = new byte[16];
        var length = await storage.GetRawAsync(key, destination);

        await Assert.That(length).IsEqualTo(value.Length);
        await Assert.That(destination.AsSpan(0, length).ToArray()).IsEquivalentTo(value, CollectionOrdering.Matching);
    }

    [Test]
    public async Task GetRawAsyncReturnsMinusOneForMissingKey()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        var length = await storage.GetRawAsync([7], new byte[8]);

        await Assert.That(length).IsEqualTo(-1);
    }

    [Test]
    public async Task GetRawAsyncReportsLengthWithoutWritingWhenDestinationTooSmall()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key = [1];
        byte[] value = [4, 5, 6, 7, 8];
        storage.Put(key, value);

        var destination = new byte[2];
        var length = await storage.GetRawAsync(key, destination);

        // The full length is reported so the caller can resize and retry; nothing was written.
        await Assert.That(length).IsEqualTo(value.Length);
        await Assert.That(destination).IsEquivalentTo(new byte[2], CollectionOrdering.Matching);
    }

    [Test]
    public async Task TryReadRawAsyncInspectsValueWithoutCopy()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        byte[] key = [1, 2, 3];
        byte[] value = [4, 5, 6, 7];
        storage.Put(key, value);

        var holder = new byte[1][];
        var found = await storage.TryReadRawAsync(key, holder, static (state, span) =>
        {
            // The raw byte[] path borrows the stored array's own memory, so the span content matches.
            state[0] = span.ToArray();
        });

        await Assert.That(found).IsTrue();
        await Assert.That(holder[0]).IsEquivalentTo(value, CollectionOrdering.Matching);
    }

    [Test]
    public async Task TryReadRawAsyncReturnsFalseForMissingKey()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = new LsmStorageInner(tempFolder, _defaultStorageOptions);

        var invoked = false;
        var found = await storage.TryReadRawAsync([5], invoked, static (_, _) => { });

        await Assert.That(found).IsFalse();
    }

    [Test]
    public async Task RawReadsResolveAgainstFlushedSsTable()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = await LsmStorage.OpenAsync<byte[], byte[]>(tempFolder, new() { UseWriteAheadLog = false });

        byte[] liveKey = [1, 2, 3];
        byte[] liveValue = [10, 20, 30, 40];
        byte[] deletedKey = [4, 5, 6];

        storage.Put(liveKey, liveValue);
        storage.Put(deletedKey, [1]);
        storage.Delete(deletedKey);

        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        await Assert.That(storage._inner._state.CurrentMemTable.Size).IsEqualTo(0L);

        var destination = new ArrayBufferWriter<byte>();
        var found = await storage.TryGetRawAsync(liveKey, destination);
        await Assert.That(found).IsTrue();
        await Assert.That(destination.WrittenSpan.ToArray()).IsEquivalentTo(liveValue, CollectionOrdering.Matching);

        // A tombstone flushed to the SST must read back as not found, not as an empty value.
        var deletedDestination = new ArrayBufferWriter<byte>();
        var deletedFound = await storage.TryGetRawAsync(deletedKey, deletedDestination);
        await Assert.That(deletedFound).IsFalse();
        await Assert.That(deletedDestination.WrittenCount).IsEqualTo(0);

        await storage.CloseAsync();
    }

    [Test]
    public async Task RawReadsDistinguishTypedDeletes()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, new() { UseWriteAheadLog = false });

        storage.Put(1, 42);
        storage.Put(2, 7);
        storage.Delete(2);

        var destination = new byte[8];
        var length = await storage.GetRawAsync(1, destination);
        await Assert.That(length).IsEqualTo(sizeof(int));
        // The stored bytes are the encoder's order-preserving form, so decode through the encoder.
        await Assert.That(new Int32Encoder().Decode(destination.AsSpan(0, length))).IsEqualTo(42);

        var deletedLength = await storage.GetRawAsync(2, destination);
        await Assert.That(deletedLength).IsEqualTo(-1);

        var missingLength = await storage.GetRawAsync(999, destination);
        await Assert.That(missingLength).IsEqualTo(-1);

        await storage.CloseAsync();
    }

    [Test]
    public async Task TypedEncoderSentinelValueRemainsStorable()
    {
        using var tempFolder = TempFolder.Create();
        using var storage = await LsmStorage.OpenAsync<int, int>(tempFolder, new() { UseWriteAheadLog = false });

        storage.Put(1, int.MaxValue);
        storage._inner.ForceFreezeMemTable();
        await storage._inner.ForceFlushNextImmutableMemTableAsync();

        await Assert.That(await storage.GetAsync(1)).IsEqualTo(int.MaxValue);
        await storage.CloseAsync();
    }

    [Test]
    [Arguments(SstCompression.Lz4)]
    [Arguments(SstCompression.Zstandard)]
    public async Task CompressedTablesReopenIndependentlyOfCurrentWriteCodec(SstCompression compression)
    {
        using var tempFolder = TempFolder.Create();
        var value = Enumerable.Repeat((byte)0x2A, 1000).ToArray();
        var options = new StorageOptions
        {
            UseWriteAheadLog = false,
            FlushPeriod = TimeSpan.Zero,
            Compression = compression,
        };

        await using (var storage = await LsmStorage.OpenAsync<int, byte[]>(tempFolder, options))
        {
            for (var i = 0; i < 32; i++)
            {
                storage.Put(i, value);
            }

            storage._inner.ForceFreezeMemTable();
            await storage._inner.ForceFlushNextImmutableMemTableAsync();
        }

        await using var reopened = await LsmStorage.OpenAsync<int, byte[]>(
            tempFolder,
            new StorageOptions
            {
                UseWriteAheadLog = false,
                Compression = SstCompression.None,
            });

        for (var i = 0; i < 32; i++)
        {
            await Assert.That(await reopened.GetAsync(i)).IsEquivalentTo(value);
        }
    }

    private static LsmStorageInner FillImmutableMemTables(string path, int entries = 100, int valueSize = 10, long memTableSizeLimit = 100)
    {
        var storageOptions = new StorageOptions { MemTableSizeLimit = memTableSizeLimit, UseWriteAheadLog = false };
        var storage = new LsmStorageInner(path, storageOptions);

        for (var i = 1; i <= entries; i++)
        {
            var key = i;
            var value = new byte[valueSize];
            Random.Shared.NextBytes(value);

            storage.Put(key, value);
        }

        return storage;
    }
}
