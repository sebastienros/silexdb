using TUnit.Assertions.Enums;

namespace Silex.Test;

public class MvccStorageTests
{
    private static readonly StorageOptions _options = new()
    {
        UseWriteAheadLog = false,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
    };

    [Test]
    public async Task SnapshotKeepsStableVersionForBinaryKey()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        byte[] key = [1, 0, 2];
        byte[] first = [10];
        byte[] second = [20];

        storage.Put(key, first);
        using var snapshot = storage.CreateSnapshot();
        storage.Put(key, second);

        await AssertValueAsync(snapshot, key, first);
        await AssertValueAsync(storage, key, second);

        storage.Delete(key);

        await AssertValueAsync(snapshot, key, first);
        await Assert.That(await storage.GetRawAsync(key, new byte[8])).IsEqualTo(-1);
    }

    [Test]
    public async Task TransactionReadsOwnWritesAndPublishesAllKeys()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        using var transaction = storage.BeginTransaction();

        transaction.Put([1], [10]);
        transaction.Put([2], [20]);

        await AssertValueAsync(transaction, [1], [10]);
        await Assert.That(await storage.GetRawAsync([1], new byte[8])).IsEqualTo(-1);

        await transaction.CommitAsync();

        await AssertValueAsync(storage, [1], [10]);
        await AssertValueAsync(storage, [2], [20]);
        await Assert.That(storage.PublishedSequence).IsEqualTo(1);
    }

    [Test]
    public async Task ConcurrentWritersConflictAtCommit()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        storage.Put([1], [1]);
        using var first = storage.BeginTransaction();
        using var second = storage.BeginTransaction();

        first.Put([1], [2]);
        second.Put([1], [3]);

        var commits = await Task.WhenAll(
            first.TryCommitAsync().AsTask(),
            second.TryCommitAsync().AsTask());

        await Assert.That(commits.Count(static committed => committed)).IsEqualTo(1);

        var destination = new byte[1];
        await storage.GetRawAsync([1], destination);
        await Assert.That(destination[0] is 2 or 3).IsTrue();
    }

    [Test]
    public async Task GetForUpdateDetectsReadWriteConflict()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        storage.Put([1], [10]);
        using var reader = storage.BeginTransaction();
        using var writer = storage.BeginTransaction();

        await AssertValueForUpdateAsync(reader, [1], [10]);
        reader.Put([2], [20]);
        writer.Put([1], [11]);

        await writer.CommitAsync();

        await Assert.That(await reader.TryCommitAsync()).IsFalse();
        await Assert.That(await storage.GetRawAsync([2], new byte[8])).IsEqualTo(-1);
    }

    [Test]
    public async Task SnapshotScanFiltersVersionsAndPreservesUserKeyOrder()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        storage.Put([1], [10]);
        storage.Put([1, 0], [11]);
        storage.Put([2], [20]);
        using var snapshot = storage.CreateSnapshot();

        storage.Put([1], [12]);
        storage.Delete([2]);
        storage.Put([3], [30]);

        var entries = new List<(byte[] Key, byte[] Value)>();
        var count = await snapshot.ScanRawAsync(entries, static (state, key, value) =>
        {
            state.Add((key.ToArray(), value.ToArray()));
            return true;
        });

        await Assert.That(count).IsEqualTo(3);
        await Assert.That(entries.Select(x => x.Key))
            .IsEquivalentTo(new byte[][] { [1], [1, 0], [2] }, CollectionOrdering.Matching);
        await Assert.That(entries.Select(x => x.Value))
            .IsEquivalentTo(new byte[][] { [10], [11], [20] }, CollectionOrdering.Matching);

        entries.Clear();
        await snapshot.SeekRawAsync([1, 0], entries, static (state, key, value) =>
        {
            state.Add((key.ToArray(), value.ToArray()));
            return true;
        });

        await Assert.That(entries.Select(x => x.Key))
            .IsEquivalentTo(new byte[][] { [1, 0], [2] }, CollectionOrdering.Matching);
    }

    [Test]
    public async Task ReopenRestoresPublishedSequenceAndVersions()
    {
        using var folder = TempFolder.Create();

        {
            await using var storage = await MvccStorage.OpenAsync(folder, _options);
            storage.Put([1], [10]);
            storage.Put([1], [20]);
            await storage.FlushAndCompactAsync();
        }

        await using var reopened = await MvccStorage.OpenAsync(folder, _options);

        await Assert.That(reopened.PublishedSequence).IsEqualTo(2);
        await AssertValueAsync(reopened, [1], [20]);
    }

    [Test]
    public async Task GarbageCollectionRetainsVersionsNeededByActiveSnapshot()
    {
        using var folder = TempFolder.Create();
        await using var storage = await MvccStorage.OpenAsync(folder, _options);
        storage.Put([1], [10]);
        var snapshot = storage.CreateSnapshot();
        storage.Put([1], [20]);

        await Assert.That(await storage.CollectGarbageAsync()).IsEqualTo(0);
        await AssertValueAsync(snapshot, [1], [10]);

        snapshot.Dispose();

        await Assert.That(await storage.CollectGarbageAsync()).IsEqualTo(1);
        await AssertValueAsync(storage, [1], [20]);
    }

    [Test]
    public async Task PlainDatabaseCannotBeOpenedAsMvcc()
    {
        using var folder = TempFolder.Create();

        {
            await using var plain = await LsmStorage.OpenAsync(folder, _options);
            plain.Put([9], [10]);
        }

        var rejected = false;
        try
        {
            await using var _ = await MvccStorage.OpenAsync(folder, _options);
        }
        catch (InvalidOperationException)
        {
            rejected = true;
        }

        await Assert.That(rejected).IsTrue();
    }

    private static async ValueTask AssertValueAsync(
        MvccStorage storage,
        byte[] key,
        byte[] expected)
    {
        var destination = new byte[expected.Length];
        var length = await storage.GetRawAsync(key, destination);

        await Assert.That(length).IsEqualTo(expected.Length);
        await Assert.That(destination).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    private static async ValueTask AssertValueAsync(
        MvccSnapshot snapshot,
        byte[] key,
        byte[] expected)
    {
        var destination = new byte[expected.Length];
        var length = await snapshot.GetRawAsync(key, destination);

        await Assert.That(length).IsEqualTo(expected.Length);
        await Assert.That(destination).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    private static async ValueTask AssertValueAsync(
        MvccTransaction transaction,
        byte[] key,
        byte[] expected)
    {
        var destination = new byte[expected.Length];
        var length = await transaction.GetRawAsync(key, destination);

        await Assert.That(length).IsEqualTo(expected.Length);
        await Assert.That(destination).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }

    private static async ValueTask AssertValueForUpdateAsync(
        MvccTransaction transaction,
        byte[] key,
        byte[] expected)
    {
        var destination = new byte[expected.Length];
        var length = await transaction.GetForUpdateRawAsync(key, destination);

        await Assert.That(length).IsEqualTo(expected.Length);
        await Assert.That(destination).IsEquivalentTo(expected, CollectionOrdering.Matching);
    }
}
