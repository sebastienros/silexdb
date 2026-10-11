namespace Silex.Test;

public class PointReadBatchTests
{
    private static StorageOptions Options() => new()
    {
        UseWriteAheadLog = false,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
        BlockSize = 128,
    };

    private static ReadOnlyMemory<byte>[] Keys(params string[] keys) =>
        keys.Select(k => (ReadOnlyMemory<byte>)System.Text.Encoding.UTF8.GetBytes(k)).ToArray();

    private static string? Value(PointReadBatch batch, int index) =>
        batch.TryGetValue(index, out var v) ? System.Text.Encoding.UTF8.GetString(v.Span) : null;

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReadsOneSnapshotInRequestOrderWithDuplicatesAndMisses(bool sst)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put("a"u8, "1"u8);
        db.Put("b"u8, "2"u8);
        db.Put("c"u8, ""u8);
        db.Put("d"u8, "4"u8);
        db.Delete("d"u8);
        if (sst) await db.FlushAndCompactAsync();
        using var snapshot = db.CreateSnapshot();
        db.Put("a"u8, "new"u8);
        db.Put("z"u8, "26"u8);

        using var old = await snapshot.ReadManyRawAsync(Keys("c", "a", "zz", "d", "a", "z", "b"), 64);
        await Assert.That(old.IsComplete).IsTrue();
        await Assert.That(old.Count).IsEqualTo(7);
        await Assert.That(Value(old, 0)).IsEqualTo("");
        await Assert.That(Value(old, 1)).IsEqualTo("1");
        await Assert.That(Value(old, 2)).IsNull();
        await Assert.That(Value(old, 3)).IsNull();
        await Assert.That(Value(old, 4)).IsEqualTo("1");
        await Assert.That(Value(old, 5)).IsNull();
        await Assert.That(Value(old, 6)).IsEqualTo("2");
        await Assert.That(old.PayloadBytes).IsEqualTo(3);

        using var latest = await db.ReadManyRawAsync(Keys("a", "z"), 64);
        await Assert.That(Value(latest, 0)).IsEqualTo("new");
        await Assert.That(Value(latest, 1)).IsEqualTo("26");
    }

    [Test]
    public async Task ByteBudgetReturnsResumablePrefixAndRejectsOversizedFirstValue()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put("a"u8, "1111"u8);
        db.Put("b"u8, "2222"u8);
        db.Put("c"u8, "3333"u8);
        var keys = Keys("a", "missing", "b", "c");
        using var snapshot = db.CreateSnapshot();
        using var first = await snapshot.ReadManyRawAsync(keys, 5);
        await Assert.That(first.IsComplete).IsFalse();
        await Assert.That(first.Count).IsEqualTo(2);
        await Assert.That(first.RequestedCount).IsEqualTo(4);
        await Assert.That(Value(first, 0)).IsEqualTo("1111");
        await Assert.That(Value(first, 1)).IsNull();
        await Assert.That(() => first.TryGetValue(2, out _)).Throws<ArgumentOutOfRangeException>();

        using var rest = await snapshot.ReadManyRawAsync(keys.AsMemory(first.Count), 8);
        await Assert.That(rest.IsComplete).IsTrue();
        await Assert.That(Value(rest, 0)).IsEqualTo("2222");
        await Assert.That(Value(rest, 1)).IsEqualTo("3333");

        await Assert.That(async () => await snapshot.ReadManyRawAsync(keys, 3)).Throws<ArgumentException>();
        await Assert.That(async () => await snapshot.ReadManyRawAsync(keys, 0)).Throws<ArgumentOutOfRangeException>();
        using var empty = await snapshot.ReadManyRawAsync(ReadOnlyMemory<ReadOnlyMemory<byte>>.Empty, 1);
        await Assert.That(empty.IsComplete && empty.Count == 0).IsTrue();
    }

    [Test]
    public async Task IndexRangeIdsFeedBatchAtCursorSnapshot()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        for (var i = 0; i < 20; i++)
        {
            db.Put(System.Text.Encoding.UTF8.GetBytes($"i{i:D2}"), System.Text.Encoding.UTF8.GetBytes($"r{i:D2}"));
            db.Put(System.Text.Encoding.UTF8.GetBytes($"r{i:D2}"), System.Text.Encoding.UTF8.GetBytes($"doc{i}"));
        }
        using var snapshot = db.CreateSnapshot();
        db.Put("r05"u8, "changed"u8);
        using var cursor = snapshot.CreateOrderedReadCursor(new OrderedReadRange("i04"u8.ToArray(), true, "i08"u8.ToArray(), false));
        using var ids = await cursor.ReadBatchAsync(10, 1024);
        var keys = new ReadOnlyMemory<byte>[ids.Count];
        for (var i = 0; i < keys.Length; i++) keys[i] = ids[i].Value;
        using var docs = await snapshot.ReadManyRawAsync(keys, 1024);
        await Assert.That(Enumerable.Range(0, docs.Count).Select(i => Value(docs, i)))
            .IsEquivalentTo(new[] { "doc4", "doc5", "doc6", "doc7" });
    }

    [Test]
    public async Task TransactionOverlayAndSnapshotIsolationWithoutTrackingDependencies()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put("a"u8, "1"u8);
        db.Put("b"u8, "2"u8);
        using var tx = db.BeginTransaction();
        tx.Put("a"u8, "pending"u8);
        tx.Delete("b"u8);
        tx.Put("n"u8, "new"u8);
        db.Put("c"u8, "outside"u8);
        var keys = Keys("a", "b", "n", "c");
        using var batch = await tx.ReadManyRawAsync(keys, 64);
        await Assert.That(Value(batch, 0)).IsEqualTo("pending");
        await Assert.That(Value(batch, 1)).IsNull();
        await Assert.That(Value(batch, 2)).IsEqualTo("new");
        await Assert.That(Value(batch, 3)).IsNull();
        await Assert.That(tx.TrackedReadCount).IsEqualTo(0);
        using var capped = await tx.ReadManyRawAsync(keys, 7);
        await Assert.That(capped.Count).IsEqualTo(2);
        await Assert.That(capped.IsComplete).IsFalse();
        tx.Put("a"u8, "later"u8);
        await Assert.That(Value(batch, 0)).IsEqualTo("pending");
        await Assert.That(await tx.TryCommitAsync()).IsTrue();
    }

    [Test]
    public async Task LifetimeCancellationAndDisposalContracts()
    {
        using var folder = TempFolder.Create();
        var db = await MvccStorage.OpenAsync(folder, Options());
        db.Put("a"u8, "1"u8);
        var snapshot = db.CreateSnapshot();
        var keys = Keys("a");
        var batch = await snapshot.ReadManyRawAsync(keys, 8);
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.That(async () => await snapshot.ReadManyRawAsync(keys, 8, cts.Token))
            .Throws<OperationCanceledException>();
        snapshot.Dispose();
        await Assert.That(Value(batch, 0)).IsEqualTo("1");
        await Assert.That(async () => await snapshot.ReadManyRawAsync(keys, 8)).Throws<ObjectDisposedException>();
        await db.DisposeAsync();
        await Assert.That(Value(batch, 0)).IsEqualTo("1");
        batch.Dispose();
        batch.Dispose();
        await Assert.That(() => batch.TryGetValue(0, out _)).Throws<ObjectDisposedException>();
        await Assert.That(async () => await db.ReadManyRawAsync(keys, 8)).Throws<ObjectDisposedException>();
    }
}
