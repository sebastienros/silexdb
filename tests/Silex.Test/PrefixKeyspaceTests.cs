namespace Silex.Test;

public class PrefixKeyspaceTests
{
    private static StorageOptions Options(bool group = false) => new()
    {
        UseWriteAheadLog = group,
        SyncWriteAheadLogToDisk = group,
        MvccCommitGroupSize = group ? 8 : 1,
        FlushPeriod = TimeSpan.Zero,
        CompactionStrategy = CompactionStrategy.None,
    };

    private static async Task<string[]> KeysAsync(OrderedReadCursor cursor, int maxBytes = 32)
    {
        var keys = new List<string>();
        while (true)
        {
            using var batch = await cursor.ReadBatchAsync(1, maxBytes);
            for (var i = 0; i < batch.Count; i++)
            {
                keys.Add(Convert.ToHexString(batch[i].Key.Span));
            }
            if (batch.IsExhausted) return keys.ToArray();
        }
    }

    [Test]
    public async Task OwnsPrefixAndComputesExactBinaryBounds()
    {
        byte[] bytes = [0, 254, 255, 255];
        var space = new PrefixKeyspace(bytes);
        bytes[0] = 9;
        var range = space.BoundRange();
        await Assert.That(space.Prefix.SequenceEqual(new byte[] { 0, 254, 255, 255 })).IsTrue();
        await Assert.That(range.Lower!.Value.Span.SequenceEqual(space.Prefix)).IsTrue();
        await Assert.That(range.Upper!.Value.Span.SequenceEqual(new byte[] { 0, 255 })).IsTrue();
        await Assert.That(range.LowerInclusive).IsTrue();
        await Assert.That(range.UpperInclusive).IsFalse();
        await Assert.That(space.Contains([0, 254, 255, 255])).IsTrue();
        await Assert.That(space.Contains([0, 254, 255, 255, 0])).IsTrue();
        await Assert.That(space.Contains([0, 254, 255])).IsFalse();
        await Assert.That(new PrefixKeyspace([255, 255]).BoundRange().Upper is null).IsTrue();
        await Assert.That(new PrefixKeyspace([]).Contains([])).IsTrue();
        await Assert.That(new PrefixKeyspace([]).BoundRange().Upper is null).IsTrue();
        var exclusive = space.BoundRange(new OrderedReadRange(upper: new byte[] { 0, 255 }, upperInclusive: false));
        await Assert.That(exclusive.Upper!.Value.Span.SequenceEqual(new byte[] { 0, 255 })).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task LsmPointBatchAndBothScanDirectionsStayInsideBinaryPrefix(bool sst)
    {
        using var folder = TempFolder.Create();
        await using var db = await LsmStorage.OpenAsync(folder, Options());
        var space = new PrefixKeyspace([0, 255]);
        var other = new PrefixKeyspace([1]);
        var writes = new LsmWriteBatch();
        space.Put(writes, [0, 255], [1]);
        space.Put(writes, [0, 255, 0], [2]);
        space.Put(writes, [0, 255, 255], [3]);
        other.Put(writes, [1], [4]);
        db.Write(writes);
        space.Put(db, [0, 255, 1], [5]);
        space.Delete(db, [0, 255, 1]);
        other.Delete(writes, [1]);
        await Assert.That(writes.Count).IsEqualTo(5);
        db.Put([0], [8]);
        db.Put([0, 254, 255], [8]);
        if (sst) await db.FlushAndCompactAsync();

        using var forward = space.CreateOrderedReadCursor(db);
        using var reverse = space.CreateOrderedReadCursor(db, direction: OrderedReadDirection.Reverse);
        await Assert.That(await KeysAsync(forward)).IsEquivalentTo(new[] { "00FF", "00FF00", "00FFFF" });
        await Assert.That(string.Join(",", await KeysAsync(reverse))).IsEqualTo("00FFFF,00FF00,00FF");
        var value = new byte[1];
        await Assert.That(await space.GetRawAsync(db, [0, 255, 0], value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)2);
        var borrowed = new List<byte>();
        await Assert.That(await space.TryReadRawAsync(db, [0, 255, 255], borrowed,
            static (list, v) => list.Add(v[0]))).IsTrue();
        await Assert.That(borrowed[0]).IsEqualTo((byte)3);
        await Assert.That(() => space.Put(db, [1], [1])).Throws<ArgumentException>();
        await Assert.That(() => space.Delete(db, [1])).Throws<ArgumentException>();
        await Assert.That(async () => await space.GetRawAsync(db, [1], value)).Throws<ArgumentException>();
        await Assert.That(() => space.CreateOrderedReadCursor(db, new OrderedReadRange(upper: new byte[] { 1 })))
            .Throws<ArgumentException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EmptyAndAllFfPrefixesAndExplicitEndpointsAreExact(bool sst)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        byte[][] keys = [[], [0], [254, 255], [255], [255, 254], [255, 255], [255, 255, 0], [255, 255, 255]];
        foreach (var key in keys) db.Put(key, [1]);
        if (sst) await db.FlushAndCompactAsync();
        var all = new PrefixKeyspace([]);
        var ff = new PrefixKeyspace([255, 255]);
        using var allCursor = all.CreateOrderedReadCursor(db);
        await Assert.That(await KeysAsync(allCursor)).IsEquivalentTo(keys.Select(Convert.ToHexString).ToArray());
        using var cursor = ff.CreateOrderedReadCursor(db);
        await Assert.That(string.Join(",", await KeysAsync(cursor))).IsEqualTo("FFFF,FFFF00,FFFFFF");
        using var reverse = ff.CreateOrderedReadCursor(db, direction: OrderedReadDirection.Reverse);
        await Assert.That(string.Join(",", await KeysAsync(reverse))).IsEqualTo("FFFFFF,FFFF00,FFFF");
        using var bounded = ff.CreateOrderedReadCursor(db,
            new OrderedReadRange(new byte[] { 255, 255 }, false, new byte[] { 255, 255, 255 }, false));
        await Assert.That(string.Join(",", await KeysAsync(bounded))).IsEqualTo("FFFF00");
        using var empty = ff.CreateOrderedReadCursor(db,
            new OrderedReadRange(new byte[] { 255, 255, 255 }, true, new byte[] { 255, 255 }));
        await Assert.That((await KeysAsync(empty)).Length).IsEqualTo(0);
        using var tx = db.BeginTransaction();
        var result = await ff.DeleteRangeAsync(tx, 10);
        await Assert.That(result.DeletedCount).IsEqualTo(3);
        await tx.CommitAsync();
        await Assert.That(await all.GetRawAsync(db, [], new byte[1])).IsEqualTo(1);
        await Assert.That(await all.GetRawAsync(db, [255, 254], new byte[1])).IsEqualTo(1);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CrossHandleWritesAreAtomicAndRetainedCursorsFreezeOwnWrites(bool group)
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options(group));
        var a = new PrefixKeyspace([1]);
        var b = new PrefixKeyspace([2]);
        a.Put(db, [1, 0], [10]);
        b.Put(db, [2, 0], [20]);
        using var old = db.CreateSnapshot();
        using var tx = db.BeginTransaction();
        a.Put(tx, [1, 0], [11]);
        a.Put(tx, [1, 1], [12]);
        b.Delete(tx, [2, 0]);
        b.Put(tx, [2, 1], [21]);
        var checkpoint = tx.CreateSavepoint();
        a.Put(tx, [1, 2], [13]);
        b.Put(tx, [2, 2], [22]);
        tx.RollbackToSavepoint(checkpoint);
        tx.ReleaseSavepoint(checkpoint);
        using var cursor = a.CreateOrderedReadCursor(tx);
        using var oldCursor = b.CreateOrderedReadCursor(old);
        a.Delete(tx, [1, 1]);
        var value = new byte[1];
        await Assert.That(await a.GetRawAsync(tx, [1, 0], value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);
        await Assert.That(await b.GetRawAsync(tx, [2, 0], value)).IsEqualTo(-1);
        await Assert.That(await a.GetRawAsync(db, [1, 1], value)).IsEqualTo(-1);
        await Assert.That(await b.GetRawAsync(db, [2, 1], value)).IsEqualTo(-1);
        await tx.CommitAsync();
        tx.Dispose();
        old.Dispose();
        await db.CollectGarbageAsync();
        await Assert.That(cursor.Sequence).IsEqualTo((long?)tx.Sequence);
        await Assert.That(string.Join(",", await KeysAsync(cursor))).IsEqualTo("0100,0101");
        await Assert.That(string.Join(",", await KeysAsync(oldCursor))).IsEqualTo("0200");
        await Assert.That(await a.GetRawAsync(db, [1, 0], value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)11);
        await Assert.That(await b.GetRawAsync(db, [2, 1], value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)21);

        using var conflict = db.BeginTransaction();
        a.Put(conflict, [1, 0], [30]);
        b.Put(conflict, [2, 1], [30]);
        b.Put(db, [2, 1], [31]);
        await Assert.That(await conflict.TryCommitAsync()).IsFalse();
        await Assert.That(conflict.Conflict!.Kind).IsEqualTo(MvccConflictKind.WrittenKey);
        await Assert.That(conflict.Conflict.Key.Span.SequenceEqual(new byte[] { 2, 1 })).IsTrue();
        await a.GetRawAsync(db, [1, 0], value);
        await Assert.That(value[0]).IsEqualTo((byte)11);
    }

    [Test]
    public async Task MultiGetUsesSuppliedViewOwnWritesAndOwnedBudgetedResults()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        var space = new PrefixKeyspace([1]);
        space.Put(db, [1, 0], [10]);
        space.Put(db, [1, 1], [11]);
        using var snapshot = db.CreateSnapshot();
        using var tx = db.BeginTransaction();
        space.Put(tx, [1, 0], [20]);
        space.Delete(tx, [1, 1]);
        space.Put(db, [1, 0], [30]);
        var value = new byte[1];
        await Assert.That(await space.GetRawAsync(snapshot, [1, 0], value)).IsEqualTo(1);
        await Assert.That(value[0]).IsEqualTo((byte)10);
        var borrowed = new List<byte>();
        await Assert.That(await space.TryReadRawAsync(snapshot, [1, 0], borrowed,
            static (list, v) => list.Add(v[0]))).IsTrue();
        await Assert.That(await space.TryReadRawAsync(db, [1, 0], borrowed,
            static (list, v) => list.Add(v[0]))).IsTrue();
        await Assert.That(string.Join(",", borrowed)).IsEqualTo("10,30");
        ReadOnlyMemory<byte>[] keys = [new byte[] { 1, 0 }, new byte[] { 1, 1 }, new byte[] { 1, 0 }];
        using var current = await space.ReadManyRawAsync(db, keys, 1);
        await Assert.That(current.Count).IsEqualTo(1);
        await Assert.That(current.IsComplete).IsFalse();
        using var old = await space.ReadManyRawAsync(snapshot, keys, 3);
        using var pending = await space.ReadManyRawAsync(tx, keys, 3);
        snapshot.Dispose();
        tx.Dispose();
        await db.DisposeAsync();
        await Assert.That(current.TryGetValue(0, out var v)).IsTrue();
        await Assert.That(v.Span[0]).IsEqualTo((byte)30);
        await Assert.That(old.TryGetValue(0, out v)).IsTrue();
        await Assert.That(v.Span[0]).IsEqualTo((byte)10);
        await Assert.That(pending.TryGetValue(0, out v)).IsTrue();
        await Assert.That(v.Span[0]).IsEqualTo((byte)20);
        await Assert.That(pending.TryGetValue(1, out _)).IsFalse();
        await Assert.That(pending.TryGetValue(2, out v)).IsTrue();
        await Assert.That(v.Span[0]).IsEqualTo((byte)20);
    }

    [Test]
    public async Task RangeAndPointDependenciesNeverIncludeAdjacentNamespaces()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        var space = new PrefixKeyspace([1, 255]);
        using var outside = db.BeginTransaction();
        space.TrackReadRange(outside);
        db.Put([2], [1]);
        db.Put([1, 254], [1]);
        await Assert.That(await outside.TryCommitAsync()).IsTrue();
        using var inside = db.BeginTransaction();
        space.TrackReadRange(inside);
        db.Put([1, 255, 0], [1]);
        await Assert.That(await inside.TryCommitAsync()).IsFalse();
        await Assert.That(inside.Conflict!.Kind).IsEqualTo(MvccConflictKind.TrackedRange);
        await Assert.That(inside.Conflict.Range!.UpperInclusive).IsFalse();
        await Assert.That(inside.Conflict.Range.Upper!.Value.Span.SequenceEqual(new byte[] { 2 })).IsTrue();
        using var point = db.BeginTransaction();
        await space.GetForUpdateRawAsync(point, [1, 255, 1], new byte[1]);
        db.Put([1, 255, 1], [1]);
        await Assert.That(await point.TryCommitAsync()).IsFalse();
        await Assert.That(point.Conflict!.Kind).IsEqualTo(MvccConflictKind.TrackedPointRead);
    }

    [Test]
    public async Task ResumableDeletionPreservesOtherNamespacesAndSnapshot()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        var space = new PrefixKeyspace([1]);
        for (byte i = 0; i < 5; i++) space.Put(db, [1, i], [i]);
        db.Put([2], [9]);
        using var snapshot = db.CreateSnapshot();
        OrderedReadRange? remaining = null;
        var deleted = 0;
        do
        {
            using var tx = db.BeginTransaction();
            var result = await space.DeleteRangeAsync(tx, 2, remaining);
            deleted += result.DeletedCount;
            remaining = result.Remaining;
            await tx.CommitAsync();
        } while (remaining is not null);
        await Assert.That(deleted).IsEqualTo(5);
        using var now = space.CreateOrderedReadCursor(db);
        using var old = space.CreateOrderedReadCursor(snapshot);
        await Assert.That((await KeysAsync(now)).Length).IsEqualTo(0);
        await Assert.That((await KeysAsync(old)).Length).IsEqualTo(5);
        await Assert.That(await db.GetRawAsync([2], new byte[1])).IsEqualTo(1);
    }

    [Test]
    public async Task EscapingRequestsAreRejectedBeforeReadsWritesOrDependencies()
    {
        using var folder = TempFolder.Create();
        await using var db = await MvccStorage.OpenAsync(folder, Options());
        var space = new PrefixKeyspace([1]);
        using var snapshot = db.CreateSnapshot();
        using var tx = db.BeginTransaction();
        var batch = new LsmWriteBatch();
        await Assert.That(() => space.Put(db, [2], [1])).Throws<ArgumentException>();
        await Assert.That(() => space.Delete(db, [2])).Throws<ArgumentException>();
        await Assert.That(() => space.Put(tx, [2], [1])).Throws<ArgumentException>();
        await Assert.That(() => space.Delete(tx, [2])).Throws<ArgumentException>();
        await Assert.That(() => space.Put(batch, [2], [1])).Throws<ArgumentException>();
        await Assert.That(() => space.Delete(batch, [2])).Throws<ArgumentException>();
        await Assert.That(async () => await space.GetRawAsync(db, [2], new byte[1])).Throws<ArgumentException>();
        await Assert.That(async () => await space.GetRawAsync(snapshot, [2], new byte[1])).Throws<ArgumentException>();
        await Assert.That(async () => await space.GetRawAsync(tx, [2], new byte[1])).Throws<ArgumentException>();
        await Assert.That(async () => await space.GetForUpdateRawAsync(tx, [2], new byte[1])).Throws<ArgumentException>();
        ReadOnlyMemory<byte>[] keys = [new byte[] { 1 }, new byte[] { 2 }];
        await Assert.That(async () => await space.ReadManyRawAsync(db, keys, 1)).Throws<ArgumentException>();
        await Assert.That(async () => await space.ReadManyRawAsync(snapshot, keys, 1)).Throws<ArgumentException>();
        await Assert.That(async () => await space.ReadManyRawAsync(tx, keys, 1)).Throws<ArgumentException>();
        foreach (var range in new[]
        {
            new OrderedReadRange(new byte[] { 0 }),
            new OrderedReadRange(upper: new byte[] { 2 }),
            new OrderedReadRange(upper: new byte[] { 3 }, upperInclusive: false),
        })
        {
            await Assert.That(() => space.CreateOrderedReadCursor(db, range)).Throws<ArgumentException>();
            await Assert.That(() => space.CreateOrderedReadCursor(snapshot, range)).Throws<ArgumentException>();
            await Assert.That(() => space.CreateOrderedReadCursor(tx, range)).Throws<ArgumentException>();
            await Assert.That(() => space.TrackReadRange(tx, range)).Throws<ArgumentException>();
            await Assert.That(async () => await space.DeleteRangeAsync(tx, 1, range)).Throws<ArgumentException>();
        }
        await Assert.That(tx.MutationCount).IsEqualTo(0);
        await Assert.That(tx.TrackedReadCount).IsEqualTo(0);
        await Assert.That(tx.TrackedReadRangeCount).IsEqualTo(0);
        await Assert.That(batch.Count).IsEqualTo(0);
    }

    [Test]
    public async Task LimitsCancellationAndDisposedViewsKeepUnderlyingContracts()
    {
        using var folder = TempFolder.Create();
        var options = Options();
        options.MvccTransactionMaxMutations = 2;
        await using var db = await MvccStorage.OpenAsync(folder, options);
        var a = new PrefixKeyspace([1]);
        var b = new PrefixKeyspace([2]);
        for (byte i = 0; i < 4; i++) a.Put(db, [1, i], [i]);
        using var tx = db.BeginTransaction();
        b.Put(tx, [2], [1]);
        await Assert.That(async () => await a.DeleteRangeAsync(tx, 4)).Throws<TransactionLimitException>();
        await Assert.That(tx.MutationCount).IsEqualTo(1);
        await Assert.That(tx.TrackedReadRangeCount).IsEqualTo(0);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.That(async () => await a.DeleteRangeAsync(tx, 1, cancellationToken: cancellation.Token))
            .Throws<OperationCanceledException>();
        using var cursor = a.CreateOrderedReadCursor(tx);
        await Assert.That(async () => { using var ignored = await cursor.ReadBatchAsync(1, 1); })
            .Throws<ArgumentException>();
        await Assert.That((await KeysAsync(cursor)).Length).IsEqualTo(4);
        tx.Dispose();
        await Assert.That(() => a.Put(tx, [1], [1])).Throws<ObjectDisposedException>();
        using var snapshot = db.CreateSnapshot();
        snapshot.Dispose();
        await Assert.That(() => a.CreateOrderedReadCursor(snapshot)).Throws<ObjectDisposedException>();
        await db.DisposeAsync();
        await Assert.That(async () => { using var ignored = await cursor.ReadBatchAsync(1, 32); })
            .Throws<ObjectDisposedException>();
        await Assert.That(async () => await a.GetRawAsync(db, [1], new byte[1])).Throws<ObjectDisposedException>();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CrossHandleCommitFailureIsNotHiddenAndRecoveryIsAtomic(bool group)
    {
        using var folder = TempFolder.Create();
        var options = Options(group);
        options.UseWriteAheadLog = true;
        var armed = false;
        options.FaultInjector = p =>
        {
            if (armed && p == StorageFaultPoint.BatchAfterApplyEntry) throw new IOException("Injected failure.");
        };
        var a = new PrefixKeyspace([1]);
        var b = new PrefixKeyspace([2]);
        await using (var db = await MvccStorage.OpenAsync(folder, options))
        {
            using var tx = db.BeginTransaction();
            a.Put(tx, [1], [11]);
            b.Put(tx, [2], [22]);
            using var cursor = a.CreateOrderedReadCursor(tx);
            armed = true;
            StorageOperationException? failure = null;
            try { await tx.CommitAsync(); }
            catch (StorageOperationException e) { failure = e; }
            await Assert.That(failure is not null).IsTrue();
            await Assert.That(failure!.Outcome).IsEqualTo(StorageCommitOutcome.Indeterminate);
            await Assert.That(db.Health).IsEqualTo(StorageHealth.Faulted);
            await Assert.That(async () => await a.GetRawAsync(db, [1], new byte[1])).Throws<StorageFaultedException>();
            await Assert.That(async () => { using var ignored = await cursor.ReadBatchAsync(1, 32); })
                .Throws<StorageFaultedException>();
        }
        var recoveryOptions = Options();
        recoveryOptions.UseWriteAheadLog = true;
        await using var recovered = await MvccStorage.OpenAsync(folder, recoveryOptions);
        await Assert.That(await a.GetRawAsync(recovered, [1], new byte[1])).IsEqualTo(1);
        await Assert.That(await b.GetRawAsync(recovered, [2], new byte[1])).IsEqualTo(1);
    }
}
