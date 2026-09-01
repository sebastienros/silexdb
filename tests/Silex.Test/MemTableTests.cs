namespace Silex.Test;

using Silex.Collections;
using Silex.MemTables;
using TUnit.Assertions.Enums;

public class MemTableTests
{
    [Test]
    public async Task PackedTablePreservesLookupOverwriteTombstonesAndSortedSeeks()
    {
        using var table = new MemTable(1, arenaBlockSize: 16);

        for (var i = 79; i >= 0; i--)
        {
            table.PutRaw([(byte)i], [(byte)(i + 1)]);
        }

        table.PutRaw([10], [200]);
        table.DeleteRaw([20]);
        table.PutRaw([30], []);

        await Assert.That(table.Count).IsEqualTo(80);
        await Assert.That(table.TryGetRaw([10], out var overwritten, out var overwrittenTombstone)).IsTrue();
        await Assert.That(overwritten.Span.ToArray()).IsEquivalentTo(new byte[] { 200 });
        await Assert.That(overwrittenTombstone).IsFalse();
        await Assert.That(table.TryGetRaw([20], out _, out var deletedTombstone)).IsTrue();
        await Assert.That(deletedTombstone).IsTrue();
        await Assert.That(table.TryGetRaw([30], out var empty, out var emptyTombstone)).IsTrue();
        await Assert.That(empty.IsEmpty).IsTrue();
        await Assert.That(emptyTombstone).IsFalse();
        await Assert.That(table.TryGetRaw([80], out _, out _)).IsFalse();

        var iterator = table.CreateIterator();
        var ascending = iterator.EnumerateAsync().ToBlockingEnumerable().Select(static x => x.Key.Span[0]).ToArray();
        var from = iterator.EnumerateAsync(ByteSlice.FromMemory(new byte[] { 37 }))
            .ToBlockingEnumerable().Select(static x => x.Key.Span[0]).ToArray();
        var backwards = iterator.EnumerateBackwardsAsync(ByteSlice.FromMemory(new byte[] { 37 }))
            .ToBlockingEnumerable().Select(static x => x.Key.Span[0]).ToArray();

        await Assert.That(ascending).IsEquivalentTo(Enumerable.Range(0, 80).Select(static x => (byte)x), CollectionOrdering.Matching);
        await Assert.That(from).IsEquivalentTo(Enumerable.Range(37, 43).Select(static x => (byte)x), CollectionOrdering.Matching);
        await Assert.That(backwards).IsEquivalentTo(Enumerable.Range(0, 38).Reverse().Select(static x => (byte)x), CollectionOrdering.Matching);

        for (var i = 80; i < 160; i++)
        {
            table.PutRaw([(byte)i], [(byte)(i + 1)]);
        }

        var afterSortedIteration = iterator.EnumerateAsync(ByteSlice.FromMemory(new byte[] { 79 }))
            .ToBlockingEnumerable().Select(static x => x.Key.Span[0]).ToArray();
        await Assert.That(afterSortedIteration)
            .IsEquivalentTo(Enumerable.Range(79, 81).Select(static x => (byte)x), CollectionOrdering.Matching);
    }

    [Test]
    public async Task OverwritingPackedTableEntryShouldNotAllocate()
    {
        using var table = new MemTable(1, arenaBlockSize: 64 * 1024);
        table.PutRaw([1], [1]);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 10_000; i++)
        {
            table.PutRaw([1], [(byte)i]);
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        await Assert.That(allocated).IsEqualTo(0);
    }

    [Test]
    public async Task SortedDictionaryShouldReturnRange()
    {
        // SortedDictionary doesn't provide a way to enumerate a range of values (as of .NET 9) so
        // we use a custom extension method to access the private SortedSet which allows it
        // c.f. https://github.com/dotnet/runtime/issues/77645

        var dic = new SortedDictionary<int, int>();

        for (int i = 0; i < 10; i++)
        {
            dic.Add(i * 10, i * 10);
        }

        var items = dic.Enumerate(51, 51, true, false);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 60, 70, 80, 90 });

        items = dic.Enumerate(51, 51, false, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0, 10, 20, 30, 40, 50 });

        items = dic.Enumerate(0, 0, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0 });

        items = dic.Enumerate(0, 0, false, false);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 });

        items = dic.Enumerate(20, 20, false, false);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 });

        items = dic.Enumerate(20, 20, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 20 });

        items = dic.Enumerate(25, 65, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 30, 40, 50, 60 });

        items = dic.Enumerate(-1, 100, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 });

        // A lower bound past the last key (from > Max) selects nothing rather than throwing.
        items = dic.Enumerate(95, 95, true, false);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());

        // An upper bound below the first key (to < Min) selects nothing rather than throwing.
        items = dic.Enumerate(-5, -5, false, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());

        // A two-sided range entirely above the data selects nothing.
        items = dic.Enumerate(100, 200, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());
    }

    [Test]
    public async Task EnumerateOnEmptyDictionaryReturnsEmpty()
    {
        var dic = new SortedDictionary<int, int>();

        await Assert.That(dic.Enumerate(5, 5, true, false).Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());
        await Assert.That(dic.Enumerate(0, 0, false, false).Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());
    }
}
