namespace Silex.Test;

using Silex.Collections;
using Silex.MemTables;
using System.Buffers.Binary;
using TUnit.Assertions.Enums;

public class MemTableTests
{
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(255)]
    [Arguments(256)]
    [Arguments(257)]
    [Arguments(8448)]
    [Arguments(8449)]
    [Arguments(32768)]
    public async Task PagedIndexPreservesBoundsAndTraversalAfterBulkLoadAndRandomSplits(int count)
    {
        using var table = new MemTable(1);
        var key = new byte[4];
        for (var i = count; i > 0; i--)
        {
            BinaryPrimitives.WriteInt32BigEndian(key, i * 2);
            table.PutRaw(key, key);
        }
        CheckBounds(0, false, false, count == 0 ? null : 2);
        var order = Enumerable.Range(0, count).ToArray();
        new Random(42).Shuffle(order);
        foreach (var i in order)
        {
            BinaryPrimitives.WriteInt32BigEndian(key, i * 2 + 1);
            table.PutRaw(key, key);
            CheckBounds(i * 2 + 1, true, false, i * 2 + 1);
        }
        for (var i = 1; i <= count * 2; i++)
        {
            CheckBounds(i, true, false, i);
            CheckBounds(i, true, true, i);
            CheckBounds(i, false, false, i == count * 2 ? null : i + 1);
            CheckBounds(i, false, true, i == 1 ? null : i - 1);
        }
        CheckBounds(0, true, true, null);
        CheckBounds(count * 2 + 1, true, false, null);
        var iterator = table.CreateIterator();
        var forward = iterator.EnumerateAsync().ToBlockingEnumerable()
            .Select(static entry => BinaryPrimitives.ReadInt32BigEndian(entry.Key.Span)).ToArray();
        var reverse = iterator.EnumerateBackwardsAsync().ToBlockingEnumerable()
            .Select(static entry => BinaryPrimitives.ReadInt32BigEndian(entry.Key.Span)).ToArray();
        await Assert.That(forward).IsEquivalentTo(Enumerable.Range(1, count * 2), CollectionOrdering.Matching);
        await Assert.That(reverse).IsEquivalentTo(Enumerable.Reverse(forward), CollectionOrdering.Matching);

        void CheckBounds(int from, bool inclusive, bool backwards, int? expected)
        {
            BinaryPrimitives.WriteInt32BigEndian(key, from);
            var found = table.TryGetNextRaw(key, true, inclusive, backwards,
                out var actualKey, out var actualValue, out var tombstone);
            if (found != expected.HasValue || found &&
                (BinaryPrimitives.ReadInt32BigEndian(actualKey) != expected || tombstone ||
                 !actualKey.SequenceEqual(actualValue)))
                throw new InvalidOperationException($"Incorrect seek from {from}, inclusive={inclusive}, backwards={backwards}.");
        }
    }

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
    public async Task IndexesStayCurrentWhenWritesInterleaveWithVersionLookupsAndSeeks()
    {
        using var table = new MemTable(1);
        var versions = new Dictionary<int, SortedSet<ulong>>();
        var all = new SortedSet<byte[]>(Comparer<byte[]>.Create(static (x, y) => x.AsSpan().SequenceCompareTo(y)));
        var random = new Random(7);

        for (var i = 0; i < 6_000; i++)
        {
            var user = random.Next(400);
            var trailer = (ulong)random.NextInt64(1, 1_000);
            var key = VersionKey(user, trailer);
            table.PutRaw(key, key);
            all.Add(key);
            if (!versions.TryGetValue(user, out var trailers))
            {
                versions[user] = trailers = [];
            }
            trailers.Add(trailer);

            if (i % 97 == 0)
            {
                // Plain keys share the table without joining any version chain.
                byte[] plain = [2, (byte)(i >> 8), (byte)i];
                table.PutRaw(plain, plain);
                all.Add(plain);
            }

            var probeUser = random.Next(400);
            var from = (ulong)random.NextInt64(0, 1_001);
            var inclusive = random.Next(2) == 0;
            var found = table.TryGetNextVersionRaw(VersionKey(probeUser, from), Prefix(probeUser), inclusive,
                out var actualKey, out var actualValue, out var tombstone);
            ulong? expected = null;
            if (versions.TryGetValue(probeUser, out var candidates))
            {
                foreach (var candidate in candidates)
                {
                    if (candidate > from || inclusive && candidate == from)
                    {
                        expected = candidate;
                        break;
                    }
                }
            }
            if (found != expected.HasValue || found &&
                (!actualKey.SequenceEqual(VersionKey(probeUser, expected!.Value)) || tombstone || !actualKey.SequenceEqual(actualValue)))
                throw new InvalidOperationException($"Incorrect version lookup for user {probeUser} from {from}, inclusive={inclusive}, after {i + 1} writes.");

            // No seeks for 2,000 writes, so the ordered index is dropped and the next seek rebuilds it.
            if (i % 5 == 0 && (i < 2_000 || i >= 4_000))
            {
                var seek = VersionKey(random.Next(400), (ulong)random.NextInt64(0, 1_001));
                var seekFound = table.TryGetNextRaw(seek, true, true, false, out var seekKey, out _, out _);
                var seekExpected = all.Comparer.Compare(seek, all.Max!) > 0 ? null : all.GetViewBetween(seek, all.Max!).Min;
                if (seekFound != seekExpected is not null || seekFound && !seekKey.SequenceEqual(seekExpected))
                    throw new InvalidOperationException($"Incorrect seek after {i + 1} writes.");
            }
        }

        var ordered = table.CreateIterator().EnumerateAsync().ToBlockingEnumerable().Select(static entry => entry.Key.Span.ToArray()).ToArray();
        await Assert.That(ordered.Length).IsEqualTo(all.Count);
        await Assert.That(ordered.Zip(all).All(static pair => pair.First.AsSpan().SequenceEqual(pair.Second))).IsTrue();

        static byte[] Prefix(int user) => [1, (byte)(1 + user / 250), (byte)(1 + user % 250), 0, 0];

        static byte[] VersionKey(int user, ulong trailer)
        {
            var key = new byte[13];
            Prefix(user).CopyTo(key, 0);
            BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(5), trailer);
            return key;
        }
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
