namespace Silex.Test;

using Silex.Collections;
using Silex.MemTables;
using Silex.Tables;
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
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task IndexesKeepInvariantsUnderRandomInterleavedOperations(int seed)
    {
        using var table = new MemTable(1);
        var model = new IndexModel(table);
        var random = new Random(seed);
        var dropped = false;
        var rebuilt = false;

        for (var step = 0; step < 12_000; step++)
        {
            // No seeks or scans for this stretch, so inserts run past the drop threshold while version lookups go on.
            var quiet = step is >= 5_000 and < 8_500;
            var operation = random.Next(100);
            if (operation < 45)
            {
                model.Put(RandomKey(random), RandomValue(random));
            }
            else if (operation < 55)
            {
                model.Delete(RandomKey(random));
            }
            else if (operation < 75)
            {
                model.CheckVersionLookup(RandomVersionPrefix(random), (ulong)random.Next(25), random.Next(2) == 0);
            }
            else if (operation < 80 || quiet)
            {
                model.CheckPointLookup(RandomKey(random));
            }
            else if (operation < 95)
            {
                model.CheckSeek(RandomKey(random), random.Next(2) == 0, random.Next(2) == 0);
            }
            else
            {
                model.CheckScan(random.Next(3) == 0 ? null : RandomKey(random), random.Next(4) == 0, random.Next(1, 300));
            }

            if (quiet && table.CurrentOrderedIndex is null)
            {
                dropped = true;
            }
            else if (dropped && !quiet && table.CurrentOrderedIndex is not null)
            {
                rebuilt = true;
            }
            if (step % 250 == 0)
            {
                CheckIndexInvariants(table);
            }
        }

        model.CheckAll();
        await Assert.That(dropped).IsTrue();
        await Assert.That(rebuilt).IsTrue();
    }

    [Test]
    [Arguments(false, 1)]
    [Arguments(false, 7)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    public async Task FailedOrderedIndexSplitMarksIndexForRebuild(bool branch, int occurrence)
    {
        // A leaf split that fails leaves an overfull leaf; a branch split that fails does so after its child split.
        var armed = true;
        var splitsInWrite = 0;
        var splits = 0;
        using var table = new MemTable(1, faultInjector: point =>
        {
            if (point == StorageFaultPoint.BatchBeforeApply)
            {
                splitsInWrite = 0;
            }
            else if (armed && point == StorageFaultPoint.MemTableBeforeOrderedIndexSplit &&
                ++splitsInWrite == (branch ? 2 : 1) && ++splits == occurrence)
            {
                armed = false;
                throw new IOException("Injected split failure.");
            }
        });
        var model = new IndexModel(table);
        var random = new Random(occurrence);
        BuildIndexes(model, random);

        byte[]? failedKey = null;
        for (var i = 0; failedKey is null; i++)
        {
            if (i == 30_000)
            {
                throw new InvalidOperationException("The injected split failure never fired.");
            }
            // Version keys only, so the failed record also belongs in a live version chain.
            var key = VersionKey(RandomVersionPrefix(random), (ulong)random.Next(1, 1_000));
            var value = RandomValue(random);
            try
            {
                model.Put(key, value);
            }
            catch (IOException)
            {
                failedKey = key;
                await Assert.That(table.WriteOutcome).IsEqualTo(StorageCommitOutcome.Indeterminate);
                await Assert.That(table.CurrentOrderedIndex).IsNull();
                model.RecordIndeterminate(key, value);
                break;
            }
            KeepIndexesLive(model, random, i);
        }

        CheckIndexInvariants(table);
        model.CheckVersionLookup(failedKey[..^sizeof(long)], 0, true);
        ContinueAfterFailure(model, random);
    }

    [Test]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task FailedVersionPrefixGrowDropsVersionIndex(int occurrence)
    {
        var grows = 0;
        using var table = new MemTable(1, faultInjector: point =>
        {
            if (point == StorageFaultPoint.MemTableBeforeVersionPrefixGrow && ++grows == occurrence)
            {
                throw new IOException("Injected grow failure.");
            }
        });
        var model = new IndexModel(table);
        var random = new Random(occurrence);
        BuildIndexes(model, random);

        byte[]? failedKey = null;
        for (var i = 0; failedKey is null; i++)
        {
            if (i == 30_000)
            {
                throw new InvalidOperationException("The injected grow failure never fired.");
            }
            var key = RandomKey(random);
            var value = RandomValue(random);
            try
            {
                model.Put(key, value);
            }
            catch (IOException)
            {
                failedKey = key;
                await Assert.That(table.WriteOutcome).IsEqualTo(StorageCommitOutcome.Indeterminate);
                var versionIndexDropped = table.VersionPrefixBuckets.IsEmpty;
                await Assert.That(versionIndexDropped).IsTrue();
                model.RecordIndeterminate(key, value);
                break;
            }
            KeepIndexesLive(model, random, i);
        }

        CheckIndexInvariants(table);
        ContinueAfterFailure(model, random);
    }

    /// <summary>Builds the ordered index with a seek and the version-prefix index once lookups stop falling back to seeks.</summary>
    private static void BuildIndexes(IndexModel model, Random random)
    {
        for (var i = 0; i < 100; i++)
        {
            model.Put(RandomKey(random), RandomValue(random));
        }
        model.CheckSeek(RandomKey(random), true, false);
        for (var i = 0; i < 10; i++)
        {
            model.CheckVersionLookup(RandomVersionPrefix(random), (ulong)random.Next(25), true);
        }
    }

    /// <summary>Reads often enough that inserts keep updating both indexes instead of leaving the ordered one to rebuild.</summary>
    private static void KeepIndexesLive(IndexModel model, Random random, int step)
    {
        if (step % 200 == 0)
        {
            model.CheckSeek(RandomKey(random), random.Next(2) == 0, random.Next(2) == 0);
            model.CheckVersionLookup(RandomVersionPrefix(random), (ulong)random.Next(25), random.Next(2) == 0);
        }
    }

    private static void ContinueAfterFailure(IndexModel model, Random random)
    {
        for (var i = 0; i < 3_000; i++)
        {
            model.Put(RandomKey(random), RandomValue(random));
            KeepIndexesLive(model, random, i);
            if (i % 500 == 0)
            {
                model.CheckAll();
            }
        }
        model.CheckAll();
    }

    private static byte[] RandomKey(Random random)
    {
        if (random.Next(4) != 0)
        {
            return VersionKey(RandomVersionPrefix(random), (ulong)random.Next(1, 24));
        }
        // Plain keys from a small alphabet share leading words across lengths.
        var key = new byte[random.Next(1, 13)];
        key[0] = 2;
        for (var i = 1; i < key.Length; i++)
        {
            key[i] = (byte)random.Next(3);
        }
        return key;
    }

    private static byte[] RandomVersionPrefix(Random random) => random.Next(10) switch
    {
        // Clean MVCC prefixes shorter than the leading word.
        < 8 => [1, (byte)(1 + random.Next(15)), (byte)(1 + random.Next(20)), 0, 0],
        // An escaped user key, so a version key that is not clean MVCC.
        8 => [1, 0, 1, (byte)(1 + random.Next(20)), 0, 0],
        // Prefixes longer than the leading word.
        _ => [1, .. Enumerable.Repeat((byte)'a', 9), (byte)('a' + random.Next(20)), 0, 0],
    };

    private static byte[] VersionKey(byte[] prefix, ulong trailer)
    {
        var key = new byte[prefix.Length + sizeof(long)];
        prefix.CopyTo(key, 0);
        BinaryPrimitives.WriteUInt64BigEndian(key.AsSpan(prefix.Length), trailer);
        return key;
    }

    private static byte[] RandomValue(Random random)
    {
        var value = new byte[random.Next(6)];
        random.NextBytes(value);
        return value;
    }

    /// <summary>
    /// Checks the structure of a table's indexes: the ordered index when it is current, and the version-prefix index
    /// when it is built. Throws on the first violation.
    /// </summary>
    private static void CheckIndexInvariants(MemTable table)
    {
        if (table.CurrentOrderedIndex is { } index)
        {
            CheckOrderedIndex(table, index);
        }
        CheckVersionPrefixIndex(table);
    }

    private static void CheckOrderedIndex(MemTable table, MemTable.OrderedIndex index)
    {
        var leaves = new List<MemTable.OrderedIndex.Leaf>();
        var leafDepth = -1;
        var treePrefix = int.MaxValue;
        for (var record = 1; record < table.Count; record++)
        {
            treePrefix = Math.Min(treePrefix, table.GetRecordKey(0).CommonPrefixLength(table.GetRecordKey(record)));
        }
        CheckNode(index.Root, 0, -1, -1);

        var seen = new bool[table.Count];
        var total = 0;
        var previous = -1;
        for (var i = 0; i < leaves.Count; i++)
        {
            var leaf = leaves[i];
            Require(leaf.Previous == (i == 0 ? null : leaves[i - 1]), $"leaf {i} has a wrong previous link");
            Require(leaf.Next == (i == leaves.Count - 1 ? null : leaves[i + 1]), $"leaf {i} has a wrong next link");
            for (var slot = 0; slot < leaf.Count; slot++)
            {
                var record = leaf.Keys[slot];
                Require((uint)record < (uint)table.Count, $"record {record} is past the table's {table.Count} records");
                Require(!seen[record], $"record {record} appears twice");
                seen[record] = true;
                total++;
                Require(previous < 0 || Compare(previous, record) < 0, $"record {record} is out of order in leaf {i}");
                previous = record;
            }
        }
        Require(total == table.Count, $"the leaves hold {total} records instead of {table.Count}");

        // Bounds are records, -1 when unbounded: keys are at least the lower bound and below the upper one.
        void CheckNode(MemTable.OrderedIndex.Node node, int depth, int lower, int upper)
        {
            CheckWords(node, lower, upper);
            if (node is MemTable.OrderedIndex.Leaf leaf)
            {
                Require(leaf.Count <= MemTable.OrderedIndex.LeafCapacity, $"a leaf holds {leaf.Count} keys");
                Require(leaf == index.Root || leaf.Count > 0, "a leaf below the root is empty");
                Require(leafDepth < 0 || leafDepth == depth, $"leaves sit at depths {leafDepth} and {depth}");
                leafDepth = depth;
                for (var slot = 0; slot < leaf.Count; slot++)
                {
                    var record = leaf.Keys[slot];
                    Require((lower < 0 || Compare(lower, record) <= 0) && (upper < 0 || Compare(record, upper) < 0),
                        $"record {record} is outside its separator bounds");
                }
                leaves.Add(leaf);
                return;
            }

            var branch = (MemTable.OrderedIndex.Branch)node;
            Require(branch.Count <= MemTable.OrderedIndex.BranchCapacity, $"a branch holds {branch.Count} separators");
            for (var i = 0; i <= branch.Count; i++)
            {
                var child = branch.Children[i];
                Require(child is not null, $"branch child {i} of {branch.Count + 1} is missing");
                if (i != 0)
                {
                    Require(Minimum(child!) == branch.Keys[i - 1], $"separator {i - 1} is not the minimum of its right child");
                }
                CheckNode(child!, depth + 1, i == 0 ? lower : branch.Keys[i - 1], i == branch.Count ? upper : branch.Keys[i]);
            }
        }

        // A node's windows skip a prefix that every key between its bounds shares: the bounds' common prefix, or on an
        // edge of the tree the prefix that all keys share.
        void CheckWords(MemTable.OrderedIndex.Node node, int lower, int upper)
        {
            var shared = lower >= 0 && upper >= 0
                ? table.GetRecordKey(lower).CommonPrefixLength(table.GetRecordKey(upper))
                : treePrefix;
            Require(node.PrefixLength <= shared, $"a node skips {node.PrefixLength} bytes where its keys share {shared}");
            var words = node is MemTable.OrderedIndex.Leaf leaf ? leaf.Words : ((MemTable.OrderedIndex.Branch)node).Words;
            var keys = node is MemTable.OrderedIndex.Leaf l ? l.Keys : ((MemTable.OrderedIndex.Branch)node).Keys;
            for (var slot = 0; slot < node.Count; slot++)
            {
                Require(words[slot] == Window(table.GetRecordKey(keys[slot]), node.PrefixLength), $"the window of record {keys[slot]} is stale");
            }
        }

        static int Minimum(MemTable.OrderedIndex.Node node)
        {
            while (node is MemTable.OrderedIndex.Branch branch)
            {
                node = branch.Children[0]!;
            }
            return node.Count == 0 ? -1 : ((MemTable.OrderedIndex.Leaf)node).Keys[0];
        }

        static ulong Window(ReadOnlySpan<byte> key, int prefixLength)
        {
            Span<byte> window = stackalloc byte[sizeof(ulong)];
            window.Clear();
            if (prefixLength < key.Length)
            {
                var rest = key[prefixLength..];
                rest[..Math.Min(rest.Length, window.Length)].CopyTo(window);
            }
            return System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(window);
        }

        int Compare(int x, int y) => table.GetRecordKey(x).SequenceCompareTo(table.GetRecordKey(y));
    }

    private static void CheckVersionPrefixIndex(MemTable table)
    {
        var buckets = table.VersionPrefixBuckets;
        if (buckets.IsEmpty)
        {
            return;
        }
        Require(int.IsPow2(buckets.Length), $"the version-prefix index has {buckets.Length} buckets");

        var mask = buckets.Length - 1;
        var chained = new bool[table.Count];
        var prefixes = new HashSet<byte[]>(IndexModel.ByteEquality);
        for (var bucket = 0; bucket < buckets.Length; bucket++)
        {
            var head = buckets[bucket] - 1;
            if (head < 0)
            {
                continue;
            }
            Require(head < table.Count, $"bucket {bucket} heads record {head}, past the table's {table.Count} records");
            var prefix = table.GetRecordKey(head)[..^sizeof(long)].ToArray();
            Require(prefixes.Add(prefix), $"prefix {Convert.ToHexString(prefix)} heads two chains");
            // A lookup probes from the prefix's hash and stops at the first empty bucket.
            for (var probe = (int)MemTable.Hash(prefix) & mask; probe != bucket; probe = (probe + 1) & mask)
            {
                Require(buckets[probe] != 0, $"the chain for {Convert.ToHexString(prefix)} is unreachable from its hash");
            }

            ulong? newer = null;
            for (var record = head; record >= 0; record = table.GetNextVersionRecord(record))
            {
                Require((uint)record < (uint)table.Count, $"a chain links record {record}, past the table's {table.Count} records");
                Require(!chained[record], $"record {record} is linked twice");
                chained[record] = true;
                var key = table.GetRecordKey(record);
                Require(key.Length == prefix.Length + sizeof(long) && key.StartsWith(prefix),
                    $"record {record} is in the chain for {Convert.ToHexString(prefix)}");
                // Newer versions have smaller trailers and come first.
                var trailer = BinaryPrimitives.ReadUInt64BigEndian(key[^sizeof(long)..]);
                Require(newer is null || trailer > newer, $"record {record} is out of version order");
                newer = trailer;
            }
        }
        for (var record = 0; record < table.Count; record++)
        {
            Require(chained[record] || !VersionPrefixBloom.IsVersionKey(table.GetRecordKey(record)),
                $"version record {record} is in no chain");
        }
    }

    private static void Require(bool condition, string violation)
    {
        if (!condition)
        {
            throw new InvalidOperationException($"Memtable index invariant violated: {violation}.");
        }
    }

    /// <summary>The expected contents of a table, with null values for tombstones, and checks of each read against them.</summary>
    private sealed class IndexModel(MemTable table)
    {
        private static readonly IComparer<byte[]> KeyOrder = Comparer<byte[]>.Create(static (x, y) => x.AsSpan().SequenceCompareTo(y));
        public static readonly IEqualityComparer<byte[]> ByteEquality = EqualityComparer<byte[]>.Create(
            static (x, y) => x.AsSpan().SequenceEqual(y),
            static x =>
            {
                var hash = new HashCode();
                hash.AddBytes(x);
                return hash.ToHashCode();
            });

        private readonly SortedSet<byte[]> _keys = new(KeyOrder);
        private readonly Dictionary<byte[], byte[]?> _values = new(ByteEquality);

        public void Put(byte[] key, byte[] value)
        {
            table.PutRaw(key, value);
            Record(key, value);
        }

        public void Delete(byte[] key)
        {
            table.DeleteRaw(key);
            Record(key, null);
        }

        /// <summary>A write that failed may or may not have been applied; reads must agree with whichever it was.</summary>
        public void RecordIndeterminate(byte[] key, byte[] value)
        {
            if (table.TryGetRaw(key, out _, out _))
            {
                Record(key, value);
            }
        }

        private void Record(byte[] key, byte[]? value)
        {
            _keys.Add(key);
            _values[key] = value;
        }

        public void CheckPointLookup(byte[] key)
        {
            var found = table.TryGetRaw(key, out var value, out var tombstone);
            if (found != _values.TryGetValue(key, out var expected) ||
                found && (tombstone != expected is null || !tombstone && !value.Span.SequenceEqual(expected)))
                throw new InvalidOperationException($"Incorrect point lookup of {Convert.ToHexString(key)}.");
        }

        public void CheckSeek(byte[] from, bool inclusive, bool backwards)
        {
            var found = table.TryGetNextRaw(from, true, inclusive, backwards, out var key, out var value, out var tombstone);
            CheckEntry(found, key, value, tombstone, backwards ? Previous(from, inclusive) : Next(from, inclusive),
                $"seek from {Convert.ToHexString(from)}, inclusive={inclusive}, backwards={backwards}");
        }

        public void CheckVersionLookup(byte[] prefix, ulong trailer, bool inclusive)
        {
            var from = VersionKey(prefix, trailer);
            var found = table.TryGetNextVersionRaw(from, prefix, inclusive, out var key, out var value, out var tombstone);
            var expected = Next(from, inclusive);
            CheckEntry(found, key, value, tombstone, expected is not null && expected.AsSpan().StartsWith(prefix) ? expected : null,
                $"version lookup from {Convert.ToHexString(from)}, inclusive={inclusive}");
        }

        /// <summary>Scans skip tombstones and stop after <paramref name="limit"/> entries.</summary>
        public void CheckScan(byte[]? from, bool backwards, int limit)
        {
            var actual = new List<(byte[] Key, byte[] Value)>();
            ReadRawEntryAction<List<(byte[] Key, byte[] Value)>> reader = static (list, key, value) =>
            {
                list.Add((key.ToArray(), value.ToArray()));
                return true;
            };
            IEnumerable<byte[]> keys;
            if (backwards)
            {
                table.ScanReverseRaw(actual, reader, limit);
                keys = _keys.Reverse();
            }
            else
            {
                table.ScanRaw(from is not null, from, actual, reader, limit);
                keys = from is null ? _keys
                    : _keys.Count == 0 || KeyOrder.Compare(from, _keys.Max!) > 0 ? [] : _keys.GetViewBetween(from, _keys.Max!);
            }
            var expected = keys.Where(key => _values[key] is not null).Take(limit).ToArray();
            if (actual.Count != expected.Length ||
                !actual.Zip(expected).All(pair => pair.First.Key.AsSpan().SequenceEqual(pair.Second) &&
                    pair.First.Value.AsSpan().SequenceEqual(_values[pair.Second])))
                throw new InvalidOperationException($"Incorrect scan from {(from is null ? "the start" : Convert.ToHexString(from))}, backwards={backwards}, limit={limit}.");
        }

        /// <summary>Compares a full scan, a point lookup and a version lookup of every key, and the index structure.</summary>
        public void CheckAll()
        {
            CheckIndexInvariants(table);
            var actual = table.CreateIterator().EnumerateAsync().ToBlockingEnumerable().ToArray();
            if (actual.Length != _keys.Count)
                throw new InvalidOperationException($"The table iterates {actual.Length} entries instead of {_keys.Count}.");
            var i = 0;
            foreach (var key in _keys)
            {
                var entry = actual[i++];
                var expected = _values[key];
                if (!entry.Key.Span.SequenceEqual(key) || entry.Value.IsTombstone != expected is null ||
                    expected is not null && !entry.Value.Span.SequenceEqual(expected))
                    throw new InvalidOperationException($"Incorrect entry {i - 1} in a full iteration.");
                CheckPointLookup(key);
                if (VersionPrefixBloom.IsVersionKey(key))
                {
                    CheckVersionLookup(key[..^sizeof(long)], BinaryPrimitives.ReadUInt64BigEndian(key.AsSpan(key.Length - sizeof(long))), true);
                }
            }
            CheckIndexInvariants(table);
        }

        private void CheckEntry(bool found, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool tombstone, byte[]? expectedKey, string description)
        {
            var expectedValue = expectedKey is null ? null : _values[expectedKey];
            if (found != expectedKey is not null || found &&
                (!key.SequenceEqual(expectedKey) || tombstone != expectedValue is null || !tombstone && !value.SequenceEqual(expectedValue)))
                throw new InvalidOperationException($"Incorrect {description}.");
        }

        private byte[]? Next(byte[] from, bool inclusive)
        {
            if (_keys.Count == 0 || KeyOrder.Compare(from, _keys.Max!) > 0)
            {
                return null;
            }
            foreach (var key in _keys.GetViewBetween(from, _keys.Max!))
            {
                if (inclusive || KeyOrder.Compare(key, from) != 0)
                {
                    return key;
                }
            }
            return null;
        }

        private byte[]? Previous(byte[] from, bool inclusive)
        {
            if (_keys.Count == 0 || KeyOrder.Compare(from, _keys.Min!) < 0)
            {
                return null;
            }
            foreach (var key in _keys.GetViewBetween(_keys.Min!, from).Reverse())
            {
                if (inclusive || KeyOrder.Compare(key, from) != 0)
                {
                    return key;
                }
            }
            return null;
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
