namespace Silex.Test;

using Silex.MemTables;
using System.Buffers.Binary;

/// <summary>
/// Readers of the active memtable run alongside one writer without a lock. Values encode the key and a
/// per-key version, and their length and fill depend on the version, so a torn or stale read shows up as
/// a mismatched key, a broken pattern or a version that goes backwards.
/// </summary>
public class OptimisticMemTableReadTests
{
    private const int InitialKeys = 2_000;

    [Test]
    public async Task OptimisticReadsStayConsistentWhileOneWriterRuns()
    {
        using var table = new MemTable(1);
        var key = new byte[16];
        var value = new byte[64];
        for (var k = 0; k < InitialKeys; k++)
        {
            table.PutRaw(WriteKey(key, k * 2), WriteValue(value, k * 2, 0).Span);
        }
        table.EnsureSortedIndex();

        using var stop = new CancellationTokenSource();
        var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
        long pointReads = 0, seeks = 0;
        var readers = Enumerable.Range(0, Math.Max(2, Environment.ProcessorCount - 1)).Select(id => Task.Run(() =>
        {
            var random = new Random(id);
            var readerKey = new byte[16];
            var lastSeen = new int[InitialKeys * 4];
            while (!stop.IsCancellationRequested)
            {
                var k = random.Next(InitialKeys * 4);
                WriteKey(readerKey, k);
                if (table.TryGetSpanOptimistic(readerKey, out var found, out var read, out var tombstone))
                {
                    Interlocked.Increment(ref pointReads);
                    if (k % 2 == 0 && k < InitialKeys * 2 && (!found || tombstone))
                    {
                        failures.Enqueue($"key {k} written before the readers started was not found");
                    }
                    if (found && !tombstone)
                    {
                        Check(failures, lastSeen, k, read);
                    }
                }
                if (table.TrySeekFirstRawOptimistic(readerKey, out found, out var foundKey, out read))
                {
                    Interlocked.Increment(ref seeks);
                    if (k % 2 == 0 && k < InitialKeys * 2 &&
                        (!found || BinaryPrimitives.ReadInt32BigEndian(foundKey) != k))
                    {
                        failures.Enqueue($"seek to {k} written before the readers started did not land on it");
                    }
                    if (found)
                    {
                        var actual = BinaryPrimitives.ReadInt32BigEndian(foundKey);
                        if (actual < k)
                        {
                            failures.Enqueue($"seek to {k} returned the smaller key {actual}");
                        }
                        else if (actual < lastSeen.Length)
                        {
                            Check(failures, lastSeen, actual, read);
                        }
                    }
                }
            }
        })).ToArray();

        // Overwrite initial keys, insert odd keys (growing the record array, rehashing the buckets and splitting
        // ordered index nodes) and delete or revive odd keys, which readers never require to exist.
        var versions = new int[InitialKeys * 4];
        var writerRandom = new Random(1234);
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            for (var i = 0; i < 256; i++)
            {
                var k = writerRandom.Next(InitialKeys * 4);
                WriteKey(key, k);
                if (k % 2 == 1 && writerRandom.Next(4) == 0)
                {
                    table.DeleteRaw(key);
                }
                else
                {
                    table.PutRaw(key, WriteValue(value, k, ++versions[k]).Span);
                }
            }
            // Inserts that run far ahead of ordered reads drop the ordered index; in the store a locked reader
            // then rebuilds it while optimistic readers keep going, so rebuild it here the same way.
            table.EnsureSortedIndex();
        }
        stop.Cancel();
        await Task.WhenAll(readers);

        await Assert.That(failures.IsEmpty ? null : failures.First()).IsNull();
        await Assert.That(pointReads).IsGreaterThan(0);
        await Assert.That(seeks).IsGreaterThan(0);
    }

    /// <summary>
    /// Keys that share a 17-byte prefix tie at the window a node starts with, so nodes take their fences'
    /// prefix as they fill and again after each split, rewriting every window; keys of other prefixes, inserted
    /// now and then, shorten the tree prefix and rewrite the nodes on its edges. Seeks run throughout.
    /// </summary>
    [Test]
    public async Task OptimisticSeeksStayConsistentWhileNodesChangePrefix()
    {
        using var table = new MemTable(1);
        var value = new byte[64];
        for (var k = 0; k < InitialKeys; k++)
        {
            table.PutRaw(PrefixedKey(Family, k * 2), WriteValue(value, k * 2, 0).Span);
        }
        table.EnsureSortedIndex();

        using var stop = new CancellationTokenSource();
        var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
        long seeks = 0;
        var readers = Enumerable.Range(0, Math.Max(2, Environment.ProcessorCount - 1)).Select(id => Task.Run(() =>
        {
            var random = new Random(id);
            var lastSeen = new int[InitialKeys * 4];
            while (!stop.IsCancellationRequested)
            {
                var k = random.Next(InitialKeys) * 2;
                var key = PrefixedKey(Family, k);
                if (!table.TrySeekFirstRawOptimistic(key, out var found, out var foundKey, out var read))
                {
                    continue;
                }
                Interlocked.Increment(ref seeks);
                if (!found || !foundKey.SequenceEqual(key))
                {
                    failures.Enqueue($"seek to {k} written before the readers started did not land on it");
                }
                else
                {
                    Check(failures, lastSeen, k, read);
                }
            }
        })).ToArray();

        string[] shorterPrefixes = ["tenant/0001/userz", "tenant/0001/v", "tenant/0002/", "tenant/1", "u", "a"];
        var versions = new int[InitialKeys * 4];
        var writerRandom = new Random(1234);
        var started = DateTime.UtcNow;
        var deadline = started.AddSeconds(2);
        var nextPrefix = 0;
        while (DateTime.UtcNow < deadline)
        {
            for (var i = 0; i < 256; i++)
            {
                var k = writerRandom.Next(InitialKeys * 4);
                table.PutRaw(PrefixedKey(Family, k), WriteValue(value, k, ++versions[k]).Span);
            }
            if (nextPrefix < shorterPrefixes.Length && DateTime.UtcNow > started.AddMilliseconds(250 * (nextPrefix + 1)))
            {
                table.PutRaw(System.Text.Encoding.ASCII.GetBytes(shorterPrefixes[nextPrefix++]), value.AsSpan(0, 8));
            }
            table.EnsureSortedIndex();
        }
        stop.Cancel();
        await Task.WhenAll(readers);

        await Assert.That(failures.IsEmpty ? null : failures.First()).IsNull();
        await Assert.That(seeks).IsGreaterThan(0);
        // Nodes took a prefix past the start of the keys, so the windows were rewritten while readers ran.
        await Assert.That(MaxLeafPrefix(table.CurrentOrderedIndex!.Root)).IsGreaterThanOrEqualTo(Family.Length);

        static int MaxLeafPrefix(MemTable.OrderedIndex.Node node)
        {
            if (node is not MemTable.OrderedIndex.Branch branch)
            {
                return node.PrefixLength;
            }
            var max = 0;
            for (var i = 0; i <= branch.Count; i++)
            {
                max = Math.Max(max, MaxLeafPrefix(branch.Children[i]!));
            }
            return max;
        }
    }

    private static readonly byte[] Family = System.Text.Encoding.ASCII.GetBytes("tenant/0001/user/");

    private static byte[] PrefixedKey(byte[] prefix, int k)
    {
        var key = new byte[prefix.Length + sizeof(int)];
        prefix.CopyTo(key, 0);
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(prefix.Length), k);
        return key;
    }

    /// <summary>
    /// Small memtables freeze and flush while readers run; a large one keeps every read on the memtable-only
    /// paths, where seeks are optimistic too.
    /// </summary>
    [Test]
    [Arguments(false, true)]
    [Arguments(false, false)]
    [Arguments(true, true)]
    [Arguments(true, false)]
    public async Task StorageReadsStayConsistentWhileOneWriterRuns(bool mvcc, bool smallMemTables)
    {
        using var folder = TempFolder.Create();
        var options = new StorageOptions
        {
            MemTableSizeLimit = smallMemTables ? 64 * 1024 : 256 * 1024 * 1024,
            UseWriteAheadLog = false,
        };
        await using var lsm = mvcc ? null : await LsmStorage.OpenAsync(folder, options);
        await using var mvccStorage = mvcc ? await MvccStorage.OpenAsync(folder, options) : null;
        var key = new byte[16];
        var value = new byte[64];
        for (var k = 0; k < InitialKeys; k++)
        {
            await PutAsync(WriteKey(key, k), WriteValue(value, k, 0));
        }

        using var stop = new CancellationTokenSource();
        var failures = new System.Collections.Concurrent.ConcurrentQueue<string>();
        var readers = Enumerable.Range(0, Math.Max(2, Environment.ProcessorCount - 1)).Select(id => Task.Run(async () =>
        {
            var random = new Random(id);
            var readerKey = new byte[16];
            var buffer = new byte[64];
            var lastSeen = new int[InitialKeys];
            var seek = new SeekResult();
            while (!stop.IsCancellationRequested)
            {
                var k = random.Next(InitialKeys);
                WriteKey(readerKey, k);
                var length = mvccStorage is not null
                    ? await mvccStorage.GetRawAsync(readerKey, buffer)
                    : await lsm!.GetRawAsync(readerKey, buffer);
                if (length < 0)
                {
                    failures.Enqueue($"key {k} was not found");
                }
                else
                {
                    Check(failures, lastSeen, k, buffer.AsSpan(0, length));
                }

                seek.Found = false;
                if (mvccStorage is not null)
                {
                    await mvccStorage.SeekRawAsync(readerKey, seek, static (s, foundKey, foundValue) => s.Accept(foundKey, foundValue), 1);
                }
                else
                {
                    await lsm!.SeekRawAsync(readerKey, seek, static (s, foundKey, foundValue) => s.Accept(foundKey, foundValue), 1);
                }
                if (!seek.Found || seek.Key != k)
                {
                    failures.Enqueue($"seek to {k} did not land on it");
                }
                else
                {
                    Check(failures, lastSeen, k, seek.Value.AsSpan(0, seek.Length));
                }
            }
        })).ToArray();

        var versions = new int[InitialKeys];
        var writerRandom = new Random(1234);
        var deadline = DateTime.UtcNow.AddSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            var k = writerRandom.Next(InitialKeys);
            await PutAsync(WriteKey(key, k), WriteValue(value, k, ++versions[k]));
        }
        stop.Cancel();
        await Task.WhenAll(readers);

        await Assert.That(failures.IsEmpty ? null : failures.First()).IsNull();

        async Task PutAsync(byte[] putKey, ReadOnlyMemory<byte> putValue)
        {
            if (mvccStorage is null)
            {
                lsm!.Put(putKey, putValue.Span);
                return;
            }
            using var tx = mvccStorage.BeginTransaction();
            tx.Put(putKey, putValue.Span);
            await tx.TryCommitAsync();
        }
    }

    private static byte[] WriteKey(byte[] key, int k)
    {
        // The 4 leading bytes sort numerically; the rest makes the key longer than the 8-byte prefix word.
        BinaryPrimitives.WriteInt32BigEndian(key, k);
        key.AsSpan(4).Fill((byte)(k * 31));
        return key;
    }

    private static ReadOnlyMemory<byte> WriteValue(byte[] value, int k, int version)
    {
        var length = 8 + (version % 7) * 8;
        BinaryPrimitives.WriteInt32BigEndian(value, k);
        BinaryPrimitives.WriteInt32BigEndian(value.AsSpan(4), version);
        value.AsSpan(8, length - 8).Fill((byte)version);
        return value.AsMemory(0, length);
    }

    private static void Check(System.Collections.Concurrent.ConcurrentQueue<string> failures, int[] lastSeen, int k, ReadOnlySpan<byte> read)
    {
        if (read.Length < 8 || BinaryPrimitives.ReadInt32BigEndian(read) != k)
        {
            failures.Enqueue($"key {k} read a value of another key or a torn value");
            return;
        }
        var version = BinaryPrimitives.ReadInt32BigEndian(read[4..]);
        if (read.Length != 8 + (version % 7) * 8 || read[8..].IndexOfAnyExcept((byte)version) >= 0)
        {
            failures.Enqueue($"key {k} version {version} read a torn value");
        }
        if (version < lastSeen[k])
        {
            failures.Enqueue($"key {k} went back from version {lastSeen[k]} to {version}");
        }
        lastSeen[k] = version;
    }

    private sealed class SeekResult
    {
        public bool Found;
        public int Key;
        public readonly byte[] Value = new byte[64];
        public int Length;

        public bool Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            Found = true;
            Key = BinaryPrimitives.ReadInt32BigEndian(key);
            Length = Math.Min(value.Length, Value.Length);
            value[..Length].CopyTo(Value);
            return false;
        }
    }
}
