using System.Buffers.Binary;
using System.Collections.Concurrent;

namespace Silex.Jepsen;

/// <summary>
/// Writers atomically replace (or delete) a group of keys, bumping a generation counter in the same
/// transaction. Readers take snapshots and scan the group forward and in reverse using small batches while
/// flush, compaction and GC run. A scan must see either every key at one generation or no keys at all, forward
/// and reverse scans must agree with each other and with point reads, and snapshots must never go back in time.
/// After the run every observed generation is matched against the committed writer log.
/// </summary>
internal static class ScanWorkload
{
    private const int GroupSize = 48;
    private const int GenerationKey = 0;
    private const int FirstDataKey = 1000;

    private sealed record Observation(int Client, long Generation, long Data, bool Complete);

    public static async Task<WorkloadResult> RunAsync(RunContext ctx)
    {
        var anomalies = new ConcurrentQueue<string>();
        var history = new History(ctx.Clients);
        var observations = new ConcurrentQueue<Observation>();
        // generation -> true when the writer put all keys, false when it deleted them.
        var commits = new ConcurrentDictionary<long, bool>();
        var unknown = new ConcurrentDictionary<long, bool>();
        await using var db = await MvccStorage.OpenAsync(ctx.DatabasePath("scan"), Profiles.Create(ctx.Profile));

        await using (var nemesis = new Nemesis(db, ctx.Seed, Profiles.HasWal(ctx.Profile)))
        {
            await Harness.RunClientsAsync(ctx.Clients, ctx.Duration, async (client, running) =>
            {
                var rng = new Random(unchecked((int)ctx.Seed * 32749 + client));
                var buffer = new byte[16];
                long lastGeneration = -1;
                long lastSequence = -1;
                var isWriter = client % 2 == 0;
                while (running())
                {
                    try
                    {
                        if (isWriter)
                        {
                            await WriteAsync(db, history, commits, unknown, anomalies, client, rng, buffer);
                        }
                        else
                        {
                            using var snapshot = db.CreateSnapshot();
                            var op = history.Begin(client, "scan", null);
                            var observation = await ScanAsync(snapshot, client, anomalies, buffer);
                            History.End(op, OpStatus.Ok, observation);
                            if (snapshot.Sequence < lastSequence || observation.Generation < lastGeneration)
                            {
                                anomalies.Enqueue($"snapshot went back in time: sequence {lastSequence}->{snapshot.Sequence}, generation {lastGeneration}->{observation.Generation}");
                            }

                            lastSequence = snapshot.Sequence;
                            lastGeneration = observation.Generation;
                            observations.Enqueue(observation);
                        }
                    }
                    catch (Exception e)
                    {
                        anomalies.Enqueue($"client threw {Harness.Describe(e)}");
                    }
                }
            });

            foreach (var error in nemesis.Errors)
            {
                anomalies.Enqueue(error);
            }
        }

        foreach (var observation in observations)
        {
            if (commits.TryGetValue(observation.Generation, out var put))
            {
                var expectedData = put ? observation.Generation : -1;
                var matches = put ? observation.Complete && observation.Data == expectedData : observation.Data == -1;
                if (!matches)
                {
                    anomalies.Enqueue($"generation {observation.Generation} ({(put ? "put" : "delete")}) observed as data={observation.Data} complete={observation.Complete} by c{observation.Client}");
                }
            }
            else if (observation.Generation != 0 && !unknown.ContainsKey(observation.Generation))
            {
                anomalies.Enqueue($"observed generation {observation.Generation} that no transaction committed");
            }
        }

        var list = anomalies.Distinct().Take(20).ToList();
        if (list.Count > 0)
        {
            ctx.DumpHistory("scan", history.All());
        }

        return new WorkloadResult("scan", ctx.Profile, ctx.Seed, list.Count == 0, false,
            $"{history.Summary()} observations={observations.Count} generations={commits.Count}", list);
    }

    private static async Task WriteAsync(MvccStorage db, History history, ConcurrentDictionary<long, bool> commits,
        ConcurrentDictionary<long, bool> unknown, ConcurrentQueue<string> anomalies, int client, Random rng, byte[] buffer)
    {
        var put = rng.Next(100) >= 20;
        var op = history.Begin(client, put ? "put-all" : "delete-all", null);
        long generation = -1;
        try
        {
            using var txn = db.BeginTransaction();
            var length = await txn.GetForUpdateRawAsync(Harness.KeyBytes(GenerationKey), buffer);
            generation = (length < 0 ? 0 : BinaryPrimitives.ReadInt64LittleEndian(buffer)) + 1;
            var value = new byte[sizeof(long)];
            BinaryPrimitives.WriteInt64LittleEndian(value, generation);
            txn.Put(Harness.KeyBytes(GenerationKey), value);
            for (var i = 0; i < GroupSize; i++)
            {
                if (put)
                {
                    txn.Put(Harness.KeyBytes(FirstDataKey + i), value);
                }
                else
                {
                    txn.Delete(Harness.KeyBytes(FirstDataKey + i));
                }
            }

            unknown[generation] = true;
            if (await txn.TryCommitAsync())
            {
                commits[generation] = put;
                History.End(op, OpStatus.Ok, generation);
            }
            else
            {
                unknown.TryRemove(generation, out _);
                History.End(op, OpStatus.Fail);
            }
        }
        catch (Exception e)
        {
            History.End(op, OpStatus.Info);
            anomalies.Enqueue($"writer threw {Harness.Describe(e)}");
        }
    }

    private static async Task<Observation> ScanAsync(MvccSnapshot snapshot, int client,
        ConcurrentQueue<string> anomalies, byte[] buffer)
    {
        var length = await snapshot.GetRawAsync(Harness.KeyBytes(GenerationKey), buffer);
        var generation = length < 0 ? 0 : BinaryPrimitives.ReadInt64LittleEndian(buffer);

        var range = new OrderedReadRange(Harness.KeyBytes(FirstDataKey), true, Harness.KeyBytes(FirstDataKey + GroupSize - 1), true);
        var forward = await ReadAllAsync(snapshot, range, OrderedReadDirection.Forward);
        var reverse = await ReadAllAsync(snapshot, range, OrderedReadDirection.Reverse);

        reverse.Reverse();
        if (!forward.SequenceEqual(reverse))
        {
            anomalies.Enqueue($"forward and reverse scans disagree at generation {generation}: {forward.Count} vs {reverse.Count} entries");
        }

        for (var i = 1; i < forward.Count; i++)
        {
            if (forward[i].Key <= forward[i - 1].Key)
            {
                anomalies.Enqueue($"scan keys are not strictly ascending: {forward[i - 1].Key}, {forward[i].Key}");
            }
        }

        long data = -1;
        var complete = forward.Count == GroupSize;
        if (forward.Count > 0)
        {
            data = forward[0].Value;
            if (forward.Any(e => e.Value != data) || forward.Count != GroupSize)
            {
                anomalies.Enqueue($"torn scan at generation {generation}: {forward.Count}/{GroupSize} entries, values {string.Join(",", forward.Select(e => e.Value).Distinct())}");
            }

            if (data > generation)
            {
                anomalies.Enqueue($"scan saw data generation {data} newer than the snapshot's generation key {generation}");
            }
        }

        // Point reads must agree with the scan.
        for (var i = 0; i < GroupSize; i += 7)
        {
            var key = FirstDataKey + i;
            var l = await snapshot.GetRawAsync(Harness.KeyBytes(key), buffer);
            var point = l < 0 ? -1 : BinaryPrimitives.ReadInt64LittleEndian(buffer);
            var scanned = forward.FirstOrDefault(e => e.Key == key, (-1, -1)).Value;
            if (point != scanned)
            {
                anomalies.Enqueue($"point read of {key} = {point} disagrees with scan = {scanned} at generation {generation}");
            }
        }

        return new Observation(client, generation, data, complete);
    }

    private static async Task<List<(int Key, long Value)>> ReadAllAsync(
        MvccSnapshot snapshot, OrderedReadRange range, OrderedReadDirection direction)
    {
        var entries = new List<(int, long)>();
        using var cursor = snapshot.CreateOrderedReadCursor(range, direction);
        while (true)
        {
            using var batch = await cursor.ReadBatchAsync(5, 4096);
            for (var i = 0; i < batch.Count; i++)
            {
                var entry = batch[i];
                entries.Add((Harness.DecodeKey(entry.Key.Span), BinaryPrimitives.ReadInt64LittleEndian(entry.Value.Span)));
            }

            if (batch.IsExhausted)
            {
                return entries;
            }
        }
    }
}
