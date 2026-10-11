using System.Buffers.Binary;
using System.Collections.Concurrent;

namespace Silex.Jepsen;

internal sealed class Mop
{
    public char F; // 'r' read or 'a' append
    public int Key;
    public int Value; // appended element
    public int[]? Result; // read result; null when unknown
}

/// <summary>
/// Elle-style list-append workload. Transactions read whole lists and append unique elements. Because every
/// read exposes the full version order of a key, a checker can infer write-write, write-read and read-write
/// dependencies without guessing and look for isolation anomalies (see <see cref="ElleChecker"/>).
/// </summary>
internal static class AppendWorkload
{
    private const int TxnsPerWindowStep = 24;
    private const int ActiveKeys = 4;

    public static async Task<WorkloadResult> RunAsync(RunContext ctx, bool serializable)
    {
        var name = serializable ? "append-ser" : "append-si";
        var anomalies = new ConcurrentQueue<string>();
        var history = new History(ctx.Clients);
        var options = Profiles.Create(ctx.Profile);
        await using var db = await MvccStorage.OpenAsync(ctx.DatabasePath(name), options);
        long txnCounter = -1;
        var valueCounter = 0;
        var maxKey = 0;

        await using (var nemesis = new Nemesis(db, ctx.Seed, Profiles.HasWal(ctx.Profile)))
        {
            await Harness.RunClientsAsync(ctx.Clients, ctx.Duration, async (client, running) =>
            {
                var rng = new Random(unchecked((int)ctx.Seed * 15485863 + client));
                var buffer = new byte[256 * 1024];
                while (running())
                {
                    var windowBase = (int)(Interlocked.Increment(ref txnCounter) / TxnsPerWindowStep);
                    var length = rng.Next(1, 5);
                    var mops = new Mop[length];
                    for (var i = 0; i < length; i++)
                    {
                        var key = windowBase + rng.Next(ActiveKeys);
                        if (rng.Next(2) == 0)
                        {
                            mops[i] = new Mop { F = 'r', Key = key };
                        }
                        else
                        {
                            mops[i] = new Mop { F = 'a', Key = key, Value = Interlocked.Increment(ref valueCounter) };
                        }

                        if (key > Volatile.Read(ref maxKey))
                        {
                            Volatile.Write(ref maxKey, key);
                        }
                    }

                    var op = history.Begin(client, "txn", mops);
                    try
                    {
                        using var txn = db.BeginTransaction();
                        var views = new Dictionary<int, List<int>>();
                        var committedReads = new int[length][];
                        for (var i = 0; i < length; i++)
                        {
                            var mop = mops[i];
                            var observed = await ReadListAsync(txn, mop.Key, serializable, buffer);
                            if (views.TryGetValue(mop.Key, out var view))
                            {
                                if (!observed.AsSpan().SequenceEqual(view.ToArray()))
                                {
                                    anomalies.Enqueue($"internal inconsistency: key {mop.Key} read {Harness.Format(observed)} within a transaction expecting {Harness.Format(view)}");
                                }
                            }
                            else
                            {
                                view = [.. observed];
                                views[mop.Key] = view;
                            }

                            if (mop.F == 'r')
                            {
                                committedReads[i] = observed;
                            }
                            else
                            {
                                view.Add(mop.Value);
                                txn.Put(Harness.KeyBytes(mop.Key), Encode(view));
                            }
                        }

                        if (await txn.TryCommitAsync())
                        {
                            for (var i = 0; i < length; i++)
                            {
                                mops[i].Result = committedReads[i];
                            }

                            History.End(op, OpStatus.Ok, mops);
                        }
                        else
                        {
                            History.End(op, OpStatus.Fail);
                        }
                    }
                    catch (Exception e)
                    {
                        History.End(op, OpStatus.Info);
                        anomalies.Enqueue($"txn threw {Harness.Describe(e)}");
                    }
                }
            });

            foreach (var error in nemesis.Errors)
            {
                anomalies.Enqueue(error);
            }
        }

        // A final read of every key supplies the authoritative version order.
        var finalReads = new Dictionary<int, int[]>();
        var buf = new byte[256 * 1024];
        using (var snapshot = db.CreateSnapshot())
        {
            for (var key = 0; key <= Volatile.Read(ref maxKey); key++)
            {
                var length = await snapshot.GetRawAsync(Harness.KeyBytes(key), buf);
                finalReads[key] = length < 0 ? [] : Decode(buf.AsSpan(0, length));
            }
        }

        var all = history.All();
        var level = serializable ? IsolationLevel.StrictSerializable : IsolationLevel.StrongSnapshotIsolation;
        var checkResult = ElleChecker.Check(all, finalReads, level, ctx.Realtime);
        foreach (var anomaly in checkResult.Anomalies)
        {
            anomalies.Enqueue(anomaly);
        }

        var list = anomalies.Distinct().Take(20).ToList();
        if (list.Count > 0)
        {
            ctx.DumpHistory(name, all);
        }

        var summary = $"{history.Summary()} nodes={checkResult.Nodes} edges={checkResult.Edges} " +
            $"level={level}{(ctx.Realtime ? "" : " (no realtime)")}";
        return new WorkloadResult(name, ctx.Profile, ctx.Seed, list.Count == 0, false, summary, list);
    }

    private static async Task<int[]> ReadListAsync(MvccTransaction txn, int key, bool tracked, byte[] buffer)
    {
        var keyBytes = Harness.KeyBytes(key);
        var length = tracked
            ? await txn.GetForUpdateRawAsync(keyBytes, buffer)
            : await txn.GetRawAsync(keyBytes, buffer);
        if (length > buffer.Length)
        {
            throw new InvalidOperationException("list exceeded the read buffer");
        }

        return length < 0 ? [] : Decode(buffer.AsSpan(0, length));
    }

    private static byte[] Encode(List<int> list)
    {
        var bytes = new byte[list.Count * sizeof(int)];
        for (var i = 0; i < list.Count; i++)
        {
            BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * sizeof(int)), list[i]);
        }

        return bytes;
    }

    private static int[] Decode(ReadOnlySpan<byte> bytes)
    {
        var list = new int[bytes.Length / sizeof(int)];
        for (var i = 0; i < list.Length; i++)
        {
            list[i] = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(i * sizeof(int)));
        }

        return list;
    }
}
