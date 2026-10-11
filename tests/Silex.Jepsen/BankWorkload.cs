using System.Buffers.Binary;
using System.Collections.Concurrent;

namespace Silex.Jepsen;

/// <summary>
/// Transfers between accounts under snapshot isolation. Whatever the interleaving, every consistent read must
/// observe the same total, no balance may go negative, and the total must survive a close/reopen cycle.
/// Transfers randomly use tracked reads (serializable) or plain reads (snapshot isolation, relying on
/// write-write conflict detection).
/// </summary>
internal static class BankWorkload
{
    private const int Accounts = 8;
    private const long Initial = 100;

    public static async Task<WorkloadResult> RunAsync(RunContext ctx)
    {
        var anomalies = new ConcurrentQueue<string>();
        var history = new History(ctx.Clients);
        var path = ctx.DatabasePath("bank");
        var options = Profiles.Create(ctx.Profile);
        var db = await MvccStorage.OpenAsync(path, options);
        try
        {
            using (var init = db.BeginTransaction())
            {
                for (var i = 0; i < Accounts; i++)
                {
                    init.Put(Harness.KeyBytes(i), Encode(Initial));
                }

                await init.CommitAsync();
            }

            await using (var nemesis = new Nemesis(db, ctx.Seed, Profiles.HasWal(ctx.Profile)))
            {
                await Harness.RunClientsAsync(ctx.Clients, ctx.Duration, (client, running) =>
                    ClientAsync(db, history, anomalies, ctx, client, running));
                foreach (var error in nemesis.Errors)
                {
                    anomalies.Enqueue(error);
                }
            }

            await CheckTotalAsync(db, anomalies, "final snapshot");
        }
        finally
        {
            await db.DisposeAsync();
        }

        await using (var reopened = await MvccStorage.OpenAsync(path, options))
        {
            await CheckTotalAsync(reopened, anomalies, "after reopen");
        }

        var list = anomalies.Distinct().Take(20).ToList();
        if (list.Count > 0)
        {
            ctx.DumpHistory("bank", history.All());
        }

        return new WorkloadResult("bank", ctx.Profile, ctx.Seed, list.Count == 0, false, history.Summary(), list);
    }

    private static async Task ClientAsync(MvccStorage db, History history, ConcurrentQueue<string> anomalies,
        RunContext ctx, int client, Func<bool> running)
    {
        var rng = new Random(unchecked((int)ctx.Seed * 104729 + client));
        var buffer = new byte[16];
        var buffer2 = new byte[16];
        while (running())
        {
            try
            {
                if (rng.Next(100) < 60)
                {
                    var from = rng.Next(Accounts);
                    var to = (from + 1 + rng.Next(Accounts - 1)) % Accounts;
                    var amount = rng.Next(1, 40);
                    var tracked = rng.Next(2) == 0;
                    var op = history.Begin(client, "transfer", (from, to, amount, tracked));
                    try
                    {
                        using var txn = db.BeginTransaction();
                        var fromLength = tracked
                            ? await txn.GetForUpdateRawAsync(Harness.KeyBytes(from), buffer)
                            : await txn.GetRawAsync(Harness.KeyBytes(from), buffer);
                        var toLength = tracked
                            ? await txn.GetForUpdateRawAsync(Harness.KeyBytes(to), buffer2)
                            : await txn.GetRawAsync(Harness.KeyBytes(to), buffer2);
                        if (fromLength != sizeof(long) || toLength != sizeof(long))
                        {
                            anomalies.Enqueue($"account missing in transfer {from}->{to} (lengths {fromLength}, {toLength})");
                            History.End(op, OpStatus.Fail);
                            continue;
                        }

                        var fromBalance = BinaryPrimitives.ReadInt64LittleEndian(buffer);
                        var toBalance = BinaryPrimitives.ReadInt64LittleEndian(buffer2);
                        if (fromBalance < amount)
                        {
                            History.End(op, OpStatus.Fail, "insufficient");
                            continue;
                        }

                        txn.Put(Harness.KeyBytes(from), Encode(fromBalance - amount));
                        txn.Put(Harness.KeyBytes(to), Encode(toBalance + amount));
                        History.End(op, await txn.TryCommitAsync() ? OpStatus.Ok : OpStatus.Fail);
                    }
                    catch (Exception e)
                    {
                        History.End(op, OpStatus.Info);
                        anomalies.Enqueue($"transfer threw {Harness.Describe(e)}");
                    }
                }
                else
                {
                    var op = history.Begin(client, "read-all", null);
                    var mode = rng.Next(3);
                    var balances = new long[Accounts];
                    var found = 0;
                    if (mode == 0)
                    {
                        using var snapshot = db.CreateSnapshot();
                        for (var i = 0; i < Accounts; i++)
                        {
                            var length = await snapshot.GetRawAsync(Harness.KeyBytes(i), buffer);
                            if (length == sizeof(long))
                            {
                                balances[i] = BinaryPrimitives.ReadInt64LittleEndian(buffer);
                                found++;
                            }
                        }
                    }
                    else if (mode == 1)
                    {
                        using var txn = db.BeginTransaction();
                        for (var i = 0; i < Accounts; i++)
                        {
                            var length = await txn.GetRawAsync(Harness.KeyBytes(i), buffer);
                            if (length == sizeof(long))
                            {
                                balances[i] = BinaryPrimitives.ReadInt64LittleEndian(buffer);
                                found++;
                            }
                        }
                    }
                    else
                    {
                        using var snapshot = db.CreateSnapshot();
                        using var cursor = snapshot.CreateOrderedReadCursor();
                        while (true)
                        {
                            using var batch = await cursor.ReadBatchAsync(3, 4096);
                            for (var i = 0; i < batch.Count; i++)
                            {
                                var entry = batch[i];
                                var account = Harness.DecodeKey(entry.Key.Span);
                                if (account is >= 0 and < Accounts)
                                {
                                    balances[account] = BinaryPrimitives.ReadInt64LittleEndian(entry.Value.Span);
                                    found++;
                                }
                            }

                            if (batch.IsExhausted)
                            {
                                break;
                            }
                        }
                    }

                    History.End(op, OpStatus.Ok, string.Join(",", balances));
                    Validate(balances, found, $"read mode {mode}", anomalies);
                }
            }
            catch (Exception e)
            {
                anomalies.Enqueue($"client threw {Harness.Describe(e)}");
            }
        }
    }

    private static void Validate(long[] balances, int found, string where, ConcurrentQueue<string> anomalies)
    {
        var sum = balances.Sum();
        if (found != Accounts || sum != Accounts * Initial || balances.Any(b => b < 0))
        {
            anomalies.Enqueue($"{where}: inconsistent bank state (found={found}, total={sum}, expected {Accounts * Initial}): [{string.Join(",", balances)}]");
        }
    }

    private static async Task CheckTotalAsync(MvccStorage db, ConcurrentQueue<string> anomalies, string where)
    {
        var balances = new long[Accounts];
        var found = 0;
        var buffer = new byte[16];
        using var snapshot = db.CreateSnapshot();
        for (var i = 0; i < Accounts; i++)
        {
            var length = await snapshot.GetRawAsync(Harness.KeyBytes(i), buffer);
            if (length == sizeof(long))
            {
                balances[i] = BinaryPrimitives.ReadInt64LittleEndian(buffer);
                found++;
            }
        }

        Validate(balances, found, where, anomalies);
    }

    private static byte[] Encode(long value)
    {
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return bytes;
    }
}
