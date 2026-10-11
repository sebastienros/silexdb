using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Text;

namespace Silex.Jepsen;

internal sealed class RegOp
{
    public const int Read = 0, Write = 1, Cas = 2;
    public int Kind;
    public int A; // write value, or CAS expected value
    public int B; // CAS new value
    public int Obs; // value returned by a read, or observed by a failed CAS
    public bool Applied; // CAS succeeded
    public bool Info;
    public long Call;
    public long Ret;
    public string Text = "";
}

/// <summary>
/// Single-register linearizability checker (Wing-Gong-Lowe search with configuration caching).
/// Indeterminate operations may take effect at any time after invocation, or never.
/// </summary>
internal sealed class LinearizabilityChecker
{
    private sealed class Node
    {
        public Node? Prev, Next, Match;
        public int Op;
        public bool IsCall;
    }

    private readonly List<RegOp> _ops;
    private readonly HashSet<(ulong, ulong, int)> _cache = [];
    private readonly Node _head = new();
    private readonly Node[] _calls;
    private readonly long _maxNodes;
    private long _nodes;

    public const int MaxOps = 128;
    public const int Absent = -1;

    private LinearizabilityChecker(List<RegOp> ops, long maxNodes)
    {
        _ops = ops;
        _maxNodes = maxNodes;
        _calls = new Node[ops.Count];

        var events = new List<(long Time, int Order, Node Node)>(ops.Count * 2);
        for (var i = 0; i < ops.Count; i++)
        {
            var call = new Node { Op = i, IsCall = true };
            var ret = new Node { Op = i, IsCall = false };
            call.Match = ret;
            ret.Match = call;
            _calls[i] = call;
            events.Add((ops[i].Call, 0, call));
            events.Add((ops[i].Ret, 1, ret));
        }

        events.Sort((a, b) => a.Time != b.Time ? a.Time.CompareTo(b.Time) : a.Order.CompareTo(b.Order));
        var prev = _head;
        foreach (var (_, _, node) in events)
        {
            prev.Next = node;
            node.Prev = prev;
            prev = node;
        }
    }

    /// <returns>true = linearizable, false = not linearizable, null = search budget exhausted.</returns>
    public static bool? Check(List<RegOp> ops, long maxNodes = 5_000_000)
    {
        if (ops.Count > MaxOps)
        {
            throw new ArgumentException($"At most {MaxOps} operations per register are supported.");
        }

        var checker = new LinearizabilityChecker(ops, maxNodes);
        var required = ops.Count(o => !o.Info);
        try
        {
            return checker.Search(Absent, 0, 0, required);
        }
        catch (TimeoutException)
        {
            return null;
        }
    }

    private bool Search(int state, ulong lo, ulong hi, int required)
    {
        if (required == 0)
        {
            return true;
        }

        if (++_nodes > _maxNodes)
        {
            throw new TimeoutException();
        }

        Span<int> next = stackalloc int[2];
        for (var e = _head.Next; e is { IsCall: true }; e = e.Next)
        {
            var op = _ops[e.Op];
            var count = Step(state, op, next);
            for (var i = 0; i < count; i++)
            {
                var (nlo, nhi) = e.Op < 64 ? (lo | (1UL << e.Op), hi) : (lo, hi | (1UL << (e.Op - 64)));
                var ns = next[i];
                if (!_cache.Add((nlo, nhi, ns)))
                {
                    continue;
                }

                Lift(e);
                var found = Search(ns, nlo, nhi, required - (op.Info ? 0 : 1));
                Unlift(e);
                if (found)
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static int Step(int state, RegOp op, Span<int> next)
    {
        switch (op.Kind)
        {
            case RegOp.Read:
                if (state != op.Obs)
                {
                    return 0;
                }

                next[0] = state;
                return 1;
            case RegOp.Write:
                next[0] = op.A;
                if (op.Info && state != op.A)
                {
                    next[1] = state;
                    return 2;
                }

                return 1;
            default:
                if (op.Info)
                {
                    if (state == op.A)
                    {
                        next[0] = op.B;
                        next[1] = state;
                        return op.B == state ? 1 : 2;
                    }

                    next[0] = state;
                    return 1;
                }

                if (op.Applied)
                {
                    if (state != op.A)
                    {
                        return 0;
                    }

                    next[0] = op.B;
                    return 1;
                }

                if (state != op.Obs)
                {
                    return 0;
                }

                next[0] = state;
                return 1;
        }
    }

    private static void Lift(Node call)
    {
        call.Prev!.Next = call.Next;
        if (call.Next is not null)
        {
            call.Next.Prev = call.Prev;
        }

        var ret = call.Match!;
        ret.Prev!.Next = ret.Next;
        if (ret.Next is not null)
        {
            ret.Next.Prev = ret.Prev;
        }
    }

    private static void Unlift(Node call)
    {
        var ret = call.Match!;
        ret.Prev!.Next = ret;
        if (ret.Next is not null)
        {
            ret.Next.Prev = ret;
        }

        call.Prev!.Next = call;
        if (call.Next is not null)
        {
            call.Next.Prev = call;
        }
    }
}

/// <summary>
/// Concurrent read / write / compare-and-set on independent registers. Each register only receives a bounded
/// number of operations so the linearizability search stays tractable. Verifies linearizability of
/// single-key <see cref="MvccStorage"/> reads, <see cref="MvccStorage.Put"/> and transactional CAS.
/// </summary>
internal static class RegisterWorkload
{
    private const int OpsPerKey = 90;

    private sealed record CasIn(int Key, int Expected, int New);

    private sealed record CasOut(bool Applied, int Observed);

    public static async Task<WorkloadResult> RunAsync(RunContext ctx)
    {
        var anomalies = new List<string>();
        var history = new History(ctx.Clients);
        var errors = new ConcurrentQueue<string>();
        var options = Profiles.Create(ctx.Profile);
        await using var db = await MvccStorage.OpenAsync(ctx.DatabasePath("register"), options);
        long opCounter = -1;
        var valueCounter = 0;

        await using (var nemesis = new Nemesis(db, ctx.Seed, Profiles.HasWal(ctx.Profile)))
        {
            await Harness.RunClientsAsync(ctx.Clients, ctx.Duration, async (client, running) =>
            {
                var rng = new Random(unchecked((int)ctx.Seed * 7919 + client));
                var buffer = new byte[16];
                var lastSeen = new Dictionary<int, int>();
                while (running())
                {
                    var key = (int)(Interlocked.Increment(ref opCounter) / OpsPerKey);
                    var keyBytes = Harness.KeyBytes(key);
                    var roll = rng.Next(100);
                    try
                    {
                        if (roll < 45)
                        {
                            var op = history.Begin(client, "read", key);
                            var length = await db.GetRawAsync(keyBytes, buffer);
                            var value = length < 0 ? LinearizabilityChecker.Absent
                                : BinaryPrimitives.ReadInt32LittleEndian(buffer);
                            History.End(op, OpStatus.Ok, value);
                            lastSeen[key] = value;
                        }
                        else if (roll < 80)
                        {
                            var value = Interlocked.Increment(ref valueCounter);
                            var op = history.Begin(client, "write", (key, value));
                            try
                            {
                                var valueBytes = new byte[sizeof(int)];
                                BinaryPrimitives.WriteInt32LittleEndian(valueBytes, value);
                                db.Put(keyBytes, valueBytes);
                                History.End(op, OpStatus.Ok);
                            }
                            catch (Exception e)
                            {
                                History.End(op, OpStatus.Info);
                                errors.Enqueue($"write failed: {Harness.Describe(e)}");
                            }
                        }
                        else
                        {
                            var expected = lastSeen.TryGetValue(key, out var seen) && seen != -1
                                ? seen
                                : rng.Next(1, Math.Max(2, Volatile.Read(ref valueCounter) + 1));
                            var next = Interlocked.Increment(ref valueCounter);
                            var op = history.Begin(client, "cas", new CasIn(key, expected, next));
                            try
                            {
                                using var txn = db.BeginTransaction();
                                var length = await txn.GetForUpdateRawAsync(keyBytes, buffer);
                                var current = length < 0 ? LinearizabilityChecker.Absent
                                    : BinaryPrimitives.ReadInt32LittleEndian(buffer);
                                if (current != expected)
                                {
                                    History.End(op, OpStatus.Ok, new CasOut(false, current));
                                    lastSeen[key] = current;
                                    continue;
                                }

                                var valueBytes = new byte[sizeof(int)];
                                BinaryPrimitives.WriteInt32LittleEndian(valueBytes, next);
                                txn.Put(keyBytes, valueBytes);
                                if (await txn.TryCommitAsync())
                                {
                                    History.End(op, OpStatus.Ok, new CasOut(true, expected));
                                    lastSeen[key] = next;
                                }
                                else
                                {
                                    History.End(op, OpStatus.Fail);
                                }
                            }
                            catch (Exception e)
                            {
                                History.End(op, OpStatus.Info);
                                errors.Enqueue($"cas failed: {Harness.Describe(e)}");
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        errors.Enqueue($"{Harness.Describe(e)}");
                    }
                }
            });

            foreach (var error in nemesis.Errors)
            {
                anomalies.Add(error);
            }
        }

        foreach (var error in errors.Distinct().Take(10))
        {
            anomalies.Add($"unexpected exception: {error}");
        }

        var all = history.All();
        var inconclusive = 0;
        var checkedKeys = 0;
        foreach (var group in all.Where(o => o.Status != OpStatus.Fail).GroupBy(KeyOf))
        {
            var ops = group.Select(ToRegOp).ToList();
            if (ops.Count > LinearizabilityChecker.MaxOps)
            {
                // A client stalled across many key rotations; skip rather than mis-check.
                inconclusive++;
                continue;
            }

            checkedKeys++;
            var verdict = LinearizabilityChecker.Check(ops);
            if (verdict is null)
            {
                inconclusive++;
            }
            else if (!verdict.Value)
            {
                var sb = new StringBuilder($"register {group.Key} is not linearizable:\n");
                foreach (var op in ops.OrderBy(o => o.Call))
                {
                    sb.Append("    ").Append(op.Text).Append('\n');
                }

                anomalies.Add(sb.ToString());
            }
        }

        if (anomalies.Count > 0)
        {
            ctx.DumpHistory("register", all);
        }

        var summary = $"{history.Summary()} registers={checkedKeys} inconclusive={inconclusive}";
        return new WorkloadResult("register", ctx.Profile, ctx.Seed, anomalies.Count == 0, false, summary, anomalies);
    }

    private static int KeyOf(Op op) => op.Input switch
    {
        int key => key,
        ValueTuple<int, int> write => write.Item1,
        CasIn cas => cas.Key,
        _ => throw new InvalidOperationException(),
    };

    private static RegOp ToRegOp(Op op)
    {
        var info = op.Status == OpStatus.Info;
        var reg = new RegOp { Info = info, Call = op.Invoke, Ret = info ? long.MaxValue : op.Complete };
        switch (op.F)
        {
            case "read":
                reg.Kind = RegOp.Read;
                reg.Obs = (int)op.Output!;
                reg.Text = $"c{op.Client} read -> {reg.Obs}  [{op.Invoke}..{op.Complete}]";
                break;
            case "write":
                reg.Kind = RegOp.Write;
                reg.A = ((ValueTuple<int, int>)op.Input!).Item2;
                reg.Text = $"c{op.Client} write {reg.A} {(info ? "(info)" : "")} [{op.Invoke}..{(info ? "inf" : op.Complete.ToString())}]";
                break;
            default:
                var input = (CasIn)op.Input!;
                reg.Kind = RegOp.Cas;
                reg.A = input.Expected;
                reg.B = input.New;
                if (!info)
                {
                    var output = (CasOut)op.Output!;
                    reg.Applied = output.Applied;
                    reg.Obs = output.Observed;
                }

                reg.Text = $"c{op.Client} cas {input.Expected}->{input.New} {(info ? "(info)" : reg.Applied ? "ok" : $"failed, saw {reg.Obs}")} [{op.Invoke}..{(info ? "inf" : op.Complete.ToString())}]";
                break;
        }

        return reg;
    }
}
