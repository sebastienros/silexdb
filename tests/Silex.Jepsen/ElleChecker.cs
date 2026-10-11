using System.Text;

namespace Silex.Jepsen;

internal enum IsolationLevel
{
    // Snapshot isolation where transactions also observe everything committed before they began.
    StrongSnapshotIsolation,
    StrictSerializable,
}

[Flags]
internal enum Edge
{
    None = 0,
    Ww = 1, // write-write: next version of a key
    Wr = 2, // write-read: the writer of the version that was read
    Rw = 4, // read-write (anti-dependency): the writer of the next version after the one read
    Rt = 8, // real time: the first transaction completed before the second began
}

internal sealed record ElleResult(List<string> Anomalies, int Nodes, int Edges);

/// <summary>
/// Checks list-append histories in the style of Elle. Final list contents give each key's version order, so
/// dependencies are inferred exactly. Detects aborted and intermediate reads (G1a, G1b), lost writes,
/// incompatible orders, and dependency cycles classified as G0, G1c, G-single (forbidden by snapshot isolation)
/// and G2 (forbidden by serializability). Cycles with several non-adjacent anti-dependencies, which snapshot
/// isolation also forbids, are only caught when the isolation level is serializable.
/// </summary>
internal static class ElleChecker
{
    private sealed class Txn
    {
        public int Id;
        public Op Op = null!;
        public Mop[] Mops = null!;
        public bool Info;
    }

    public static ElleResult Check(List<Op> history, Dictionary<int, int[]> finalLists, IsolationLevel level, bool realtime)
    {
        var anomalies = new List<string>();
        var txns = new List<Txn>();
        var failedAppends = new HashSet<(int, int)>();
        foreach (var op in history)
        {
            var mops = (Mop[])op.Input!;
            if (op.Status == OpStatus.Fail)
            {
                foreach (var mop in mops.Where(m => m.F == 'a'))
                {
                    failedAppends.Add((mop.Key, mop.Value));
                }

                continue;
            }

            txns.Add(new Txn { Id = txns.Count, Op = op, Mops = mops, Info = op.Status == OpStatus.Info });
        }

        var writer = new Dictionary<(int, int), int>();
        var lastAppend = new Dictionary<(int, int), int>();
        foreach (var txn in txns)
        {
            foreach (var mop in txn.Mops.Where(m => m.F == 'a'))
            {
                writer[(mop.Key, mop.Value)] = txn.Id;
                lastAppend[(txn.Id, mop.Key)] = mop.Value;
            }
        }

        var adj = new List<(int To, Edge Kind)>[txns.Count];
        for (var i = 0; i < adj.Length; i++)
        {
            adj[i] = [];
        }

        void AddEdge(int from, int to, Edge kind)
        {
            if (from != to)
            {
                adj[from].Add((to, kind));
            }
        }

        // Version orders come from the final contents of every key.
        var appended = new HashSet<(int, int)>();
        foreach (var (key, list) in finalLists)
        {
            var seen = new HashSet<int>();
            for (var i = 0; i < list.Length; i++)
            {
                var value = list[i];
                if (!seen.Add(value))
                {
                    anomalies.Add($"duplicate element {value} in final list of key {key}: {Harness.Format(list)}");
                }

                appended.Add((key, value));
                if (failedAppends.Contains((key, value)))
                {
                    anomalies.Add($"G1a: aborted append {value} to key {key} is present in the final list");
                }
                else if (!writer.ContainsKey((key, value)))
                {
                    anomalies.Add($"garbage element {value} in final list of key {key}");
                }
                else if (i > 0 && writer.TryGetValue((key, list[i - 1]), out var prev))
                {
                    AddEdge(prev, writer[(key, value)], Edge.Ww);
                }
            }
        }

        foreach (var txn in txns.Where(t => !t.Info))
        {
            foreach (var mop in txn.Mops.Where(m => m.F == 'a' && !appended.Contains((m.Key, m.Value))))
            {
                anomalies.Add($"lost write: acknowledged append {mop.Value} to key {mop.Key} is missing from the final list {Harness.Format(finalLists.GetValueOrDefault(mop.Key) ?? [])} ({Describe(txn)})");
            }
        }

        foreach (var txn in txns.Where(t => !t.Info))
        {
            var touched = new HashSet<int>();
            foreach (var mop in txn.Mops)
            {
                if (mop.F == 'a' || !touched.Add(mop.Key) || mop.Result is not { } read)
                {
                    touched.Add(mop.Key);
                    continue;
                }

                var final = finalLists.GetValueOrDefault(mop.Key) ?? [];
                if (read.Length > final.Length || !final.AsSpan(0, read.Length).SequenceEqual(read))
                {
                    anomalies.Add($"incompatible order: {Describe(txn)} read key {mop.Key} = {Harness.Format(read)}, which is not a prefix of the final {Harness.Format(final)}");
                    continue;
                }

                foreach (var element in read.Where(e => failedAppends.Contains((mop.Key, e))))
                {
                    anomalies.Add($"G1a: {Describe(txn)} read aborted append {element} on key {mop.Key}");
                }

                if (read.Length > 0 && writer.TryGetValue((mop.Key, read[^1]), out var w))
                {
                    if (w != txn.Id && lastAppend[(w, mop.Key)] != read[^1])
                    {
                        anomalies.Add($"G1b: {Describe(txn)} read an intermediate state of key {mop.Key}: {Harness.Format(read)} (writer {Describe(txns[w])})");
                    }

                    AddEdge(w, txn.Id, Edge.Wr);
                }

                if (read.Length < final.Length && writer.TryGetValue((mop.Key, final[read.Length]), out var next))
                {
                    AddEdge(txn.Id, next, Edge.Rw);
                }
            }
        }

        if (realtime)
        {
            AddRealtimeEdges(txns, adj);
        }

        FindCycles(txns, adj, level, anomalies);
        return new ElleResult(anomalies, txns.Count, adj.Sum(a => a.Count));
    }

    private static void AddRealtimeEdges(List<Txn> txns, List<(int To, Edge Kind)>[] adj)
    {
        var events = new List<(long Time, int Order, int Node)>(txns.Count * 2);
        foreach (var txn in txns)
        {
            events.Add((txn.Op.Invoke, 0, txn.Id));
            if (!txn.Info)
            {
                events.Add((txn.Op.Complete, 1, txn.Id));
            }
        }

        events.Sort((a, b) => a.Time != b.Time ? a.Time.CompareTo(b.Time) : a.Order.CompareTo(b.Order));
        var frontier = new HashSet<int>();
        var predecessors = new int[txns.Count][];
        foreach (var (_, order, node) in events)
        {
            if (order == 0)
            {
                predecessors[node] = [.. frontier];
                foreach (var p in predecessors[node])
                {
                    adj[p].Add((node, Edge.Rt));
                }
            }
            else
            {
                foreach (var p in predecessors[node])
                {
                    frontier.Remove(p);
                }

                frontier.Add(node);
            }
        }
    }

    private static void FindCycles(List<Txn> txns, List<(int To, Edge Kind)>[] adj, IsolationLevel level, List<string> anomalies)
    {
        const Edge All = Edge.Ww | Edge.Wr | Edge.Rw | Edge.Rt;
        var comp = ComponentIds(adj, All);
        if (comp.All(c => c < 0))
        {
            return;
        }

        var reported = false;

        // G0: a cycle made only of write-write edges.
        var ww = ComponentIds(adj, Edge.Ww);
        if (FindInComponent(adj, ww, Edge.Ww, requireKind: Edge.None) is { } g0)
        {
            anomalies.Add("G0 (write cycle): " + Format(txns, g0));
            reported = true;
        }

        // G1c: a cycle of ww/wr edges containing at least one wr edge.
        var wwWr = ComponentIds(adj, Edge.Ww | Edge.Wr);
        if (FindInComponent(adj, wwWr, Edge.Ww | Edge.Wr, requireKind: Edge.Wr) is { } g1c)
        {
            anomalies.Add("G1c (circular information flow): " + Format(txns, g1c));
            reported = true;
        }

        // G-single: exactly one anti-dependency closing a path of ordinary dependencies.
        const Edge Base = Edge.Ww | Edge.Wr | Edge.Rt;
        var gSingle = 0;
        for (var u = 0; u < adj.Length && gSingle < 3; u++)
        {
            foreach (var (v, kind) in adj[u])
            {
                if (kind != Edge.Rw || comp[u] != comp[v] || comp[u] < 0)
                {
                    continue;
                }

                if (FindPath(adj, v, u, Base, comp) is { } path)
                {
                    path.Insert(0, (u, Edge.Rw));
                    anomalies.Add("G-single (read skew / lost update): " + Format(txns, path));
                    reported = true;
                    gSingle++;
                    break;
                }
            }
        }

        if (level == IsolationLevel.StrictSerializable && !reported &&
            FindInComponent(adj, comp, All, requireKind: Edge.None) is { } g2)
        {
            anomalies.Add("G2 (anti-dependency cycle, not serializable): " + Format(txns, g2));
        }
    }

    private static List<(int Node, Edge Kind)>? FindInComponent(
        List<(int To, Edge Kind)>[] adj, int[] comp, Edge mask, Edge requireKind)
    {
        for (var u = 0; u < adj.Length; u++)
        {
            if (comp[u] < 0)
            {
                continue;
            }

            foreach (var (v, kind) in adj[u])
            {
                if ((kind & mask) == 0 || comp[v] != comp[u] || (requireKind != Edge.None && (kind & requireKind) == 0))
                {
                    continue;
                }

                if (FindPath(adj, v, u, mask, comp) is { } path)
                {
                    path.Insert(0, (u, kind));
                    return path;
                }
            }
        }

        return null;
    }

    // Shortest path from `from` to `to` along edges matching `mask`, staying inside one component.
    // Returns (node, kind-of-edge-leaving-node) pairs; the last edge returns to the first node.
    private static List<(int Node, Edge Kind)>? FindPath(
        List<(int To, Edge Kind)>[] adj, int from, int to, Edge mask, int[] comp)
    {
        var parent = new Dictionary<int, (int Prev, Edge Kind)> { [from] = (-1, Edge.None) };
        var queue = new Queue<int>();
        queue.Enqueue(from);
        while (queue.Count > 0)
        {
            var node = queue.Dequeue();
            foreach (var (next, kind) in adj[node])
            {
                if ((kind & mask) == 0 || comp[next] != comp[from] || parent.ContainsKey(next))
                {
                    continue;
                }

                parent[next] = (node, kind);
                if (next == to)
                {
                    var path = new List<(int, Edge)>();
                    var cursor = next;
                    while (cursor != from)
                    {
                        var (prev, edgeKind) = parent[cursor];
                        path.Add((prev, edgeKind));
                        cursor = prev;
                    }

                    path.Reverse();
                    return path;
                }

                queue.Enqueue(next);
            }
        }

        return null;
    }

    private static string Format(List<Txn> txns, List<(int Node, Edge Kind)> cycle)
    {
        var sb = new StringBuilder();
        foreach (var (node, kind) in cycle)
        {
            sb.Append("T").Append(node).Append(" -").Append(kind.ToString().ToLowerInvariant()).Append("-> ");
        }

        sb.Append('T').Append(cycle[0].Node).Append('\n');
        foreach (var node in cycle.Select(c => c.Node).Distinct())
        {
            sb.Append("      T").Append(node).Append(": ").Append(Describe(txns[node])).Append('\n');
        }

        return sb.ToString();
    }

    private static string Describe(Txn txn)
    {
        var sb = new StringBuilder("c").Append(txn.Op.Client).Append(txn.Info ? " (info) [" : " [");
        foreach (var mop in txn.Mops)
        {
            if (mop.F == 'a')
            {
                sb.Append($"append({mop.Key},{mop.Value}) ");
            }
            else
            {
                sb.Append($"r({mop.Key})={(mop.Result is null ? "?" : Harness.Format(mop.Result))} ");
            }
        }

        return sb.Append(']').ToString();
    }

    // Component id per node; -1 for nodes that are not part of a cycle (singleton components).
    private static int[] ComponentIds(List<(int To, Edge Kind)>[] adj, Edge mask)
    {
        var ids = new int[adj.Length];
        TarjanScc(adj, mask, ids);
        return ids;
    }

    private static void TarjanScc(List<(int To, Edge Kind)>[] adj, Edge mask, int[] result)
    {
        var n = adj.Length;
        var index = new int[n];
        var low = new int[n];
        var onStack = new bool[n];
        var stack = new Stack<int>();
        var work = new Stack<(int Node, int Next)>();
        Array.Fill(index, -1);
        Array.Fill(result, -1);
        var counter = 0;
        var nextComponent = 0;

        for (var root = 0; root < n; root++)
        {
            if (index[root] != -1)
            {
                continue;
            }

            work.Push((root, 0));
            while (work.Count > 0)
            {
                var (node, edgeIndex) = work.Pop();
                if (edgeIndex == 0)
                {
                    index[node] = low[node] = counter++;
                    stack.Push(node);
                    onStack[node] = true;
                }

                var descended = false;
                for (var i = edgeIndex; i < adj[node].Count; i++)
                {
                    var (to, kind) = adj[node][i];
                    if ((kind & mask) == 0)
                    {
                        continue;
                    }

                    if (index[to] == -1)
                    {
                        work.Push((node, i + 1));
                        work.Push((to, 0));
                        descended = true;
                        break;
                    }

                    if (onStack[to])
                    {
                        low[node] = Math.Min(low[node], index[to]);
                    }
                }

                if (descended)
                {
                    continue;
                }

                if (low[node] == index[node])
                {
                    var members = new List<int>();
                    int member;
                    do
                    {
                        member = stack.Pop();
                        onStack[member] = false;
                        members.Add(member);
                    }
                    while (member != node);

                    if (members.Count > 1)
                    {
                        foreach (var m in members)
                        {
                            result[m] = nextComponent;
                        }

                        nextComponent++;
                    }
                }

                if (work.Count > 0)
                {
                    var parent = work.Peek().Node;
                    low[parent] = Math.Min(low[parent], low[node]);
                }
            }
        }
    }
}
