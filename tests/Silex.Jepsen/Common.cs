using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace Silex.Jepsen;

internal enum OpStatus
{
    Ok,
    // Definitely had no effect (for example an MVCC conflict).
    Fail,
    // Outcome unknown; the operation may or may not have taken effect.
    Info,
}

internal sealed class Op
{
    public int Client;
    public string F = "";
    public object? Input;
    public object? Output;
    public long Invoke;
    public long Complete = long.MaxValue;
    public OpStatus Status = OpStatus.Info;

    public override string ToString() =>
        $"c{Client} {F} {Status} in={Input} out={Output} [{Invoke}..{(Complete == long.MaxValue ? "inf" : Complete.ToString())}]";
}

/// <summary>Thread-safe operation history; each client appends to its own list.</summary>
internal sealed class History
{
    private readonly List<Op>[] _perClient;

    public History(int clients)
    {
        _perClient = new List<Op>[clients];
        for (var i = 0; i < clients; i++)
        {
            _perClient[i] = [];
        }
    }

    public Op Begin(int client, string f, object? input)
    {
        var op = new Op { Client = client, F = f, Input = input, Invoke = Stopwatch.GetTimestamp() };
        _perClient[client].Add(op);
        return op;
    }

    public static void End(Op op, OpStatus status, object? output = null)
    {
        op.Output = output;
        op.Status = status;
        op.Complete = status == OpStatus.Info ? long.MaxValue : Stopwatch.GetTimestamp();
    }

    public List<Op> All()
    {
        var all = new List<Op>(_perClient.Sum(l => l.Count));
        foreach (var list in _perClient)
        {
            all.AddRange(list);
        }

        all.Sort((a, b) => a.Invoke.CompareTo(b.Invoke));
        return all;
    }

    public string Summary()
    {
        int ok = 0, fail = 0, info = 0;
        foreach (var list in _perClient)
        {
            foreach (var op in list)
            {
                switch (op.Status)
                {
                    case OpStatus.Ok: ok++; break;
                    case OpStatus.Fail: fail++; break;
                    default: info++; break;
                }
            }
        }

        return $"ok={ok} fail={fail} info={info}";
    }
}

internal sealed record WorkloadResult(
    string Workload,
    string Profile,
    long Seed,
    bool Valid,
    bool Inconclusive,
    string Summary,
    List<string> Anomalies);

internal sealed class RunContext
{
    public required string Directory { get; init; }
    public required string Profile { get; init; }
    public required long Seed { get; init; }
    public required int Clients { get; init; }
    public required TimeSpan Duration { get; init; }
    public required string OutputDirectory { get; init; }
    public int CrashCycles { get; init; } = 8;
    public bool Realtime { get; init; } = true;

    public string DatabasePath(string name)
    {
        var path = Path.Combine(Directory, name);
        System.IO.Directory.CreateDirectory(path);
        return path;
    }

    public void DumpHistory(string name, IEnumerable<Op> history)
    {
        System.IO.Directory.CreateDirectory(OutputDirectory);
        var file = Path.Combine(OutputDirectory, $"{name}-{Profile}-seed{Seed}.history.txt");
        File.WriteAllLines(file, history.Select(o => o.ToString()));
    }
}

internal static class Profiles
{
    public static readonly string[] Names = ["tiny-tiered", "tiny-leveled-group", "nowal-tiny"];

    // Tiny memtables and fast periodic flushes force constant flush/compaction while clients run.
    public static StorageOptions Create(string name, bool syncWal = false)
    {
        var options = new StorageOptions
        {
            MemTableSizeLimit = 32 * 1024,
            FlushPeriod = TimeSpan.FromMilliseconds(20),
        };

        switch (name)
        {
            case "tiny-tiered":
                options.CompactionStrategy = CompactionStrategy.Tiered;
                options.SyncWriteAheadLogToDisk = syncWal;
                break;
            case "tiny-leveled-group":
                options.CompactionStrategy = CompactionStrategy.Leveled;
                options.BaseLevelTargetBytes = 64 * 1024;
                options.Level0CompactionThreshold = 2;
                options.SyncWriteAheadLogToDisk = true;
                options.MvccCommitGroupSize = 4;
                break;
            case "nowal-tiny":
                options.UseWriteAheadLog = false;
                options.CompactionStrategy = CompactionStrategy.Tiered;
                break;
            default:
                throw new ArgumentException($"Unknown profile '{name}'.");
        }

        return options;
    }

    public static bool HasWal(string name) => name != "nowal-tiny";
}

/// <summary>Runs flush, compaction, GC and WAL synchronization concurrently with the workload.</summary>
internal sealed class Nemesis : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _task;

    public ConcurrentQueue<string> Errors { get; } = new();

    public Nemesis(MvccStorage db, long seed, bool synchronize = true)
    {
        _task = Task.Run(async () =>
        {
            var rng = new Random(unchecked((int)seed) ^ 0x5eed);
            while (!_cts.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(rng.Next(1, 25), _cts.Token).ConfigureAwait(false);
                    // Maintenance is never cancelled mid-flight: cancelling a flush faults the store.
                    switch (rng.Next(synchronize ? 4 : 3))
                    {
                        case 0: await db.FlushAndCompactAsync().ConfigureAwait(false); break;
                        case 1: await db.CollectGarbageAsync(compact: false).ConfigureAwait(false); break;
                        case 2: await db.CollectGarbageAsync(compact: true).ConfigureAwait(false); break;
                        default: await db.SynchronizeAsync().ConfigureAwait(false); break;
                    }
                }
                catch (OperationCanceledException) when (_cts.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception e)
                {
                    Errors.Enqueue($"nemesis: {e.GetType().Name}: {e.Message}");
                }
            }
        });
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        await _task.ConfigureAwait(false);
        _cts.Dispose();
    }
}

internal static class Harness
{
    public static async Task RunClientsAsync(int clients, TimeSpan duration, Func<int, Func<bool>, Task> body)
    {
        var deadline = Stopwatch.GetTimestamp() + (long)(duration.TotalSeconds * Stopwatch.Frequency);
        bool Running() => Stopwatch.GetTimestamp() < deadline;
        var tasks = new Task[clients];
        for (var i = 0; i < clients; i++)
        {
            var client = i;
            tasks[i] = Task.Run(() => body(client, Running));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    public static void EncodeKey(Span<byte> destination, int key) =>
        BinaryPrimitives.WriteInt32BigEndian(destination, key);

    public static byte[] KeyBytes(int key)
    {
        var bytes = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, key);
        return bytes;
    }

    public static int DecodeKey(ReadOnlySpan<byte> key) => BinaryPrimitives.ReadInt32BigEndian(key);

    public static string Format(IEnumerable<int> values) => "[" + string.Join(",", values) + "]";

    public static string Describe(Exception e) =>
        new StringBuilder().Append(e.GetType().Name).Append(": ").Append(e.Message).ToString();
}
