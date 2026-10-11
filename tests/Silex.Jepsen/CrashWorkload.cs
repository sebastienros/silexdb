using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace Silex.Jepsen;

/// <summary>
/// Repeatedly SIGKILLs a child process that is committing multi-key transactions, reopens the database and
/// checks durability and atomicity: every acknowledged commit must be present, nothing beyond the last started
/// commit may appear, and every key written by one transaction must carry the same value. The child also runs
/// flush, compaction and GC in the background so kills land inside those operations too.
/// </summary>
internal static class CrashWorkload
{
    private const int Lanes = 4;
    private const int KeysPerLane = 16;
    private const int LaneStride = 1000;
    private const string ChildFlag = "--crash-child";

    public static bool IsChild(string[] args) => args.Length > 0 && args[0] == ChildFlag;

    public static async Task<WorkloadResult> RunAsync(RunContext ctx)
    {
        var anomalies = new List<string>();
        var path = ctx.DatabasePath("crash");
        var syncWal = Profiles.HasWal(ctx.Profile);
        var options = Profiles.Create(ctx.Profile, syncWal);
        var rng = new Random(unchecked((int)ctx.Seed));
        var recovered = new long[Lanes];
        long totalAcked = 0;
        var cycles = 0;

        for (var cycle = 0; cycle < ctx.CrashCycles; cycle++)
        {
            var lastOk = (long[])recovered.Clone();
            var lastInvoked = (long[])recovered.Clone();
            var info = new ProcessStartInfo
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            var host = Environment.ProcessPath!;
            info.FileName = host;
            if (Path.GetFileNameWithoutExtension(host).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            {
                info.ArgumentList.Add(typeof(CrashWorkload).Assembly.Location);
            }

            info.ArgumentList.Add(ChildFlag);
            info.ArgumentList.Add(path);
            info.ArgumentList.Add(ctx.Profile);
            info.ArgumentList.Add(syncWal ? "1" : "0");
            info.ArgumentList.Add((ctx.Seed + cycle).ToString());

            using var child = Process.Start(info)!;
            var stderr = new ConcurrentQueue<string>();
            child.ErrorDataReceived += (_, e) =>
            {
                if (e.Data is not null)
                {
                    stderr.Enqueue(e.Data);
                }
            };
            child.BeginErrorReadLine();

            var ready = new TaskCompletionSource();
            var reader = Task.Run(() =>
            {
                while (child.StandardOutput.ReadLine() is { } line)
                {
                    var parts = line.Split(' ');
                    if (parts[0] == "ready")
                    {
                        ready.TrySetResult();
                    }
                    else if (parts.Length == 3 && int.TryParse(parts[1], out var lane) && long.TryParse(parts[2], out var n))
                    {
                        lock (lastOk)
                        {
                            if (parts[0] == "invoke")
                            {
                                lastInvoked[lane] = Math.Max(lastInvoked[lane], n);
                            }
                            else if (parts[0] == "ok")
                            {
                                lastOk[lane] = Math.Max(lastOk[lane], n);
                            }
                        }
                    }
                }

                ready.TrySetResult();
            });

            await Task.WhenAny(ready.Task, Task.Delay(TimeSpan.FromSeconds(30)));
            if (!ready.Task.IsCompleted || child.HasExited)
            {
                anomalies.Add($"cycle {cycle}: child failed to start: {string.Join(" | ", stderr)}");
                if (!child.HasExited)
                {
                    child.Kill(entireProcessTree: true);
                }

                break;
            }

            await Task.Delay(rng.Next(150, 1500));
            if (child.HasExited)
            {
                anomalies.Add($"cycle {cycle}: child exited before it was killed (exit {child.ExitCode}): {string.Join(" | ", stderr)}");
                break;
            }

            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync();
            await reader;

            long[] ok, invoked;
            lock (lastOk)
            {
                ok = (long[])lastOk.Clone();
                invoked = (long[])lastInvoked.Clone();
            }

            try
            {
                await using var db = await MvccStorage.OpenAsync(path, options);
                var buffer = new byte[16];
                for (var lane = 0; lane < Lanes; lane++)
                {
                    var length = await db.GetRawAsync(Harness.KeyBytes(lane * LaneStride), buffer);
                    var counter = length < 0 ? 0 : BinaryPrimitives.ReadInt64LittleEndian(buffer);
                    if (counter < ok[lane])
                    {
                        anomalies.Add($"cycle {cycle} lane {lane}: lost acknowledged commit(s): recovered {counter} < last acknowledged {ok[lane]}");
                    }

                    if (counter > invoked[lane])
                    {
                        anomalies.Add($"cycle {cycle} lane {lane}: recovered {counter} > last started commit {invoked[lane]} (phantom write)");
                    }

                    for (var k = 1; k <= KeysPerLane; k++)
                    {
                        var l = await db.GetRawAsync(Harness.KeyBytes(lane * LaneStride + k), buffer);
                        var value = l < 0 ? 0 : BinaryPrimitives.ReadInt64LittleEndian(buffer);
                        if (value != counter)
                        {
                            anomalies.Add($"cycle {cycle} lane {lane}: torn transaction: key {k} = {value} but counter = {counter}");
                            break;
                        }
                    }

                    totalAcked += Math.Max(0, ok[lane] - recovered[lane]);
                    recovered[lane] = counter;
                }

                cycles++;
            }
            catch (Exception e)
            {
                anomalies.Add($"cycle {cycle}: recovery failed: {Harness.Describe(e)}");
                break;
            }

            if (anomalies.Count > 0)
            {
                break;
            }
        }

        return new WorkloadResult("crash", ctx.Profile, ctx.Seed, anomalies.Count == 0, false,
            $"cycles={cycles} acknowledged-commits={totalAcked} final-counters={string.Join(",", recovered)} sync-wal={syncWal}",
            anomalies);
    }

    public static async Task<int> RunChildAsync(string[] args)
    {
        var path = args[1];
        var profile = args[2];
        var sync = args[3] == "1";
        var seed = long.Parse(args[4]);
        var options = Profiles.Create(profile, sync);
        var db = await MvccStorage.OpenAsync(path, options);
        var output = Console.Out;
        var gate = new object();

        void Emit(string text)
        {
            lock (gate)
            {
                output.WriteLine(text);
                output.Flush();
            }
        }

        var nemesis = new Nemesis(db, seed);
        var tasks = new Task[Lanes];
        for (var lane = 0; lane < Lanes; lane++)
        {
            var l = lane;
            tasks[lane] = Task.Run(async () =>
            {
                var buffer = new byte[16];
                var counterKey = Harness.KeyBytes(l * LaneStride);
                while (true)
                {
                    using var txn = db.BeginTransaction();
                    var length = await txn.GetForUpdateRawAsync(counterKey, buffer);
                    var next = (length < 0 ? 0 : BinaryPrimitives.ReadInt64LittleEndian(buffer)) + 1;
                    var value = new byte[sizeof(long)];
                    BinaryPrimitives.WriteInt64LittleEndian(value, next);
                    txn.Put(counterKey, value);
                    for (var k = 1; k <= KeysPerLane; k++)
                    {
                        txn.Put(Harness.KeyBytes(l * LaneStride + k), value);
                    }

                    Emit($"invoke {l} {next}");
                    await txn.CommitAsync();
                    Emit($"ok {l} {next}");
                }
            });
        }

        Emit("ready");
        await Task.WhenAll(tasks);
        GC.KeepAlive(nemesis);
        return 0;
    }
}
