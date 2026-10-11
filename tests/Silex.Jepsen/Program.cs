using System.Diagnostics;

namespace Silex.Jepsen;

internal static class Program
{
    private const string Usage = """
        Silex Jepsen-style consistency harness (CI runs a short smoke pass).

        Usage: dotnet run -c Release --project tests/Silex.Jepsen -- [workload ...] [options]

        Workloads: register, register-lsm, bank, append-si, append-ser, scan, crash, crash-lsm, selftest, all (default)
          register    linearizability of reads, writes and CAS on single keys
          bank        total-balance invariant under transfers, scans and close/reopen
          append-si   Elle-style list-append, checked against strong snapshot isolation
          append-ser  Elle-style list-append with tracked reads, checked against strict serializability
          scan        atomic multi-key writes vs. forward/reverse snapshot scans
          selftest    verify the checkers against histories with known verdicts (runs first in "all")
          register-lsm  linearizability of plain LsmStorage Get/Put/Delete (no transactions)
          crash-lsm   SIGKILL a child committing atomic LsmWriteBatch writes
          crash       SIGKILL a committing child process, reopen, check durability and atomicity

        Options:
          --seed <n>         first seed (default: random); every run is reproducible from its seed
          --iterations <n>   number of consecutive seeds to run (default: 1)
          --seconds <n>      duration of each concurrent run (default: 5)
          --clients <n>      concurrent clients per run (default: 6)
          --profile <name>   tiny-tiered, tiny-leveled-group, nowal-tiny or all (default: all)
          --crash-cycles <n> kill/restart cycles per crash run (default: 8)
          --no-realtime      do not add real-time edges to the append checkers
          --output <dir>     where failing histories are written (default: jepsen-results)
          --keep             keep database directories
        """;

    public static async Task<int> Main(string[] args)
    {
        if (CrashWorkload.IsChild(args))
        {
            return await CrashWorkload.RunChildAsync(args);
        }

        var workloads = new List<string>();
        long seed = Random.Shared.Next(1, 1_000_000);
        int iterations = 1, seconds = 5, clients = 6, crashCycles = 8;
        var profile = "all";
        var realtime = true;
        var keep = false;
        var output = "jepsen-results";

        for (var i = 0; i < args.Length; i++)
        {
            string Next() => i + 1 < args.Length ? args[++i] : throw new ArgumentException($"{args[i]} needs a value");
            switch (args[i])
            {
                case "-h" or "--help":
                    Console.WriteLine(Usage);
                    return 0;
                case "--seed": seed = long.Parse(Next()); break;
                case "--iterations": iterations = int.Parse(Next()); break;
                case "--seconds": seconds = int.Parse(Next()); break;
                case "--clients": clients = int.Parse(Next()); break;
                case "--profile": profile = Next(); break;
                case "--crash-cycles": crashCycles = int.Parse(Next()); break;
                case "--no-realtime": realtime = false; break;
                case "--output": output = Next(); break;
                case "--keep": keep = true; break;
                default:
                    if (args[i].StartsWith('-'))
                    {
                        Console.Error.WriteLine($"Unknown option {args[i]}\n\n{Usage}");
                        return 2;
                    }

                    workloads.Add(args[i]);
                    break;
            }
        }

        string[] allWorkloads = ["selftest", "register", "register-lsm", "bank", "append-si", "append-ser", "scan", "crash", "crash-lsm"];
        if (workloads.Count == 0 || workloads.Contains("all"))
        {
            workloads = [.. allWorkloads];
        }

        var unknown = workloads.Except(allWorkloads).ToList();
        if (unknown.Count > 0)
        {
            Console.Error.WriteLine($"Unknown workload(s): {string.Join(", ", unknown)}\n\n{Usage}");
            return 2;
        }

        var profiles = profile == "all" ? Profiles.Names : [profile];
        var root = Path.Combine(Path.GetTempPath(), "silex-jepsen-" + Guid.NewGuid().ToString("N")[..8]);
        var failures = 0;
        var total = 0;
        var timer = Stopwatch.StartNew();
        Console.WriteLine($"Silex Jepsen harness: seed={seed} iterations={iterations} seconds={seconds} clients={clients}");

        try
        {
            for (var iteration = 0; iteration < iterations; iteration++)
            {
                var runSeed = seed + iteration;
                foreach (var name in workloads)
                {
                    if (name == "selftest")
                    {
                        if (iteration == 0)
                        {
                            total++;
                            var selfTest = SelfTest.Run();
                            Report(selfTest, TimeSpan.Zero);
                            failures += selfTest.Valid ? 0 : 1;
                        }

                        continue;
                    }

                    foreach (var p in profiles)
                    {
                        if (name.StartsWith("crash") && !Profiles.HasWal(p))
                        {
                            continue;
                        }

                        var directory = Path.Combine(root, $"{name}-{p}-{runSeed}");
                        Directory.CreateDirectory(directory);
                        var ctx = new RunContext
                        {
                            Directory = directory,
                            Profile = p,
                            Seed = runSeed,
                            Clients = clients,
                            Duration = TimeSpan.FromSeconds(seconds),
                            OutputDirectory = output,
                            CrashCycles = crashCycles,
                            Realtime = realtime,
                        };

                        total++;
                        var runTimer = Stopwatch.StartNew();
                        WorkloadResult result;
                        try
                        {
                            result = name switch
                            {
                                "register" => await RegisterWorkload.RunAsync(ctx),
                                "register-lsm" => await RegisterWorkload.RunAsync(ctx, lsm: true),
                                "bank" => await BankWorkload.RunAsync(ctx),
                                "append-si" => await AppendWorkload.RunAsync(ctx, serializable: false),
                                "append-ser" => await AppendWorkload.RunAsync(ctx, serializable: true),
                                "scan" => await ScanWorkload.RunAsync(ctx),
                                "crash-lsm" => await CrashWorkload.RunAsync(ctx, lsm: true),
                                _ => await CrashWorkload.RunAsync(ctx),
                            };
                        }
                        catch (Exception e)
                        {
                            result = new WorkloadResult(name, p, runSeed, false, false, "harness crashed",
                                [e.ToString()]);
                        }

                        Report(result, runTimer.Elapsed);
                        if (!result.Valid)
                        {
                            failures++;
                        }
                    }
                }
            }
        }
        finally
        {
            if (!keep)
            {
                try
                {
                    Directory.Delete(root, recursive: true);
                }
                catch (IOException)
                {
                }
            }
            else
            {
                Console.WriteLine($"Databases kept in {root}");
            }
        }

        Console.WriteLine($"\n{total - failures}/{total} runs passed in {timer.Elapsed:mm\\:ss}.");
        if (failures > 0)
        {
            Console.WriteLine($"Failing histories were written to {Path.GetFullPath(output)}.");
        }

        return failures == 0 ? 0 : 1;
    }

    private static void Report(WorkloadResult result, TimeSpan elapsed)
    {
        var status = result.Valid ? "PASS" : "FAIL";
        Console.WriteLine($"[{status}] {result.Workload,-10} {result.Profile,-20} seed={result.Seed,-8} {elapsed.TotalSeconds,5:F1}s  {result.Summary}");
        foreach (var anomaly in result.Anomalies.Take(10))
        {
            Console.WriteLine($"    - {anomaly.TrimEnd()}");
        }

        if (result.Anomalies.Count > 10)
        {
            Console.WriteLine($"    ... {result.Anomalies.Count - 10} more");
        }
    }
}
