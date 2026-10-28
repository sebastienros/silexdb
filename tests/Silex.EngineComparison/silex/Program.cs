using System.Diagnostics;
using System.Globalization;
using System.Runtime.InteropServices;
using Silex;

namespace Silex.EngineComparison;

/// <summary>
/// The Silex adapter of the engine comparison harness: runs one suite of the workloads described in
/// <c>tests/Silex.EngineComparison/README.md</c> against <see cref="LsmStorage"/> or <see cref="MvccStorage"/>.
/// </summary>
internal static class Program
{
    private const int N = 50_000;
    private const int KeySize = 16;
    private const int ValueSize = 100;
    private const int ReadsPerThread = 50_000;
    private const int BoundedScansPerThread = 5_000;
    private const int CountsPerThread = 50;
    private const int SyncWrites = 50;

    private static string _engine = "";
    private static string _suite = "";
    private static int _threads;

    private static async Task<int> Main(string[] args)
    {
        string? dir = null;
        for (var i = 0; i + 1 < args.Length; i += 2)
        {
            switch (args[i])
            {
                case "--engine": _engine = args[i + 1]; break;
                case "--suite": _suite = args[i + 1]; break;
                case "--threads": _threads = int.Parse(args[i + 1], CultureInfo.InvariantCulture); break;
                case "--dir": dir = args[i + 1]; break;
                default: return Usage($"unknown option {args[i]}");
            }
        }

        if (_engine is not ("silex-lsm" or "silex-mvcc")) return Usage("--engine must be silex-lsm or silex-mvcc");
        if (_threads < 1) return Usage("--threads must be positive");
        if (dir is null) return Usage("--dir is required");

        var options = new StorageOptions
        {
            MemTableSizeLimit = 64L * 1024 * 1024,
            BlockCacheSizeLimit = 8L * 1024 * 1024,
            Compression = SstCompression.None,
            UseWriteAheadLog = true,
            SyncWriteAheadLogToDisk = _suite == "sync",
        };

        Engine engine;
        if (_engine == "silex-lsm")
        {
            engine = new LsmEngine(await LsmStorage.OpenAsync(dir, options));
        }
        else
        {
            options.MvccCommitGroupSize = Math.Min(_threads, 64);
            engine = new MvccEngine(await MvccStorage.OpenAsync(dir, options));
        }

        switch (_suite)
        {
            case "write":
                RunWrites("sequential-writes", engine, Enumerable.Range(0, N).ToArray(), generation: 0, delete: false);
                RunWrites("overwrites", engine, Permutation(2), generation: 1, delete: false);
                RunWrites("random-deletes", engine, Permutation(3), generation: 0, delete: true);
                break;
            case "randomfill":
                RunWrites("random-writes", engine, Permutation(4), generation: 0, delete: false);
                break;
            case "sync":
                RunWrites("sync-writes", engine, Enumerable.Range(0, SyncWrites).ToArray(), generation: 0, delete: false);
                break;
            case "read-memtable":
                Load(engine);
                RunReads(engine);
                break;
            case "read-sst":
                Load(engine);
                await engine.FlushAndCompactAsync();
                RunReads(engine);
                break;
            case "mixed":
                Load(engine);
                RunMixed(engine);
                break;
            default:
                return Usage($"unknown suite {_suite}");
        }

        var rss = Memory.ResidentBytes();
        await engine.DisposeAsync();
        var peak = Memory.PeakResidentBytes();
        Console.Out.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $$"""{"type":"memory","engine":"{{_engine}}","suite":"{{_suite}}","threads":{{_threads}},"rss_bytes":{{rss}},"peak_rss_bytes":{{peak}}}"""));
        Console.Out.Flush();
        return 0;
    }

    private static int Usage(string error)
    {
        Console.Error.WriteLine(error);
        Console.Error.WriteLine("usage: silex-bench --engine <silex-lsm|silex-mvcc> --suite <suite> --threads <T> --dir <path>");
        return 2;
    }

    private static void Load(Engine engine)
    {
        var key = new byte[KeySize];
        var value = new byte[ValueSize];
        for (var i = 0; i < N; i++)
        {
            FormatKey(i, key);
            FillValue(i, 0, value);
            engine.Load(key, value);
        }
    }

    private static void RunWrites(string name, Engine engine, int[] indexes, int generation, bool delete)
    {
        var count = indexes.Length;
        RunWritePhase(engine, name, count, async thread =>
        {
            var key = new byte[KeySize];
            var value = new byte[ValueSize];
            var start = (int)((long)thread * count / _threads);
            var end = (int)((long)(thread + 1) * count / _threads);
            for (var i = start; i < end; i++)
            {
                var index = indexes[i];
                FormatKey(index, key);
                if (!delete) FillValue(index, generation, value);
                await engine.WriteAsync(key, value, delete);
            }
            return end - start;
        });
    }

    private static void RunReads(Engine engine)
    {
        for (var phase = 10; phase <= 11; phase++)
        {
            RunPhase(phase == 10 ? "random-reads-first" : "random-reads-warm", "ops/s", (long)ReadsPerThread * _threads, thread =>
            {
                var rng = new SplitMix64(phase, thread);
                var key = new byte[KeySize];
                var buffer = new byte[ValueSize];
                long found = 0;
                for (var i = 0; i < ReadsPerThread; i++)
                {
                    FormatKey((int)(rng.Next() % N), key);
                    if (engine.Get(key, buffer)) found++;
                }
                return found;
            });
        }

        for (var phase = 12; phase <= 13; phase++)
        {
            RunPhase(phase == 12 ? "random-seeks-first" : "random-seeks-warm", "ops/s", (long)ReadsPerThread * _threads, thread =>
            {
                var rng = new SplitMix64(phase, thread);
                var state = new SeekState { Target = new byte[KeySize] };
                long found = 0;
                for (var i = 0; i < ReadsPerThread; i++)
                {
                    FormatKey((int)(rng.Next() % N), state.Target);
                    if (engine.Seek(state)) found++;
                }
                return found;
            });
        }

        RunPhase("forward-scan", "entries/s", (long)N * _threads, _ => engine.Scan(new ScanState(), reverse: false));
        RunPhase("reverse-scan", "entries/s", (long)N * _threads, _ => engine.Scan(new ScanState(), reverse: true));

        RunBoundedScans(engine, 16, "scan-100", skip: 0);
        RunBoundedScans(engine, 17, "skip-5000-scan-100", skip: 5_000);

        RunPhase("count-all", "scans/s", (long)CountsPerThread * _threads, _ =>
        {
            var state = new ScanState();
            long counted = 0;
            for (var i = 0; i < CountsPerThread; i++) counted += engine.Count(state);
            return counted;
        });
    }

    private static void RunBoundedScans(Engine engine, int phase, string name, int skip)
    {
        const int take = 100;
        RunPhase(name, "scans/s", (long)BoundedScansPerThread * _threads, thread =>
        {
            var rng = new SplitMix64(phase, thread);
            var state = new ScanState { From = new byte[KeySize] };
            long read = 0;
            for (var i = 0; i < BoundedScansPerThread; i++)
            {
                FormatKey((int)(rng.Next() % (ulong)(N - skip - take)), state.From);
                read += engine.ScanFrom(state, skip, take);
            }
            return read;
        });
    }

    private static void RunMixed(Engine engine)
    {
        const int phase = 20;
        RunWritePhase(engine, "read-write-90-10", (long)ReadsPerThread * _threads, async thread =>
        {
            var rng = new SplitMix64(phase, thread);
            var key = new byte[KeySize];
            var value = new byte[ValueSize];
            long done = 0;
            for (var i = 0; i < ReadsPerThread; i++)
            {
                var read = rng.Next() % 100 < 90;
                var index = (int)(rng.Next() % N);
                FormatKey(index, key);
                if (read)
                {
                    if (engine.Get(key, value)) done++;
                }
                else
                {
                    FillValue(index, 1, value);
                    await engine.WriteAsync(key, value, delete: false);
                    done++;
                }
            }
            return done;
        });
    }

    /// <summary>
    /// Runs a phase that writes. Synchronous writes (<see cref="LsmStorage"/>) run on dedicated threads like every
    /// read phase; awaited transaction commits (<see cref="MvccStorage"/>) run as thread-pool tasks, the way the
    /// async API is meant to be called, so a commit that completes asynchronously does not block a thread.
    /// </summary>
    private static void RunWritePhase(Engine engine, string name, long work, Func<int, ValueTask<long>> body)
    {
        if (engine.AsyncWrites) RunPhaseAsync(name, "ops/s", work, thread => body(thread).AsTask());
        else RunPhase(name, "ops/s", work, thread => Engine.Wait(body(thread)));
    }

    /// <summary>Runs <paramref name="body"/> on every thread, released together, and prints the phase's result line.</summary>
    private static void RunPhase(string name, string unit, long work, Func<int, long> body)
    {
        var checks = new long[_threads];
        var errors = new Exception?[_threads];
        using var ready = new CountdownEvent(_threads);
        using var start = new ManualResetEventSlim();
        var workers = new Thread[_threads];
        for (var t = 0; t < _threads; t++)
        {
            var thread = t;
            workers[t] = new Thread(() =>
            {
                ready.Signal();
                start.Wait();
                try
                {
                    checks[thread] = body(thread);
                }
                catch (Exception ex)
                {
                    errors[thread] = ex;
                }
            }) { IsBackground = true, Name = $"{name}-{thread}" };
            workers[t].Start();
        }

        ready.Wait();
        var stopwatch = Stopwatch.StartNew();
        start.Set();
        foreach (var worker in workers) worker.Join();
        var seconds = stopwatch.Elapsed.TotalSeconds;

        foreach (var error in errors)
        {
            if (error is not null) throw new InvalidOperationException($"phase {name} failed", error);
        }

        Console.Out.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $$"""{"type":"result","engine":"{{_engine}}","suite":"{{_suite}}","threads":{{_threads}},"phase":"{{name}}","unit":"{{unit}}","work":{{work}},"seconds":{{seconds:R}},"check":{{checks.Sum()}}}"""));
    }

    private static void RunPhaseAsync(string name, string unit, long work, Func<int, Task<long>> body)
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var tasks = new Task<long>[_threads];
        for (var t = 0; t < _threads; t++)
        {
            var thread = t;
            tasks[t] = Task.Run(async () =>
            {
                await gate.Task;
                return await body(thread);
            });
        }

        var stopwatch = Stopwatch.StartNew();
        gate.SetResult();
        Task.WaitAll(tasks);
        var seconds = stopwatch.Elapsed.TotalSeconds;
        Console.Out.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $$"""{"type":"result","engine":"{{_engine}}","suite":"{{_suite}}","threads":{{_threads}},"phase":"{{name}}","unit":"{{unit}}","work":{{work}},"seconds":{{seconds:R}},"check":{{tasks.Sum(t => t.Result)}}}"""));
    }

    private static void FormatKey(int index, byte[] key)
    {
        for (var j = KeySize - 1; j >= 0; j--)
        {
            key[j] = (byte)('0' + index % 10);
            index /= 10;
        }
    }

    private static void FillValue(int index, int generation, byte[] value)
    {
        for (var j = 0; j < ValueSize; j++) value[j] = (byte)(index + j + 31 * generation);
    }

    private static int[] Permutation(int phase)
    {
        var perm = Enumerable.Range(0, N).ToArray();
        var rng = new SplitMix64((ulong)phase);
        for (var i = N - 1; i >= 1; i--)
        {
            var j = (int)(rng.Next() % (ulong)(i + 1));
            (perm[i], perm[j]) = (perm[j], perm[i]);
        }
        return perm;
    }

    private struct SplitMix64(ulong state)
    {
        private ulong _state = state;

        public SplitMix64(int phase, int thread) : this((ulong)phase * 1_000_003 + (ulong)thread)
        {
        }

        public ulong Next()
        {
            _state += 0x9E3779B97F4A7C15;
            var z = _state;
            z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9;
            z = (z ^ (z >> 27)) * 0x94D049BB133111EB;
            return z ^ (z >> 31);
        }
    }

    private sealed class SeekState
    {
        public byte[] Target = [];
        public bool Found;
    }

    private sealed class ScanState
    {
        public byte[] From = [];
        public int Skip;
        public long Seen;
        public long Read;
        public long Bytes;

        public void Reset(int skip)
        {
            Skip = skip;
            Seen = 0;
            Read = 0;
        }
    }

    private abstract class Engine : IAsyncDisposable
    {
        protected static readonly ReadRawEntryAction<SeekState> SeekReader = static (s, key, _) =>
        {
            s.Found = key.SequenceEqual(s.Target);
            return false;
        };

        protected static readonly ReadRawEntryAction<ScanState> ScanReader = static (s, key, value) =>
        {
            if (s.Seen++ < s.Skip) return true;
            s.Bytes += key.Length + value.Length;
            s.Read++;
            return true;
        };

        protected static readonly ReadRawEntryAction<ScanState> CountReader = static (s, _, _) =>
        {
            s.Seen++;
            return true;
        };

        /// <summary>Whether <see cref="WriteAsync"/> can complete asynchronously.</summary>
        public virtual bool AsyncWrites => false;

        /// <summary>One timed write: a put, or a delete when <paramref name="delete"/> is set.</summary>
        public abstract ValueTask WriteAsync(byte[] key, byte[] value, bool delete);

        /// <summary>Writes one entry of the untimed load.</summary>
        public abstract void Load(byte[] key, byte[] value);
        public abstract bool Get(byte[] key, byte[] buffer);
        public abstract bool Seek(SeekState state);
        public abstract long Scan(ScanState state, bool reverse);
        public abstract long ScanFrom(ScanState state, int skip, int take);
        public abstract long Count(ScanState state);
        public abstract Task FlushAndCompactAsync();
        public abstract ValueTask DisposeAsync();

        public static T Wait<T>(ValueTask<T> task) => task.IsCompletedSuccessfully ? task.Result : task.AsTask().GetAwaiter().GetResult();
    }

    private sealed class LsmEngine(LsmStorage db) : Engine
    {
        public override void Load(byte[] key, byte[] value) => db.Put(key, value);

        public override ValueTask WriteAsync(byte[] key, byte[] value, bool delete)
        {
            if (delete) db.Delete(key);
            else db.Put(key, value);
            return default;
        }

        public override bool Get(byte[] key, byte[] buffer) => Wait(db.GetRawAsync(key, buffer)) >= 0;

        public override bool Seek(SeekState state)
        {
            state.Found = false;
            Wait(db.SeekRawAsync(state.Target, state, SeekReader, 1));
            return state.Found;
        }

        public override long Scan(ScanState state, bool reverse)
        {
            state.Reset(0);
            Wait(reverse ? db.ScanReverseRawAsync(state, ScanReader) : db.ScanRawAsync(state, ScanReader));
            return state.Read;
        }

        public override long ScanFrom(ScanState state, int skip, int take)
        {
            state.Reset(skip);
            Wait(db.SeekRawAsync(state.From, state, ScanReader, skip + take));
            return state.Read;
        }

        public override long Count(ScanState state)
        {
            state.Reset(0);
            Wait(db.ScanRawAsync(state, CountReader));
            return state.Seen;
        }

        public override Task FlushAndCompactAsync() => db.FlushAndCompactAsync();

        public override ValueTask DisposeAsync() => db.DisposeAsync();
    }

    private sealed class MvccEngine(MvccStorage db) : Engine
    {
        // The untimed load writes directly; every timed write is its own transaction.
        public override void Load(byte[] key, byte[] value) => db.Put(key, value);

        public override bool AsyncWrites => true;

        /// <summary>One transaction per mutation, retried from a new transaction on a conflict.</summary>
        public override async ValueTask WriteAsync(byte[] key, byte[] value, bool delete)
        {
            while (true)
            {
                using var transaction = db.BeginTransaction();
                if (delete) transaction.Delete(key);
                else transaction.Put(key, value);
                if (await transaction.TryCommitAsync()) return;
            }
        }

        public override bool Get(byte[] key, byte[] buffer) => Wait(db.GetRawAsync(key, buffer)) >= 0;

        public override bool Seek(SeekState state)
        {
            state.Found = false;
            Wait(db.SeekRawAsync(state.Target, state, SeekReader, 1));
            return state.Found;
        }

        public override long Scan(ScanState state, bool reverse)
        {
            state.Reset(0);
            Wait(reverse ? db.ScanReverseRawAsync(state, ScanReader) : db.ScanRawAsync(state, ScanReader));
            return state.Read;
        }

        public override long ScanFrom(ScanState state, int skip, int take)
        {
            state.Reset(skip);
            Wait(db.SeekRawAsync(state.From, state, ScanReader, skip + take));
            return state.Read;
        }

        public override long Count(ScanState state)
        {
            state.Reset(0);
            Wait(db.ScanRawAsync(state, CountReader));
            return state.Seen;
        }

        public override Task FlushAndCompactAsync() => db.FlushAndCompactAsync();

        public override ValueTask DisposeAsync() => db.DisposeAsync();
    }

    private static class Memory
    {
        private const int ProcPidTaskInfo = 4;

        public static long ResidentBytes()
        {
            if (!OperatingSystem.IsMacOS()) return Environment.WorkingSet;
            var info = new ProcTaskInfo();
            var size = Marshal.SizeOf<ProcTaskInfo>();
            return proc_pidinfo(Environment.ProcessId, ProcPidTaskInfo, 0, ref info, size) == size ? (long)info.ResidentSize : -1;
        }

        public static long PeakResidentBytes()
        {
            if (!OperatingSystem.IsMacOS()) return Process.GetCurrentProcess().PeakWorkingSet64;
            return getrusage(0, out var usage) == 0 ? usage.MaxResidentSize : -1;
        }

        [DllImport("libproc", SetLastError = true)]
        private static extern int proc_pidinfo(int pid, int flavor, ulong arg, ref ProcTaskInfo buffer, int size);

        [DllImport("libc", SetLastError = true)]
        private static extern int getrusage(int who, out RUsage usage);

        [StructLayout(LayoutKind.Sequential)]
        private struct ProcTaskInfo
        {
            public ulong VirtualSize;
            public ulong ResidentSize;
            public ulong TotalUser;
            public ulong TotalSystem;
            public ulong ThreadsUser;
            public ulong ThreadsSystem;
            public int Policy;
            public int Faults;
            public int PageIns;
            public int CowFaults;
            public int MessagesSent;
            public int MessagesReceived;
            public int SyscallsMach;
            public int SyscallsUnix;
            public int ContextSwitches;
            public int ThreadCount;
            public int RunningThreads;
            public int Priority;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct RUsage
        {
            public long UserSeconds;
            public long UserMicroseconds;
            public long SystemSeconds;
            public long SystemMicroseconds;
            public long MaxResidentSize;
            public long SharedSize;
            public long UnsharedDataSize;
            public long UnsharedStackSize;
            public long MinorFaults;
            public long MajorFaults;
            public long Swaps;
            public long BlockInputs;
            public long BlockOutputs;
            public long MessagesSent;
            public long MessagesReceived;
            public long Signals;
            public long VoluntaryContextSwitches;
            public long InvoluntaryContextSwitches;
        }
    }
}
