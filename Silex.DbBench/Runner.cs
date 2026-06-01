using System.Diagnostics;
using Silex;

namespace Silex.DbBench;

/// <summary>Per-thread accumulators for a single benchmark run.</summary>
internal sealed class ThreadStats
{
    public long Ops;
    public long ByteSlice;
    public long Found;
    public double ElapsedMicros;
    public Histogram? Histogram;
}

/// <summary>Aggregated outcome of one benchmark, formatted db_bench style.</summary>
internal sealed class BenchmarkResult
{
    public required string Name;
    public long TotalOps;
    public long TotalBytes;
    public long Found;
    public double SumThreadMicros;
    public double WallSeconds;
    public string Extra = string.Empty;
    public Histogram? Histogram;

    // Allocation accounting for the benchmark body (captured only when SILEX_BENCH_GC is set).
    public long AllocatedBytes;
    public int Gen0Collections;
    public int Gen1Collections;
    public int Gen2Collections;
}

/// <summary>
/// A thread's per-operation work plus an optional resource (e.g. an iterator's async enumerator) that must
/// be disposed when the thread finishes — including early termination — so engine locks are released.
/// </summary>
internal readonly record struct ThreadWorker(Func<long, ValueTask<bool>> RunOp, IAsyncDisposable? Resource = null);

/// <summary>
/// Drives the benchmark list against a single Silex database, managing the per-benchmark database
/// lifecycle (fresh-open for fill benchmarks, reuse for reads) and printing db_bench-style results.
/// </summary>
internal sealed class Runner
{
    private const string FillSeq = "fillseq";
    private const string FillRandom = "fillrandom";
    private const string FillSync = "fillsync";
    private const string Overwrite = "overwrite";
    private const string DeleteRandom = "deleterandom";
    private const string ReadRandom = "readrandom";
    private const string ReadMissing = "readmissing";
    private const string ReadSeq = "readseq";
    private const string SeekRandom = "seekrandom";
    private const string ReadReverse = "readreverse";
    private const string Separator = "----------------------------------------------------------------";

    private readonly BenchmarkOptions _options;
    private readonly string _dbPath;
    private LsmStorage? _db;
    private bool _currentWalSync;
    private bool _needsReadBarrier;

    public Runner(BenchmarkOptions options, string dbPath)
    {
        _options = options;
        _dbPath = dbPath;
    }

    public async Task RunAsync()
    {
        PrintHeader();

        try
        {
            var benchmarks = _options.Benchmarks.AsMemory();

            while (!benchmarks.IsEmpty)
            {
                var comma = benchmarks.Span.IndexOf(',');
                ReadOnlyMemory<char> name;

                if (comma < 0)
                {
                    name = benchmarks;
                    benchmarks = ReadOnlyMemory<char>.Empty;
                }
                else
                {
                    name = benchmarks[..comma];
                    benchmarks = benchmarks[(comma + 1)..];
                }

                name = Trim(name);

                if (!name.IsEmpty)
                {
                    await RunOneAsync(name);
                }
            }
        }
        finally
        {
            if (_db != null)
            {
                await _db.DisposeAsync();
            }
        }
    }

    private Task RunOneAsync(ReadOnlyMemory<char> name)
    {
        var span = name.Span;
        var benchmarkName = GetBenchmarkName(span);

        if (benchmarkName is null)
        {
            WriteSkippedBenchmark(span, "unknown benchmark, skipped");
            return Task.CompletedTask;
        }

        return RunKnownBenchmarkAsync(benchmarkName);
    }

    private async Task RunKnownBenchmarkAsync(string name)
    {
        switch (name)
        {
            case FillSeq:
                await OpenFreshAsync(walSync: false);
                var fillSeq = await RunWritesAsync(name, _options.Num, sequential: true);
                _needsReadBarrier = true;
                Report(fillSeq);
                break;
            case FillRandom:
                await OpenFreshAsync(walSync: false);
                var fillRandom = await RunWritesAsync(name, _options.Num, sequential: false);
                _needsReadBarrier = true;
                Report(fillRandom);
                break;
            case FillSync:
                await OpenFreshAsync(walSync: true);
                var fillSync = await RunWritesAsync(name, Math.Max(1, _options.Num / 1000), sequential: false, extra: "(num/1000 ops, WAL fsync per write)");
                _needsReadBarrier = true;
                Report(fillSync);
                break;
            case Overwrite:
                await EnsureWriteableAsync();
                var overwrite = await RunWritesAsync(name, _options.Num, sequential: false);
                _needsReadBarrier = true;
                Report(overwrite);
                break;
            case DeleteRandom:
                await EnsureWriteableAsync();
                var deletes = await RunDeletesAsync(name);
                _needsReadBarrier = true;
                Report(deletes);
                break;
            case ReadRandom:
                await EnsureReadableAsync();
                Report(await RunReadsAsync(name, missing: false));
                break;
            case ReadMissing:
                await EnsureReadableAsync();
                Report(await RunReadsAsync(name, missing: true));
                break;
            case ReadSeq:
                await EnsureReadableAsync();
                Report(await RunReadSeqAsync(name));
                break;
            case SeekRandom:
                await EnsureReadableAsync();
                Report(await RunSeekRandomAsync(name));
                break;
            case ReadReverse:
                WriteSkippedBenchmark(name, "not supported (Silex iterators are forward-only)");
                break;
        }
    }

    private static string? GetBenchmarkName(ReadOnlySpan<char> value)
    {
        if (BenchmarkEquals(value, FillSeq)) return FillSeq;
        if (BenchmarkEquals(value, FillRandom)) return FillRandom;
        if (BenchmarkEquals(value, FillSync)) return FillSync;
        if (BenchmarkEquals(value, Overwrite)) return Overwrite;
        if (BenchmarkEquals(value, DeleteRandom)) return DeleteRandom;
        if (BenchmarkEquals(value, ReadRandom)) return ReadRandom;
        if (BenchmarkEquals(value, ReadMissing)) return ReadMissing;
        if (BenchmarkEquals(value, ReadSeq)) return ReadSeq;
        if (BenchmarkEquals(value, SeekRandom)) return SeekRandom;
        if (BenchmarkEquals(value, ReadReverse)) return ReadReverse;

        return null;
    }

    private static bool BenchmarkEquals(ReadOnlySpan<char> value, string benchmark) =>
        MemoryExtensions.Equals(value, benchmark.AsSpan(), StringComparison.Ordinal);

    private static ReadOnlyMemory<char> Trim(ReadOnlyMemory<char> value)
    {
        var span = value.Span;
        var start = 0;

        while (start < span.Length && char.IsWhiteSpace(span[start]))
        {
            start++;
        }

        var end = span.Length - 1;

        while (end >= start && char.IsWhiteSpace(span[end]))
        {
            end--;
        }

        return start > end ? ReadOnlyMemory<char>.Empty : value.Slice(start, end - start + 1);
    }

    private static void WriteSkippedBenchmark(ReadOnlySpan<char> name, string message)
    {
        Console.Out.Write(name);

        for (var i = name.Length; i < 14; i++)
        {
            Console.Write(' ');
        }

        Console.WriteLine($" : {message}");
    }

    // ----- database lifecycle -----

    private async Task EnsureOpenAsync()
    {
        if (_db == null)
        {
            await OpenAsync(walSync: false);
        }
    }

    private async Task EnsureReadableAsync()
    {
        await EnsureOpenAsync();

        if (_needsReadBarrier)
        {
            await _db!.FlushAndCompactAsync();
            _needsReadBarrier = false;
        }
    }

    /// <summary>
    /// Ensures the database is open for writing without WAL sync. A preceding <c>fillsync</c> leaves the
    /// database open with sync enabled (it is an open-time option in Silex); reopen it without sync so a
    /// following write benchmark is not unintentionally measured with fsync-per-write.
    /// </summary>
    private async Task EnsureWriteableAsync()
    {
        await EnsureOpenAsync();

        if (_currentWalSync)
        {
            await _db!.DisposeAsync();
            _db = null;
            await OpenAsync(walSync: false); // reuse the same directory, no wipe
        }
    }

    private async Task OpenFreshAsync(bool walSync)
    {
        if (_db != null)
        {
            await _db.DisposeAsync();
            _db = null;
        }

        if (!_options.UseExistingDb && Directory.Exists(_dbPath))
        {
            Directory.Delete(_dbPath, true);
        }

        await OpenAsync(walSync);
        _needsReadBarrier = false;
    }

    private async Task OpenAsync(bool walSync)
    {
        Directory.CreateDirectory(_dbPath);
        _db = await LsmStorage.OpenAsync(_dbPath, _options.ToStorageOptions(walSync));
        _currentWalSync = walSync;
        _needsReadBarrier = false;
    }

    // ----- benchmark bodies -----
    //
    // Each benchmark provides a per-thread "setup" delegate that creates thread-local generators/RNG once
    // and returns a per-operation delegate. The per-op delegate returns false to stop the thread early
    // (e.g. when a sequential scan is exhausted). Keeping setup out of the op loop is what lets histogram
    // mode time individual ops without resetting the RNG between them.

    private Task<BenchmarkResult> RunWritesAsync(string name, long totalOps, bool sequential, string extra = "")
    {
        var db = _db!;
        var entryBytes = _options.KeySize + _options.ValueSize;

        // Writes total exactly totalOps so the resulting database is well defined for later read
        // benchmarks; the work is partitioned across threads (sequential fills stay globally ordered).
        return RunParallelAsync(name, totalOps, partitioned: true, extra, (threadId, start, count, stats) =>
        {
            var keyGen = new KeyGenerator(_options.KeySize);
            var valueGen = new ValueGenerator(_options.Seed + threadId, _options.ValueSize);
            var rng = RngStreams.Create(_options.Seed, threadId, RngStreams.Write);

            return new ThreadWorker(op =>
            {
                var keyIndex = sequential ? start + op : rng.NextInt64(totalOps);
                db.Put(keyGen.Generate(keyIndex), valueGen.Generate(_options.ValueSize));
                stats.Ops++;
                stats.ByteSlice += entryBytes;
                return new ValueTask<bool>(true);
            });
        });
    }

    private Task<BenchmarkResult> RunDeletesAsync(string name)
    {
        var db = _db!;

        return RunParallelAsync(name, _options.Num, partitioned: true, extra: string.Empty, (threadId, start, count, stats) =>
        {
            var keyGen = new KeyGenerator(_options.KeySize);
            var rng = RngStreams.Create(_options.Seed, threadId, RngStreams.Write);

            return new ThreadWorker(op =>
            {
                db.Delete(keyGen.Generate(rng.NextInt64(_options.Num)));
                stats.Ops++;
                stats.ByteSlice += _options.KeySize;
                return new ValueTask<bool>(true);
            });
        });
    }

    private Task<BenchmarkResult> RunReadsAsync(string name, bool missing)
    {
        var db = _db!;

        // Each thread performs a full Reads budget; total work scales with thread count.
        return RunParallelAsync(name, _options.EffectiveReads, partitioned: false, extra: string.Empty, (threadId, start, count, stats) =>
        {
            var keyGen = new KeyGenerator(_options.KeySize);
            var rng = RngStreams.Create(_options.Seed, threadId, RngStreams.Read);

            // Reused per-thread buffers: the key array is mutated in place for each lookup (reads never take
            // ownership of the key), and the value buffer receives the stored bytes via the raw read path so
            // no per-op byte[] is allocated. Operations run sequentially within a thread, so reuse is safe.
            var key = new byte[_options.KeySize];
            var valueBuffer = new byte[_options.ValueSize];

            return new ThreadWorker(async op =>
            {
                // readmissing reads from a key range that was never written ([num, 2*num)).
                var keyIndex = rng.NextInt64(_options.Num) + (missing ? _options.Num : 0);
                keyGen.GenerateInto(keyIndex, key);
                var length = await db.GetRawAsync(key, valueBuffer);

                stats.Ops++;
                stats.ByteSlice += _options.KeySize;

                if (length >= 0)
                {
                    stats.Found++;
                    stats.ByteSlice += length;
                }

                return true;
            });
        });
    }

    private async Task<BenchmarkResult> RunReadSeqAsync(string name)
    {
        var db = _db!;
        var threads = _options.Threads;
        var perThread = new ThreadStats[threads];
        var tasks = new Task[threads];
        var extra = _options.Histogram ? "(raw scan; histogram unavailable)" : string.Empty;

        var gcStats = Environment.GetEnvironmentVariable("SILEX_BENCH_GC") is { Length: > 0 };
        var allocBefore = gcStats ? GC.GetTotalAllocatedBytes(precise: false) : 0;
        var gen0Before = gcStats ? GC.CollectionCount(0) : 0;
        var gen1Before = gcStats ? GC.CollectionCount(1) : 0;
        var gen2Before = gcStats ? GC.CollectionCount(2) : 0;

        var wall = Stopwatch.StartNew();

        // Each thread independently scans forward from the start, up to the Reads budget. The op loop stops
        // when the store is exhausted.
        for (var t = 0; t < threads; t++)
        {
            var stats = new ThreadStats();
            perThread[t] = stats;

            tasks[t] = Task.Run(async () =>
            {
                var scanState = new ReadSeqScanState(stats);
                var sw = Stopwatch.StartNew();

                await db.ScanRawAsync(scanState, static (s, key, value) =>
                {
                    s.Stats.Ops++;
                    s.Stats.Found++;
                    s.Stats.ByteSlice += key.Length + value.Length;
                    return true;
                }, _options.EffectiveReads);

                stats.ElapsedMicros = sw.Elapsed.TotalMicroseconds;
            });
        }

        await Task.WhenAll(tasks);
        wall.Stop();

        var result = new BenchmarkResult { Name = name, WallSeconds = wall.Elapsed.TotalSeconds, Extra = extra };

        if (gcStats)
        {
            result.AllocatedBytes = GC.GetTotalAllocatedBytes(precise: false) - allocBefore;
            result.Gen0Collections = GC.CollectionCount(0) - gen0Before;
            result.Gen1Collections = GC.CollectionCount(1) - gen1Before;
            result.Gen2Collections = GC.CollectionCount(2) - gen2Before;
        }

        foreach (var stats in perThread)
        {
            result.TotalOps += stats.Ops;
            result.TotalBytes += stats.ByteSlice;
            result.Found += stats.Found;
            result.SumThreadMicros += stats.ElapsedMicros;
        }

        return result;
    }

    private sealed class ReadSeqScanState(ThreadStats stats)
    {
        public ThreadStats Stats { get; } = stats;
    }

    private Task<BenchmarkResult> RunSeekRandomAsync(string name)
    {
        var db = _db!;

        // One op == one seek; after landing we materialize up to SeekNexts following entries.
        return RunParallelAsync(name, _options.EffectiveReads, partitioned: false, extra: string.Empty, (threadId, start, count, stats) =>
        {
            var keyGen = new KeyGenerator(_options.KeySize);
            var rng = RngStreams.Create(_options.Seed, threadId, RngStreams.Seek);
            var toRead = 1 + _options.SeekNexts;
            var seekState = new SeekScanState(stats);

            return new ThreadWorker(async op =>
            {
                var target = keyGen.Generate(rng.NextInt64(_options.Num));
                seekState.Reset(target);

                await db.SeekRawAsync(target, seekState, static (s, key, value) =>
                {
                    if (s.Read == 0 && key.SequenceEqual(s.Target))
                    {
                        s.Stats.Found++;
                    }

                    s.Stats.ByteSlice += key.Length + value.Length;
                    s.Read++;
                    return true;
                }, toRead);

                stats.Ops++;
                return true;
            });
        });
    }

    private sealed class SeekScanState(ThreadStats stats)
    {
        public ThreadStats Stats { get; } = stats;

        public byte[] Target { get; private set; } = [];

        public int Read { get; set; }

        public void Reset(byte[] target)
        {
            Target = target;
            Read = 0;
        }
    }

    // ----- parallel scaffolding + reporting -----

    /// <summary>
    /// Runs <paramref name="setup"/> on <see cref="BenchmarkOptions.Threads"/> threads. When
    /// <paramref name="partitioned"/> is true the <paramref name="totalOps"/> are split into disjoint
    /// contiguous ranges (one per thread); otherwise every thread runs the full <paramref name="totalOps"/>
    /// budget and the totals scale with the thread count. <paramref name="setup"/> returns a per-operation
    /// delegate; returning false from it stops that thread early.
    /// </summary>
    private async Task<BenchmarkResult> RunParallelAsync(
        string name,
        long totalOps,
        bool partitioned,
        string extra,
        Func<int, long, long, ThreadStats, ThreadWorker> setup)
    {
        var threads = _options.Threads;
        var perThread = new ThreadStats[threads];
        var tasks = new Task[threads];

        // Capture process-wide allocation/GC counters around the benchmark body so a before/after run can
        // attribute allocation changes to a specific benchmark. Only reported when SILEX_BENCH_GC is set.
        var gcStats = Environment.GetEnvironmentVariable("SILEX_BENCH_GC") is { Length: > 0 };
        var allocBefore = gcStats ? GC.GetTotalAllocatedBytes(precise: false) : 0;
        var gen0Before = gcStats ? GC.CollectionCount(0) : 0;
        var gen1Before = gcStats ? GC.CollectionCount(1) : 0;
        var gen2Before = gcStats ? GC.CollectionCount(2) : 0;

        var wall = Stopwatch.StartNew();

        for (var t = 0; t < threads; t++)
        {
            var threadId = t;
            var stats = new ThreadStats { Histogram = _options.Histogram ? new Histogram() : null };
            perThread[t] = stats;

            long start;
            long count;

            if (partitioned)
            {
                var baseCount = totalOps / threads;
                var remainder = totalOps % threads;
                count = baseCount + (threadId < remainder ? 1 : 0);
                start = threadId * baseCount + Math.Min(threadId, remainder);
            }
            else
            {
                start = 0;
                count = totalOps;
            }

            tasks[t] = Task.Run(async () =>
            {
                var worker = setup(threadId, start, count, stats);
                var runOp = worker.RunOp;
                var histogram = stats.Histogram;
                var sw = Stopwatch.StartNew();

                try
                {
                    for (long i = 0; i < count; i++)
                    {
                        bool more;

                        if (histogram == null)
                        {
                            more = await runOp(i);
                        }
                        else
                        {
                            var opWatch = Stopwatch.StartNew();
                            more = await runOp(i);
                            opWatch.Stop();

                            if (more)
                            {
                                histogram.Add(opWatch.Elapsed.TotalMicroseconds);
                            }
                        }

                        if (!more)
                        {
                            break;
                        }
                    }

                    stats.ElapsedMicros = sw.Elapsed.TotalMicroseconds;
                }
                finally
                {
                    // Dispose any per-thread resource (e.g. a scan enumerator) so engine locks it holds are
                    // released even when the loop stops early at the read budget.
                    if (worker.Resource != null)
                    {
                        await worker.Resource.DisposeAsync();
                    }
                }
            });
        }

        await Task.WhenAll(tasks);
        wall.Stop();

        var result = new BenchmarkResult { Name = name, WallSeconds = wall.Elapsed.TotalSeconds, Extra = extra };

        if (gcStats)
        {
            result.AllocatedBytes = GC.GetTotalAllocatedBytes(precise: false) - allocBefore;
            result.Gen0Collections = GC.CollectionCount(0) - gen0Before;
            result.Gen1Collections = GC.CollectionCount(1) - gen1Before;
            result.Gen2Collections = GC.CollectionCount(2) - gen2Before;
        }

        foreach (var stats in perThread)
        {
            result.TotalOps += stats.Ops;
            result.TotalBytes += stats.ByteSlice;
            result.Found += stats.Found;
            result.SumThreadMicros += stats.ElapsedMicros;

            if (stats.Histogram != null)
            {
                result.Histogram ??= new Histogram();
                result.Histogram.Merge(stats.Histogram);
            }
        }

        return result;
    }

    private void Report(BenchmarkResult result)
    {
        if (result.TotalOps == 0)
        {
            Console.WriteLine($"{result.Name,-14} : no operations");
            return;
        }

        var microsPerOp = result.SumThreadMicros / result.TotalOps;
        var opsPerSec = result.WallSeconds > 0 ? result.TotalOps / result.WallSeconds : 0;
        var mbPerSec = result.WallSeconds > 0 ? result.TotalBytes / 1_048_576.0 / result.WallSeconds : 0;

        var line = $"{result.Name,-14} : {microsPerOp,11:F3} micros/op {opsPerSec,12:N0} ops/sec; {mbPerSec,8:F1} MB/s";

        if (result.Name is "readrandom" or "readmissing" or "seekrandom")
        {
            line += $" (found {result.Found:N0} of {result.TotalOps:N0})";
        }

        if (!string.IsNullOrEmpty(result.Extra))
        {
            line += $" {result.Extra}";
        }

        Console.WriteLine(line);

        if (result.AllocatedBytes > 0 || result.Gen0Collections > 0 || result.Gen1Collections > 0 || result.Gen2Collections > 0)
        {
            var bytesPerOp = (double)result.AllocatedBytes / result.TotalOps;
            Console.WriteLine($"{"",-14}   alloc: {result.AllocatedBytes / 1_048_576.0,8:F1} MB total, {bytesPerOp,8:F1} B/op; GC g0/g1/g2: {result.Gen0Collections}/{result.Gen1Collections}/{result.Gen2Collections}");
        }

        if (result.Histogram != null)
        {
            var summary = result.Histogram.Summary();
            if (summary.Length > 0)
            {
                Console.WriteLine(summary);
            }
        }
    }

    private void PrintHeader()
    {
        var rawSizeMb = (_options.KeySize + _options.ValueSize) * (double)_options.Num / 1_048_576.0;

        Console.WriteLine($"Silex DbBench");
        Console.WriteLine($"Keys:       {_options.KeySize} bytes each");
        Console.WriteLine($"Values:     {_options.ValueSize} bytes each (uncompressed)");
        Console.WriteLine($"Entries:    {_options.Num:N0}");
        Console.WriteLine($"RawSize:    {rawSizeMb:F1} MB (estimated)");
        Console.WriteLine($"Threads:    {_options.Threads}");
        Console.WriteLine($"Compaction: {_options.Compaction}  WAL: {(_options.Wal ? "on" : "off")}{(_options.WalSync ? " (sync)" : "")}");
        Console.WriteLine($"DB path:    {_dbPath}");
        Console.WriteLine(Separator);
    }
}
