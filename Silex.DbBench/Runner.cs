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
    public long RestingMemoryBytes;
    public long PeakMemoryBytes;
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
    private const string ScanLimit = "scanlimit";
    private const string ScanSkip = "scanskip";
    private const string ScanCount = "scancount";
    private const string ReadRandomWriteRandom = "readrandomwriterandom";
    private const string SeekRandomWriteRandom = "seekrandomwriterandom";
    private const string Separator = "----------------------------------------------------------------";

    private readonly BenchmarkOptions _options;
    private readonly string _dbPath;
    private LsmStorage? _db;
    private MvccStorage? _mvccDb;
    private bool _currentWalSync;
    private bool _needsReadBarrier;
    private long _minRssBytes = long.MaxValue;
    private long _maxRssBytes;

    public Runner(BenchmarkOptions options, string dbPath)
    {
        _options = options;
        _dbPath = dbPath;
    }

    private void SampleRss(long rssBytes)
    {
        if (rssBytes < _minRssBytes)
        {
            _minRssBytes = rssBytes;
        }
        if (rssBytes > _maxRssBytes)
        {
            _maxRssBytes = rssBytes;
        }
    }

    public async Task RunAsync()
    {
        PrintHeader();
        SampleRss(Environment.WorkingSet);

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

            if (_minRssBytes <= _maxRssBytes)
            {
                Console.WriteLine(Separator);
                Console.WriteLine($"Memory (RSS)   : {_minRssBytes / 1_048_576.0,11:F1} MiB resting; {_maxRssBytes / 1_048_576.0,8:F1} MiB peak");
            }
        }
        finally
        {
            if (_mvccDb != null)
            {
                await _mvccDb.DisposeAsync();
            }

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
                await EnsureReadableAsync();
                Report(await RunReadReverseAsync(name));
                break;
            case ScanLimit:
                await EnsureReadableAsync();
                Report(await RunScanBoundedAsync(name, skip: 0, limit: _options.ScanLimit, countOnly: false));
                break;
            case ScanSkip:
                await EnsureReadableAsync();
                Report(await RunScanBoundedAsync(name, skip: _options.ScanSkip, limit: _options.ScanLimit, countOnly: false));
                break;
            case ScanCount:
                await EnsureReadableAsync();
                Report(await RunScanBoundedAsync(name, skip: 0, limit: int.MaxValue, countOnly: true));
                break;
            case ReadRandomWriteRandom:
                await EnsureWriteableAsync();
                Report(await RunReadWriteMixAsync(name, seek: false));
                _needsReadBarrier = true;
                break;
            case SeekRandomWriteRandom:
                await EnsureWriteableAsync();
                Report(await RunReadWriteMixAsync(name, seek: true));
                _needsReadBarrier = true;
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
        if (BenchmarkEquals(value, ScanLimit)) return ScanLimit;
        if (BenchmarkEquals(value, ScanSkip)) return ScanSkip;
        if (BenchmarkEquals(value, ScanCount)) return ScanCount;
        if (BenchmarkEquals(value, ReadRandomWriteRandom)) return ReadRandomWriteRandom;
        if (BenchmarkEquals(value, SeekRandomWriteRandom)) return SeekRandomWriteRandom;

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
        if (_db == null && _mvccDb == null)
        {
            await OpenAsync(walSync: false);
        }
    }

    private async Task EnsureReadableAsync()
    {
        await EnsureOpenAsync();

        if (_needsReadBarrier)
        {
            if (_options.FlushBeforeReads)
            {
                if (_mvccDb != null)
                {
                    await _mvccDb.FlushAndCompactAsync();
                }
                else
                {
                    await _db!.FlushAndCompactAsync();
                }
            }

            if (_mvccDb != null)
            {
                await _mvccDb.ScanRawAsync(0, static (_, _, _) => true);
            }
            else
            {
                await _db!.ScanRawAsync(0, static (_, _, _) => true);
            }

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
            await CloseCurrentAsync();
            await OpenAsync(walSync: false); // reuse the same directory, no wipe
        }
    }

    private async Task OpenFreshAsync(bool walSync)
    {
        await CloseCurrentAsync();

        if (!_options.UseExistingDb && Directory.Exists(_dbPath))
        {
            Directory.Delete(_dbPath, true);
        }

        await OpenAsync(walSync);
        _needsReadBarrier = false;
    }

    private async Task CloseCurrentAsync()
    {
        if (_mvccDb != null)
        {
            await _mvccDb.DisposeAsync();
            _mvccDb = null;
        }

        if (_db != null)
        {
            await _db.DisposeAsync();
            _db = null;
        }
    }

    private async Task OpenAsync(bool walSync)
    {
        Directory.CreateDirectory(_dbPath);
        var storageOptions = _options.ToStorageOptions(walSync);

        if (_options.Mvcc)
        {
            _mvccDb = await MvccStorage.OpenAsync(_dbPath, storageOptions);
        }
        else
        {
            _db = await LsmStorage.OpenAsync(_dbPath, storageOptions);
        }

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
        var db = _db;
        var mvccDb = _mvccDb;
        var entryBytes = _options.KeySize + _options.ValueSize;

        // Writes total exactly totalOps so the resulting database is well defined for later read
        // benchmarks; the work is partitioned across threads (sequential fills stay globally ordered).
        return RunParallelAsync(name, totalOps, partitioned: true, extra, (threadId, start, count, stats) =>
        {
            var keyGen = new KeyGenerator(_options.KeySize);
            var valueGen = new ValueGenerator(_options.Seed + threadId, _options.ValueSize, _options.CompressionRatio);
            var rng = RngStreams.Create(_options.Seed, threadId, RngStreams.Write);
            var key = new byte[_options.KeySize];
            var value = new byte[_options.ValueSize];
            var batchSize = _options.BatchSize;

            if (mvccDb != null)
            {
                MvccTransaction? tx = null;
                var staged = 0;

                return new ThreadWorker(op =>
                {
                    var keyIndex = sequential ? start + op : rng.NextInt64(totalOps);
                    keyGen.GenerateInto(keyIndex, key);
                    valueGen.GenerateInto(value);

                    tx ??= mvccDb.BeginTransaction();
                    tx.Put(key, value);
                    staged++;

                    if (staged == batchSize || op + 1 == count)
                    {
                        var commitTx = tx;
                        tx = null;
                        staged = 0;
                        var commitTask = commitTx.TryCommitAsync();
                        if (commitTask.IsCompletedSuccessfully)
                        {
                            commitTask.GetAwaiter().GetResult();
                            commitTx.Dispose();
                            stats.Ops++;
                            stats.ByteSlice += entryBytes;
                            return new ValueTask<bool>(true);
                        }
                        return AwaitCommitAsync(commitTask, commitTx, stats, entryBytes);
                    }

                    stats.Ops++;
                    stats.ByteSlice += entryBytes;
                    return new ValueTask<bool>(true);
                });
            }

            var batch = batchSize == 1 ? null : new LsmWriteBatch();

            return new ThreadWorker(op =>
            {
                var keyIndex = sequential ? start + op : rng.NextInt64(totalOps);
                keyGen.GenerateInto(keyIndex, key);
                valueGen.GenerateInto(value);

                if (batch is null)
                {
                    db!.Put(key, value);
                }
                else
                {
                    batch.Put(key, value);
                    if (batch.Count == batchSize || op + 1 == count)
                    {
                        db!.Write(batch);
                        batch.Clear();
                    }
                }

                stats.Ops++;
                stats.ByteSlice += entryBytes;
                return new ValueTask<bool>(true);
            });
        });
    }

    private static async ValueTask<bool> AwaitCommitAsync(ValueTask<bool> commitTask, MvccTransaction commitTx, ThreadStats stats, int entryBytes)
    {
        try
        {
            await commitTask;
        }
        finally
        {
            commitTx.Dispose();
        }
        stats.Ops++;
        stats.ByteSlice += entryBytes;
        return true;
    }

    private Task<BenchmarkResult> RunDeletesAsync(string name)
    {
        var db = _db;
        var mvccDb = _mvccDb;

        return RunParallelAsync(name, _options.Num, partitioned: true, extra: string.Empty, (threadId, start, count, stats) =>
        {
            var keyGen = new KeyGenerator(_options.KeySize);
            var rng = RngStreams.Create(_options.Seed, threadId, RngStreams.Write);
            var key = new byte[_options.KeySize];
            var batchSize = _options.BatchSize;

            if (mvccDb != null)
            {
                MvccTransaction? tx = null;
                var staged = 0;

                return new ThreadWorker(op =>
                {
                    keyGen.GenerateInto(rng.NextInt64(_options.Num), key);

                    tx ??= mvccDb.BeginTransaction();
                    tx.Delete(key);
                    staged++;

                    if (staged == batchSize || op + 1 == count)
                    {
                        var commitTx = tx;
                        tx = null;
                        staged = 0;
                        var commitTask = commitTx.TryCommitAsync();
                        if (commitTask.IsCompletedSuccessfully)
                        {
                            commitTask.GetAwaiter().GetResult();
                            commitTx.Dispose();
                            stats.Ops++;
                            stats.ByteSlice += _options.KeySize;
                            return new ValueTask<bool>(true);
                        }
                        return AwaitCommitAsync(commitTask, commitTx, stats, _options.KeySize);
                    }

                    stats.Ops++;
                    stats.ByteSlice += _options.KeySize;
                    return new ValueTask<bool>(true);
                });
            }

            var batch = batchSize == 1 ? null : new LsmWriteBatch();

            return new ThreadWorker(op =>
            {
                keyGen.GenerateInto(rng.NextInt64(_options.Num), key);

                if (batch is null)
                {
                    db!.Delete(key);
                }
                else
                {
                    batch.Delete(key);
                    if (batch.Count == batchSize || op + 1 == count)
                    {
                        db!.Write(batch);
                        batch.Clear();
                    }
                }

                stats.Ops++;
                stats.ByteSlice += _options.KeySize;
                return new ValueTask<bool>(true);
            });
        });
    }

    private Task<BenchmarkResult> RunReadsAsync(string name, bool missing)
    {
        var db = _db;
        var mvccDb = _mvccDb;

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

            return new ThreadWorker(op =>
            {
                // readmissing reads from a key range that was never written ([num, 2*num)).
                var keyIndex = rng.NextInt64(_options.Num) + (missing ? _options.Num : 0);
                keyGen.GenerateInto(keyIndex, key);
                var task = mvccDb != null
                    ? mvccDb.GetRawAsync(key, valueBuffer)
                    : db!.GetRawAsync(key.AsSpan(), valueBuffer);

                if (task.IsCompletedSuccessfully)
                {
                    var length = task.GetAwaiter().GetResult();
                    stats.Ops++;
                    stats.ByteSlice += _options.KeySize;
                    if (length >= 0)
                    {
                        stats.Found++;
                        stats.ByteSlice += length;
                    }
                    return new ValueTask<bool>(true);
                }

                return AwaitReadAsync(task, stats, _options.KeySize);
            });
        });

        static async ValueTask<bool> AwaitReadAsync(ValueTask<int> task, ThreadStats stats, int keySize)
        {
            var length = await task;
            stats.Ops++;
            stats.ByteSlice += keySize;
            if (length >= 0)
            {
                stats.Found++;
                stats.ByteSlice += length;
            }
            return true;
        }
    }

    private async Task<BenchmarkResult> RunReadSeqAsync(string name)
    {
        var db = _db;
        var mvccDb = _mvccDb;
        var threads = _options.Threads;
        var perThread = new ThreadStats[threads];
        var tasks = new Task[threads];
        var extra = _options.Histogram ? "(raw scan; histogram unavailable)" : string.Empty;

        var gcStats = Environment.GetEnvironmentVariable("SILEX_BENCH_GC") is { Length: > 0 };
        var allocBefore = gcStats ? GC.GetTotalAllocatedBytes(precise: false) : 0;
        var gen0Before = gcStats ? GC.CollectionCount(0) : 0;
        var gen1Before = gcStats ? GC.CollectionCount(1) : 0;
        var gen2Before = gcStats ? GC.CollectionCount(2) : 0;
        var restingMem = Environment.WorkingSet;

        var wall = Stopwatch.StartNew();

        // Each thread independently scans forward from the start, up to the Reads budget. The op loop stops
        // when the store is exhausted.
        var maxReads = _options.EffectiveReads;
        for (var t = 0; t < threads; t++)
        {
            var stats = new ThreadStats();
            perThread[t] = stats;

            tasks[t] = Task.Run(async () =>
            {
                var scanState = new ReadSeqScanState(stats);
                var sw = Stopwatch.StartNew();

                if (mvccDb != null)
                {
                    await mvccDb.ScanRawAsync(scanState, static (s, key, value) =>
                    {
                        s.Stats.Ops++;
                        s.Stats.Found++;
                        s.Stats.ByteSlice += key.Length + value.Length;
                        return true;
                    }, maxReads);
                }
                else
                {
                    await db!.ScanRawAsync(scanState, static (s, key, value) =>
                    {
                        s.Stats.Ops++;
                        s.Stats.Found++;
                        s.Stats.ByteSlice += key.Length + value.Length;
                        return true;
                    }, maxReads);
                }

                stats.ElapsedMicros = sw.Elapsed.TotalMicroseconds;
            });
        }

        await Task.WhenAll(tasks);
        wall.Stop();

        var peakMem = Math.Max(restingMem, Environment.WorkingSet);
        var result = new BenchmarkResult
        {
            Name = name,
            WallSeconds = wall.Elapsed.TotalSeconds,
            Extra = extra,
            RestingMemoryBytes = restingMem,
            PeakMemoryBytes = peakMem,
        };

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

    private async Task<BenchmarkResult> RunReadReverseAsync(string name)
    {
        var db = _db;
        var mvccDb = _mvccDb;
        var threads = _options.Threads;
        var perThread = new ThreadStats[threads];
        var tasks = new Task[threads];
        var extra = _options.Histogram ? "(cursor scan; histogram unavailable)" : string.Empty;

        var gcStats = Environment.GetEnvironmentVariable("SILEX_BENCH_GC") is { Length: > 0 };
        var allocBefore = gcStats ? GC.GetTotalAllocatedBytes(precise: false) : 0;
        var gen0Before = gcStats ? GC.CollectionCount(0) : 0;
        var gen1Before = gcStats ? GC.CollectionCount(1) : 0;
        var gen2Before = gcStats ? GC.CollectionCount(2) : 0;
        var restingMem = Environment.WorkingSet;

        var wall = Stopwatch.StartNew();
        var maxReads = _options.EffectiveReads;
        var batchBytes = Math.Max(256 * 1024, (_options.KeySize + _options.ValueSize) * 1024);

        for (var t = 0; t < threads; t++)
        {
            var stats = new ThreadStats();
            perThread[t] = stats;

            tasks[t] = Task.Run(async () =>
            {
                var sw = Stopwatch.StartNew();

                if (mvccDb != null)
                {
                    await mvccDb.ScanReverseRawAsync(stats, static (s, key, value) =>
                    {
                        s.Ops++;
                        s.Found++;
                        s.ByteSlice += key.Length + value.Length;
                        return true;
                    }, maxReads);
                }
                else
                {
                    await db!.ScanReverseRawAsync(stats, static (s, key, value) =>
                    {
                        s.Ops++;
                        s.Found++;
                        s.ByteSlice += key.Length + value.Length;
                        return true;
                    }, maxReads);
                }

                stats.ElapsedMicros = sw.Elapsed.TotalMicroseconds;
            });
        }

        await Task.WhenAll(tasks);
        wall.Stop();

        var peakMem = Math.Max(restingMem, Environment.WorkingSet);
        var result = new BenchmarkResult
        {
            Name = name,
            WallSeconds = wall.Elapsed.TotalSeconds,
            Extra = extra,
            RestingMemoryBytes = restingMem,
            PeakMemoryBytes = peakMem,
        };

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

    private Task<BenchmarkResult> RunScanBoundedAsync(string name, int skip, int limit, bool countOnly)
    {
        var db = _db;
        var mvccDb = _mvccDb;
        var maxEntries = countOnly
            ? long.MaxValue
            : (long)skip + limit;
        var totalScans = countOnly ? _options.EffectiveCountScans : _options.EffectiveScans;
        var extra = countOnly
            ? $"(full count scan, {totalScans:N0} scans/thread)"
            : skip > 0
                ? $"(skip={skip:N0}, limit={limit:N0}, {totalScans:N0} scans/thread)"
                : $"(limit={limit:N0}, {totalScans:N0} scans/thread)";

        return RunParallelAsync(name, totalScans, partitioned: false, extra, (threadId, start, count, stats) =>
        {
            var scanState = new BoundedScanState(stats, skip, limit, countOnly);

            return new ThreadWorker(op =>
            {
                scanState.Reset();

                var task = mvccDb != null
                    ? mvccDb.ScanRawAsync(scanState, static (s, key, value) => s.Accept(key, value), maxEntries)
                    : db!.ScanRawAsync(scanState, static (s, key, value) => s.Accept(key, value), maxEntries);

                if (task.IsCompletedSuccessfully)
                {
                    task.GetAwaiter().GetResult();
                    if (scanState.Collected > 0)
                    {
                        stats.Found++;
                    }
                    stats.Ops++;
                    return new ValueTask<bool>(true);
                }

                return AwaitScanAsync(task, scanState, stats);
            });
        });

        static async ValueTask<bool> AwaitScanAsync(ValueTask<long> task, BoundedScanState scanState, ThreadStats stats)
        {
            await task;
            if (scanState.Collected > 0)
            {
                stats.Found++;
            }
            stats.Ops++;
            return true;
        }
    }

    private sealed class BoundedScanState(ThreadStats stats, int skip, int limit, bool countOnly)
    {
        public ThreadStats Stats { get; } = stats;
        public int Seen { get; private set; }
        public int Collected { get; private set; }

        public void Reset()
        {
            Seen = 0;
            Collected = 0;
        }

        public bool Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (Seen < skip)
            {
                Seen++;
                return true;
            }

            Seen++;
            Collected++;
            Stats.ByteSlice += countOnly ? key.Length : key.Length + value.Length;
            return Collected < limit;
        }
    }

    private sealed class ReadSeqScanState(ThreadStats stats)
    {
        public ThreadStats Stats { get; } = stats;
    }

    private Task<BenchmarkResult> RunSeekRandomAsync(string name)
    {
        var db = _db;
        var mvccDb = _mvccDb;

        // One op == one seek; after landing we materialize up to SeekNexts following entries.
        return RunParallelAsync(name, _options.EffectiveReads, partitioned: false, extra: string.Empty, (threadId, start, count, stats) =>
        {
            var keyGen = new KeyGenerator(_options.KeySize);
            var rng = RngStreams.Create(_options.Seed, threadId, RngStreams.Seek);
            var toRead = 1 + _options.SeekNexts;
            var seekState = new SeekScanState(stats);
            var target = new byte[_options.KeySize];

            return new ThreadWorker(op =>
            {
                keyGen.GenerateInto(rng.NextInt64(_options.Num), target);
                seekState.Reset(target);

                var task = mvccDb != null
                    ? mvccDb.SeekRawAsync(target, seekState, static (s, key, value) =>
                    {
                        if (s.Read == 0 && key.SequenceEqual(s.Target))
                        {
                            s.Stats.Found++;
                        }

                        s.Stats.ByteSlice += key.Length + value.Length;
                        s.Read++;
                        return true;
                    }, toRead)
                    : db!.SeekRawAsync(target.AsSpan(), seekState, static (s, key, value) =>
                    {
                        if (s.Read == 0 && key.SequenceEqual(s.Target))
                        {
                            s.Stats.Found++;
                        }

                        s.Stats.ByteSlice += key.Length + value.Length;
                        s.Read++;
                        return true;
                    }, toRead);

                if (task.IsCompletedSuccessfully)
                {
                    task.GetAwaiter().GetResult();
                    stats.Ops++;
                    return new ValueTask<bool>(true);
                }

                return AwaitSeekAsync(task, stats);
            });
        });

        static async ValueTask<bool> AwaitSeekAsync(ValueTask<long> task, ThreadStats stats)
        {
            await task;
            stats.Ops++;
            return true;
        }
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

        public bool Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (Read == 0 && key.SequenceEqual(Target))
            {
                Stats.Found++;
            }

            Stats.ByteSlice += key.Length + value.Length;
            Read++;
            return true;
        }
    }

    /// <summary>
    /// Interleaves random writes with random point reads (or seeks) on one thread, so reads observe a
    /// memtable that changed since the previous read. <see cref="BenchmarkOptions.ReadWritePercent"/> of the
    /// operations are reads; the rest overwrite random keys in <c>[0, num)</c>, one write per commit.
    /// </summary>
    private Task<BenchmarkResult> RunReadWriteMixAsync(string name, bool seek)
    {
        var db = _db;
        var mvccDb = _mvccDb;
        var readPercent = _options.ReadWritePercent;
        var entryBytes = _options.KeySize + _options.ValueSize;
        var extra = $"(reads {readPercent}%, writes {100 - readPercent}%)";

        return RunParallelAsync(name, _options.EffectiveReads, partitioned: false, extra, (threadId, start, count, stats) =>
        {
            var keyGen = new KeyGenerator(_options.KeySize);
            var valueGen = new ValueGenerator(_options.Seed + threadId, _options.ValueSize, _options.CompressionRatio);
            // Neither the fill's nor the other mix's write stream: replaying one would only overwrite keys the
            // memtable already holds, so reads would never see a newly inserted key.
            var writeRng = RngStreams.Create(_options.Seed, threadId, seek ? RngStreams.SeekMixWrite : RngStreams.ReadMixWrite);
            var readRng = RngStreams.Create(_options.Seed, threadId, seek ? RngStreams.Seek : RngStreams.Read);
            var mixRng = RngStreams.Create(_options.Seed, threadId, RngStreams.Mix);
            var key = new byte[_options.KeySize];
            var value = new byte[_options.ValueSize];
            var valueBuffer = new byte[_options.ValueSize];
            var seekState = new SeekScanState(stats);

            return new ThreadWorker(op =>
            {
                if (mixRng.Next(100) >= readPercent)
                {
                    keyGen.GenerateInto(writeRng.NextInt64(_options.Num), key);
                    valueGen.GenerateInto(value);
                    stats.Ops++;
                    stats.ByteSlice += entryBytes;

                    if (mvccDb == null)
                    {
                        db!.Put(key, value);
                        return new ValueTask<bool>(true);
                    }

                    var tx = mvccDb.BeginTransaction();
                    tx.Put(key, value);
                    var commitTask = tx.TryCommitAsync();
                    if (commitTask.IsCompletedSuccessfully)
                    {
                        commitTask.GetAwaiter().GetResult();
                        tx.Dispose();
                        return new ValueTask<bool>(true);
                    }
                    return AwaitMixCommitAsync(commitTask, tx);
                }

                keyGen.GenerateInto(readRng.NextInt64(_options.Num), key);
                stats.Ops++;

                if (seek)
                {
                    seekState.Reset(key);
                    var seekTask = mvccDb != null
                        ? mvccDb.SeekRawAsync(key, seekState, static (s, k, v) => s.Accept(k, v), 1)
                        : db!.SeekRawAsync(key.AsSpan(), seekState, static (s, k, v) => s.Accept(k, v), 1);
                    if (seekTask.IsCompletedSuccessfully)
                    {
                        seekTask.GetAwaiter().GetResult();
                        return new ValueTask<bool>(true);
                    }
                    return AwaitMixSeekAsync(seekTask);
                }

                var readTask = mvccDb != null
                    ? mvccDb.GetRawAsync(key, valueBuffer)
                    : db!.GetRawAsync(key.AsSpan(), valueBuffer);
                if (readTask.IsCompletedSuccessfully)
                {
                    RecordRead(stats, readTask.GetAwaiter().GetResult(), _options.KeySize);
                    return new ValueTask<bool>(true);
                }
                return AwaitMixReadAsync(readTask, stats, _options.KeySize);
            });
        });

        static void RecordRead(ThreadStats stats, int length, int keySize)
        {
            stats.ByteSlice += keySize;
            if (length >= 0)
            {
                stats.Found++;
                stats.ByteSlice += length;
            }
        }

        static async ValueTask<bool> AwaitMixCommitAsync(ValueTask<bool> commitTask, MvccTransaction tx)
        {
            try
            {
                await commitTask;
            }
            finally
            {
                tx.Dispose();
            }
            return true;
        }

        static async ValueTask<bool> AwaitMixReadAsync(ValueTask<int> task, ThreadStats stats, int keySize)
        {
            RecordRead(stats, await task, keySize);
            return true;
        }

        static async ValueTask<bool> AwaitMixSeekAsync(ValueTask<long> task)
        {
            await task;
            return true;
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
        var workers = new ThreadWorker[threads];
        var counts = new long[threads];
        var tasks = new Task[threads];

        for (var t = 0; t < threads; t++)
        {
            var stats = new ThreadStats { Histogram = _options.Histogram ? new Histogram() : null };
            perThread[t] = stats;

            long start;
            long count;

            if (partitioned)
            {
                var baseCount = totalOps / threads;
                var remainder = totalOps % threads;
                count = baseCount + (t < remainder ? 1 : 0);
                start = t * baseCount + Math.Min(t, remainder);
            }
            else
            {
                start = 0;
                count = totalOps;
            }

            counts[t] = count;
            workers[t] = setup(t, start, count, stats);
        }

        // Capture process-wide allocation/GC counters around the benchmark body so a before/after run can
        // attribute allocation changes to a specific benchmark. Only reported when SILEX_BENCH_GC is set.
        var gcStats = Environment.GetEnvironmentVariable("SILEX_BENCH_GC") is { Length: > 0 };
        var allocBefore = gcStats ? GC.GetTotalAllocatedBytes(precise: false) : 0;
        var gen0Before = gcStats ? GC.CollectionCount(0) : 0;
        var gen1Before = gcStats ? GC.CollectionCount(1) : 0;
        var gen2Before = gcStats ? GC.CollectionCount(2) : 0;
        var restingMem = Environment.WorkingSet;
        var peakMem = restingMem;

        var wall = Stopwatch.StartNew();

        for (var t = 0; t < threads; t++)
        {
            var threadId = t;
            var stats = perThread[threadId];
            var worker = workers[threadId];
            var count = counts[threadId];

            tasks[t] = Task.Run(async () =>
            {
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
        peakMem = Math.Max(peakMem, Environment.WorkingSet);

        var result = new BenchmarkResult
        {
            Name = name,
            WallSeconds = wall.Elapsed.TotalSeconds,
            Extra = extra,
            RestingMemoryBytes = restingMem,
            PeakMemoryBytes = peakMem,
        };

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
        SampleRss(result.RestingMemoryBytes);
        SampleRss(result.PeakMemoryBytes);

        if (result.TotalOps == 0)
        {
            Console.WriteLine($"{result.Name,-14} : no operations");
            return;
        }

        var microsPerOp = result.SumThreadMicros / result.TotalOps;
        var opsPerSec = result.WallSeconds > 0 ? result.TotalOps / result.WallSeconds : 0;
        var mbPerSec = result.WallSeconds > 0 ? result.TotalBytes / 1_048_576.0 / result.WallSeconds : 0;

        var line = $"{result.Name,-14} : {microsPerOp,11:F3} micros/op {opsPerSec,12:N0} ops/sec; {mbPerSec,8:F1} MB/s";

        if (result.Name is "readrandom" or "readmissing" or "seekrandom" or "readrandomwriterandom" or "seekrandomwriterandom")
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
            Console.WriteLine($"{"",-14}   alloc: {result.AllocatedBytes / 1_048_576.0,8:F1} MB total, {bytesPerOp,8:F1} B/op; GC g0/g1/g2: {result.Gen0Collections}/{result.Gen1Collections}/{result.Gen2Collections}; RSS: {result.RestingMemoryBytes / 1_048_576.0:F1} -> {result.PeakMemoryBytes / 1_048_576.0:F1} MB");
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
        var modeDesc = _options.Mvcc
            ? $"MVCC (group_size={_options.MvccCommitGroupSize})"
            : "LSM";

        Console.WriteLine($"Silex DbBench");
        Console.WriteLine($"Mode:       {modeDesc}");
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
