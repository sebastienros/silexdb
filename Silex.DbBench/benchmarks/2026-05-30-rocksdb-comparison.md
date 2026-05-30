# Silex vs RocksDB `db_bench` — 2026-05-30

Cross-engine comparison using RocksDB's `db_bench` ported workloads against
`Silex.DbBench`. Captured for later reference / perf tracking.

## Latest rerun setup

- **Host**: macOS, Apple Silicon, 14 cores
- **Threads**: 1 (single-threaded)
- **Dataset**: 200,000 entries, 16-byte keys, 100-byte values (~22 MB raw)
- **Compression**: OFF on both (fairness — Silex does not compress)
- **RocksDB**: built `db_bench` from `facebook/rocksdb` `main` (v11.4.0), release
  (`make db_bench DEBUG_LEVEL=0`), linked against Homebrew gflags. Run with
  `--compression_type=none`.
- **Silex**: `Silex.DbBench` (Release), tiered compaction, WAL on, default options.

> Caveat: the dataset (~22 MB) fits comfortably in memory, which favors Silex's
> in-memory-heavy path. RocksDB also pays for bloom filters, per-block checksums,
> and a more mature WAL that this micro-benchmark does not reward. These numbers
> favor Silex more than a larger-than-RAM workload would.

## Results — post-block-cache targeted rerun

The custom decoded-block LRU cache keeps correctness under churn and removes most cache-hit overhead.
This run uses a 128 MiB Silex block cache and runs `readrandom` twice so the second pass measures the
warm cache-hit path.

| Benchmark        | Silex                                | RocksDB                              | Result                 |
|------------------|--------------------------------------|--------------------------------------|------------------------|
| fillseq          | 1.186 µs/op · 831K ops/s · 92.0 MB/s | 1.407 µs/op · 711K ops/s · 78.7 MB/s | **Silex ~1.19×**       |
| readrandom first | 1.515 µs/op · 660K ops/s · 73.0 MB/s | 0.614 µs/op · 1.63M ops/s · 180 MB/s | **RocksDB ~2.5×**      |
| readrandom warm  | 0.631 µs/op · 1.58M ops/s · 175 MB/s | 0.556 µs/op · 1.80M ops/s · 199 MB/s | **RocksDB ~1.13×**     |

The default 8 MiB cache still churns on the 200K-key random workload, but remains correct; the targeted
default-cache run found 200,000 of 200,000 keys at 5.474 µs/op.

## Results — post miss-path allocation rerun

Two further no-allocation changes on the read path:

1. **No double-buffer copy on a cache miss.** `SsTable.ReadBlockAsync` now reads straight into the
   `MemoryPool`-owned buffer that backs the decoded block, and `DefaultBlockEncoder.Decode(IMemoryOwner, length)`
   scans the offset section in place. This removes one pooled rent and a full ~4 KB block memcpy per miss.
2. **No per-read closure.** `BlockCache.GetOrLoadAsync` now takes a generic `struct` loader instead of a
   `Func<Task<Block?>>`, so every read — cache hit included — allocates no delegate/closure.

Same setup (128 MiB cache, `readrandom` twice), median of repeated runs:

| Benchmark        | Silex (before) | Silex (after)                        | RocksDB                              | Result             |
|------------------|----------------|--------------------------------------|--------------------------------------|--------------------|
| fillseq          | 1.186 µs/op    | 1.203 µs/op · 815K ops/s · 90.2 MB/s | 1.407 µs/op · 711K ops/s · 78.7 MB/s | **Silex ~1.17×**   |
| readrandom first | 1.515 µs/op    | 1.52 µs/op · 658K ops/s · 72.8 MB/s  | 0.614 µs/op · 1.63M ops/s · 180 MB/s | **RocksDB ~2.5×**  |
| readrandom warm  | 0.631 µs/op    | 0.50 µs/op · 2.0M ops/s · 222 MB/s   | 0.556 µs/op · 1.80M ops/s · 199 MB/s | **Silex ~1.11×**   |

- **Warm path now beats RocksDB** (~0.50 vs 0.556 µs/op) — removing the per-read closure allocation cut
  the cache-hit path by ~15–20%.
- **Cold (first) path is unchanged within noise.** The copy elimination removes allocations/GC pressure but
  the first-pass latency is dominated by the async positioned read (`RandomAccess.ReadAsync`) and block
  decode, not the copy. Closing the remaining cold gap needs a synchronous-read fast path (proposed but not
  yet implemented, since it trades multi-reader concurrency for single-read latency).

## Historical results — post-lookup-optimization targeted rerun

Apples-to-apples: both engines hold all 200K keys, and the read/seek workloads both find
100,000 of 100,000 requested keys.

| Benchmark  | Silex                                | RocksDB                              | Winner             |
|------------|--------------------------------------|--------------------------------------|--------------------|
| fillseq    | 1.233 µs/op · 796K ops/s · 88.0 MB/s | 1.435 µs/op · 697K ops/s · 77.1 MB/s | **Silex ~1.16×**   |
| readrandom | 6.132 µs/op · 163K ops/s · 18.0 MB/s | 0.625 µs/op · 1.60M ops/s · 177 MB/s | **RocksDB ~9.8×**  |
| seekrandom | 9.570 µs/op · 104K ops/s · 11.6 MB/s | 0.880 µs/op · 1.14M ops/s            | **RocksDB ~10.9×** |

Silex `readrandom` improved from the earlier 200K clean-read baseline of 45.304 µs/op to
6.132 µs/op after replacing the per-table block metadata scan with binary search, but
RocksDB still has a large point/seek lookup advantage.

## Historical results — `fillseq` then reads over a fully-populated 1M-key DB

Apples-to-apples: both engines hold all 1M keys, both find 1,000,000 of 1,000,000.

| Benchmark  | Silex                              | RocksDB                            | Winner          |
|------------|------------------------------------|------------------------------------|-----------------|
| fillseq    | 5.54 µs/op · 180K ops/s · 20.0 MB/s | 8.73 µs/op · 115K ops/s · 12.7 MB/s | **Silex ~1.6×** |
| readrandom | 1.18 µs/op · 846K ops/s · 93.6 MB/s | 6.34 µs/op · 158K ops/s · 17.5 MB/s | **Silex ~5×**   |
| readseq    | 1.29 µs/op · 775K ops/s · 85.7 MB/s | 0.40 µs/op · 2.49M ops/s · 276 MB/s | **RocksDB ~3×** |

## Results — mixed write pipeline (`fillseq,fillrandom,overwrite,readrandom,readseq`)

Note: the two tools differ in lifecycle here. `Silex.DbBench` wipes the DB before
each *fill* benchmark, so its read benchmarks run over the most recent random-fill
(~63% key-space coverage). `db_bench` keeps one DB growing across the list. The
write rows below are directly comparable; the read rows are not (different coverage).

| Benchmark  | Silex                               | RocksDB                              |
|------------|-------------------------------------|--------------------------------------|
| fillseq    | 7.29 µs/op · 137K ops/s · 15.1 MB/s | 8.91 µs/op · 112K ops/s · 12.4 MB/s  |
| fillrandom | 8.85 µs/op · 113K ops/s · 12.5 MB/s | 11.19 µs/op · 89K ops/s · 9.9 MB/s   |
| overwrite  | 8.08 µs/op · 124K ops/s · 13.7 MB/s | 11.37 µs/op · 88K ops/s · 9.7 MB/s   |
| readrandom | 2.19 µs/op (found 631,850/1M)       | 7.16 µs/op (found 864,657/1M)        |
| readseq    | 6.91 µs/op · 16.0 MB/s              | 0.31 µs/op · 359 MB/s                |

## Takeaways

- **Writes**: Silex remains faster on the latest `fillseq` rerun and was also
  faster on the historical `fillseq`/`fillrandom`/`overwrite` runs.
- **Point lookups**: the block-metadata binary search and custom decoded-block
  cache reduced Silex warm random-read latency to near RocksDB parity when the
  working set fits in cache. The first pass and small-cache/churn cases remain slower.
- **Seeks**: the latest seek rerun still favors RocksDB by roughly 11×; seekrandom
  needs separate iterator/path work.
- **Sequential scans**: historical data shows RocksDB wins decisively (~3× clean, up to ~22× under
  fragmentation). In the mixed pipeline (many SST sources) Silex `readseq`
  degraded to 6.9 µs/op while RocksDB stayed at ~0.3 µs/op. Each Silex scan
  rebuilds a merge iterator across all memtables + SSTs — the standout
  optimization target. A reusable / seekable iterator handle would help.

## Reproduce

Silex (clean read methodology):

```sh
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=fillseq,readrandom,readrandom --num=200000 --reads=200000 \
  --value_size=100 --threads=1 --cache_size=134217728
```

RocksDB (build once, then run):

```sh
# build db_bench from a rocksdb checkout (needs gflags; on macOS: brew install gflags)
make db_bench DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1 \
  EXTRA_CXXFLAGS="-I$(brew --prefix gflags)/include" \
  EXTRA_LDFLAGS="-L$(brew --prefix gflags)/lib" -j14

./db_bench --benchmarks=fillseq,readrandom,seekrandom \
  --num=200000 --reads=100000 --value_size=100 --key_size=16 --threads=1 \
  --compression_type=none --db=/tmp/rocks-bench-db
```

---

## Multi-threaded thread sweep (1 / 4 / 8 threads)

Same host/dataset (1M entries, 100-byte values, compression off).
`--benchmarks=fillseq,fillrandom,readrandom`.

> **Methodology difference — read carefully.** The two tools partition work
> differently:
> - **Silex.DbBench**: *writes* are partitioned (total ops = `--num` regardless
>   of thread count, split into disjoint ranges); *reads* are NOT partitioned
>   (each thread runs the full `--num` budget, so total reads = `threads × num`).
> - **RocksDB db_bench**: every thread runs `--num` ops for *all* benchmarks, so
>   total ops = `threads × num` for both writes and reads.
>
> Therefore compare **ops/sec and MB/s (throughput)**, not total operations. The
> Silex write benchmark does a *fixed* total amount of work regardless of threads,
> so any throughput change is pure concurrency overhead/scaling.

### Throughput (ops/sec)

| Benchmark  | Engine  | 1 thread | 4 threads | 8 threads | Scaling 1→8 |
|------------|---------|----------|-----------|-----------|-------------|
| fillrandom | Silex   | 165,775  | 94,259    | 96,462    | 0.58× (degrades) |
| fillrandom | RocksDB | 84,138   | 81,874    | 106,740   | 1.27×       |
| readrandom | Silex   | 716,674  | 1,891,737 | 2,319,771 | **3.24×**   |
| readrandom | RocksDB | 98,926   | 536,826   | 534,250   | 5.40× (plateaus at 4) |

### Throughput (MB/s)

| Benchmark  | Engine  | 1 thread | 4 threads | 8 threads |
|------------|---------|----------|-----------|-----------|
| fillrandom | Silex   | 18.3     | 10.4      | 10.7      |
| fillrandom | RocksDB | 9.3      | 9.1       | 11.8      |
| readrandom | Silex   | 54.1     | 142.9     | 175.2     |
| readrandom | RocksDB | 6.9      | 58.3      | 59.1      |

### Takeaways

- **Reads scale on both, Silex stays well ahead in absolute throughput** at this
  in-memory dataset size: Silex hits ~2.3M ops/s (175 MB/s) at 8 threads vs
  RocksDB ~534K ops/s (59 MB/s). RocksDB read throughput plateaus past 4 threads
  here; Silex keeps gaining but sub-linearly (3.2× over 8 threads).
- **Writes do not parallelize on Silex** — the single global writer lock means
  more threads only add contention, so throughput *drops* (166K → 96K ops/s) even
  though the total write work is fixed. This is expected and by design (single
  writer, parallelism is in compaction + read path, not the write lock).
- **RocksDB writes are roughly flat** (~84K → 107K ops/s) — WAL/memtable is the
  bottleneck, but it does not degrade with more threads.
- Net: Silex's threading story is "scale reads, serialize writes." If concurrent
  write throughput becomes a goal, the global writer lock is the thing to revisit
  (e.g. sharded memtables or a concurrent skip-list write path).
