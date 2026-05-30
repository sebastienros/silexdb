# Silex vs RocksDB `db_bench` — 2026-05-30

First cross-engine comparison using RocksDB's `db_bench` ported workloads against
`Silex.DbBench`. Captured for later reference / perf tracking.

## Setup

- **Host**: macOS, Apple Silicon, 14 cores
- **Threads**: 1 (single-threaded)
- **Dataset**: 1,000,000 entries, 16-byte keys, 100-byte values (~110 MB raw)
- **Compression**: OFF on both (fairness — Silex does not compress)
- **RocksDB**: built `db_bench` from `facebook/rocksdb` `main` (v11.4.0), release
  (`make db_bench DEBUG_LEVEL=0`), linked against Homebrew gflags. Run with
  `--compression_type=none`.
- **Silex**: `Silex.DbBench` (Release), tiered compaction, WAL on, default options.

> Caveat: the dataset (~110 MB) fits comfortably in memory, which favors Silex's
> in-memory-heavy path. RocksDB also pays for bloom filters, per-block checksums,
> and a more mature WAL that this micro-benchmark does not reward. These numbers
> favor Silex more than a larger-than-RAM workload would.

## Results — `fillseq` then reads over a fully-populated DB

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

- **Writes**: Silex is ~1.3–1.6× faster on `fillseq`/`fillrandom`/`overwrite` in
  this configuration.
- **Random reads**: Silex is faster here, but mostly because the working set fits
  in memory; RocksDB's bloom/checksum/WAL overhead is not amortized at this scale.
- **Sequential scans**: RocksDB wins decisively (~3× clean, up to ~22× under
  fragmentation). In the mixed pipeline (many SST sources) Silex `readseq`
  degraded to 6.9 µs/op while RocksDB stayed at ~0.3 µs/op. Each Silex scan
  rebuilds a merge iterator across all memtables + SSTs — the standout
  optimization target. A reusable / seekable iterator handle would help.

## Reproduce

Silex (clean read methodology):

```sh
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=fillseq,readrandom,readseq --num=1000000 --value_size=100 --threads=1
```

RocksDB (build once, then run):

```sh
# build db_bench from a rocksdb checkout (needs gflags; on macOS: brew install gflags)
make db_bench DEBUG_LEVEL=0 DISABLE_WARNING_AS_ERROR=1 \
  EXTRA_CXXFLAGS="-I$(brew --prefix gflags)/include" \
  EXTRA_LDFLAGS="-L$(brew --prefix gflags)/lib" -j14

./db_bench --benchmarks=fillseq,readrandom,readseq \
  --num=1000000 --value_size=100 --key_size=16 --threads=1 \
  --compression_type=none --db=/tmp/rocks-bench-db
```
