# Silex.DbBench

A standalone benchmark tool for the Silex LSM key/value store, modelled on RocksDB's
[`db_bench`](https://github.com/facebook/rocksdb/wiki/Benchmarking-tools). Flag names and benchmark
names match `db_bench` wherever an equivalent concept exists, so the same scenarios and parameters can be
run against both engines for comparison. A few Silex-specific knobs are added for tuning during perf work.

## Running

```bash
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=fillseq,readrandom,readseq \
  --num=1000000 --value_size=100 --key_size=16
```

### Build once, then run `dbbench`

`dotnet run` rebuilds and adds startup overhead on every invocation. For repeated
runs, build the project once — the output binary is named **`dbbench`** — and call
it directly:

```bash
# Build once (Release)
dotnet build Silex.DbBench -c Release

# Then run the compiled binary directly
./Silex.DbBench/bin/Release/net10.0/dbbench --benchmarks=fillrandom,readrandom --num=1000000
```

To type just `dbbench` from anywhere, put it on your `PATH` for the session or add a
shell alias:

```bash
# Option A: prepend the build output to PATH (current shell only)
export PATH="$PWD/Silex.DbBench/bin/Release/net10.0:$PATH"
dbbench --benchmarks=fillseq,readrandom --num=1000000

# Option B: a persistent alias (add to ~/.zshrc or ~/.bashrc)
alias dbbench="$PWD/Silex.DbBench/bin/Release/net10.0/dbbench"
```

For a self-contained, framework-independent binary you can copy elsewhere, publish it:

```bash
dotnet publish Silex.DbBench -c Release -o ./dist
./dist/dbbench --benchmarks=fillseq,readrandom --num=1000000
```

Use `--help` to list every flag (full output in [All flags](#all-flags) below).

### Examples

```bash
# Default run: fill 1M entries sequentially, then random + sequential reads
dotnet run --project Silex.DbBench -c Release

# Multi-threaded read throughput (8 concurrent reader threads)
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=fillrandom,readrandom --num=1000000 --threads=8

# Per-operation latency percentiles (avg / p50 / p95 / p99 / max)
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=readrandom --num=500000 --histogram

# Try a different compaction strategy and larger values, no WAL
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=fillrandom,overwrite,readseq --num=1000000 \
  --value_size=1024 --compaction=Leveled --wal=false

# Durability path: WAL fsync'd on every write
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=fillsync --num=100000 --wal_sync

# Reproducible run against a fixed database directory
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=fillseq,readrandom --num=1000000 --seed=42 --db=/tmp/silex-bench
```

### Performance investigation recipes

These runs keep the current RocksDB comparison gaps reproducible while working through point lookup,
block sizing, WAL cost and concurrency changes:

```bash
# Point lookup and seek latency, with cache effects visible in histograms
dbbench --benchmarks=fillseq,readrandom,seekrandom --num=1000000 --reads=200000 --histogram

# Block-size sweep; compare 4 KiB default with larger blocks that reduce index/cache fan-out
for block in 4096 8192 16384 32768; do
  dbbench --benchmarks=fillseq,readrandom --num=500000 --reads=200000 --block_size=$block
done

# Cold-cache read path, useful for separating cache hit rate from block/index lookup work
dbbench --benchmarks=fillseq,readrandom --num=500000 --reads=200000 --cache_size=0

# WAL overhead and fsync durability cost
dbbench --benchmarks=fillrandom --num=500000 --wal=true
dbbench --benchmarks=fillrandom --num=500000 --wal=false
dbbench --benchmarks=fillsync --num=100000 --wal_sync

# Concurrent write/read scaling
for threads in 1 2 4 8; do
  dbbench --benchmarks=fillrandom,readrandom --num=500000 --reads=100000 --threads=$threads
done
```

## Benchmarks (`--benchmarks`, comma-separated, run in order)

| Name           | Description                                                                 |
| -------------- | --------------------------------------------------------------------------- |
| `fillseq`      | Write `--num` entries in ascending key order (fresh database).              |
| `fillrandom`   | Write `--num` entries at random keys in `[0, num)` (fresh database).        |
| `fillsync`     | Write `num/1000` random entries with the WAL fsync'd on every write.        |
| `overwrite`    | Write `--num` random entries over the existing database.                    |
| `readrandom`   | Point-read `--reads` random keys; reports `(found N of M)`.                 |
| `readmissing`  | Point-read `--reads` keys that were never written (all miss).               |
| `readseq`      | Forward-scan the database via an iterator, up to `--reads` entries.         |
| `seekrandom`   | Seek to a random key and read `--seek_nexts`+1 entries; one op per seek.    |
| `deleterandom` | Delete `--num` random keys.                                                 |

`readreverse` is intentionally unsupported — Silex iterators are forward-only.

## Database lifecycle

`fillseq`, `fillrandom` and `fillsync` open a **fresh** database (the directory is wiped first unless
`--use_existing_db` is set), matching `db_bench`. Every other benchmark reuses the database left by the
preceding benchmark, so chains like `fillseq,readrandom,readseq` behave as expected.

## Threads

`--threads` runs that many concurrent client threads.

- **Write benchmarks** (`fillseq`, `fillrandom`, `overwrite`, `deleterandom`, `fillsync`) partition the
  total op count (`--num`) across the threads, so the resulting database always contains a well-defined
  number of entries for the read benchmarks that follow. `fillseq` gives each thread a disjoint contiguous
  key range, keeping the overall order ascending.
- **Read benchmarks** (`readrandom`, `readmissing`, `readseq`, `seekrandom`) give *each* thread the full
  `--reads` budget, so total work scales with the thread count — this measures concurrent read throughput.

Random key streams for reads are independent of the write streams (different mixed seeds), so found counts
reflect real key coverage rather than replaying the write sequence.

## Output

```
fillseq        :       1.234 micros/op      810,000 ops/sec;     92.1 MB/s
readrandom     :       0.950 micros/op    1,050,000 ops/sec;    116.0 MB/s (found 631,000 of 1,000,000)
```

`micros/op` is the summed per-thread busy time divided by total ops; `ops/sec` and `MB/s` use wall-clock
time. `--histogram` additionally prints avg / p50 / p95 / p99 / max latency per op.

## All flags

| Flag | Default | Description |
| ---- | ------- | ----------- |
| `--benchmarks` | `fillseq,readrandom,readseq` | Comma-separated benchmarks to run, in order. |
| `--num` | `1000000` | Number of key/value entries the database is filled with. |
| `--reads` | `-1` (= `--num`) | Number of read operations per thread. |
| `--value_size` | `100` | Size of each value in bytes. |
| `--key_size` | `16` | Size of each key in bytes (8–1024). |
| `--db` | temp folder | Database directory. |
| `--use_existing_db` | `false` | Reuse (do not wipe) the database directory for fill benchmarks. |
| `--seed` | `0` | Base RNG seed (each thread offsets it by its id); runs are reproducible. |
| `--histogram` | `false` | Collect and print per-operation latency percentiles. |
| `--threads` | `1` | Number of concurrent client threads. |
| `--write_buffer_size` | `67108864` (64 MiB) | Memtable size limit in bytes before it is frozen/flushed. |
| `--block_size` | `4096` | SSTable block size in bytes. |
| `--cache_size` | `8388608` (8 MiB) | Block cache size limit in bytes. |
| `--seek_nexts` | `0` | Entries read after each seek in `seekrandom`. |
| `--compaction` | `Tiered` | Compaction strategy: `None`, `Tiered` or `Leveled`. |
| `--wal` | `true` | Enable the write-ahead log (`--wal=false` to disable). |
| `--wal_sync` | `false` | fsync the WAL on every write. |
| `--target_sst_size` | `2097152` (2 MiB) | Target size of a compacted SSTable in bytes. |
| `--compaction_parallelism` | CPU count | Max degree of parallelism for leveled subcompactions. |
| `--read_parallelism` | CPU count | Max degree of parallelism for SST loading and L0 probing. |
| `--compression_type` | `lz4` | SST block compression: `none`, `lz4`, or `zstd`. |
| `--compression_level` | `0` | Fast LZ4/default Zstandard, or a codec-specific level. |
| `--compression_ratio` | `1` | Generated-value compressibility from 0 (highly compressible) to 1 (random). |
| `--batch_size` | `1` | Mutations per atomic write batch. |

`db_bench`-compatible flags use the same names so the same command line can drive both tools.
The `--compaction*`, `--wal*`, `--target_sst_size` and `--read_parallelism` flags are Silex-specific
tuning knobs.

### `--help`

```
Description:
  Silex DbBench — a RocksDB db_bench-style benchmark tool for the Silex LSM store.

Usage:
  silex-db-bench [options]

Options:
  --benchmarks <benchmarks>                          Comma-separated list: fillseq, fillrandom, fillsync, overwrite,
                                                     readrandom, readmissing, readseq, seekrandom, deleterandom.
                                                     [default: fillseq,readrandom,readseq]
  --num <num>                                        Number of key/value entries the database is filled with. [default:
                                                     1000000]
  --reads <reads>                                    Number of read operations per thread (defaults to --num).
                                                     [default: -1]
  --value_size <value_size>                          Size of each value in bytes. [default: 100]
  --key_size <key_size>                              Size of each key in bytes. [default: 16]
  --db <db>                                          Database directory (defaults to a temp folder).
  --use_existing_db                                  Reuse (do not wipe) the database directory for fill benchmarks.
  --seed <seed>                                      Base RNG seed (each thread offsets it by its id). [default: 0]
  --histogram                                        Collect and print per-operation latency percentiles.
  --threads <threads>                                Number of concurrent client threads. [default: 1]
  --write_buffer_size <write_buffer_size>            Memtable size limit in bytes before it is frozen/flushed.
                                                     [default: 67108864]
  --block_size <block_size>                          SSTable block size in bytes. [default: 4096]
  --cache_size <cache_size>                          Block cache size limit in bytes. [default: 8388608]
  --seek_nexts <seek_nexts>                          Entries read after each seek in seekrandom. [default: 0]
  --compaction <Leveled|None|Tiered>                 Compaction strategy: None, Tiered or Leveled. [default: Tiered]
  --wal                                              Enable the write-ahead log.
  --wal_sync                                         fsync the WAL on every write.
  --target_sst_size <target_sst_size>                Target size of a compacted SSTable in bytes. [default: 2097152]
  --compaction_parallelism <compaction_parallelism>  Max degree of parallelism for leveled subcompactions. [default: 14]
  --read_parallelism <read_parallelism>              Max degree of parallelism for SST loading and L0 probing.
                                                     [default: 14]
  --compression_type <compression_type>              SST block compression: none, lz4, or zstd. [default: lz4]
  --compression_level <compression_level>            Codec-specific compression level. Zero selects fast LZ4 or the
                                                     default Zstandard level. [default: 0]
  --compression_ratio <compression_ratio>            Approximate compressibility of generated values from 0 (highly
                                                     compressible) to 1 (random). [default: 1]
  --batch_size <batch_size>                          Mutations per atomic write batch. [default: 1]
  -?, -h, --help                                     Show help and usage information
  --version                                          Show version information
```

> The `--compaction_parallelism` / `--read_parallelism` defaults shown above are
> `14` because that was the CPU count on the machine that generated this output;
> they default to `Environment.ProcessorCount` on yours.

## Comparing against RocksDB

Silex supports `none`, `lz4`, and `zstd` compression. For a fair comparison, use the same
`--compression_type`, `--compression_level`, and `--compression_ratio` with both tools and set
`--batch_size=1` when comparing individual writes. Keys are generated
exactly like `db_bench`'s `GenerateKeyFromInt` (the integer is written big-endian in the first
`min(8, key_size)` bytes, the rest padded with `'0'`).

### Matched comparison commands

Run from the repository root in Bash. Build once and use fresh database paths for each repetition.
Set `ROCKSDB_BENCH` if the native executable is not at `~/github/rocksdb/db_bench`.

```bash
dotnet build Silex.DbBench/Silex.DbBench.csproj -c Release

silex="$PWD/Silex.DbBench/bin/Release/net10.0/dbbench"
rocksdb="${ROCKSDB_BENCH:-$HOME/github/rocksdb/db_bench}"
comparison_dir="$(mktemp -d "${TMPDIR:-/tmp}/silex-rocksdb.XXXXXX")"

common=(
  --num=200000 --reads=200000 --key_size=16 --value_size=100
  --threads=1 --seed=570123434 --batch_size=1
  --write_buffer_size=67108864 --block_size=4096 --cache_size=134217728
  --bloom_bits=10 --compression_type=none --compression_level=0 --compression_ratio=1
)
compaction=(--compaction_style=1)

# Sequential writes, two random-read passes, scan, and two random-seek passes.
DOTNET_TieredCompilation=0 "$silex" "${common[@]}" "${compaction[@]}" \
  --wal=true --wal_sync=false --db="$comparison_dir/silex-read" \
  --benchmarks=fillseq,readrandom,readrandom,readseq,seekrandom,seekrandom

"$rocksdb" "${common[@]}" "${compaction[@]}" \
  --disable_wal=false --sync=false --db="$comparison_dir/rocksdb-read" \
  --benchmarks=fillseq,flush,readrandom,readrandom,readseq,seekrandom,seekrandom

# Random writes and overwrites use separate databases from the read suite.
DOTNET_TieredCompilation=0 "$silex" "${common[@]}" "${compaction[@]}" \
  --wal=true --wal_sync=false --db="$comparison_dir/silex-write" \
  --benchmarks=fillrandom,overwrite

"$rocksdb" "${common[@]}" "${compaction[@]}" \
  --disable_wal=false --sync=false --db="$comparison_dir/rocksdb-write" \
  --benchmarks=fillrandom,overwrite
```

Use three repetitions, creating a new `comparison_dir` each time, and alternate engine order:
Silex/RocksDB, RocksDB/Silex, Silex/RocksDB. Report per-workload medians. Both engines have WAL enabled
without per-write synchronization. The 200,000-entry dataset fits in memory; the first read pass is not
a guaranteed cold OS-cache measurement.

RocksDB's explicit `flush` makes the read phases SST-backed, matching Silex's automatic read barrier.
Verify every read/seek reports all requested keys found; random writes alone do not guarantee full
key coverage. These commands exercise the plain LSM API, not the MVCC transaction comparison.

For explicitly matched tiered/universal tuning, replace the `compaction` assignment above with:

```bash
compaction=(
  --compaction_style=1 --universal_max_read_amp=8
  --universal_max_size_amplification_percent=200 --universal_size_ratio=1
  --universal_min_merge_width=2
)
```

For a leveled comparison, instead use the following assignment and fresh database paths:

```bash
compaction=(
  --compaction_style=0 --level0_file_num_compaction_trigger=4 --num_levels=7
  --max_bytes_for_level_base=262144 --max_bytes_for_level_multiplier=10
  --target_file_size_base=2097152
)
```

Recorded comparison runs (with methodology notes) live in
[`benchmarks/`](benchmarks/) — e.g. [`2026-05-30-rocksdb-comparison.md`](benchmarks/2026-05-30-rocksdb-comparison.md).
