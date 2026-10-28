# Engine comparison harness

Paired adapters that run the same workloads against Silex and two native engines, and the runner that
produced the comparison tables in the repository [README](../../README.md#comparisons-with-surrealkv-and-rocksdb).

| Adapter | Engine | Paired with | Folder |
|---|---|---|---|
| `silex-lsm` | Silex `LsmStorage` | `rocksdb` | [`silex/`](silex/) |
| `silex-mvcc` | Silex `MvccStorage` | `surrealkv` | [`silex/`](silex/) |
| `rocksdb` | RocksDB plain `DB` (C++) | `silex-lsm` | [`rocksdb/`](rocksdb/) |
| `surrealkv` | SurrealKV transactions (Rust) | `silex-mvcc` | [`surrealkv/`](surrealkv/) |

## Running

```bash
# Build the adapters (RocksDB needs a source checkout with `make static_lib` already run).
dotnet build tests/Silex.EngineComparison/silex -c Release
ROCKSDB_DIR=~/github/rocksdb tests/Silex.EngineComparison/rocksdb/build.sh
cargo build --release --manifest-path tests/Silex.EngineComparison/surrealkv/Cargo.toml

# Run every pair, thread count, suite and repetition, then print the README tables.
python3 tests/Silex.EngineComparison/run.py --out /tmp/engine-comparison
python3 tests/Silex.EngineComparison/run.py --render /tmp/engine-comparison
```

`run.py --help` lists the filters (`--pairs`, `--threads`, `--suites`, `--reps`) and lets one adapter be
replaced by another build, for example Silex compiled from an older commit:

```bash
python3 tests/Silex.EngineComparison/run.py --out /tmp/silex-old --engines silex-lsm,silex-mvcc \
  --label-suffix @old --adapter silex-lsm=/path/to/old/silex-bench --adapter silex-mvcc=/path/to/old/silex-bench
python3 tests/Silex.EngineComparison/run.py --render /tmp/engine-comparison /tmp/silex-old \
  --columns silex-lsm@old,silex-lsm,silex-mvcc@old,silex-mvcc
```

On macOS the runner waits before each run until the CPU is at least 85% idle (`--min-idle`), and records
the idle level and load average with each result, so other work on the machine does not skew a run.

## Adapter contract

Every adapter is a command-line program:

```
<adapter> --engine <silex-lsm|silex-mvcc|rocksdb|surrealkv> --suite <suite> --threads <T> --dir <path>
```

`--dir` is an empty directory the adapter owns for the run. One process runs one suite and exits.
Each timed phase prints one JSON line to standard output, and the suite ends with one memory line:

```json
{"type":"result","engine":"rocksdb","suite":"write","threads":8,"phase":"sequential-writes","unit":"ops/s","work":50000,"seconds":0.0512,"check":50000}
{"type":"memory","engine":"rocksdb","suite":"write","threads":8,"rss_bytes":24215552,"peak_rss_bytes":24346624}
```

- `work` is the completed work in `unit` terms (operations, entries, or scans), and throughput is
  `work / seconds`. A phase's time runs from releasing every thread together to the last thread finishing.
- `check` is a correctness count (below) that the runner compares with the expected value.
- `rss_bytes` is the process resident size sampled after the last timed phase and before closing the
  database (`proc_pidinfo(PROC_PIDTASKINFO).pti_resident_size` on macOS). `peak_rss_bytes` is
  `getrusage(RUSAGE_SELF).ru_maxrss` read after the database is closed (bytes on macOS), so it covers
  setup, flushing and shutdown.
- Anything else (diagnostics) goes to standard error.

## Data

- `N = 50,000` keys. Key `i` is the 16 ASCII decimal digits of `i`, zero-padded (`0000000000000042`), so
  byte order equals numeric order.
- Values are 100 bytes. Value `i` of generation `g` has byte `j = (i + j + 31 * g) mod 256`.
- Random numbers come from SplitMix64, identical in every adapter:

  ```
  state += 0x9E3779B97F4A7C15
  z = state; z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9; z = (z ^ (z >> 27)) * 0x94D049BB133111EB
  return z ^ (z >> 31)
  ```

  A per-thread stream for phase `p` and thread `t` starts at `state = p * 1_000_003 + t`. A random key index
  is `next() mod range`.
- A permutation for phase `p` is a Fisher–Yates shuffle of `0..N-1` with one stream starting at
  `state = p`: for `i` from `N-1` down to `1`, swap `perm[i]` with `perm[next() mod (i + 1)]`.
- Partitioning `M` operations across `T` threads gives thread `t` the indexes `[t*M/T, (t+1)*M/T)`
  (integer division).

## Engine configuration

| Setting | Silex | RocksDB | SurrealKV |
|---|---|---|---|
| Memtable | `MemTableSizeLimit` 64 MiB | `write_buffer_size` 64 MiB | max memtable size 64 MiB |
| Block cache | `BlockCacheSizeLimit` 8 MiB | LRU cache 8 MiB | block cache 8 MiB |
| Compression | `SstCompression.None` | `kNoCompression` | none |
| WAL | on; synchronized only in `sync` | on; `WriteOptions.sync` only in `sync` | on; `Durability::Immediate` only in `sync` |
| Filters | default Bloom filter | 10 bits/key Bloom filter | default |
| Other | MVCC: `MvccCommitGroupSize = T` | defaults | versioning (historical retention) off |

Everything else is each engine's default.

## Operations

- **Plain engines** (`silex-lsm`, `rocksdb`) write with direct puts and deletes and read the current value.
  RocksDB's snapshot and transaction APIs are not used.
- **Transactional engines** (`silex-mvcc`, `surrealkv`): each write is its own transaction (begin, one
  put or delete, commit), retried from a new transaction on a conflict, which disjoint partitions never
  cause. A point read or seek in SurrealKV runs in its own read transaction, and a scan in one read
  transaction per scan. Silex MVCC reads through `MvccStorage` directly, which reads the latest published
  sequence per call.
- **Driving:** each worker issues its operations one after another, never overlapping its own requests.
  Synchronous APIs run on one OS thread per worker. Awaited commits run the way each engine's async API
  is meant to be used: Silex MVCC workers are thread-pool tasks, and SurrealKV workers are tokio tasks on
  a multi-threaded runtime with exactly `T` worker threads. That keeps a commit's hand-off to SurrealKV's
  flusher on the committing thread (a plain thread blocking on each commit measured about 4.5× slower at
  one thread). Blocking a dedicated thread on each Silex commit measured the same at one thread and
  1.5–1.8× slower at 8 threads.
- A **seek** positions at the first key `>= target` and reads that entry. A **scan** reads key and value
  of each entry it visits (the adapter must touch both, for example by adding their lengths). **Skipping**
  and **counting** advance without reading values.

## Suites

Each suite opens a fresh, empty database in `--dir`, runs its phases in order, prints its memory line,
and closes the database. Phases that write use the thread count `T`; for reads, every thread does the
full amount, so total work is `T` times the per-thread amount.

| Suite | Setup (untimed) | Phases (`p`: phase id) |
|---|---|---|
| `write` | none | `sequential-writes` (1), `overwrites` (2), `random-deletes` (3) |
| `randomfill` | none | `random-writes` (4) |
| `sync` | none | `sync-writes` (5) |
| `read-memtable` | load | phases 10–18 below, data in the memtable |
| `read-sst` | load, then flush and compact | phases 10–18 below, data in SSTs |
| `mixed` | load | `read-write-90-10` (20) |

**Load:** one thread writes keys `0..N-1` in ascending order with generation 0 values, outside any timing.
For `read-sst` the engine then flushes its memtable to SSTs and waits for compaction to finish (Silex
`FlushAndCompactAsync`, RocksDB `Flush` and `CompactRange` over everything). SurrealKV 1.0.0 has no
public flush or compaction call, so its adapter calls `create_checkpoint`, which flushes every memtable
to level-0 SSTs before copying them, and deletes the copy. The 50,000 entries become one SST in every
engine; RocksDB moves it to its last level, SurrealKV keeps it in level 0.

| Phase | Work per thread | Unit | `check` (summed over threads) |
|---|---|---|---|
| `sequential-writes` (1) | partition of keys `0..N-1` ascending, generation 0 | ops/s | operations: `N` |
| `overwrites` (2) | partition of permutation 2, generation 1 | ops/s | `N` |
| `random-deletes` (3) | partition of permutation 3, delete | ops/s | `N` |
| `random-writes` (4) | partition of permutation 4, generation 0 | ops/s | `N` |
| `sync-writes` (5) | partition of keys `0..49` ascending, synchronized | ops/s | `50` |
| `random-reads-first` (10) | 50,000 point reads of `next() mod N` | ops/s | found: `50,000 T` |
| `random-reads-warm` (11) | same, phase 11 stream | ops/s | `50,000 T` |
| `random-seeks-first` (12) | 50,000 seeks to `next() mod N` | ops/s | first key equals target: `50,000 T` |
| `random-seeks-warm` (13) | same, phase 13 stream | ops/s | `50,000 T` |
| `forward-scan` (14) | one scan of every entry, ascending | entries/s | entries: `N T` |
| `reverse-scan` (15) | one scan of every entry, descending | entries/s | `N T` |
| `scan-100` (16) | 5,000 scans: seek to `next() mod (N - 100)`, read 100 entries | scans/s | entries read: `500,000 T` |
| `skip-5000-scan-100` (17) | 5,000 scans: seek to `next() mod (N - 5,100)`, skip 5,000, read 100 | scans/s | entries read after skipping: `500,000 T` |
| `count-all` (18) | 50 counts of every entry | scans/s | entries counted: `2,500,000 T` |
| `read-write-90-10` (20) | 50,000 operations; for each, `next() mod 100 < 90` reads key `next() mod N`, otherwise overwrites key `next() mod N` with generation 1 | ops/s | reads found plus writes: `50,000 T` |

## Runner

`run.py` runs every pair, thread count (1 and 8), suite and repetition (3) as fresh processes, alternating
which engine of a pair runs first in each repetition. It rejects any run whose `check` differs from the
expected value, and reports medians. Memory cells are RSS / peak RSS of the indicated suite in MiB.
