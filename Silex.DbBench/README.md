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

Use `--help` to list every flag.

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

## Key flags

`db_bench`-compatible: `--benchmarks`, `--num`, `--reads`, `--value_size`, `--key_size`, `--db`,
`--use_existing_db`, `--seed`, `--histogram`, `--threads`, `--write_buffer_size`, `--block_size`,
`--cache_size`, `--seek_nexts`.

Silex-specific: `--compaction` (`None`|`Tiered`|`Leveled`), `--wal`, `--wal_sync`, `--target_sst_size`,
`--compaction_parallelism`, `--read_parallelism`.

## Comparing against RocksDB

Silex stores values **uncompressed** and has no write-batch API, so for a fair comparison run RocksDB's
`db_bench` with compression disabled (`--compression_type=none`) and `--batch_size=1`. Keys are generated
exactly like `db_bench`'s `GenerateKeyFromInt` (the integer is written big-endian in the first
`min(8, key_size)` bytes, the rest padded with `'0'`). `--compression_type`, `--compression_ratio` and a
non-1 `--batch_size` are accepted but ignored, with a warning.
