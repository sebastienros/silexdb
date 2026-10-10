# Durable MVCC group commit measurements

Measured on 2026-10-10 with an Apple M4 Pro (14 cores), macOS 15.8.1, local
temporary-directory storage, and .NET 8.0.30 for the MVCC benchmark. Release
builds used SDK 11.0.100-rc.1.26425.128. These are local file-content flush
measurements, not a certification of device power-loss behavior.

## Baseline and implementation choice

The starting point was origin/main `d1654f968188540adcf7d4a2319970477b9e8bac`.
Existing MVCC commits already used serialized validation/publication and one
atomic WAL frame per transaction. No application batching was substituted for
engine-owned group commit.

Before changing the engine, the existing DbBench tool measured:

```sh
dotnet build Silex.DbBench -c Release
./Silex.DbBench/bin/Release/net10.0/dbbench --benchmarks=fillrandom \
  --num=10000 --wal_sync --threads=1 --compaction=None --histogram
./Silex.DbBench/bin/Release/net10.0/dbbench --benchmarks=fillrandom \
  --num=10000 --threads=1 --compaction=None --histogram
```

| Plain LSM configuration | Operations/sec | Mean latency | p50 | p95 |
| --- | ---: | ---: | ---: | ---: |
| Synchronous WAL | 240 | 4,171.802 us | 4,033.200 us | 5,017.700 us |
| OS-written WAL, no disk synchronization | 423,950 | 1.631 us | 1.300 us | 1.800 us |

These use .NET 10.0.11, 16-byte keys, 100-byte values, one mutation per write.
The roughly 4 ms synchronous-write cost, rather than mutation preparation,
dominates this workload. The MVCC probe added to the existing WAL benchmark
project then measured **237.4 transactions/sec with eight clients** (1,000
transactions) and **232.9 with one client** (250 transactions), before the engine
changes. More concurrent clients alone did not amortize synchronization.

That supports a bounded, opt-in ready queue, without a collection timer or
parallel validation/application. The queue shares synchronization, not recovery
frames. One ready transaction needs no deliberate wait for partners.

## Reproduction

The existing BenchmarkDotNet frame/batch benchmarks remain the default mode.
The additional standalone probe measures complete MVCC transaction preparation
and durable acknowledgment:

```sh
dotnet build tests/Silex.WalBenchmarks -c Release
dotnet run --project tests/Silex.WalBenchmarks -c Release --no-build -- --mvcc 1 500 1
dotnet run --project tests/Silex.WalBenchmarks -c Release --no-build -- --mvcc 1 500 8
dotnet run --project tests/Silex.WalBenchmarks -c Release --no-build -- --mvcc 8 250 1
dotnet run --project tests/Silex.WalBenchmarks -c Release --no-build -- --mvcc 8 250 8
```

Arguments are clients, transactions per client, and optional maximum group size
(default 1). Each client has one outstanding transaction, writes its own
16-byte key with a 100-byte value, and awaits success before starting another
transaction. Keys are disjoint so the workload measures durability, not conflict
retries. Each run creates and removes its own database. WAL synchronization is
enabled, periodic flushing disabled, and the memtable threshold set to
`long.MaxValue`. Opening and closing are outside the timing interval. Wall time
measures aggregate throughput; per-transaction samples measure preparation
through acknowledgment.

## Final runs

| Clients | Maximum group | Transactions | Seconds | Transactions/sec | p50 ms | p95 ms | p99 ms | Max ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 500 | 2.214 | 225.9 | 4.143 | 5.147 | 6.204 | 13.066 |
| 1 | 8 | 500 | 2.075 | 240.9 | 4.023 | 4.959 | 5.981 | 17.585 |
| 8 | 1 | 2,000 | 8.493 | 235.5 | 33.002 | 39.877 | 40.994 | 43.931 |
| 8 | 8 | 2,000 | 2.141 | 934.0 | 8.062 | 10.036 | 11.033 | 17.309 |
| 8 | 1 | 2,000 | 8.473 | 236.1 | 32.988 | 39.906 | 41.139 | 48.843 |
| 8 | 8 | 2,000 | 2.453 | 815.3 | 9.970 | 11.039 | 12.030 | 17.344 |

The two repeated eight-client comparisons show **3.45-3.97x throughput**, with
p95 acknowledgment latency reduced from about 40 ms to 10-11 ms. A separate
2,000-transaction sweep measured group size 1 at 215.9 transactions/sec,
size 8 at 859.2, and size 32 at 856.1. A larger cap did not help this eight-client
workload, so there is no evidence to justify a timer, more parallelism, or a
larger default. Grouping remains disabled by default.

## Limits and correctness checks

- Short local runs have scheduler, JIT, filesystem, and other-host-work variance;
  there is no warmup or statistical confidence interval. Single-client differences
  are not evidence of a speedup: no synchronization is shared in that case.
- The maximum group is a cap, not a promised batch size. Ready-queue scheduling
  can produce smaller groups. Actual flush time is device/filesystem dependent;
  measurements do not extrapolate to Linux, Windows, remote storage, or long runs.
- The benchmark does not measure conflict-heavy transactions, large mutation
  buffers, compaction, GC pressure, or per-request allocation. Opt-in commits
  allocate request/completion objects; batch arrays and frame buffers are reused.
  The pre-existing allocation-free ordinary raw-write tests remain enabled.
- Group cardinality and admission are bounded (up to 64 per group, twice the cap
  admitted). Transaction byte/mutation limits are not added by this catalog item.
  Rotation may overshoot its byte threshold by one group.
- Deterministic tests independently prove two transactions use two WAL frames but
  one synchronization, no application/acknowledgment precedes that synchronization,
  and conflicts include earlier accepted writes and tracked point reads.
- Failure tests exercise incomplete/complete submissions, synchronization,
  partial application, publication, rotation, independent transaction recovery,
  explicit barriers, cancellation, admission limits, shutdown, and the ordinary
  path. They also exposed a directly relevant missing-key seek bug after reopening:
  an exhausted single-entry SST seek dereferenced its deliberately absent scan
  state. That path now correctly returns zero without an allocation.
