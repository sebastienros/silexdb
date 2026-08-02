# SilexDB vs ZoneTree profile-store benchmark - 2026-08-02

Comparison using ZoneTree's application-style profile-store benchmark. The workload performs
individual writes to a primary profile store and four application-managed secondary indexes,
point reads, email lookups, ordered index scans, profile-fetching queries, updates, compaction,
reopen, and verification.

## Revisions and host

- **ZoneTree**: `4e3c8909375fc030971af9ee3658667590cb060d`
- **SilexDB**: `e53e50e9ca9cd49d295e557d6534cf3752aae61b`, plus the typed-encoding
  optimization measured below
- **Host**: macOS 15.7.8, Apple M4 Pro, 14 logical processors, 48 GB RAM, Apple SSD
- **Runtime**: .NET 10.0.9, Arm64, Server GC
- **Compilation**: Release, `DOTNET_TieredCompilation=0`
- **Parallelism**: 1 unless noted

Each engine ran in a fresh child process. Every run started from empty stores and used seed
`570123434`. Timed phases did not include initialization, stabilization, final settle, reopen,
or verification.

## Engine layouts

Both engines used five independent stores: profiles, email, country/status, created-at, and
reputation. Writes were individual operations, not batches.

| Setting | ZoneTree | SilexDB |
| --- | --- | --- |
| WAL | Async compressed | Enabled, no per-write fsync |
| SST compression | ZoneTree default | LZ4 |
| Mutable data | 250,000 items/tree | 64 MiB/store |
| Block cache | 1 minute lifetime; key/value caches 1,024 | 256 MiB/store limit |
| Compaction | Background maintainers | Tiered background compaction |
| Read stabilization | Evict and settle all trees | Flush and compact all stores |

The SilexDB adapter used the public typed put/read APIs and raw seek/scan callbacks. SilexDB
does not permit re-entering the same store from a raw scan callback, so profile-fetching queries
first collected at most `query-limit` user ids and then read those profiles from the primary
store. This adds a small temporary id list per query and should be considered when interpreting
query results.

## Published 100K workload reproduction

ZoneTree's committed Linux 100K reference used `--query-limit 100`, while the current harness
defaults to 50. This run explicitly used 100. All 16 phase checksums and the final checksum
matched the committed ZoneTree reference exactly, and SilexDB produced the same checksums.

| Engine | Run time | Completed phases | Insert ready | Update ready | Storage | Peak memory | Final checksum |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| ZoneTree | 10.91 s | 9.34 s | 152,799/s | 136,513/s | 7.4 MB | 1.5 GB | `1C7232F217FD84C5` |
| SilexDB | 23.24 s | 22.42 s | 78,650/s | 72,363/s | 22.6 MB | 1.8 GB | `1C7232F217FD84C5` |

ZoneTree completed the measured phases **2.40x faster** and the full run **2.13x faster**.
SilexDB used about **3.1x** the storage and had a roughly **20%** higher peak process working set.

The published Linux ZoneTree reference reports 8.38 s of completed phases and 9.22 s total on
an AMD EPYC 4345P. This Apple M4 Pro reproduction was 11% slower in completed phases and 18%
slower overall, while producing the exact published checksums. Cross-host timing differences
are expected; checksum equality confirms that the same logical work was performed.

### Exact-limit phase throughput

| Phase | ZoneTree | SilexDB | Faster |
| --- | ---: | ---: | ---: |
| Insert profiles | 479,110/s | 112,620/s | ZoneTree 4.25x |
| Read by user id | 900,421/s | 608,564/s | ZoneTree 1.48x |
| Lookup by email | 546,141/s | 329,169/s | ZoneTree 1.66x |
| Scan country/status index | 108,751/s | 520,654/s | SilexDB 4.79x |
| Query country/status | 14,629/s | 10,328/s | ZoneTree 1.42x |
| Scan created-at index | 120,065/s | 426,516/s | SilexDB 3.55x |
| Query created-at range | 22,104/s | 11,157/s | ZoneTree 1.98x |
| Scan top reputation index | 137,587/s | 56,129/s | ZoneTree 2.45x |
| Query top reputation | 19,615/s | 8,552/s | ZoneTree 2.29x |
| Update profiles | 281,310/s | 90,172/s | ZoneTree 3.12x |
| Post-update read by user id | 1,040,420/s | 670,771/s | ZoneTree 1.55x |
| Post-update lookup by email | 555,152/s | 356,525/s | ZoneTree 1.56x |
| Post-update scan country/status | 120,336/s | 19,819/s | ZoneTree 6.07x |
| Post-update query country/status | 13,875/s | 6,088/s | ZoneTree 2.28x |
| Post-update scan top reputation | 142,583/s | 14,854/s | ZoneTree 9.60x |
| Post-update query top reputation | 19,503/s | 5,769/s | ZoneTree 3.38x |

SilexDB's raw seek path is strong on the pre-update bounded country/status and created-at index
scans. Its advantage disappears after updates introduce tombstones and overlapping table
generations. Top-reputation scans repeatedly start at the beginning of the index and favor
ZoneTree both before and after updates.

## Current default workload

The current upstream default is `query-limit=50`. The 100K result is the median of three runs;
500K is one scaling run.

| Profiles | Engine | Run time | Completed phases | Storage | Peak memory | Final checksum |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 100K | ZoneTree | 6.65 s | 5.11 s | 7.4 MB | 1.6 GB | `1C7232F217FD84C5` |
| 100K | SilexDB | 13.94 s | 13.16 s | 22.6 MB | 1.8 GB | `1C7232F217FD84C5` |
| 500K | ZoneTree | 31.01 s | 28.70 s | 36.9 MB | 2.4 GB | `DF2D9443B36E4083` |
| 500K | SilexDB | 73.61 s | 69.06 s | 119.0 MB | 2.7 GB | `DF2D9443B36E4083` |

ZoneTree was **2.58x** faster across the median 100K measured phases and **2.41x** faster at
500K. The relative gap remained stable after both engines crossed their mutable-segment
thresholds. SilexDB storage remained about **3.2x** larger at 500K.

## Parallelism 16

One current-default 100K run used 16 workers. Checksums matched between engines.

| Engine | Run time | Completed phases | Insert | Update | Post-update point read |
| --- | ---: | ---: | ---: | ---: | ---: |
| ZoneTree | 3.94 s | 2.37 s | 913,437/s | 596,699/s | 1,791,556/s |
| SilexDB | 6.46 s | 5.61 s | 100,602/s | 69,225/s | 3,107,375/s |

SilexDB's point reads scaled well and exceeded ZoneTree after updates, but its serialized write
path did not scale with 16 writers. ZoneTree was 9.1x faster on inserts, 8.6x faster on updates,
and 2.37x faster across all measured phases.

## Reproduction commands

The upstream project was built after adding a SilexDB implementation of
`IProfileStoreEngine` and selecting it as `--engine silex`.

```bash
git clone https://github.com/ZoneTree/ZoneTree.git
cd ZoneTree
git checkout 4e3c8909375fc030971af9ee3658667590cb060d

dotnet build benchmarks/profile-store/src/ProfileStore.Benchmark.csproj -c Release

DOTNET_TieredCompilation=0 dotnet run --no-build \
  --project benchmarks/profile-store/src/ProfileStore.Benchmark.csproj -c Release -- \
  --engine zonetree,silex \
  --profiles 100K \
  --parallelism 1 \
  --query-limit 100 \
  --timeout-seconds 1200 \
  --output results-100k-q100 \
  --data data-100k-q100
```

For the repeated current-default and scaling runs, omit `--query-limit 100` and change
`--profiles` to `100K` or `500K`. For the concurrency run, add `--parallelism 16`.

## Follow-up audit: why ZoneTree is faster

The benchmark is not disabling WAL or compression, and both engines perform the same logical
operations. However, it measures a deliberately high-throughput configuration rather than
strict per-operation durability or transactional index maintenance.

### ZoneTree defers work aggressively

1. **The timed writes stay in memory.** Every ZoneTree has a 250,000-item mutable-segment limit.
   The 100K insert phase therefore never fills any of the five mutable segments. The same is
   true again for the 100K update phase after pre-read stabilization has emptied the mutable
   segments. Writes update the in-memory B+Tree and enqueue WAL work; segment creation and
   merging happen later. SilexDB's 100K data also fits its configured memtables, so memory
   residency alone does not explain the full difference.
2. **`AsyncCompressed` WAL is enqueue-only on the caller path.** ZoneTree documents that recent
   writes can be lost if the process terminates before the background WAL path becomes durable.
   SilexDB's configured WAL also avoids fsync, but performs an immediate `FileStream.Write` for
   every operation. With five stores, this syscall difference is substantial.
3. **Flush and compaction are outside insert/update throughput.** The harness stops the write
   stopwatch before calling read stabilization. It reports stabilization separately, then
   reads from settled disk segments. This is explicit in the report, but raw insert/update
   throughput should not be read as fully settled write throughput.
4. **The five stores are not transactional.** Profile and secondary-index writes are independent.
   A process failure can leave them inconsistent. The clean-reopen verification follows an
   explicit settle and checks the profile count plus three primary records; it does not simulate
   a crash or verify all secondary indexes.
5. **Read settings are tuned.** The benchmark uses sparse-array step 16 instead of ZoneTree's
   default 1,024 and iterator prefetch 16 instead of 0. Index-only scans precede profile-fetching
   queries and contribute their blocks to the cache, so the query phases are intentionally warm.

### ZoneTree controlled A/B results

Forcing the 100K workload to spill every 10,000 items changed only
`MutableSegmentMaxItemCount`:

| ZoneTree configuration | Completed phases | Insert | Update | Peak memory |
| --- | ---: | ---: | ---: | ---: |
| Published tuning, median | 5.11 s | 501,946/s | 283,018/s | 1.6 GB |
| 10,000-item mutable segments | 8.76 s | 45,937/s | 47,741/s | 912.5 MB |

The lower cap made the measured phases 71% slower, inserts 10.9x slower, and updates 5.9x
slower, while substantially reducing peak memory. This confirms that ZoneTree's write result
depends heavily on the benchmark's large mutable segments.

Restoring the ZoneTree read defaults (`SparseArrayStepSize=1024`,
`IteratorPrefetchSize=0`) increased completed phase time from 5.11 s to 6.08 s, about 19%.
The benchmark therefore contains meaningful, workload-specific read tuning.

Three-run medians from a write-focused 10K workload show the cost of ZoneTree's WAL modes:

| WAL mode | Insert | Update | Caller-path behavior |
| --- | ---: | ---: | --- |
| None | 489,963/s | 335,158/s | No WAL |
| AsyncCompressed | 426,508/s | 337,419/s | Queue for background compression/write |
| Sync | 122,450/s | 110,902/s | Synchronous plain buffered-stream append |
| SyncCompressed | 65,777/s | 76,845/s | Synchronous compressed-stream append |

`Sync` and `SyncCompressed` are not fsync-per-operation modes. Plain `Sync` durably flushes on
dispose and compressed mode has explicitly weaker tail durability. The published
`AsyncCompressed` setting is 3.5-6.5x faster than the synchronous caller paths for inserts, but
has a documented recent-write loss window on process termination.

## Follow-up audit: SilexDB adapter misses

The initial SilexDB adapter left two important paths on poor defaults:

1. **Tiered compaction never ran after updates.** `MaxCompactionTiers` defaulted to 8, but the
   update workload produced only two overlapping SST generations for the country/status and
   reputation indexes. `FlushAndCompactAsync` therefore left both generations unmerged. The
   overlap disabled SilexDB's globally-sorted raw seek path and forced every post-update scan
   through the allocating merge-iterator fallback.
2. **Top-reputation scans bypassed the block cache.** The adapter used `ScanRawAsync` for the
   empty start key. `SeekRawAsync` with an empty key is equivalent for this workload and uses
   the cached block-level fast path.
3. **Parallel profile fetch was not a win.** Issuing each query's profile reads with
   `Task.WhenAll` increased completed phase time, because the query limit is only 50 and task,
   lock, and cache contention outweighed concurrency.
4. **Typed writes and WAL were expensive.** The original typed extensions encoded into a pooled
   writer, copied into another owned buffer, and then copied into the memtable arena. The typed
   encoding follow-up below removes the redundant intermediate owner and copy. The WAL still
   performs one immediate write syscall per operation.

### SilexDB controlled A/B results

All variants used the current 100K, query-limit-50 workload and produced the same phase and
reopen checksums:

| SilexDB variant | Completed phases | Change | Storage |
| --- | ---: | ---: | ---: |
| Initial adapter, three-run median | 13.16 s | baseline | 22.6 MB |
| `MaxCompactionTiers=2` | 10.58 s | -20% | 15.0 MB |
| Empty-key cached seek | 12.52 s | -5% | 22.6 MB |
| Parallel profile fetch | 13.69 s | +4% | 22.6 MB |
| Both useful fixes | 8.96 s | **-32%** | 15.0 MB |
| Both fixes, WAL disabled | 7.82 s | -41% | 15.0 MB |

With the two valid tuning fixes and WAL still enabled, SilexDB's gap to ZoneTree falls from
2.58x to **1.75x** across measured phases. The post-update country/status scan improves from
32,904/s to 1,030,669/s, and the post-update reputation scan improves from 32,022/s to
1,378,124/s. The tuned SilexDB index-only scans are then 3.5-5.6x faster than ZoneTree; the
remaining total gap is concentrated in writes, primary lookups, and profile-fetching queries.

Disabling SilexDB's WAL is diagnostic only, not a fair production recommendation. It raises
insert throughput from about 106K/s to 314K/s and update throughput from 90K/s to 192K/s,
showing that immediate WAL writes and typed encoding are the main remaining write-path costs.

### Typed encoding optimization

The typed write path now asks the encoder for its exact length, rents one shared byte array for
the encoded key and value, encodes directly into fixed spans, copies those spans once into
storage, and returns the array. Byte-backed encoders bypass encoding entirely. Async reads still
need an owned key across `await`, but now encode directly into one exact-sized owner instead of
encoding into a pooled writer and copying into a second owner.

A thread-allocation probe measured 100,000 warmed-up `Put(int, int)` calls with WAL disabled and
the same storage implementation. Five process runs were stable within 0.1 byte/operation:

| Typed put path | Allocated bytes/put | Change |
| --- | ---: | ---: |
| Original pooled writer plus owned copies | 515.01 B | baseline |
| Direct fixed-span encoding | 194.98 B | **-320.03 B (-62%)** |

The remaining allocation belongs to the storage write path and memtable arena growth, not the
temporary typed encoder. To isolate throughput from query and cache variance, seven alternating
baseline/optimized pairs used 100K inserts followed by 100K updates, with every read and query
count set to zero. Every phase and reopen checksum matched:

| Configuration | Path | Insert | Update | Combined write time |
| --- | --- | ---: | ---: | ---: |
| WAL enabled | Original | 897.3 ms | 1,174.3 ms | 2,076.5 ms |
| WAL enabled | Direct encoding | 736.1 ms | 1,006.5 ms | 1,742.1 ms |
| WAL disabled | Original | 310.3 ms | 570.0 ms | 885.3 ms |
| WAL disabled | Direct encoding | 173.1 ms | 413.5 ms | 588.2 ms |

The median paired speedups were **1.23x insert, 1.16x update, and 1.19x combined** with the WAL,
and **1.79x, 1.38x, and 1.50x** without it. The smaller WAL-enabled gain is expected because the
immediate file write is then the dominant cost. In three complete tuned workload reruns, insert
throughput rose from 106K/s to a 127K/s median; total measured time was 8.93 s versus the earlier
8.96 s single-run baseline because unchanged read/query phases dominate the total and vary more
than the isolated writes.

## Conclusions

- The published ZoneTree workload was reproduced successfully: exact phase and reopen
  checksums matched.
- ZoneTree's result is real for its documented high-throughput profile, but it is not a
  transactionally indexed, crash-tested, fsync-per-write durability result. Large mutable
  segments, enqueue-only WAL, excluded stabilization, warm query ordering, and explicit read
  tuning all materially contribute.
- Two adapter/configuration mistakes overstated the SilexDB gap. Correcting compaction threshold
  and empty-key seek behavior reduces the measured gap from 2.58x to 1.75x with WAL enabled.
- Direct fixed-span typed encoding removes 320 allocated bytes per primitive put and improves the
  isolated WAL-enabled write workload by 19%. The highest-value remaining engine work is now to
  buffer WAL appends without weakening recovery semantics, trigger compaction when overlap
  disables raw scans, and improve primary/profile-fetch paths. Concurrent writes remain a
  separate architectural limitation.
