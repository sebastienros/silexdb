# Silex — Engineering Notes

Silex is an LSM-tree (Log-Structured Merge-tree) key/value storage engine, generic over
`TKey`/`TValue`, inspired by RocksDB/LevelDB and the "mini-lsm" design.

## Architecture overview

```
            Put/Delete                         GetAsync
                |                                  |
                v                                  v
        +-----------------+              read path (most -> least recent):
        | Current MemTable|  (mutable)     1. Current MemTable
        +-----------------+                2. Immutable MemTables (newest first)
                | freeze (size limit)      3. Level-0 SSTs (reverse order)
                v                          4. Leveled SSTs        <-- NOT YET WIRED
     +----------------------+
     | Immutable MemTables  |  (ImmutableQueue)
     +----------------------+
                | flush (background timer)
                v
        +-----------------+       compaction      +------------------+
        | Level-0 SSTs    |  ----------------->    | Leveled SSTs     |  <-- NOT YET IMPLEMENTED
        +-----------------+      (missing)         +------------------+
```

The mutable `StorageState` is swapped under lock to produce immutable snapshots, so reads operate on
a consistent point-in-time view without long-held locks. Thread-safety lives in `LsmStorageInner`,
not in the individual collections.

---

## Implemented features

### Public API (`LsmStorage` / `LsmStorageInner`)
- `LsmStorage.OpenAsync<TKey, TValue>(path, options)` opens/creates a store at a directory.
- `Put(key, value)`, `Delete(key)` (synchronous, writes to the current MemTable).
- `GetAsync(key)` (async; may touch disk).
- `CreateIterator()` for full and range (`EnumerateAsync(from)`) scans.
- `CloseAsync()` / `DisposeAsync()` flush all pending data and stop background work.
- Implementation note: writes are synchronous because only background work (disk flush) and explicit
  filesystem reads are async. `Put`/`Delete` trigger a freeze when the MemTable hits the size limit.

### MemTable (`MemTables/`)
- Starts as a `Dictionary` for fast inserts, then mutates **in place** to a custom
  `SortedDictionary` the first time it is enumerated/flushed (`EnsureSortedMap`). The custom sorted
  map adds `Enumerate(from, to)` without cloning the key collection.
- Tracks an approximate byte `Size` (key + value + `sizeof(int)`), kept consistent on overwrite by
  subtracting the previous value's size.
- Not thread-safe on its own by design — `LsmStorageInner` owns the locking and knows when a table
  is frozen vs. concurrently read/written.
- A MemTable is **not** a read cache; values read from SSTs are never inserted back into it.
- `Delete` is a tombstone write (`GetTombstoneValue()`), filtered out during iteration/compaction.

### Immutable MemTables
- Held in an `ImmutableQueue`. Created by freezing the current MemTable (size-triggered or forced).
- Read without locks since they are frozen; the queue reference is swapped atomically under lock.

### SSTables (`Tables/`)
- On-disk sorted file built block-by-block by `BufferedSsTableBuilder`.
- File layout (written in order): block data … | metadata block | metadata offset (u32) |
  bloom filter bytes | bloom filter `K` (u32) | bloom filter offset (u32). The trailing fixed-size
  footer lets `LoadSsTableAsync` seek backwards to find each section.
- `BufferedSsTableBuilder` uses a bounded (default 32 KiB) pooled buffer and flushes to disk as
  blocks fill, bounding peak memory during SST creation. File opened `WriteThrough | Asynchronous`
  (OS cache bypassed; we manage buffering).
- `BlockMetadata` stores per-block `Index`, `Offset`, `FirstKey`, `LastKey` — a coarse
  (block-granularity) sparse index used to prune blocks on read.
- Reads use range pruning (first/last key at both table and block level) + bloom filter before
  touching disk.

### Blocks (`Blocks/`)
- A block is the unit of I/O (default 4 KiB, configurable). Holds packed entries plus an offsets
  table; `GetValue(key)` does a binary search over offsets.
- Entries larger than the block size are handled (a block accepts at least one entry even if oversized).
- Pluggable `IBlockEncoder` / `IBlockEncoderFactory` (default implementation provided).

### Bloom filters (`BloomFilters/`)
- Built per-SST during construction (target false-positive rate 0.01). Keys are encoded once into a
  reusable pooled buffer, added to the filter, then probed on read to skip whole tables.
- Pluggable via `IBloomFilterFactory`.

### Block cache (read cache)
- `LsmStorageInner` holds an `IMemoryCache` (`_blockCache`) sized by `BlockCacheSizeLimit`, with
  configurable sliding/absolute expiration; entries sized by block byte length.
- `SsTable.ReadBlockCachedAsync` checks the cache first, then funnels misses through a
  `WorkDispatcher` to prevent cache stampede (one loader per `(tableId, blockIndex)` key; all
  waiters share the result).
- Note: `IMemoryCache` does size-based eviction, not strict LRU.

### Concurrency
- `_currentMemTableLock` and `_immutableMemTablesLock`: synchronous `ReaderWriterLockSlim`.
- `_level0Lock` and `_leveledTablesLock`: custom `AsyncReaderWriterLock` (for async, disk-bound work).
- Separate locks per data category so e.g. MemTable writes are not blocked by L0 reads/compaction.
- Reads take a short lock to clone a `StorageState` snapshot, then work lock-free off the snapshot.

### Serialization (`Serialization/`)
- Pluggable `IBinaryEncoder<T>` with a comparer, equality comparer, length, encode/decode, and
  tombstone support. Built-in encoders: `int`, `long`, `uint`, `ushort`, `char`, UTF-8 string,
  `byte[]`, and `Bytes`.
- `Bytes` is a value-type wrapper that **rents a pooled copy** of the input on construction
  (`MemoryOwner<byte>.RentCopy`), giving the engine an owned copy independent of caller memory.

### Background flush & compaction (`Compaction/Compacter.cs`)
- A `PeriodicTimer` (default 50 ms; `TimeSpan.Zero` disables it) drives a single background loop that
  flushes immutable MemTables to L0 once their count exceeds `MemTableMaxCount`, then runs one
  compaction step. Uses an injectable `TimeProvider` (testable). Each tick is awaited fully, so flush
  and compaction never overlap.
- **Tiered (RocksDB universal) compaction** (`LsmStorageInner.TryTieredCompactionAsync`). Configurable
  via `StorageOptions.CompactionStrategy` (`Tiered` default, or `None` to disable). The tiers are the
  existing L0 list (oldest-first; each flushed memtable and each compaction output is one SST = one
  sorted run). `SelectTieredCompaction` evaluates, once `tierCount >= MaxCompactionTiers`, three
  triggers in order: (1) **space amplification** — `sum(all but oldest)/oldest >=
  MaxSizeAmplificationPercent%` → merge all; (2) **size ratio** — from the newest tier, the first older
  tier larger than `(100+SizeRatioPercent)%` of the sum of newer tiers (with at least `MinMergeWidth`
  newer tiers) → merge the newer suffix; (3) **reduce sorted runs** — merge the newest tiers to bring
  the count back under the limit. Tier size is the SST file length in bytes. Merge uses
  `SsTableIterator → MergeIterator` (newest wins); the output gets a fresh highest id appended at the
  end, keeping reopen-by-id recency correct without a manifest.
- **Tombstones** are dropped only when the oldest tier participates (a full compaction, `startIndex==0`);
  otherwise they are kept because an older tier could still hold the deleted key.
- **Concurrency / crash safety:** flush and compaction are serialized by a maintenance lock; the state
  swap happens in place under the `_level0Lock` write lock (with a runtime guard that bails safely if
  the tail unexpectedly changed); inputs are written via temp-file + atomic rename and replaced inputs
  are deleted **oldest-first, stopping at the first failure** so a newer tombstone is never removed
  while an older value it shadows still exists (no resurrection across a crash). Orphan `*.sst.tmp`
  files are cleaned up on `OpenAsync`. `SsTable` block reads use `RandomAccess` positioned reads, so a
  shared handle is safe for concurrent readers during compaction.

### Durability / crash recovery (`Wal/WriteAheadLog.cs`)
- **Write-ahead log (WAL).** Each writable MemTable owns a `WriteAheadLog<TKey,TValue>` (file
  `{id}.wal` next to the SSTs). `MemTable.Put` journals the record **before** applying it in memory,
  so an acknowledged write is always recoverable. Enabled by default (`UseWriteAheadLog`).
- **Record format:** `[7-bit key length][key][7-bit value length][value]`, encoded once into a reused
  pooled buffer via `EncoderBinaryWriter`. Tombstones are the usual zero-length value, so deletes are
  journaled too. Each append is flushed to the OS; `SyncWriteAheadLogToDisk` additionally `fsync`s
  per append (survives power loss, slower). Appends are serialized by the current-memtable write lock.
- **WAL lifecycle / cleanup:** the file is deleted only once its data is durable elsewhere —
  `ForceFlushNextImmutableMemTableAsync` deletes `{id}.wal` *after* the SST is durable and visible in
  L0, and a clean `CloseAsync` deletes the (empty) current memtable's WAL. Deletion never happens from
  a finalizer, so an abandoned (crashed) process leaves its WALs on disk for recovery.
- **Recovery on `OpenAsync`:** existing `*.wal` files are captured *before* the inner is built (and
  ids reserved via `EnsureGreaterThan` first, so a fresh WAL can't truncate a not-yet-replayed one).
  For each WAL: if a matching `{id}.sst` was loaded the memtable was already flushed → the stale WAL is
  deleted and skipped (idempotent crash-between-flush-and-delete); otherwise it is replayed into a
  recovered immutable MemTable. Recovered tables are enqueued oldest-first (ids are always greater than
  every loaded SST's id, so they correctly win over L0). A torn trailing record (crash mid-append) is
  tolerated and stops replay.

### Resource management & disposal
- `LsmStorage` is `IDisposable` + `IAsyncDisposable`; `CloseAsync()` == `DisposeAsync()`. Disposal is
  idempotent (`_disposed` guard) and the only *clean* durability boundary: it stops the compacter,
  freezes and flushes all immutable MemTables to L0, deletes the now-empty current WAL, then disposes
  the inner.
- **No flushing during finalization.** There is intentionally no `~LsmStorage()` /
  `~BufferedSsTableBuilder()` finalizer — persisting requires blocking disk I/O, which must never run
  during GC. An unclosed storage is not flushed during finalization; with the WAL enabled its
  unflushed writes are instead **recovered on the next `OpenAsync`**. Native file handles are released
  by `LsmStorageInner`'s and `FileStream`'s own finalizers, so leaving a storage unclosed leaks no OS
  resources and can never crash the process.
- Callers (and tests) must `CloseAsync()` before relying on data being compacted to disk. Tests
  dispose deterministically before deleting their temp folders so finalizers stay true no-ops.

### Memory management buffers (`Buffers/`)
- `RecyclableMemoryStream : IBufferWriter` — grows by chaining pooled blocks; buffers returned to the
  pool on dispose. `GetReadOnlySequence()` exposes the chained blocks; `GetBuffer()` returns one
  pooled `byte[]`.
- `PooledArrayBufferWriter` — when full, rents a larger buffer and copies the previous content over.

### Tests & tooling
- xUnit tests cover blocks, tables, MemTables, bloom filters, encoders, storage, `Bytes`, and
  `AsyncReaderWriterLock`.
- `Silex.Benchmarks` (BenchmarkDotNet) and `Silex.Playground` (1M-entry write/flush demo).

---

## What's left to do

### Durability / crash recovery
- **DONE — Write-ahead log (WAL).** Unflushed MemTable data now survives a process crash and is
  replayed on the next `OpenAsync` (see *Durability / crash recovery (`Wal/WriteAheadLog.cs`)* above).
- **No manifest (still deferred).** No persisted record of which SSTs exist or which level they belong
  to. Only needed once compaction/levels exist; today's directory scan of `*.sst` (all loaded as L0)
  plus WAL recovery is a self-consistent protocol. Revisit with compaction.
- Recovery on `OpenAsync` is still L0-only: every `*.sst` is treated as L0
  (`// TODO: For now we only load l0 SSTs`); higher levels would be ignored once they exist.
- SST loading on open is sequential (`// TODO: [PERF] Can be parallelized`).
- Follow-up (with the manifest work): `fsync` the parent directory after creating an SST and after
  deleting a WAL so the "SST exists ⇒ skip WAL" invariant also holds across power loss; and add a
  per-record CRC to the WAL to detect (not just truncated) corruption.

### Compaction
- **DONE — Tiered (RocksDB universal) compaction** (configurable via `StorageOptions.CompactionStrategy`,
  default `Tiered`; `None` disables it). See *Background flush & compaction* above. Bounds the number of
  L0 sorted runs and reclaims stale/tombstoned data on full compactions.
- **Leveled compaction (still missing).** `LeveledSsTables` is declared but never populated, and
  `GetAsync` never reads it. Leveled needs a persisted manifest (to record which SST is at which level
  across reopen) — deferred with the manifest work below.
- **DONE — Scans include on-disk SST data.** Range/full iteration now merges the current MemTable, the
  immutable MemTables and the L0 SSTs (most-recent-first), so a scan no longer misses already-flushed
  data. The scan holds the level0 read lock for its whole duration (freezing L0 and preventing disposal
  of the SSTs/immutable MemTables it references) and materializes the current MemTable into a list under
  the (thread-affine) current-MemTable lock so the rest of the scan can run async SST I/O off that lock.
  Flush was made atomic from a scanner's viewpoint: the immutable MemTable is *peeked* (not dequeued)
  before its SST is built, then removed from the queue and published into L0 under the **same** level0
  write lock — closing a pre-existing window where a mid-flush MemTable was visible in neither place.
  Tombstones are filtered out of scan results via `IsTombstoneValue`.

### Serialization / value-ownership semantics
- **Decision: zero-copy is a core principle.** The engine deliberately does **not** defensively copy
  keys/values on `Put` or `Get`/scan — no per-operation allocations. Instead correctness is governed
  by an **ownership-transfer / borrow contract**:
  - `Put(key, value)` *transfers ownership* of `key`/`value` to the engine. The caller must not
    mutate or release (e.g. return a pooled buffer) the memory afterwards. The engine owns it for the
    lifetime of the memtable.
  - `GetAsync`/scan return a *read-only borrow* of engine-owned memory. The caller must treat the
    result as immutable and must not dispose it.
  - This is exactly the single-owner, pooled model that `Bytes`/`MemoryOwner<byte>` already encode; a
    caller that needs an independent, safely-mutable copy wraps the data in `Bytes` (whose constructor
    copies once) before handing it over.
  - The only copy that is actually required — making the on-disk SST independent of memtable memory —
    happens exactly once, at flush time, via the encoder's `Encode` into the SST buffer. Reads from an
    SST likewise allocate once in `Decode`. There is no copy while data lives in a memtable.
  - Rejected alternative: a defensive `IBinaryEncoder.Copy` with copy-in at `Put` and copy-out at
    `Get`/scan. It made the engine safe against caller misuse but added an allocation on every write
    and every read, which conflicts with the zero-allocation principle. Not adopted.
- Open follow-up (separate from the contract above): a block-builder-level copy so values are
  serialized into the SST buffer at `Add` time instead of holding the `TValue` reference until
  `BuildBlock` (see the skipped `EntryBuffersShouldBeCopied` test).
- Open follow-up: returning engine-owned pooled buffers to the `ArrayPool` on memtable dispose. This
  needs reference-counted memtables first, because `ForceFlushNextImmutableMemTableAsync` disposes a
  memtable outside `_immutableMemTablesLock` while a snapshot reader may still be borrowing it — naive
  disposal would be a use-after-free.
- Evaluate CBOR as a binary value format (independent of *when* the copy happens):
  https://cborbook.com/part_1/practical_introduction_to_cbor.html

### Known smaller TODOs / cleanups
- Check `ByteArrayComparer` performance.
- `LsmStorageInner` read strategy may be optimized (e.g. parallelize L0 probing — multiple tables may
  hold the key; the most recent wins).
- Store the encoder version/type in the block so encoders can be switched dynamically (old encodings
  get replaced during compaction; a CLI could migrate storage / compaction strategies).
- Introduce a dedicated type for `KeyValuePair<ReadOnlyMemory<byte>, ReadOnlyMemory<byte>>` (used in
  many places).
- Should the encoder own file creation? Allow choosing which files an SST produces (e.g. split
  content vs. metadata) or where they live (e.g. blobs).

#### Done
- ~~Use `IStorageIterator` for MemTables and a `MergeIterator` implementing it, reused by
  `LsmStorageInner`.~~ Implemented: a single reusable `MergeIterator` merges MemTable iterators and
  is reused by `LsmStorageInner` for scans.
- ~~In `BufferedSsTableBuilder.AddAsync`, a key is encoded once for the bloom filter and again into
  block memory.~~ Implemented: `BlockBuilder` now encodes each key once into a pooled buffer
  (`LastEncodedKey`) and reuses it for the bloom filter. (Also fixed a latent bloom-filter bug where
  unflushed writer bytes meant empty keys were fed to the filter.)
- ~~Make disposal idempotent and finalizers no-ops.~~ Implemented: deterministic
  `CloseAsync`/`DisposeAsync` is the sole flush path; finalizers were removed (see
  *Resource management & disposal*).
- ~~Fix latent correctness gaps found during review.~~ Implemented (with regression tests):
  - Reads/scans now iterate immutable mem tables most-recent-first (`ImmutableQueue` enumerates
    oldest-first), so the newest value for a key wins.
  - L0 recovery loads `*.sst` sorted by parsed numeric id, preserves the id, and bumps
    `IdGenerator` past persisted ids so recency order survives reopen.
  - `byte[]` keys use content equality in MemTable dictionary lookups (`ByteArrayEncoder.Comparer`
    now implements `IEqualityComparer<byte[]>`), consistent with the sorted-comparer phase.
  - Removed a bogus 4-byte length assert in `BytesEncoder.Decode` that fired on arbitrary-length
    `Bytes` values read from an SST under Debug.
  - `SsTableIterator.EnumerateAsync(from)` clamps its start block index to 0 when the from-key
    precedes the first block's first key.
- ~~Tiered compaction + L0 read-path tombstone fixes.~~ Implemented (with tests):
  - Configurable **tiered compaction** (`CompactionStrategy.Tiered`/`None`) run from the single flush
    loop; merges a newest suffix of L0, drops tombstones only on full compactions, atomic temp+rename
    output, oldest-first stop-on-failure input deletion, serialized by a maintenance lock (see
    *Background flush & compaction*).
  - `GetAsync` now recognises **sentinel-based tombstones** in SSTs (e.g. `int`/`long`/`char`, whose
    deletion is a fixed non-empty value): previously it returned the raw sentinel instead of the
    default for a deleted key read from an SST.
  - `GetAsync` no longer lets a **bloom-filter false positive** in a newer SST mask an older SST: a key
    that is genuinely absent from the covering block falls through to older tables (via a presence-aware
    `Block.TryGetValue`) instead of returning a premature default.
  - `BytesEncoder.IsTombstoneValue` no longer throws on a non-empty value (it compared against
    `Bytes.Empty`, whose `Span` dereferenced a null backing buffer); it is now an empty check.

---

## Future ideas (longer-term / speculative)

### Sparse index (finer than today)
Index only sampled entries (e.g. every Nth) instead of every entry: smaller in memory/disk, faster
to load, faster seeks than scanning a block. Block-granularity first/last keys already exist in
`BlockMetadata`; the next step is intra-block sampled offsets in `BlockMetadata` + `BlockIterator` so
we can seek *within* a block without scanning from its start.

### Block cache → true LRU
The block cache exists (`IMemoryCache`, size-limited) but is not strict LRU. If eviction quality
matters in benchmarks, replace it with a custom LRU. Low priority otherwise.

### Multi-threading background work
Single writer means background operations (notably compaction) should parallelize across CPUs, and
must not block MemTable writes. The snapshot + per-level `AsyncReaderWriterLock` design already
supports building new SSTs off-lock and swapping them in under a short write-lock. Blocked on
compaction existing first.

### Lifting recently used entries
Keep hot entries at the highest level possible and cold ones lower. The block cache already provides
most of the read-locality benefit; an additional idea is to "touch"/re-write a hot entry so it is
stored again at the top level and migrates down during compaction — at the cost of extra
write/space amplification and tombstone/ordering complexity. Speculative; defer until benchmarked.

---

## Suggested priority

1. ~~Serialization / value copy-in semantics.~~ **Decided: keep zero-copy** as a core principle —
   ownership-transfer / borrow contract rather than defensive copies (see *Serialization /
   value-ownership semantics*).
2. ~~WAL~~ **(done — crash recovery via write-ahead log)** + manifest (manifest deferred until
   compaction/levels exist).
3. ~~Compaction~~ **(tiered done)** + ~~SST-level scan iterator so range scans include on-disk data~~
   **(done)** + leveling (needs a manifest), then parallelism. Next: leveled compaction + minimal manifest.
4. Finer sparse index.
5. Defer: LRU block cache, entry-lifting.
