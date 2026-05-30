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

### Background flush (`Compaction/Compacter.cs`)
- A `PeriodicTimer` (default 50 ms; `TimeSpan.Zero` disables it) flushes immutable MemTables to L0
  once their count exceeds `MemTableMaxCount`. Uses an injectable `TimeProvider` (testable).

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

### Durability / crash recovery (missing)
- **No write-ahead log (WAL).** Unflushed MemTable data is lost on crash.
- **No manifest.** No persisted record of which SSTs exist or which level they belong to.
- Recovery on `OpenAsync` is partial: only `*.sst` files are loaded and all are treated as L0
  (`// TODO: For now we only load l0 SSTs`); higher levels would be ignored once they exist.
- SST loading on open is sequential (`// TODO: [PERF] Can be parallelized`).

### Compaction (missing — biggest gap)
- `Compacter` only *flushes* L0; there is no real compaction. `LeveledSsTables` is declared but never
  populated, and `GetAsync` never reads it.
- Without compaction, L0 grows unbounded → read amplification, and stale/tombstoned data is never
  reclaimed.
- Needs a leveled and/or tiered strategy, plus an SST-level merge/concat iterator so on-disk tables
  participate in range scans (today iteration covers MemTables only).

### Serialization / value-ownership semantics
- Decide when caller values are copied into engine-owned memory. Today MemTable holds the caller's
  `TKey`/`TValue` reference; a mutable/pooled value could be mutated or returned to a pool while
  still referenced by the store.
- Direction (Solution 1): MemTables manipulate `TKey`/`TValue`; SSTables use `byte[]` once only the
  memory representation matters. Safer still: encode to owned bytes at `Put` time (copy-in) so the
  engine never aliases caller memory. `Bytes` already follows the copy-in model.
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
- Use `IStorageIterator` for MemTables and a `MergeIterator` implementing it, reused by
  `LsmStorageInner`.
- In `BufferedSsTableBuilder.AddAsync`, a key is encoded once for the bloom filter and again into
  block memory; encode it once as entries are added.

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

1. Serialization / value copy-in semantics (correctness).
2. WAL + manifest (durability & full recovery).
3. Compaction + leveling, then parallelism.
4. Finer sparse index.
5. Defer: LRU block cache, entry-lifting.
