# Silex

Silex is an embedded, log-structured merge-tree (LSM) storage engine for .NET. It is a single-process,
in-process key/value store designed for high write throughput and low allocation. Data is buffered in
memory, persisted to immutable sorted-string tables (SSTs) on disk, and compacted in the background to
bound read and space amplification.

## Features

- **Generic key/value store** – `LsmStorage<TKey, TValue>` works with any supported key and value type.
- **Durable** – an optional write-ahead log (WAL) recovers unflushed data after a crash.
- **Zero-copy reads** – values served from memory are returned as read-only borrows of engine-owned
  memory rather than defensive copies.
- **Ordered range scans** – iterate the whole key space, or from a given key, in ascending key order.
- **Pluggable compaction** – choose `Tiered` (write-optimized), `Leveled` (read-optimized), or `None`.
- **Bloom filters and a block cache** – skip SSTs that cannot contain a key and cache hot blocks.
- **Multi-targeted** – builds for `net8.0` and `net10.0`.

## Requirements

- .NET 8.0 or .NET 10.0 SDK (see `global.json`).

## Building and testing

```bash
dotnet build Silex.slnx
dotnet test  Silex.slnx
```

## Getting started

Open (or create) a store, write some entries, read them back, and close it to flush to disk.

```csharp
using Silex;

var options = new StorageOptions();

// Keys and values are strongly typed. The directory is created if it does not exist.
await using var db = await LsmStorage.OpenAsync<int, string>("my-db", options);

db.Put(1, "one");
db.Put(2, "two");

string value = await db.GetAsync(1);   // "one"
string missing = await db.GetAsync(99); // null (default(TValue)) when absent

db.Delete(2);

// Closing (or disposing) flushes pending data to disk and stops background work.
await db.CloseAsync();
```

`LsmStorage.OpenAsync<TKey, TValue>` reopens an existing store at the same path, replaying the WAL and
loading existing SSTs, so persisted data survives process restarts.

## Supported key and value types

Silex ships with built-in binary encoders for the following types, usable as either `TKey` or `TValue`:

| Type      | Notes                                                  |
|-----------|--------------------------------------------------------|
| `int`     | Order-preserving (sign-flipped big-endian) key bytes   |
| `uint`    | Order-preserving (big-endian) key bytes                |
| `ushort`  | Order-preserving (big-endian) key bytes                |
| `long`    | Order-preserving (sign-flipped big-endian) key bytes   |
| `char`    | Fixed 2-byte big-endian (UTF-16 code unit)             |
| `string`  | UTF-8 encoded; keys ordered by Unicode code point      |
| `byte[]`  | Stored as-is; an empty value is treated as a deletion  |
| `Bytes`   | An owned, comparable byte buffer (see below)            |

Using an unsupported type throws `NotSupportedException` when the store is opened.

> **Key ordering.** Keys are encoded in an *order-preserving* form, so the engine can binary-search a
> block by comparing raw encoded bytes on the hot read path — no per-entry key is materialized and no
> allocation occurs. Numeric keys therefore sort by their natural numeric value (negatives before
> non-negatives), and `string` keys sort by Unicode code point (equivalent to UTF-8 byte order), which is
> ordinal — not culture-sensitive.

## Reading and writing

### Put

```csharp
db.Put(key, value);
```

`Put` is synchronous and inserts or replaces the value for `key`. Ownership of `key` and `value`
transfers to the engine: do not mutate or release them (for example, do not return a pooled buffer)
after the call, because the engine keeps and reads them until the owning memtable is flushed.

### Get

```csharp
TValue value = await db.GetAsync(key);
```

Returns the stored value, or `default(TValue)` (for example `null` for `string`/`byte[]`, `0` for `int`)
when the key is absent or has been deleted. The returned key/value is a read-only borrow of
engine-owned memory; copy it yourself (for example by wrapping it in `Bytes`) if you need an
independently owned, mutable copy.

### Raw value reads (zero-allocation)

When you store byte payloads under a typed key, `GetAsync` materializes a fresh `byte[]` for every read.
The raw read overloads avoid that copy: they look the value up by its typed key and hand you the stored
bytes directly. They are the allocation-free path for the common "typed key, byte value" usage and work
with any value type by exposing its encoded bytes. All three report a missing or deleted key as
not-found (unlike `GetAsync`, which surfaces `default(TValue)`).

Write the value into an `IBufferWriter<byte>` (for example a pooled writer):

```csharp
var writer = new ArrayBufferWriter<byte>();
bool found = await db.TryGetRawAsync(key, writer);
if (found)
{
    ReadOnlySpan<byte> value = writer.WrittenSpan;
    // ...
}
```

Copy into a caller-owned buffer and get the value length back:

```csharp
byte[] buffer = new byte[256];
int length = await db.GetRawAsync(key, buffer);
// length == -1            -> key missing or deleted
// length >  buffer.Length -> buffer too small, nothing was written; resize to `length` and retry
// otherwise               -> value is buffer.AsSpan(0, length)
```

`GetRawAsync` never throws or partially writes on a short buffer: it reports the full length so you can
resize and retry.

Inspect the value in place with no copy at all, passing state through `arg` to avoid a closure
allocation:

```csharp
bool found = await db.TryReadRawAsync(key, myState, static (state, value) =>
{
    // `value` is the stored bytes, borrowed for the duration of this callback only.
    state.Process(value);
});
```

The callback runs synchronously while the value's source is locked. It must not await, block, store the
span, or call back into the store; doing so risks deadlock or reading freed memory. Copy the bytes out if
you need them past the callback.

### Delete

```csharp
db.Delete(key);
```

Records a tombstone for `key`. Subsequent reads return `default(TValue)`, and the space is reclaimed
during compaction.

### Range scans

`CreateIterator` returns an iterator that yields entries in ascending key order across all memtables and
on-disk levels.

```csharp
using Silex;

IStorageIterator<int, string> iterator = db.CreateIterator();

// Scan the entire key space.
await foreach (KeyValuePair<int, string> entry in iterator.EnumerateAsync())
{
    Console.WriteLine($"{entry.Key} = {entry.Value}");
}

// Or scan from a starting key (inclusive), in ascending order.
await foreach (KeyValuePair<int, string> entry in iterator.EnumerateAsync(from: 100))
{
    Console.WriteLine($"{entry.Key} = {entry.Value}");
}
```

As with `GetAsync`, each yielded key/value is a zero-copy borrow of engine-owned memory.

## Closing the store

Always close the store to flush in-memory data to disk and stop the background compaction/flush threads.
`CloseAsync`, `DisposeAsync`, and `Dispose` are equivalent — call one:

```csharp
await db.CloseAsync();
// or
await db.DisposeAsync();
// or (via a using block)
await using var db = await LsmStorage.OpenAsync<int, string>("my-db", options);
```

There is no finalizer: durability is provided solely by deterministic disposal. An undisposed store
leaks no native handles, but any data still buffered in memory is not flushed.

## The `Bytes` type

`Bytes` is a comparable, owned wrapper over a byte buffer. Use it when you want value semantics for
binary keys/values, or to take an independently owned copy of a zero-copy borrow returned by a read.

```csharp
var key = new Bytes(new byte[] { 1, 2, 3 });
db.Put(key, new Bytes("hello world"u8.ToArray()));
```

## Configuration

`StorageOptions` exposes the engine's tuning knobs. All have sensible defaults, so `new StorageOptions()`
is a valid starting point. The most commonly used options:

| Option                     | Default            | Description                                                              |
|----------------------------|--------------------|--------------------------------------------------------------------------|
| `MemTableSizeLimit`        | 64 MiB             | Size at which the active memtable is frozen and queued for flushing.     |
| `MemTableMaxCount`         | 50                 | Max immutable memtables kept in memory before flushing.                  |
| `BlockSize`                | 4 KiB              | Unit of data read from/written to disk at once.                          |
| `FlushPeriod`              | 50 ms              | Interval between background flushes. `TimeSpan.Zero` disables the thread.|
| `BlockCacheSizeLimit`      | 1 MiB              | Size of the in-memory cache of decoded blocks.                           |
| `UseWriteAheadLog`         | `true`             | Maintain a WAL so unflushed data is recovered after a crash.             |
| `SyncWriteAheadLogToDisk`  | `false`            | `fsync` every WAL append (slower, survives power loss).                  |
| `CompactionStrategy`       | `Tiered`           | `None`, `Tiered` (write-optimized), or `Leveled` (read-optimized).       |

Convenient size helpers are available as extension methods: `64.MiB()`, `4.KiB()`, `1.GiB()`,
`512.B()`.

```csharp
var options = new StorageOptions
{
    MemTableSizeLimit = 16.MiB(),
    CompactionStrategy = CompactionStrategy.Leveled,
    UseWriteAheadLog = true,
};
```

### Compaction strategies

- **`None`** – flushed SSTs accumulate and are never merged. Read and space amplification grow without
  bound; deleted/overwritten data is never reclaimed.
- **`Tiered`** (default) – each flushed memtable forms a new sorted run; runs are merged as they
  accumulate. Lowest write amplification — best for write-heavy workloads. Tuned via
  `MaxCompactionTiers`, `MaxSizeAmplificationPercent`, `SizeRatioPercent`, and `MinMergeWidth`.
- **`Leveled`** – SSTs are organized into levels of geometrically increasing size, each (below L0) a
  single non-overlapping sorted run. Lowest read and space amplification — best for read-heavy
  workloads. Tuned via `Level0CompactionThreshold`, `BaseLevelTargetBytes`, `LevelSizeMultiplier`,
  `MaxLevels`, `TargetSstSizeBytes`, and `MaxCompactionParallelism`.

## Example: storing one million entries

The `Silex.Playground` project contains a runnable example:

```bash
dotnet run --project Silex.Playground -c Release
```

```csharp
var options = new StorageOptions { MemTableSizeLimit = 1.MiB(), FlushPeriod = TimeSpan.Zero };
await using var db = await LsmStorage.OpenAsync<int, int>("db", options);

foreach (var x in Enumerable.Range(0, 1_000_000))
{
    db.Put(x, x);
}

await db.CloseAsync(); // flush everything to disk
```

## db_bench comparisons with RocksDB

`Silex.DbBench` accepts the RocksDB-style options that affect the main LSM shape so comparisons can use
the same strategy instead of comparing RocksDB's default leveled compaction with Silex's default tiered
compaction. For fair runs, keep compression disabled on RocksDB (`--compression_type=none`) because Silex
currently stores values uncompressed, use `--compression_ratio=1` so RocksDB generates fully random
payloads like Silex, and use the same values for `--num`, `--key_size`, `--value_size`,
`--write_buffer_size`, `--block_size`, `--bloom_bits`, `--threads`, `--seed`, and the compaction knobs.

Tiered/universal comparison:

```bash
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=fillseq,readrandom,readseq \
  --num=1000000 --key_size=16 --value_size=100 --threads=1 --seed=42 \
  --write_buffer_size=67108864 --max_write_buffer_number=50 \
  --block_size=4096 --cache_size=8388608 --bloom_bits=10 \
  --compaction_style=1 \
  --universal_max_read_amp=8 \
  --universal_max_size_amplification_percent=200 \
  --universal_size_ratio=1 \
  --universal_min_merge_width=2 \
  --compression_type=none --compression_ratio=1 --db=/tmp/silex-universal

db_bench \
  --benchmarks=fillseq,readrandom,readseq \
  --num=1000000 --key_size=16 --value_size=100 --threads=1 --seed=42 \
  --write_buffer_size=67108864 --max_write_buffer_number=50 \
  --block_size=4096 --cache_size=8388608 --bloom_bits=10 \
  --compaction_style=1 \
  --universal_max_read_amp=8 \
  --universal_max_size_amplification_percent=200 \
  --universal_size_ratio=1 \
  --universal_min_merge_width=2 \
  --compression_type=none --compression_ratio=1 --db=/tmp/rocksdb-universal
```

Leveled comparison:

```bash
dotnet run --project Silex.DbBench -c Release -- \
  --benchmarks=fillseq,readrandom,readseq \
  --num=1000000 --key_size=16 --value_size=100 --threads=1 --seed=42 \
  --write_buffer_size=67108864 --max_write_buffer_number=50 \
  --block_size=4096 --cache_size=8388608 --bloom_bits=10 \
  --compaction_style=0 \
  --level0_file_num_compaction_trigger=4 \
  --num_levels=7 \
  --max_bytes_for_level_base=262144 \
  --max_bytes_for_level_multiplier=10 \
  --target_file_size_base=2097152 \
  --compression_type=none --compression_ratio=1 --db=/tmp/silex-leveled

db_bench \
  --benchmarks=fillseq,readrandom,readseq \
  --num=1000000 --key_size=16 --value_size=100 --threads=1 --seed=42 \
  --write_buffer_size=67108864 --max_write_buffer_number=50 \
  --block_size=4096 --cache_size=8388608 --bloom_bits=10 \
  --compaction_style=0 \
  --level0_file_num_compaction_trigger=4 \
  --num_levels=7 \
  --max_bytes_for_level_base=262144 \
  --max_bytes_for_level_multiplier=10 \
  --target_file_size_base=2097152 \
  --compression_type=none --compression_ratio=1 --db=/tmp/rocksdb-leveled
```

The mappings are intentionally close but not identical to RocksDB internals: Silex runs one background
compaction loop, maps `--bloom_bits` to an equivalent false-positive probability, and `Silex.DbBench`
forces a flush/compaction barrier before read phases so reads measure on-disk SST state rather than
leftover MemTables. `readseq` uses Silex's raw scan path for the `byte[]` benchmark workload.

## Project layout

| Project                  | Description                                      |
|--------------------------|--------------------------------------------------|
| `src/Silex`              | The storage engine library.                      |
| `Silex.Playground`       | A small runnable usage sample.                   |
| `Silex.DbBench`          | Database benchmarking harness.                   |
| `tests/Silex.Test`       | Unit tests (TUnit).                              |
| `tests/Silex.Benchmarks` | Micro-benchmarks (BenchmarkDotNet).              |

## Thread-safety and lifetime notes

- A single `LsmStorage<TKey, TValue>` instance is intended to be shared for concurrent reads and writes;
  writes are serialized internally.
- Because reads return zero-copy borrows of engine-owned memory, do not retain a returned key/value
  beyond its immediate use unless you copy it. Treat returned data as read-only.
