# Silex

Silex is an embedded, log-structured merge-tree (LSM) storage engine for .NET. It is a single-process,
in-process key/value store designed for high write throughput and low allocation. Data is buffered in
memory, persisted to immutable sorted-string tables (SSTs) on disk, and compacted in the background to
bound read and space amplification.

## Features

- **Byte-oriented key/value store** – core writes copy borrowed bytes into Silex-owned memtable memory.
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

// The directory is created if it does not exist. The core API stores encoded bytes.
await using var db = await LsmStorage.OpenAsync("my-db", options);

db.Put(1, "one");
db.Put(2, "two");

string? value = await db.GetStringAsync(1);   // "one"
string? missing = await db.GetStringAsync(99); // null when absent

db.Delete(2);

// Closing (or disposing) flushes pending data to disk and stops background work.
await db.CloseAsync();
```

`LsmStorage.OpenAsync` reopens an existing store at the same path, replaying the WAL and loading existing
SSTs, so persisted data survives process restarts.

## Supported key and value helpers

The core store accepts `ReadOnlySpan<byte>` and copies keys and values into memtable-owned arena blocks.
Convenience extension methods encode the following
types as keys and values:

| Type      | Notes                                                  |
|-----------|--------------------------------------------------------|
| `int`     | Order-preserving (sign-flipped big-endian) key bytes   |
| `uint`    | Order-preserving (big-endian) key bytes                |
| `long`    | Order-preserving (sign-flipped big-endian) key bytes   |
| `ulong`   | Order-preserving (big-endian) key bytes                |
| `string`  | UTF-8 encoded; keys ordered by Unicode code point      |

The internal encoders still define the on-disk representation. Typed helpers use those encoders before
delegating to the byte API.

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

`Put(ReadOnlySpan<byte>, ReadOnlySpan<byte>)` is the core path: Silex copies borrowed bytes immediately
into the active memtable's append-only arena. A zero-length value is currently reserved as the delete
marker.

Copy-from extension overloads can materialize values from richer sources before copying them to the store:

```csharp
db.Put("payload", stream);
await db.PutAsync("payload-async", stream, cancellationToken);
db.Put("payload-sequence", readOnlySequence);
db.Put("payload-token", in utf8JsonReader); // current token value bytes
```

These overloads copy the source data into Silex-owned memory first. `Utf8JsonReader` string and property
name tokens are copied as unescaped UTF-8 bytes.

### Get

```csharp
int value = await db.GetInt32Async(key);
```

Typed `Get*Async` helpers decode raw stored bytes with the matching built-in encoder. Raw reads are the
lowest-allocation path and expose borrowed value bytes only for the duration of the call.

### Raw value reads (zero-allocation)

The raw read overloads look the value up and hand you the stored bytes directly. They report a missing or
deleted key as not-found.

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

Use raw scans to enumerate entries in ascending encoded-key order across all memtables and on-disk levels.

```csharp
using Silex;

await db.ScanRawAsync(Console.Out, static (writer, key, value) =>
{
    writer.WriteLine($"{Convert.ToHexString(key)} = {Convert.ToHexString(value)}");
    return true;
});
```

## Closing the store

Always close the store to flush in-memory data to disk and stop the background compaction/flush threads.
`CloseAsync`, `DisposeAsync`, and `Dispose` are equivalent — call one:

```csharp
await db.CloseAsync();
// or
await db.DisposeAsync();
// or (via a using block)
await using var db = await LsmStorage.OpenAsync("my-db", options);
```

There is no finalizer: durability is provided solely by deterministic disposal. An undisposed store
leaks no native handles, but any data still buffered in memory is not flushed.

## Configuration

`StorageOptions` exposes the engine's tuning knobs. All have sensible defaults, so `new StorageOptions()`
is a valid starting point. The most commonly used options:

| Option                     | Default            | Description                                                              |
|----------------------------|--------------------|--------------------------------------------------------------------------|
| `MemTableSizeLimit`        | 64 MiB             | Size at which the active memtable is frozen and queued for flushing.     |
| `MemTableArenaBlockSize`   | 32 KiB             | Append-only block size used by byte-oriented memtables.                  |
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
await using var db = await LsmStorage.OpenAsync("db", options);

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
