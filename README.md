# Silex

Silex is an embedded, log-structured merge-tree (LSM) storage engine for .NET. It is a single-process,
in-process key/value store designed for high write throughput and low allocation. Data is buffered in
memory, persisted to immutable sorted-string tables (SSTs) on disk, and compacted in the background to
bound read and space amplification.

## Features

- **Generic-key store** – `LsmStorage<TKey>` works with any supported key type. Values are always
  opaque byte sequences (`byte[]`, `ReadOnlySpan<byte>`, or `Bytes`).
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

// The key is strongly typed; values are byte payloads. The directory is created if it does not exist.
await using var db = await LsmStorage.OpenAsync<uint>("my-db", options);

db.Put(1, "one"u8.ToArray());
db.Put(2, "two"u8.ToArray());

byte[]? value = await db.GetAsync(1);   // "one" as UTF-8 bytes
byte[]? missing = await db.GetAsync(99); // null when absent

db.Delete(2);

// Closing (or disposing) flushes pending data to disk and stops background work.
await db.CloseAsync();
```

`LsmStorage.OpenAsync<TKey>` reopens an existing store at the same path, replaying the WAL and
loading existing SSTs, so persisted data survives process restarts.

## Supported key types and values

Only the **key** is a generic type parameter; **values are always opaque byte sequences**. The two
roles are deliberately different: keys must be ordered, values never are.

**Key types** must be encoded in an *order-preserving* form, because the engine binary-searches blocks by
comparing raw encoded bytes. Only types with a well-defined byte ordering are allowed:

| Key type | Notes                                                                       |
|----------|-----------------------------------------------------------------------------|
| `uint`   | Order-preserving, fixed 4-byte big-endian                                   |
| `ulong`  | Order-preserving, fixed 8-byte big-endian                                   |
| `string` | UTF-8 encoded; ordered by Unicode code point (ordinal, not culture-aware)   |
| `byte[]` | Stored as-is; ordered by raw byte sequence                                  |
| `Bytes`  | An owned, comparable byte buffer (see below); ordered by raw byte sequence  |

Signed integers (`int`, `long`) and `char`/`ushort` are intentionally **not** supported as keys. Use an
unsigned type if you need numeric ordering, or a byte buffer / string otherwise. Using an unsupported key
type throws `NotSupportedException` the first time its encoder is resolved (when the store is opened).

**Values** are not a type parameter at all — every value is just a sequence of bytes, written through
`Put` overloads that accept a `byte[]`, a `ReadOnlySpan<byte>`, or a `Bytes`. There is no value encoder,
no per-store value type, and no on-disk type marker for values. An **empty value** (`Length == 0`) is the
deletion tombstone, so the entire byte range — including the all-`0xFF` value — is storable; there is no
reserved sentinel and therefore no latent data-loss bug. To store a number as a value, encode it into a
fixed-width `byte[]`/span yourself (for example with `BinaryPrimitives`).

> **Key ordering.** Keys are encoded in an *order-preserving* form, so the engine can binary-search a
> block by comparing raw encoded bytes on the hot read path — no per-entry key is materialized and no
> allocation occurs. `uint`/`ulong` keys therefore sort by their natural numeric value, and `string` keys
> sort by Unicode code point (equivalent to UTF-8 byte order), which is ordinal — not culture-sensitive.

> **On-disk format.** SST and WAL files store raw encoded key/value bytes with no embedded type marker, so
> the type set is part of the format contract, not something recorded per store. Changing the encoders
> (including this restriction) is a breaking on-disk change: there is no migration — open a fresh store
> with the new types.

## Reading and writing

### Put

```csharp
db.Put(key, value);
```

`Put` is synchronous and inserts or replaces the value for `key`. It is overloaded on the value type:

| Overload                          | Allocation                          | Ownership                                                        |
|-----------------------------------|-------------------------------------|-----------------------------------------------------------------|
| `Put(TKey, byte[])`               | Zero-copy; the array is stored as-is| Ownership transfers to the engine; do not mutate/release it     |
| `Put(TKey, ReadOnlySpan<byte>)`   | One `byte[]` copy of the span       | Caller keeps owning the span's backing memory                   |
| `Put(TKey, Bytes)`                | Copies the bytes out               | Caller keeps owning (and must `Dispose`) the `Bytes`            |

For `byte[]` (and any pooled `key`), ownership transfers to the engine: do not mutate or release them
after the call (for example, do not return a pooled buffer), because the engine keeps and reads them
until the owning memtable is flushed. The span and `Bytes` overloads copy, so the caller retains
ownership of the source buffer.

An **empty value** — `Array.Empty<byte>()`, a `default`/empty span, or `Bytes.Empty` — is treated as a
deletion, identical to calling `Delete(key)`.

To store a fixed-width number without allocating an intermediate array, reinterpret it as bytes and use
the span overload:

```csharp
long id = 42;
ReadOnlySpan<byte> bytes = MemoryMarshal.AsBytes(
    MemoryMarshal.CreateReadOnlySpan(ref id, 1));
db.Put(key, bytes);
```

### Get

```csharp
byte[]? value = await db.GetAsync(key);
```

Returns the stored value as a freshly allocated `byte[]`, or `null` when the key is absent or has been
deleted. For zero-allocation reads, use the raw read overloads below.

### Raw value reads (zero-allocation)

When you store byte payloads under a typed key, `GetAsync` materializes a fresh `byte[]` for every read.
The raw read overloads avoid that copy: they look the value up by its typed key and hand you the stored
bytes directly. They are the allocation-free path for reads. All three report a missing or deleted key as
not-found (unlike `GetAsync`, which returns `null`).

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

Records a tombstone for `key`. Subsequent reads return `null`, and the space is reclaimed
during compaction.

### Range scans

`CreateIterator` returns an iterator that yields entries in ascending key order across all memtables and
on-disk levels.

```csharp
using Silex;

IStorageIterator<uint, byte[]> iterator = db.CreateIterator();

// Scan the entire key space.
await foreach (KeyValuePair<uint, byte[]> entry in iterator.EnumerateAsync())
{
    Console.WriteLine($"{entry.Key} = {entry.Value}");
}

// Or scan from a starting key (inclusive), in ascending order.
await foreach (KeyValuePair<uint, byte[]> entry in iterator.EnumerateAsync(from: 100))
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
await using var db = await LsmStorage.OpenAsync<uint>("my-db", options);
```

There is no finalizer: durability is provided solely by deterministic disposal. An undisposed store
leaks no native handles, but any data still buffered in memory is not flushed.

## The `Bytes` type

`Bytes` is a value-typed, content-comparable wrapper over a byte buffer. It is the **low-allocation
alternative to `byte[]`**: where `byte[]` is simple and GC-allocated, `Bytes` is backed by a pooled buffer
(`MemoryOwner<byte>` over `ArrayPool<byte>.Shared`) so that keys and values can be created and discarded on
the hot path without producing garbage. Both are valid as keys and values; pick based on the trade-off:

| Aspect      | `byte[]`                               | `Bytes`                                                |
|-------------|----------------------------------------|--------------------------------------------------------|
| Allocation  | Allocates on the managed heap (GC)     | Rents from `ArrayPool`; near-zero steady-state garbage |
| Lifetime    | Reclaimed automatically by the GC      | You **must** `Dispose()` to return the buffer to the pool |
| Comparison  | Reference type; compared by content    | Value type; compared and hashed by content             |
| Best for    | Simplicity, occasional or borrowed data| High-throughput, allocation-sensitive workloads        |

Because the buffer is pooled, **ownership matters**:

- When you `Put` a `Bytes`, ownership of the buffer transfers to the engine; do not dispose or mutate it
  afterwards.
- A read returns a borrow valid only until the next operation; wrap it in a new `Bytes` (which copies) if
  you need to keep it.
- `default(Bytes)` and `Bytes.Empty` own no buffer. They are safe to use and safe to `Dispose()` (it is a
  no-op), so empty values never need special handling.

```csharp
await using var db = await LsmStorage.OpenAsync<Bytes>("my-db", options);

using var key = new Bytes(new byte[] { 1, 2, 3 });
db.Put(key, new Bytes("hello world"u8.ToArray())); // value ownership transfers to the engine
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
await using var db = await LsmStorage.OpenAsync<uint>("db", options);

foreach (var x in Enumerable.Range(0, 1_000_000))
{
    var value = new byte[sizeof(int)];
    BinaryPrimitives.WriteInt32BigEndian(value, x);
    db.Put((uint)x, value);
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

- A single `LsmStorage<TKey>` instance is intended to be shared for concurrent reads and writes;
  writes are serialized internally.
- Because reads return zero-copy borrows of engine-owned memory, do not retain a returned key/value
  beyond its immediate use unless you copy it. Treat returned data as read-only.
