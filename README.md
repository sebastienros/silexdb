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
- **Optional MVCC** – stable snapshots and optimistic transactions are available through a separate facade,
  without adding sequence checks or larger internal keys to the default store.
- **Pluggable compaction** – choose `Tiered` (write-optimized), `Leveled` (read-optimized), or `None`.
- **Bloom filters and a block cache** – skip SSTs that cannot contain a key and cache hot blocks.
- **Adaptive SST compression** – LZ4 by default, optional Zstandard or none, with raw fallback for
  blocks that do not compress enough.
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

### Recovery, integrity, and salvage

Normal opening refuses a database with a missing manifest-referenced SST, even when a WAL with
the same id survives: that WAL alone does not prove that all committed contents can be recovered.
`StorageRecoveryException.Report` identifies the missing files and recommends restoring a consistent
backup. A failed recovery leaves the committed manifest and source files intact.

Inspect a database **offline**, with no other handle using its directory:

```csharp
var report = await LsmStorage.VerifyIntegrityAsync("my-db", options);
foreach (var issue in report.Issues)
    Console.WriteLine($"{issue.Kind}: {issue.Path}: {issue.Message}");
```

Verification reads live SST metadata and every data block (including checksums, decoding, entry
extents, and index/key ordering), and validates WAL frames needed for recovery. It does not modify
recovery files; verification and salvage may create the persistent `LOCK` ownership file. Orphan SSTs
and stale WALs superseded by verified SSTs are ignored. An incomplete trailing WAL
frame is reported as `IncompleteWalTail`, not an error: earlier committed frames still recover normally.
Missing committed SSTs and corrupt committed WAL frames instead make `IsRecoveryComplete` false.
Checksums cannot detect every possible alteration of legacy formats or unchecksummed metadata;
completeness is relative to the manifest and surviving WALs, not a substitute for backups.
Legacy stores without a manifest remain supported, but missing files in those stores cannot be detected.

For explicit best-effort extraction, open a **read-only** salvage handle:

```csharp
await using var salvage = await LsmStorage.OpenSalvageAsync("my-db", options);
Console.WriteLine($"Complete: {salvage.RecoveryReport!.IsRecoveryComplete}");
await salvage.ScanRawAsync(destination, static (db, key, value) =>
{
    db.Put(key, value);
    return true;
});
```

Here `destination` is a different, normally opened database. Salvage skips missing/corrupt SSTs and
retains valid prefixes of damaged WALs. It never deletes evidence, creates WALs, rewrites the manifest,
flushes, or compacts, even on disposal; writes and manual maintenance throw. A corrupt manifest must
be restored before salvage because guessing which SSTs are live can resurrect deleted or obsolete data.
Salvaged results can omit committed data or expose older values/tombstones hidden by a lost newer SST;
review the report and extracted data before replacing the original database. Use the same WAL setting
as the original store when opening either normally or for salvage.

Use `LsmWriteBatch` when several byte-oriented mutations can share one durability boundary. The batch is
written as one checksum-protected WAL frame, reducing write and `fsync` calls while preserving the default
process-crash recovery and optional file-content synchronization boundaries:

```csharp
var batch = new LsmWriteBatch();
batch.Put("user:1"u8, "active"u8);
batch.Delete("user:2"u8);
db.Write(batch);
```

### Physical backup/checkpoint and restore

Create an independently recoverable checkpoint while the source remains open:

```csharp
await db.CreateBackupAsync("my-db-backup");
await LsmStorage.RestoreBackupAsync("my-db-backup", "my-db-restored", options);
await using var restored = await LsmStorage.OpenAsync("my-db-restored", options);
```

The checkpoint captures one serialized write boundary, including all previously completed writes
and excluding writes acquired afterward. It holds the maintenance and write locks while copying:
compaction cannot replace/delete required SSTs, and writers cannot append or rotate the copied WALs.
The committed manifest, live SSTs, and current/rotated/recovered WALs form the recovery set; orphans,
stale WALs, temporary files, and the source `LOCK` are not copied. WAL-disabled stores instead write
their memory tables into backup-only SSTs. The source is not flushed or otherwise modified.
This favors a simple correctness boundary over uninterrupted writes during large backups.

`MvccStorage.CreateBackupAsync` additionally holds the commit gate, preserving a published sequence
and complete transaction/group-commit boundaries. Staged transactions and subsequent commits are
excluded. Stored versions are preserved, but live snapshot/transaction objects do not transfer.
Use `MvccStorage.RestoreBackupAsync` and reopen through `MvccStorage`; the restore also checks MVCC
format and sequence metadata. Backups from the wrong facade are rejected.

Both APIs require an empty destination (a persistent `LOCK` alone is allowed), exclusively own it
until completion, and never overwrite existing data. Restore also exclusively owns the backup source.
Neither directory may contain the other; directory aliases are resolved and recovery-file links are
rejected. External file/directory replacement while these operations run is unsupported, just as with
an open database. Use matching encoding factories and enable WAL recovery if the backup contains WALs.
Keep completed backup directories **immutable**: do not open them as writable stores. Restore copies
them without consuming them, so one backup can restore multiple independent databases.

A completion inventory records filenames, lengths, and SHA-256 checksums. Creation verifies recovery
integrity before publishing that inventory. Restore validates the inventory and every recovery file,
then verifies the copied files and recovery structure before returning a normally openable destination.
Missing WALs/manifests are detected, even where ordinary legacy recovery could not detect their loss.
Malformed, corrupt, missing, or unexpected files and unfinished copies are rejected. An incomplete
uncommitted WAL tail inherited from source recovery is still recoverable and is not confused with
committed corruption; its exact bytes are protected by the inventory checksum.
Checksums detect accidental changes, not malicious replacement of
both the inventory and files; this is not an authenticated backup format.

Cancellation is checked during copying and verification, and while waiting for maintenance/commit
gates; a synchronous write-lock wait cannot be interrupted. Copying uses bounded pooled buffers and
runs off the caller's synchronization context. Failure/cancellation removes only files created by
that call, leaving the destination directory and its persistent `LOCK`, so it can be retried.
Cleanup errors are reported explicitly as `AggregateException`; do not use that partial destination.
A `backup.pending` guard makes process-interrupted copies reject normal opening and restore. Start
again with an empty destination rather than trying to recover an interrupted copy. Backup/copy failure
does not undo commits or fault an otherwise healthy source. Salvage and faulted sources are rejected.
Do not invoke these APIs from borrowed-read callbacks or wait for shutdown inside one.

Copied/generated files and completion metadata request OS file-content synchronization. As with
`SynchronizeAsync`, directory-entry persistence and power-loss safety depend on the filesystem/device;
Silex does not promise directory synchronization or power-loss certification.

### Durability boundaries and explicit synchronization

**Applied/visible**, **written to the operating system**, and **synchronized** are different boundaries.
Writes are serialized under the mutable-table lock. With WAL enabled, the complete checksummed recovery
frame is written first, then optionally synchronized, then applied in memory. Readers cannot observe a
partially applied successful batch. MVCC additionally publishes its sequence after applying the frame;
only published sequences are visible to new snapshots.

| Configuration | Successful `Put`, `Delete`, `Write`, or MVCC commit guarantees |
|---|---|
| `UseWriteAheadLog = false` (`SyncWriteAheadLogToDisk` ignored) | Entire mutation applied/visible; no recoverable file for unflushed memory, so no process-crash or stable-storage guarantee. |
| WAL enabled, `SyncWriteAheadLogToDisk = false` (default) | Entire mutation applied/visible and complete WAL frame submitted to the OS. Supports process-crash recovery with the OS still running; stable-storage synchronization is not promised. |
| WAL enabled, `SyncWriteAheadLogToDisk = true` | The preceding guarantees, plus successful OS file-content synchronization (`FileStream.Flush(true)`) of the WAL before in-memory application and acknowledgment. |

For a precise explicit boundary without synchronizing every append:

```csharp
db.Put("user:1"u8, "active"u8); // applied/visible and written to the OS
await db.SynchronizeAsync();   // synchronize through this successfully completed write
```

`LsmStorage.SynchronizeAsync` and `MvccStorage.SynchronizeAsync` cover **all writes/commits completed before
the call** (and any racing writes serialized before the barrier acquires the write lock). No guarantee
is made for writes serialized afterward. This is a whole-store barrier, not a write receipt, transaction,
checkpoint, or group-commit API; it does not stage, apply, or publish new mutations. Await a successful
MVCC commit first; pending transaction buffers are excluded.

The barrier blocks writes/rotation and serializes with flush/compaction while requesting disk
synchronization for the current WAL, every rotated or recovered WAL, every live L0/leveled SST, and
finally the manifest. Thus a write is still covered after its original WAL has been flushed/deleted or
its SST compacted. It does not flush memtables or run compaction. Repeated barriers are supported and
may be expensive: they synchronize the live file set, not just a tracked unsynchronized suffix.
WAL-disabled stores throw `NotSupportedException` without faulting, rather than falsely acknowledging
unflushed memory. Read-only salvage stores reject it like other writable-only operations.

Cancellation is honored while asynchronously waiting for the maintenance/MVCC commit gate and once more
after acquiring the synchronous mutable-table lock, before the first file I/O. Waiting for that synchronous
lock cannot be interrupted; file synchronization may run synchronously on the calling thread, and after
file I/O begins cancellation is not polled. Successful completion reports the actual boundary, even if
the token was canceled during synchronization. An admitted barrier is drained by shutdown; new barriers are rejected
once shutdown starts. Do not invoke it from a borrowed-read callback (it requires the write lock).

Flush/compaction synchronize each replacement SST's contents and the new manifest's contents before
retiring the old recovery files, preserving the file-content boundary across maintenance and reopen.
**These are OS file-content synchronization requests, not power-loss certification.** Silex does not
explicitly synchronize directory entries for WAL creation, SST/manifest renames, or file deletion.
Their persistence, the OS/filesystem's synchronization semantics, and device/controller caches still
matter. No configuration or barrier promises survival of every power loss, hardware/media failure, or
filesystem failure. Source inspection and process-crash tests are not power-loss tests.

### Operation failures and instance health

Successful `Put`, `Delete`, `Write`, and MVCC commits apply the entire mutation and reach the
configured WAL boundary. `UseWriteAheadLog = true` writes a complete recovery frame to the operating
system; `SyncWriteAheadLogToDisk = true` additionally requests disk synchronization. With WAL disabled,
success guarantees visibility, not crash durability. These are not guarantees against hardware,
filesystem, or media failure, and process-crash tests do not certify power-loss safety.

Mutation failures originating inside the write engine throw `StorageOperationException`, preserving
the original exception in `InnerException`. Inspect `Outcome` and `Health`, not just the exception type:

| Failure boundary | Outcome | Instance |
|---|---|---|
| Argument validation, caller-side encoding/batch preparation, MVCC conflict, or cancellation before committing starts | Definitely not committed; original validation/cancellation exception, or `false` for `TryCommitAsync` conflicts | Healthy |
| WAL frame preparation fails before any frame bytes are submitted | `NotCommitted` | Healthy |
| WAL write starts and fails (including disk full or a torn write), or synchronization fails | `Indeterminate` | Faulted |
| In-memory mutation/batch application fails after WAL persistence, or while applying with WAL disabled | `Indeterminate`; memory may contain only part of the batch | Faulted |
| The entire mutation reaches its boundary, but subsequent publication or automatic memtable rotation fails | `Committed`; for MVCC, the persisted sequence may not yet be published to readers in this instance | Faulted |

An explicit barrier's I/O failure throws `StorageSynchronizationException` with the original cause in
`InnerException`. The requested synchronization boundary is **unconfirmed** (some or all files may
already have synchronized), and the instance becomes faulted. Previously successful mutations remain
committed to their original acknowledgment boundary; the barrier does not undo them and its exception
is not a mutation's `NotCommitted`/`Indeterminate` outcome. Dispose and reopen, do not retry writes merely
because synchronization failed. Cancellation before file I/O, unsupported WAL configuration, and
read-only rejection leave health unchanged.

`Indeterminate` is deliberately conservative: recovery may apply the entire WAL frame, or discard an
incomplete trailing frame. It never means "nothing committed." A complete checksummed batch frame is
replayed as a whole when recovery succeeds; failed recovery returns no usable instance. With WAL
disabled, unflushed changes can be lost on reopening. Batches must not be mutated concurrently with
`Write`. Deduplication, retry safety, and request identity belong to callers; never blindly retry an
indeterminate mutation.

`LsmStorage.Health` and `MvccStorage.Health` report `Healthy`, `Faulted`, or `Disposed`; `Fault` retains
the first unsafe failure. Faulting is permanent for that instance. New reads, writes, snapshots,
transactions, scans, and maintenance reject it with `StorageFaultedException`. Operations already in
flight may finish; readers waiting for mutable-table access check health before reading, so they cannot
observe a failed batch's partial application. Health inspection and disposal remain available. Health is
an operational state, not an integrity certification: an explicitly salvaged store is usable only for
read-only extraction and still requires inspection of its incomplete `RecoveryReport`.

Cancellation before an ordinary MVCC commit's final write begins throws `OperationCanceledException` and does
not commit. Once writing starts, cancellation is not polled: completion reports the actual write
result rather than a misleading cancellation. With optional group commit, cancellation instead ends at
acceptance after validation and batch preparation (see below). The synchronous byte-store mutations do not accept a
cancellation token.

Flush/compaction failures do not undo previously acknowledged writes. Cancellation before maintenance
starts (including while waiting for its serialization lock) leaves the instance healthy. Once maintenance
starts, any exception, including cancellation, conservatively faults the instance and requires reopening;
the exception itself is propagated to the caller. Background maintenance records failures in `Fault` and
stops instead of silently continuing. Stopping the background loop drains an already-started maintenance
operation rather than canceling it halfway through.

Read cancellation or an exception in a caller's read callback makes no mutations and does not fault the
instance. `CollectGarbageAsync` performs individual deletions, not one atomic transaction: cancellation
can leave a successfully committed prefix of its work while the instance remains healthy. Its optional
flush/compaction phase follows the structural-maintenance contract above.

Dispose a faulted instance before reopening the same directory. Faulted disposal stops background work
and releases resources **without flushing partial memory, retrying WAL synchronization, publishing a new
manifest, or deleting recovery files**. Reopening resolves recoverable WAL/manifest state; if it fails,
restore or use explicit integrity/salvage tooling instead of treating the instance as healthy. A failed
`CloseAsync` can fault the instance; subsequent disposal performs this recovery-preserving cleanup.
If creation of the next WAL fails during its header write, the valid header prefix contains no mutations:
strict recovery treats it as an incomplete tail, retains earlier committed WAL frames, and removes the
empty WAL only after opening succeeds. Invalid signatures or corrupt complete headers remain errors.
Directory ownership and shutdown admission are enforced as described below.

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
into the active memtable's append-only arena. Empty values are stored as live values; deletion uses a
distinct internal tombstone marker.

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
// length ==  0            -> key exists with an empty value
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

## Optional MVCC and transactions

Open a path with `MvccStorage` when it needs stable snapshots or multi-key transactions:

```csharp
await using var db = await MvccStorage.OpenAsync("accounts-db", new StorageOptions());

db.Put("alice"u8, "30"u8);
db.Put("bob"u8, "0"u8);

using var snapshot = db.CreateSnapshot();

using var transaction = db.BeginTransaction();
var balance = new byte[16];
int length = await transaction.GetForUpdateRawAsync("alice"u8, balance);

transaction.Put("alice"u8, "20"u8);
transaction.Put("bob"u8, "10"u8);

if (!await transaction.TryCommitAsync())
{
    // A tracked or written key changed after the transaction started; retry from a new transaction.
}
```

`MvccStorage` is deliberately a separate facade rather than a `StorageOptions` switch in the core engine.
Regular `LsmStorage` databases keep their original key format and hot paths: no sequence allocation, version
suffix, snapshot registry, or transaction validation is paid by applications that do not use MVCC. A database
path must always be opened through the same facade; an existing plain database is rejected by
`MvccStorage.OpenAsync`.

The MVCC facade stores user keys in escaped lexicographic form followed by a descending sequence number.
Commits reserve a sequence, write every version, and publish the sequence last. Readers capture only the
published sequence, so they see either all of a multi-key commit or none of it. With the WAL enabled, the
allocated/published sequence barrier also leaves interrupted commits invisible after recovery.

Transactions provide snapshot isolation with optimistic commit-time validation:

- Written keys detect write-write conflicts automatically.
- `GetForUpdateRawAsync` also tracks read-write conflicts and prevents point-read write skew.
- Plain `GetRawAsync` reads do not participate in conflict detection.
- Range reads are not range-locked, so transactions can still observe phantom conflicts at the application
  level; serializable range transactions are not provided.

MVCC users pay for larger physical keys (11 bytes plus one escape byte per zero in the user key), retained old
versions, transaction buffers, and sequence metadata writes. Dispose snapshots and transactions promptly, then
call `CollectGarbageAsync` periodically to remove versions that no active view can observe. Pass
`compact: true` when the call should also flush and compact the resulting physical tombstones.

### Durable transaction group commit

Set `MvccCommitGroupSize` to a value from 2 through 64 to let concurrent transaction commits share WAL
file-content synchronization; the default of 1 preserves the ordinary commit path. Grouping requires
both `UseWriteAheadLog = true` and `SyncWriteAheadLogToDisk = true`. It does not change plain LSM writes,
MVCC `Put`/`Delete`, or the WAL format.

The engine admits at most twice the configured group size, with further commit callers waiting on
cancelable admission. A worker takes up to the configured number of **already ready** transactions in
queue-admission order after acquiring the MVCC commit gate. There is no timer, minimum group size,
or deliberate latency window; a single ready transaction can proceed immediately. Scheduler, gate,
validation, and I/O contention can still delay completion; this is not a bounded wall-clock latency
promise. Transaction buffers must not be changed or disposed during a commit.

Validation and publication remain serialized. Each transaction checks both changes published since
its starting snapshot and the written keys of earlier accepted transactions in its group, including
tracked point-read dependencies. Conflicts return `false` and contribute no frame or sequence.
Cancellation is checked during admission, validation, and once more after preparing the transaction's
frame data. Acceptance ends cancellation: canceling an accepted transaction does not interrupt the
group or return a misleading cancellation result. Read-only transactions require no WAL boundary.

Each accepted writing transaction retains its **own atomic recovery frame**, including its sequence
metadata. All frames in the group are written under one mutable-table lock, synchronized once, then
applied and published in order. Successful writing commits are acknowledged only after synchronization
and publication succeed; readers still capture an atomic transaction snapshot. Memtable rotation happens
after the group, so the size threshold can be exceeded by one group before rotation.

If frame submission fails, an unsubmitted transaction reports `NotCommitted`; submitted transactions
report `Indeterminate`, including complete earlier frames whose synchronization was not confirmed.
Synchronization or application failure faults the instance and does not acknowledge any writing
transaction as successful. Transactions fully applied after successful synchronization report
`Committed` if later application, publication, or rotation fails; remaining submitted transactions
report `Indeterminate`. A completed frame may recover even when its caller received an exception.
The store retains the first unsafe failure, rejects further work, and preserves WAL evidence on disposal,
as on the ordinary path. A failure before any frame submission leaves the instance healthy.

Explicit `SynchronizeAsync` barriers still serialize with commit publication and cover previously
completed commits; await the commit first to require its inclusion. Shutdown rejects new admission,
drains an already accepted group, completes queued but unaccepted commits with disposal errors, and
then closes the underlying store. The existing file-content (not directory or universal power-loss)
durability limitations apply unchanged.

Local throughput and latency measurements, commands, and limitations are recorded in
[`tests/Silex.WalBenchmarks/group-commit.md`](tests/Silex.WalBenchmarks/group-commit.md).

## Closing the store

### Exclusive directory ownership

Each normally opened database, MVCC database, and read-only salvage handle exclusively owns its
directory until resource cleanup completes. `VerifyIntegrityAsync` takes the same exclusive ownership
for its entire inspection. A competing open or verification, from the same process or a different process,
fails promptly with `IOException` before reading or modifying recovery files. The error identifies the
directory and advises closing the existing owner; it does not wait or return a partially opened handle.
Path aliases that reach the same directory use the same lock file.

Ownership uses an OS-backed exclusive handle to the persistent `LOCK` file. Process exit releases
the handle, including after an unclean exit; the file's existence is not evidence of an active owner.
**Do not delete, replace, or move the lock file or database directory while any handle owns it.**
Deleting an open lock file can defeat exclusion on Unix. Legacy directories acquire this metadata file
on first use; verification/salvage still never change WAL, SST, or manifest evidence. An existing lock
file can be opened read-only, but creating it requires directory write permission.
All participants must use this ownership protocol on a filesystem that supports OS file sharing locks.
It does not protect against external tools bypassing locks or certify network-filesystem locking.

### Coordinated shutdown

Always close the store to flush healthy in-memory data to disk and stop background maintenance.
`CloseAsync`, `DisposeAsync`, and `Dispose` are equivalent — call one:

```csharp
await db.CloseAsync();
// or
await db.DisposeAsync();
// or (via a using block)
await using var db = await LsmStorage.OpenAsync("my-db", options);
```

WAL-enabled writes reach their configured durability boundary before returning, independently of clean
shutdown. Deterministic disposal is still required to release resources and flush healthy memtables into
SSTs; with WAL disabled, unflushed memory is not crash-durable. Faulted instances are closed without
flushing, preserving their recovery evidence as described above.

The supported application lifecycle is to **stop accepting work, drain requests, and then dispose
storage**. The engine also defines races: shutdown atomically closes operation admission, rejects new
reads/writes/scans/maintenance with `ObjectDisposedException`, and waits for admitted operations and
background maintenance before releasing memory, file handles, and directory ownership. An admitted
operation may finish or report its normal cancellation/failure outcome; disposal does not cancel it
or turn a completed mutation into an uncommitted result. Health remains available throughout cleanup.
Concurrent close/dispose calls share completion, rather than letting a second disposer return early.

An iterator holds admission for its entire active enumeration, including between advances. Finish or
dispose enumerators before awaiting shutdown; snapshots/transactions cannot be used after their MVCC
store starts shutdown. Never invoke or synchronously wait for disposal from a storage callback: shutdown
would wait for that callback itself. Borrowed read callbacks must remain short and must not reenter storage.
Shutdown can wait indefinitely for a caller that does not release a borrow or enumerator.

`CloseAsync` cancellation is admission-only: a token already canceled before shutdown starts leaves the
store open. Once shutdown starts it drains and cleans up without cancellation; concurrent callers await
that same shutdown regardless of their tokens. A shutdown failure is propagated, faults the instance,
and retains directory ownership and recovery evidence. Call disposal again to perform faulted cleanup
without flushing; operations remain rejected in the meantime. MVCC disposal performs that cleanup itself
before propagating a shutdown failure.

## Configuration

`StorageOptions` exposes the engine's tuning knobs. All have sensible defaults, so `new StorageOptions()`
is a valid starting point. The most commonly used options:

| Option                     | Default            | Description                                                              |
|----------------------------|--------------------|--------------------------------------------------------------------------|
| `MemTableSizeLimit`        | 64 MiB             | Size at which the active memtable is frozen and queued for flushing.     |
| `MemTableArenaBlockSize`   | 32 KiB             | Append-only block size used by byte-oriented memtables.                  |
| `MemTableMaxCount`         | 50                 | Max immutable memtables kept in memory before flushing.                  |
| `BlockSize`                | 4 KiB              | Unit of data read from/written to disk at once.                          |
| `Compression`              | `Lz4`              | SST data-block compression: `None`, `Lz4`, or `Zstandard`.               |
| `CompressionLevel`         | `0`                | Fast LZ4/default Zstandard; LZ4 HC accepts levels 3–12.                  |
| `MinimumCompressionSavingsPercent` | 12.5       | Store a block raw unless compression saves at least this percentage.     |
| `FlushPeriod`              | 50 ms              | Interval between background flushes. `TimeSpan.Zero` disables the thread.|
| `BlockCacheSizeLimit`      | 1 MiB              | Size of the in-memory cache of decoded blocks.                           |
| `UseWriteAheadLog`         | `true`             | Maintain a WAL so unflushed data is recovered after a crash.             |
| `SyncWriteAheadLogToDisk`  | `false`            | Request OS file-content synchronization for every WAL frame before applying/acknowledging it; see durability limitations above. |
| `MvccCommitGroupSize`      | `1`                | Maximum ready MVCC transactions sharing one synchronization (1 disables; 2-64 require synchronous WAL durability). |
| `CompactionStrategy`       | `Tiered`           | `None`, `Tiered` (write-optimized), or `Leveled` (read-optimized).       |

Convenient size helpers are available as extension methods: `64.MiB()`, `4.KiB()`, `1.GiB()`,
`512.B()`.

```csharp
var options = new StorageOptions
{
    MemTableSizeLimit = 16.MiB(),
    CompactionStrategy = CompactionStrategy.Leveled,
    Compression = SstCompression.Zstandard,
    UseWriteAheadLog = true,
};
```

Compression is selected independently for every SST data block and recorded in the file, so raw, LZ4,
and Zstandard blocks can coexist and remain readable when the database is reopened with different write
options. The block cache stores decompressed blocks, keeping cache hits free of decompression work. Stored
blocks are protected by an XXH32 checksum, and existing legacy uncompressed SSTs remain readable.

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
compaction. It accepts `--compression_type=none|lz4|zstd`, `--compression_level`, and
`--compression_ratio`. For fair runs, use matching compression settings and the same values for
`--num`, `--key_size`, `--value_size`,
`--write_buffer_size`, `--block_size`, `--bloom_bits`, `--threads`, `--seed`, and the compaction knobs.

Current single-thread snapshot (2026-08-02, Apple M4 Pro, median of three alternating runs):

| Workload | Relative result |
| --- | ---: |
| Sequential, random, and overwrite writes | **Silex 1.19-1.52x faster** |
| First random-read pass | **RocksDB 1.07x faster** |
| Warm random reads | **Silex 1.12x faster** |
| Sequential scans | **Silex 1.15x faster** |
| Random seeks | **Silex 1.74x faster** |
| WAL fsync per write | **Tie at about 4.14 ms/op** |

These 200,000-entry, uncompressed runs fit in memory and measure warm-page/cache behavior rather
than device throughput. See the
[full comparison](Silex.DbBench/benchmarks/2026-05-30-rocksdb-comparison.md) for exact revisions,
settings, absolute measurements, corrected concurrency methodology, and durability caveats.

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
