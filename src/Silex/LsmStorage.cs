using Silex.Blocks;
using Silex.Compaction;
using Silex.Buffers;
using Silex.MemTables;
using Silex.Serialization;
using Silex.Ownership;
using Silex.Tables;
using Silex.Wal;
using System.Buffers;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace Silex;

public sealed partial class LsmStorage : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Opens or create a store at the specified location.
    /// </summary>
    /// <param name="path">The path of the store. If it doesn't exist it is created.</param>
    /// <param name="options">The storage options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    /// <remarks>
    /// Exclusively owns the directory until shutdown completes. A competing owner causes an
    /// <see cref="IOException"/>; do not delete the persistent LOCK file to bypass ownership.
    /// </remarks>
    public static Task<LsmStorage> OpenAsync(string path, StorageOptions options, CancellationToken cancellationToken = default)
    {
        return OpenCoreAsync(path, options, cancellationToken);
    }

    internal sealed class LsmStorageTyped<TKey, TValue> : IDisposable, IAsyncDisposable where TKey : notnull
    {
        private static readonly IBinaryEncoder<TKey> _keyEncoder = BinaryEncoderFactory<TKey>.BinarySerializer;
        private static readonly IBinaryEncoder<TValue> _valueEncoder = BinaryEncoderFactory<TValue>.BinarySerializer;

        internal readonly LsmStorageInner _inner;
        internal readonly Compacter _compacter;
        private readonly LsmStorage _storage;

        internal LsmStorageTyped(LsmStorage storage)
        {
            _storage = storage;
            _inner = storage._inner;
            _compacter = storage._compacter;
        }

        public async ValueTask<TValue> GetAsync(TKey key, CancellationToken cancellationToken = default)
        {
            CheckDisposed();
            using var encodedKey = Encode(_keyEncoder, key);
            using var value = await _inner.GetAsync(encodedKey.Slice, cancellationToken);
            return value is null ? default! : _valueEncoder.Decode(value.Span);
        }

        public async ValueTask<bool> TryGetRawAsync(TKey key, IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
        {
            CheckDisposed();
            using var encodedKey = Encode(_keyEncoder, key);
            return await _inner.TryGetRawAsync(encodedKey.Slice, destination, cancellationToken);
        }

        public async ValueTask<int> GetRawAsync(TKey key, Memory<byte> destination, CancellationToken cancellationToken = default)
        {
            CheckDisposed();
            using var encodedKey = Encode(_keyEncoder, key);
            return await _inner.GetRawAsync(encodedKey.Slice, destination, cancellationToken);
        }

        public async ValueTask<bool> TryReadRawAsync<TArg>(TKey key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
        {
            CheckDisposed();
            using var encodedKey = Encode(_keyEncoder, key);
            return await _inner.TryReadRawAsync(encodedKey.Slice, arg, reader, cancellationToken);
        }

        public ValueTask<long> ScanRawAsync<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
        {
            CheckDisposed();
            return _inner.ScanRawAsync(arg, reader, maxEntries, cancellationToken);
        }

        public async ValueTask<long> SeekRawAsync<TArg>(TKey from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
        {
            CheckDisposed();
            using var encodedFrom = Encode(_keyEncoder, from);
            return await _inner.SeekRawAsync(encodedFrom.Slice, arg, reader, maxEntries, cancellationToken);
        }

        public void Put(TKey key, TValue value)
        {
            CheckDisposed();
            using var encodedKey = Encode(_keyEncoder, key);
            using var encodedValue = Encode(_valueEncoder, value);
            _inner.PutRaw(encodedKey.Span, encodedValue.Span);
        }

        public void Delete(TKey key)
        {
            CheckDisposed();
            using var encodedKey = Encode(_keyEncoder, key);
            _inner.DeleteRaw(encodedKey.Span);
        }

        public IStorageIterator CreateIterator()
        {
            CheckDisposed();
            return _inner.CreateIterator();
        }

        public Task CloseAsync(CancellationToken cancellationToken = default) => _storage.CloseAsync(cancellationToken);

        public Task FlushAndCompactAsync(CancellationToken cancellationToken = default) => _storage.FlushAndCompactAsync(cancellationToken);

        public Task SynchronizeAsync(CancellationToken cancellationToken = default) => _storage.SynchronizeAsync(cancellationToken);

        public ValueTask DisposeAsync() => _storage.DisposeAsync();

        public void Dispose() => _storage.Dispose();

        private static OwnedByteSlice Encode<T>(IBinaryEncoder<T> encoder, T value)
        {
            return LsmStorageTypedExtensions.EncodeOwned(encoder, value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            _storage.CheckDisposed();
        }
    }

    internal static async Task<LsmStorageTyped<TKey, TValue>> OpenAsync<TKey, TValue>(string path, StorageOptions options, CancellationToken cancellationToken = default) where TKey : notnull
    {
        var storage = await OpenCoreAsync(path, options, cancellationToken);
        return new LsmStorageTyped<TKey, TValue>(storage);
    }

    private static async Task<LsmStorage> OpenCoreAsync(string path, StorageOptions options,
        CancellationToken cancellationToken = default, bool readOnly = false)
    {
        ArgumentNullException.ThrowIfNull(options);
        cancellationToken.ThrowIfCancellationRequested();
        if (!Directory.Exists(path))
        {
            if (readOnly)
            {
                throw new DirectoryNotFoundException($"Storage directory '{path}' does not exist.");
            }
            Directory.CreateDirectory(path);
        }

        var ownership = DirectoryOwnership.Acquire(path);
        try
        {
            if (File.Exists(Path.Combine(path, StorageBackup.PendingFileName)))
            {
                throw new InvalidDataException("The directory contains an incomplete physical backup/restore. Use a completed backup and an empty destination.");
            }
            if (File.Exists(Path.Combine(path, StorageBackup.FileName)))
            {
                throw new InvalidOperationException("A completed physical backup is immutable. Use RestoreBackupAsync to create a writable database.");
            }
            return await OpenOwnedAsync(path, options, ownership, cancellationToken, readOnly: readOnly);
        }
        catch
        {
            ownership.Dispose();
            throw;
        }
    }

    private static async Task<LsmStorage> OpenOwnedAsync(string path, StorageOptions options,
        DirectoryOwnership ownership, CancellationToken cancellationToken, StorageIntegrityReport? salvageReport = null,
        bool readOnly = false)
    {
        // Capture the WAL files that exist *before* the inner is constructed: the inner immediately
        // creates a fresh WAL for its initial current memtable, and that one must not be replayed.
        var walFiles = options.UseWriteAheadLog
            ? Directory.EnumerateFiles(path, "*.wal")
                .Select(filename => (filename, id: TryParseId(filename)))
                .Where(x => x.id.HasValue)
                .OrderBy(x => x.id!.Value)
                .ToList()
            : [];

        // SST files are named "{id}.sst" with monotonically increasing ids. Level-0 precedence
        // depends on creation order (the most recent table wins on duplicate keys), so load them
        // ordered by id rather than in arbitrary filesystem enumeration order.
        var sstFiles = Directory.EnumerateFiles(path, "*.sst")
            .Select(filename => (filename, id: TryParseId(filename)))
            .Where(x => x.id.HasValue)
            .OrderBy(x => x.id!.Value)
            .ToList();

        // Make sure freshly generated ids are strictly greater than every id already on disk *before*
        // the inner creates its initial current memtable (and its WAL). Otherwise a new WAL could be
        // opened with FileMode.Create over an existing, not-yet-replayed WAL and silently truncate it.
        foreach (var id in sstFiles.Select(x => x.id!.Value).Concat(walFiles.Select(x => x.id!.Value)))
        {
            IdGenerator.EnsureGreaterThan(id);
        }

        // The manifest is the authoritative record of which SSTs are live and how they map to levels. It is
        // read for every strategy so changing CompactionStrategy on reopen only changes future compactions,
        // not how existing files are interpreted. Older stores without a manifest fall back to id-ordered L0
        // recovery once and are migrated by the manifest write near the end of OpenAsync.
        var manifest = Manifest.TryRead(path);

        if (manifest != null && salvageReport == null)
        {
            var presentIds = new HashSet<long>(sstFiles.Select(item => item.id!.Value));
            var missing = manifest.AllSstIds()
                .Where(id => !presentIds.Contains(id))
                .Select(id => MissingSstIssue(path, id))
                .ToArray();
            if (missing.Length > 0)
            {
                throw new StorageRecoveryException(new StorageIntegrityReport(missing));
            }
        }

        var isReadOnly = readOnly || salvageReport != null;
        var storageInner = new LsmStorageInner(path, options, readOnly: isReadOnly);
        var opened = false;
        try
        {
            // Ids of the SSTs whose data is actually present in the recovered state. Used below to decide
            // whether a surviving WAL is stale (its data was already flushed and committed and is loaded) or
            // must be replayed. Only successfully loaded SSTs count: if a referenced SST is missing in salvage,
            // its WAL must still be replayed rather than discarded.
            HashSet<long> committedSstIds;

            var loadParallelism = Math.Max(1, options.MaxReadParallelism);

            // Loads the given SST files concurrently (bounded by MaxReadParallelism) into a position-indexed
            // array, preserving the requested order. On any failure every already-loaded table is disposed so a
            // failed open never leaks file handles.
            async Task<SsTable[]> LoadManyAsync((string filename, long id)[] items)
            {
                var loaded = new SsTable[items.Length];

                try
                {
                    await Parallel.ForEachAsync(
                        Enumerable.Range(0, items.Length),
                        new ParallelOptions { MaxDegreeOfParallelism = loadParallelism, CancellationToken = cancellationToken },
                        async (index, ct) =>
                        {
                            var (filename, id) = items[index];
                            var blockBuilder = new BlockBuilder(options.BlockEncoderFactory.Create());
                            try
                            {
                                loaded[index] = await SsTable.LoadSsTableAsync(filename, options.SsTableEncoderFactory.Create(), blockBuilder, options.BloomFilterFactory, id, ct);
                            }
                            catch
                            {
                                blockBuilder.Dispose();
                                throw;
                            }
                        });
                }
                catch
                {
                    foreach (var table in loaded)
                    {
                        table?.Dispose();
                    }

                    throw;
                }

                return loaded;
            }

            if (manifest != null)
            {
                var fileById = sstFiles.ToDictionary(x => x.id!.Value, x => x.filename);
                committedSstIds = new HashSet<long>();

                // Strict opens have checked all references; salvage keeps only verified surviving tables.
                async Task<List<SsTable>> LoadLevelAsync(IEnumerable<long> ids)
                {
                    var items = ids
                        .Where(id => fileById.ContainsKey(id) && !IsDamagedSst(fileById[id]))
                        .Select(id => (filename: fileById[id], id))
                        .ToArray();

                    var tables = await LoadManyAsync(items);

                    foreach (var (_, id) in items)
                    {
                        committedSstIds.Add(id);
                    }

                    return tables.ToList();
                }

                storageInner._state.LevelZeroTables = await LoadLevelAsync(manifest.L0);

                var levels = new List<List<SsTable>>();
                storageInner._state.LeveledSsTables = levels;

                foreach (var levelIds in manifest.Levels)
                {
                    levels.Add(await LoadLevelAsync(levelIds));
                }
            }
            else
            {
                // Load every "{id}.sst" (already ordered by id, which encodes L0 recency) in parallel.
                var items = sstFiles.Where(x => !IsDamagedSst(x.filename)).Select(x => (x.filename, id: x.id!.Value)).ToArray();
                var ssTables = await LoadManyAsync(items);

                storageInner._state.LevelZeroTables = ssTables.ToList();
                committedSstIds = new HashSet<long>(items.Select(x => x.id));
            }

            // Recover memtables that hadn't been flushed when the previous process exited. Memtables are
            // flushed oldest-id-first, so any WAL without a matching committed SST is newer than every loaded
            // SST; enqueuing them oldest-first (reads reverse the queue) preserves recency above L0.
            HashSet<long>? emptyWalIds = null;
            if (options.UseWriteAheadLog && walFiles.Count > 0)
            {
                var recovered = new List<IMemTable>();
                try
                {
                    foreach (var (filename, id) in walFiles)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        // A loaded committed SST supersedes its stale WAL, deleted only after recovery succeeds.
                        if (committedSstIds.Contains(id!.Value))
                        {
                            continue;
                        }

                        var memTable = new MemTable(id!.Value, arenaBlockSize: options.MemTableArenaBlockSize);
                        try
                        {
                            try
                            {
                                options.FaultInjector?.Invoke(StorageFaultPoint.RecoveryBeforeReplay);
                                WriteAheadLog.Replay(filename, memTable, options.FaultInjector);
                            }
                            catch (Exception exception) when (salvageReport != null && IsCorruption(exception)
                                && salvageReport.Issues.Any(issue => issue.Path == filename && issue.Kind == StorageIntegrityIssueKind.CorruptWal))
                            {
                                // Verification reports this corruption; retain only the valid prefix.
                            }

                            if (memTable.Count == 0)
                            {
                                // A header-only or torn first frame has no data to flush into an SST.
                                (emptyWalIds ??= []).Add(id.Value);
                                memTable.Dispose();
                            }
                            else
                            {
                                recovered.Add(memTable);
                            }
                        }
                        catch
                        {
                            memTable.Dispose();
                            throw;
                        }
                    }

                    storageInner._state.ImmutableMemTables = ImmutableArray.CreateRange(recovered);
                }
                catch
                {
                    foreach (var memTable in recovered)
                    {
                        memTable.Dispose();
                    }

                    throw;
                }
            }

            // Ensure every successfully opened store has a manifest, including manifest-less stores created by
            // older versions or by strategies that used to infer L0 from filenames.
            if (!isReadOnly)
            {
                cancellationToken.ThrowIfCancellationRequested();
                storageInner.BuildManifestSnapshot().Write(path);
                foreach (var tmp in Directory.EnumerateFiles(path, "*.sst.tmp"))
                {
                    TryDeleteFile(tmp);
                }

                var referencedSstIds = manifest == null ? committedSstIds : new HashSet<long>(manifest.AllSstIds());
                foreach (var (filename, id) in sstFiles)
                {
                    if (!referencedSstIds.Contains(id!.Value))
                    {
                        TryDeleteFile(filename);
                    }
                }

                foreach (var (filename, id) in walFiles)
                {
                    if (committedSstIds.Contains(id!.Value) || emptyWalIds?.Contains(id.Value) == true)
                    {
                        TryDeleteFile(filename);
                    }
                }
            }

            var compacter = new Compacter(storageInner, TimeProvider.System, options, enableBackgroundFlush: !isReadOnly);
            var storage = new LsmStorage(storageInner, compacter) { RecoveryReport = salvageReport, IsReadOnly = isReadOnly };
            StorageMeter.Register(storageInner.Metrics, path);

            if (!isReadOnly)
            {
                compacter.StartBackgroundFlush();
            }

            storageInner.Ownership = ownership;
            opened = true;
            return storage;

            bool IsDamagedSst(string filename) => salvageReport?.Issues.Any(
                issue => issue.Path == filename && issue.Kind == StorageIntegrityIssueKind.CorruptSst) == true;
        }
        finally
        {
            if (!opened)
            {
                var currentWalPath = Path.Combine(path, $"{storageInner._state.CurrentMemTable.Id}.wal");
                storageInner.Dispose();
                if (!isReadOnly && options.UseWriteAheadLog)
                {
                    TryDeleteFile(currentWalPath);
                }
            }
        }
    }

    private static long? TryParseId(string filename)
    {
        return long.TryParse(Path.GetFileNameWithoutExtension(filename), out var id) ? id : null;
    }

    private static void TryDeleteFile(string filename)
    {
        try
        {
            File.Delete(filename);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    internal readonly LsmStorageInner _inner;
    private readonly Compacter _compacter;
    private readonly object _closeLock = new();
    private readonly SemaphoreSlim _manualMaintenance = new(1, 1);
    private Task? _closeTask;
    private volatile bool _disposed;

    /// <summary>The health of this instance. A faulted instance must be disposed and reopened.</summary>
    public StorageHealth Health => _disposed ? StorageHealth.Disposed : _inner.Health;

    /// <summary>The first unsafe failure, or null if no failure has faulted the instance.</summary>
    public Exception? Fault => _inner.Fault;

    internal void CheckHealthy() => CheckDisposed();

    internal void BeforeMvccPublication()
    {
        try
        {
            _inner.FaultInjector?.Invoke(StorageFaultPoint.MvccBeforePublication);
        }
        catch (Exception exception)
        {
            var failure = new StorageOperationException(StorageCommitOutcome.Committed, exception);
            _inner.MarkFaulted(failure);
            throw failure;
        }
    }

    private LsmStorage(LsmStorageInner inner, Compacter compacter)
    {
        _inner = inner;
        _compacter = compacter;
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        CheckWritable();
        _inner.PutRaw(key, value);
    }

    public void Delete(ReadOnlySpan<byte> key)
    {
        CheckWritable();
        _inner.DeleteRaw(key);
    }

    /// <summary>
    /// Applies every mutation in <paramref name="batch"/> under one storage lock and one WAL frame.
    /// </summary>
    /// <remarks>
    /// With WAL enabled, the call returns after the frame has reached the operating system and, when
    /// <see cref="StorageOptions.SyncWriteAheadLogToDisk"/> is enabled, file-content synchronization
    /// has succeeded. With WAL disabled, success guarantees in-memory visibility only.
    /// A failure does not imply that nothing committed: inspect <see cref="StorageOperationException.Outcome"/>.
    /// Faulted instances reject further operations and must be disposed and reopened.
    /// </remarks>
    public void Write(LsmWriteBatch batch)
    {
        CheckWritable();
        ArgumentNullException.ThrowIfNull(batch);
        _inner.WriteBatch(batch);
    }

    internal void WriteGroup(LsmWriteBatch[] batches, StorageCommitOutcome[] outcomes, int count)
    {
        CheckWritable();
        _inner.WriteGroup(batches, outcomes, count);
    }

    /// <summary>Synchronizes the recoverable file contents through all previously completed writes.</summary>
    /// <remarks>
    /// Call after a successful Put, Delete, or Write to include that mutation. The boundary is acquired
    /// under the write lock; racing writes acquired before it are included, later writes are not promised.
    /// Current and recovered/rotated WALs, live SSTs, and the manifest are synchronized without flushing
    /// memtables or compacting. WAL-disabled and read-only stores reject this operation.
    /// Cancellation is honored before file I/O begins, not after; synchronous write-lock waiting
    /// cannot be interrupted. File synchronization may run synchronously on the calling thread. I/O failure throws
    /// <see cref="StorageSynchronizationException"/> and faults the instance without undoing prior writes.
    /// This requests OS file-content synchronization, not directory synchronization or power-loss
    /// certification. Do not reenter this operation from a borrowed-read callback.
    /// </remarks>
    public Task SynchronizeAsync(CancellationToken cancellationToken = default)
    {
        CheckWritable();
        return _inner.SynchronizeAsync(cancellationToken);
    }

    public ValueTask<bool> TryGetRawAsync(ReadOnlySpan<byte> key, IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        ArgumentNullException.ThrowIfNull(destination);

        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(_inner.TryGetRawAsync(ownedKey.Slice, destination, cancellationToken), ownedKey);

        static async ValueTask<bool> DisposeKeyAsync(ValueTask<bool> result, [ConsumesOwnership] OwnedByteSlice ownedKey)
        {
            try
            {
                return await result;
            }
            finally
            {
                ownedKey.Dispose();
            }
        }
    }

    public ValueTask<bool> TryGetRawAsync(byte[] key, IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(destination);
        return _inner.TryGetRawAsync(key.AsMemory(), destination, cancellationToken);
    }

    public ValueTask<int> GetRawAsync(ReadOnlySpan<byte> key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();

        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(_inner.GetRawAsync(ownedKey.Slice, destination, cancellationToken), ownedKey);

        static async ValueTask<int> DisposeKeyAsync(ValueTask<int> result, [ConsumesOwnership] OwnedByteSlice ownedKey)
        {
            try
            {
                return await result;
            }
            finally
            {
                ownedKey.Dispose();
            }
        }
    }

    public ValueTask<int> GetRawAsync(byte[] key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        ArgumentNullException.ThrowIfNull(key);
        return _inner.GetRawAsync(key.AsMemory(), destination, cancellationToken);
    }

    public ValueTask<bool> TryReadRawAsync<TArg>(ReadOnlySpan<byte> key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        ArgumentNullException.ThrowIfNull(reader);

        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(_inner.TryReadRawAsync(ownedKey.Slice, arg, reader, cancellationToken), ownedKey);

        static async ValueTask<bool> DisposeKeyAsync(ValueTask<bool> result, [ConsumesOwnership] OwnedByteSlice ownedKey)
        {
            try
            {
                return await result;
            }
            finally
            {
                ownedKey.Dispose();
            }
        }
    }

    public ValueTask<bool> TryReadRawAsync<TArg>(byte[] key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(reader);
        return _inner.TryReadRawAsync(key.AsMemory(), arg, reader, cancellationToken);
    }

    public ValueTask<long> ScanRawAsync<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        return _inner.ScanRawAsync(arg, reader, maxEntries, cancellationToken);
    }

    public ValueTask<long> SeekRawAsync<TArg>(ReadOnlySpan<byte> from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        ArgumentNullException.ThrowIfNull(reader);

        var ownedFrom = OwnedByteSlice.CopyFrom(from);
        return DisposeKeyAsync(_inner.SeekRawAsync(ownedFrom.Slice, arg, reader, maxEntries, cancellationToken), ownedFrom);

        static async ValueTask<long> DisposeKeyAsync(ValueTask<long> result, [ConsumesOwnership] OwnedByteSlice ownedFrom)
        {
            try
            {
                return await result;
            }
            finally
            {
                ownedFrom.Dispose();
            }
        }
    }

    public ValueTask<long> SeekRawAsync<TArg>(byte[] from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        ArgumentNullException.ThrowIfNull(from);
        ArgumentNullException.ThrowIfNull(reader);
        return _inner.SeekRawAsync(from.AsMemory(), arg, reader, maxEntries, cancellationToken);
    }

    /// <summary>Rejects new work, drains admitted operations, and closes the store.</summary>
    /// <remarks>
    /// Cancellation is checked only before shutdown starts. Concurrent disposers share completion.
    /// Finish or dispose active enumerators first; never wait for shutdown inside a storage callback.
    /// If shutdown fails, dispose again to release the faulted store without flushing recovery evidence.
    /// </remarks>
    public Task CloseAsync(CancellationToken cancellationToken = default)
    {
        lock (_closeLock)
        {
            if (_disposed)
            {
                return Task.CompletedTask;
            }
            if (_closeTask is { IsCompleted: false })
            {
                return _closeTask;
            }
            cancellationToken.ThrowIfCancellationRequested();
            return _closeTask = DisposeInternalAsync();
        }
    }

    public async Task FlushAndCompactAsync(CancellationToken cancellationToken = default)
    {
        using var operation = _inner.Lifetime.Enter();
        CheckWritable();
        await _manualMaintenance.WaitAsync(cancellationToken);
        try
        {
            await _compacter.StopBackgroundFlushAsync();
            try
            {
                await _inner.FlushAndCompactAsync(cancellationToken);
            }
            finally
            {
                _compacter.StartBackgroundFlush();
            }
        }
        finally
        {
            _manualMaintenance.Release();
        }
    }

    public ValueTask DisposeAsync() => new(CloseAsync());

    public void Dispose() => CloseAsync().GetAwaiter().GetResult();

    private async Task DisposeInternalAsync()
    {
        StorageMeter.Unregister(_inner.Metrics);
        await _inner.Lifetime.CloseAsync().ConfigureAwait(false);
        await _compacter.DisposeAsync().ConfigureAwait(false);
        try
        {
            if (!IsReadOnly && _inner.Health != StorageHealth.Faulted)
            {
                _inner.ForceFreezeMemTable();
                while (!_inner._state.ImmutableMemTables.IsEmpty)
                {
                    await _inner.ForceFlushNextImmutableMemTableAsync().ConfigureAwait(false);
                }
                _inner.DeleteCurrentMemTableWal();
            }
            _inner.Dispose();
            _disposed = true;
        }
        catch (Exception exception)
        {
            _inner.MarkFaulted(exception);
            throw;
        }
    }

    private void CheckDisposed()
    {
        _inner.Lifetime.CheckOpen();
        ObjectDisposedException.ThrowIf(_disposed, this);
        _inner.CheckHealthy();
    }
}
