using Silex.Blocks;
using Silex.Compaction;
using Silex.Buffers;
using Silex.MemTables;
using Silex.Serialization;
using Silex.Tables;
using Silex.Wal;
using System.Buffers;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace Silex;

public sealed class LsmStorage : IDisposable, IAsyncDisposable
{
    /// <summary>
    /// Opens or create a store at the specified location.
    /// </summary>
    /// <param name="path">The path of the store. If it doesn't exist it is created.</param>
    /// <param name="options">The storage options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
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
        private bool _disposed;

        internal LsmStorageTyped(LsmStorageInner inner, Compacter compacter)
        {
            _inner = inner;
            _compacter = compacter;
        }

        public async ValueTask<TValue> GetAsync(TKey key, CancellationToken cancellationToken = default)
        {
            CheckDisposed();
            using var encodedKey = Encode(_keyEncoder, key);
            using var value = await _inner.GetAsync(encodedKey.Slice, cancellationToken);
            return value is null || value.IsEmpty ? default! : _valueEncoder.Decode(value.Span);
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

        public async Task CloseAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed)
            {
                return;
            }

            cancellationToken.ThrowIfCancellationRequested();
            GC.SuppressFinalize(this);
            await DisposeInternalAsync(cancellationToken);
            _disposed = true;
        }

        public async Task FlushAndCompactAsync(CancellationToken cancellationToken = default)
        {
            CheckDisposed();
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

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                return;
            }

            GC.SuppressFinalize(this);
            await DisposeInternalAsync();
            _disposed = true;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            GC.SuppressFinalize(this);
            DisposeInternalAsync().GetAwaiter().GetResult();
            _disposed = true;
        }

        public async Task DisposeInternalAsync(CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await _compacter.CloseAsync(cancellationToken);
            _inner.ForceFreezeMemTable();

            while (!_inner._state.ImmutableMemTables.IsEmpty)
            {
                await _inner.ForceFlushNextImmutableMemTableAsync(cancellationToken);
            }

            _inner.DeleteCurrentMemTableWal();
            _inner.Dispose();
        }

        private static OwnedByteSlice Encode<T>(IBinaryEncoder<T> encoder, T value)
        {
            using var bufferWriter = new PooledArrayBufferWriter<byte>(Math.Max(1, encoder.GetLength(value)));
            var writer = new EncoderBinaryWriter(bufferWriter);
            encoder.Encode(value, ref writer);
            writer.Flush();
            return OwnedByteSlice.CopyFrom(bufferWriter.WrittenMemory.Span);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CheckDisposed()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
        }
    }

    internal static async Task<LsmStorageTyped<TKey, TValue>> OpenAsync<TKey, TValue>(string path, StorageOptions options, CancellationToken cancellationToken = default) where TKey : notnull
    {
        var storage = await OpenCoreAsync(path, options, cancellationToken);
        return new LsmStorageTyped<TKey, TValue>(storage._inner, storage._compacter);
    }

    private static async Task<LsmStorage> OpenCoreAsync(string path, StorageOptions options, CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }

        // Remove any leftover temporary SST files from a build that crashed before its atomic rename.
        // These are never valid tables and must not be loaded.
        foreach (var tmp in Directory.EnumerateFiles(path, "*.sst.tmp"))
        {
            TryDeleteFile(tmp);
        }

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

        var storageInner = new LsmStorageInner(path, options);

        // The manifest is the authoritative record of which SSTs are live and how they map to levels. It is
        // read for every strategy so changing CompactionStrategy on reopen only changes future compactions,
        // not how existing files are interpreted. Older stores without a manifest fall back to id-ordered L0
        // recovery once and are migrated by the manifest write near the end of OpenAsync.
        var manifest = Manifest.TryRead(path);

        // Ids of the SSTs whose data is actually present in the recovered state. Used below to decide
        // whether a surviving WAL is stale (its data was already flushed and committed and is loaded) or
        // must be replayed. Only successfully loaded SSTs count: if a referenced SST is missing (fail open),
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
                        loaded[index] = await SsTable.LoadSsTableAsync(filename, options.SsTableEncoderFactory.Create(), blockBuilder, options.BloomFilterFactory, id, ct);
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

            // Resolves a level's manifest ids to on-disk files, preserving order. Fail open: an id whose
            // file is missing is skipped (the store loses that SST's data but stays usable). The surviving
            // ids are loaded in parallel and recorded as committed.
            async Task<List<SsTable>> LoadLevelAsync(IEnumerable<long> ids)
            {
                var items = ids
                    .Where(id => fileById.ContainsKey(id))
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

            foreach (var levelIds in manifest.Levels)
            {
                levels.Add(await LoadLevelAsync(levelIds));
            }

            storageInner._state.LeveledSsTables = levels;

            // Any SST on disk that the committed manifest does not reference is an orphan (a flush or
            // compaction output that crashed before the manifest commit) and is deleted. This uses the
            // full referenced set, not just the loaded ids, so a referenced-but-missing SST does not cause
            // an unrelated file to be treated as an orphan.
            var referencedSstIds = new HashSet<long>(manifest.AllSstIds());

            foreach (var (filename, id) in sstFiles)
            {
                if (!referencedSstIds.Contains(id!.Value))
                {
                    TryDeleteFile(filename);
                }
            }
        }
        else
        {
            // Load every "{id}.sst" (already ordered by id, which encodes L0 recency) in parallel.
            var items = sstFiles.Select(x => (x.filename, id: x.id!.Value)).ToArray();
            var ssTables = await LoadManyAsync(items);

            storageInner._state.LevelZeroTables = ssTables.ToList();
            committedSstIds = new HashSet<long>(items.Select(x => x.id));
        }

        // Recover memtables that hadn't been flushed when the previous process exited. Memtables are
        // flushed oldest-id-first, so any WAL without a matching committed SST is newer than every loaded
        // SST; enqueuing them oldest-first (reads reverse the queue) preserves recency above L0.
        if (options.UseWriteAheadLog && walFiles.Count > 0)
        {
            var recovered = new List<IMemTable>();

            foreach (var (filename, id) in walFiles)
            {
                // If a committed SST with this id exists the memtable was flushed and committed to the
                // manifest before the crash and this WAL is stale; remove it and skip replay. This makes
                // the flush/delete sequence idempotent.
                if (committedSstIds.Contains(id!.Value))
                {
                    TryDeleteFile(filename);
                    continue;
                }

                var memTable = new MemTable(id!.Value, arenaBlockSize: options.MemTableArenaBlockSize);
                WriteAheadLog.Replay(filename, memTable);
                recovered.Add(memTable);
            }

            if (recovered.Count > 0)
            {
                storageInner._state.ImmutableMemTables = ImmutableQueue.CreateRange(recovered);
            }
        }

        // Ensure every successfully opened store has a manifest, including manifest-less stores created by
        // older versions or by strategies that used to infer L0 from filenames.
        storageInner.BuildManifestSnapshot().Write(path);

        var compacter = new Compacter(storageInner, TimeProvider.System, options);

        compacter.StartBackgroundFlush();

        return new LsmStorage(storageInner, compacter);
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

    private readonly LsmStorageInner _inner;
    private readonly Compacter _compacter;
    private bool _disposed;

    private LsmStorage(LsmStorageInner inner, Compacter compacter)
    {
        _inner = inner;
        _compacter = compacter;
    }

    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        CheckDisposed();
        _inner.PutRaw(key, value);
    }

    public void Delete(ReadOnlySpan<byte> key)
    {
        CheckDisposed();
        _inner.DeleteRaw(key);
    }

    public ValueTask<bool> TryGetRawAsync(ReadOnlySpan<byte> key, IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        ArgumentNullException.ThrowIfNull(destination);

        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(_inner.TryGetRawAsync(ownedKey.Slice, destination, cancellationToken), ownedKey);

        static async ValueTask<bool> DisposeKeyAsync(ValueTask<bool> result, OwnedByteSlice ownedKey)
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

    public ValueTask<int> GetRawAsync(ReadOnlySpan<byte> key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();

        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(_inner.GetRawAsync(ownedKey.Slice, destination, cancellationToken), ownedKey);

        static async ValueTask<int> DisposeKeyAsync(ValueTask<int> result, OwnedByteSlice ownedKey)
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

    public ValueTask<bool> TryReadRawAsync<TArg>(ReadOnlySpan<byte> key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        ArgumentNullException.ThrowIfNull(reader);

        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(_inner.TryReadRawAsync(ownedKey.Slice, arg, reader, cancellationToken), ownedKey);

        static async ValueTask<bool> DisposeKeyAsync(ValueTask<bool> result, OwnedByteSlice ownedKey)
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

        static async ValueTask<long> DisposeKeyAsync(ValueTask<long> result, OwnedByteSlice ownedFrom)
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

    public async Task CloseAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed)
        {
            return;
        }

        cancellationToken.ThrowIfCancellationRequested();

        GC.SuppressFinalize(this);
        await DisposeInternalAsync(cancellationToken);

        _disposed = true;
    }

    public async Task FlushAndCompactAsync(CancellationToken cancellationToken = default)
    {
        CheckDisposed();

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

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);
        await DisposeInternalAsync();

        _disposed = true;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);
        DisposeInternalAsync().GetAwaiter().GetResult();

        _disposed = true;
    }

    private async Task DisposeInternalAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        await _compacter.CloseAsync(cancellationToken);

        _inner.ForceFreezeMemTable();

        while (!_inner._state.ImmutableMemTables.IsEmpty)
        {
            await _inner.ForceFlushNextImmutableMemTableAsync(cancellationToken);
        }

        _inner.DeleteCurrentMemTableWal();
        _inner.Dispose();
    }

    private void CheckDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
