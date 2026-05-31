using Silex.Blocks;
using Silex.Compaction;
using Silex.MemTables;
using Silex.Tables;
using Silex.Wal;
using System.Buffers;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace Silex;

public static class LsmStorage
{
    /// <summary>
    /// Opens or create a store at the specified location.
    /// </summary>
    /// <typeparam name="TKey">The type of keys for the store.</typeparam>
    /// <param name="path">The path of the store. If it doesn't exist it is created.</param>
    /// <param name="options">The storage options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    public static async Task<LsmStorage<TKey>> OpenAsync<TKey>(string path, StorageOptions options, CancellationToken cancellationToken = default) where TKey : notnull
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

        var storageInner = new LsmStorageInner<TKey>(path, options);

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
        async Task<SsTable<TKey>[]> LoadManyAsync((string filename, long id)[] items)
        {
            var loaded = new SsTable<TKey>[items.Length];

            try
            {
                await Parallel.ForEachAsync(
                    Enumerable.Range(0, items.Length),
                    new ParallelOptions { MaxDegreeOfParallelism = loadParallelism, CancellationToken = cancellationToken },
                    async (index, ct) =>
                    {
                        var (filename, id) = items[index];
                        var blockBuilder = new BlockBuilder<TKey>(options.BlockEncoderFactory.Create<TKey>());
                        loaded[index] = await SsTable<TKey>.LoadSsTableAsync(filename, options.SsTableEncoderFactory.Create<TKey>(), blockBuilder, options.BloomFilterFactory, id, ct);
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
            async Task<List<SsTable<TKey>>> LoadLevelAsync(IEnumerable<long> ids)
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

            var levels = new List<List<SsTable<TKey>>>();

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
            var recovered = new List<IMemTable<TKey>>();

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

                var memTable = new MemTable<TKey>(id!.Value);
                WriteAheadLog<TKey>.Replay(filename, memTable);
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

        var compacter = new Compacter<TKey>(storageInner, TimeProvider.System, options);

        compacter.StartBackgroundFlush();

        return new LsmStorage<TKey>(storageInner, compacter);
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
}

public class LsmStorage<TKey> : IDisposable, IAsyncDisposable where TKey : notnull
{
    internal readonly LsmStorageInner<TKey> _inner;
    internal readonly Compacter<TKey> _compacter;

    private bool _disposed;

    internal LsmStorage(LsmStorageInner<TKey> inner, Compacter<TKey> compacter)
    {
        _inner = inner;
        _compacter= compacter;
    }

    /// <summary>
    /// Gets the value associated with the specified key, or <see langword="null"/> when the key is absent
    /// or has been deleted.
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <remarks>
    /// The returned array is freshly allocated for on-disk reads, but for values still resident in a
    /// memtable it is the engine-owned array stored at <see cref="Put(TKey, byte[])"/> time. Treat it as
    /// read-only: do not mutate it. The raw read APIs (<see cref="TryGetRawAsync"/>,
    /// <see cref="GetRawAsync"/>, <see cref="TryReadRawAsync"/>) avoid this allocation.
    /// </remarks>
    public async ValueTask<byte[]?> GetAsync(TKey key, CancellationToken cancellationToken = default)
    {
        CheckDisposed();

        var value = await _inner.GetAsync(key, cancellationToken);

        return value.ToNullableArray();
    }

    /// <inheritdoc cref="LsmStorageInner{TKey}.TryGetRawAsync(TKey, IBufferWriter{byte}, CancellationToken)"/>
    public ValueTask<bool> TryGetRawAsync(TKey key, IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();

        return _inner.TryGetRawAsync(key, destination, cancellationToken);
    }

    /// <inheritdoc cref="LsmStorageInner{TKey}.GetRawAsync(TKey, Memory{byte}, CancellationToken)"/>
    public ValueTask<int> GetRawAsync(TKey key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();

        return _inner.GetRawAsync(key, destination, cancellationToken);
    }

    /// <inheritdoc cref="LsmStorageInner{TKey}.TryReadRawAsync{TArg}(TKey, TArg, ReadValueAction{TArg}, CancellationToken)"/>
    public ValueTask<bool> TryReadRawAsync<TArg>(TKey key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        CheckDisposed();

        return _inner.TryReadRawAsync(key, arg, reader, cancellationToken);
    }

    /// <inheritdoc cref="LsmStorageInner{TKey}.ScanRawAsync{TArg}(TArg, ReadRawEntryAction{TArg}, long, CancellationToken)"/>
    public ValueTask<long> ScanRawAsync<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
    {
        CheckDisposed();

        return _inner.ScanRawAsync(arg, reader, maxEntries, cancellationToken);
    }

    /// <inheritdoc cref="LsmStorageInner{TKey}.SeekRawAsync{TArg}(TKey, TArg, ReadRawEntryAction{TArg}, long, CancellationToken)"/>
    public ValueTask<long> SeekRawAsync<TArg>(TKey from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
    {
        CheckDisposed();

        return _inner.SeekRawAsync(from, arg, reader, maxEntries, cancellationToken);
    }

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/>, taking ownership of the array.
    /// </summary>
    /// <param name="key">The key to store the value under.</param>
    /// <param name="value">The value bytes. The engine keeps and reads this array until the owning
    /// memtable is flushed, so do not mutate or release it afterwards. An empty array deletes the key
    /// (it is stored as a tombstone), exactly like <see cref="Delete(TKey)"/>.</param>
    public void Put(TKey key, byte[] value)
    {
        CheckDisposed();

        _inner.Put(key, new ValueBuffer(value));
    }

    /// <summary>
    /// Stores a copy of <paramref name="value"/> under <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The key to store the value under.</param>
    /// <param name="value">The value bytes, copied into an engine-owned array since a span does not own
    /// its memory. An empty span deletes the key (it is stored as a tombstone), like
    /// <see cref="Delete(TKey)"/>.</param>
    public void Put(TKey key, ReadOnlySpan<byte> value)
    {
        CheckDisposed();

        _inner.Put(key, ValueBuffer.FromSpan(value));
    }

    /// <summary>
    /// Stores a copy of <paramref name="value"/> under <paramref name="key"/>.
    /// </summary>
    /// <param name="key">The key to store the value under.</param>
    /// <param name="value">The value bytes, copied so the caller keeps owning its <see cref="Bytes"/>. An
    /// empty value deletes the key (it is stored as a tombstone), like <see cref="Delete(TKey)"/>.</param>
    public void Put(TKey key, Bytes value)
    {
        CheckDisposed();

        _inner.Put(key, ValueBuffer.FromSpan(value.Span));
    }

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/> as four little-endian bytes. This is an
    /// allocation-light convenience over <see cref="Put(TKey, ReadOnlySpan{byte})"/> for fixed-width numeric
    /// values; only the engine-owned backing array is allocated.
    /// </summary>
    public void Put(TKey key, int value)
    {
        CheckDisposed();

        _inner.Put(key, ValueBuffer.FromInt32(value));
    }

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/> as four little-endian bytes. This is an
    /// allocation-light convenience over <see cref="Put(TKey, ReadOnlySpan{byte})"/> for fixed-width numeric
    /// values; only the engine-owned backing array is allocated.
    /// </summary>
    public void Put(TKey key, uint value)
    {
        CheckDisposed();

        _inner.Put(key, ValueBuffer.FromUInt32(value));
    }

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/> as eight little-endian bytes. This is an
    /// allocation-light convenience over <see cref="Put(TKey, ReadOnlySpan{byte})"/> for fixed-width numeric
    /// values; only the engine-owned backing array is allocated.
    /// </summary>
    public void Put(TKey key, long value)
    {
        CheckDisposed();

        _inner.Put(key, ValueBuffer.FromInt64(value));
    }

    /// <summary>
    /// Stores <paramref name="value"/> under <paramref name="key"/> as eight little-endian bytes. This is an
    /// allocation-light convenience over <see cref="Put(TKey, ReadOnlySpan{byte})"/> for fixed-width numeric
    /// values; only the engine-owned backing array is allocated.
    /// </summary>
    public void Put(TKey key, ulong value)
    {
        CheckDisposed();

        _inner.Put(key, ValueBuffer.FromUInt64(value));
    }

    /// <inheritdoc cref="LsmStorageInner.Delete(TKey)"/>
    public void Delete(TKey key)
    {
        CheckDisposed();

        _inner.Delete(key);
    }

    /// <summary>
    /// Creates a forward iterator over the entire key space, or from a given key when one is supplied via
    /// <see cref="IStorageIterator{TKey, TValue}.EnumerateAsync(TKey, CancellationToken)"/>. Entries are
    /// yielded in ascending key order across every memtable and on-disk level.
    /// </summary>
    /// <remarks>
    /// Each yielded value is a freshly allocated array. Deleted keys are skipped.
    /// </remarks>
    public IStorageIterator<TKey, byte[]> CreateIterator()
    {
        CheckDisposed();

        return new ByteArrayIteratorAdapter(_inner.CreateIterator());
    }

    /// <summary>
    /// Adapts the engine's <see cref="ValueBuffer"/> iterator to the public <see cref="byte"/>[] surface by
    /// materializing each live value into its own array.
    /// </summary>
    private sealed class ByteArrayIteratorAdapter : IStorageIterator<TKey, byte[]>
    {
        private readonly IStorageIterator<TKey, ValueBuffer> _source;

        public ByteArrayIteratorAdapter(IStorageIterator<TKey, ValueBuffer> source)
        {
            _source = source;
        }

        public IAsyncEnumerable<KeyValuePair<TKey, byte[]>> EnumerateAsync(CancellationToken cancellationToken = default)
        {
            return Adapt(_source.EnumerateAsync(cancellationToken), cancellationToken);
        }

        public IAsyncEnumerable<KeyValuePair<TKey, byte[]>> EnumerateAsync(TKey from, CancellationToken cancellationToken = default)
        {
            return Adapt(_source.EnumerateAsync(from, cancellationToken), cancellationToken);
        }

        private static async IAsyncEnumerable<KeyValuePair<TKey, byte[]>> Adapt(
            IAsyncEnumerable<KeyValuePair<TKey, ValueBuffer>> source,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var entry in source.WithCancellation(cancellationToken))
            {
                yield return new KeyValuePair<TKey, byte[]>(entry.Key, entry.Value.Span.ToArray());
            }
        }
    }

    /// <summary>
    /// Flushes any pending data to disk and stops the compacter background threads.
    /// </summary>
    /// <remarks>
    /// Equivalent to <see cref="DisposeAsync()"/>. Call one or the other.
    /// </remarks>
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

    /// <summary>
    /// Flushes pending writes and runs compaction until the configured compaction strategy reaches a stable
    /// state. The background compacter is paused while this explicit maintenance pass runs.
    /// </summary>
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

        // The current memtable is now empty; drop its write-ahead log so a clean shutdown leaves no
        // files to replay on the next open.
        _inner.DeleteCurrentMemTableWal();

        _inner.Dispose();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void CheckDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    // No finalizer on purpose: persisting data requires blocking disk I/O, which must never run
    // during finalization. Durability is provided solely by deterministic disposal
    // (CloseAsync/DisposeAsync/Dispose). Any file handles held by the inner storage are released by
    // its own finalizer, so an undisposed instance leaks no native resources (it just isn't flushed).
}
