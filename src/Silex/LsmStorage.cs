using Silex.Blocks;
using Silex.Compaction;
using Silex.MemTables;
using Silex.Tables;
using Silex.Wal;
using System.Collections.Immutable;
using System.Runtime.CompilerServices;

namespace Silex;

public static class LsmStorage
{
    /// <summary>
    /// Opens or create a store at the specified location.
    /// </summary>
    /// <typeparam name="TKey">The type of keys for the store.</typeparam>
    /// <typeparam name="TValue">The type of values of the store.</typeparam>
    /// <param name="path">The path of the store. If it doesn't exist it is created.</param>
    /// <param name="options">The storage options.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns></returns>
    public static async Task<LsmStorage<TKey, TValue>> OpenAsync<TKey, TValue>(string path, StorageOptions options, CancellationToken cancellationToken = default) where TKey : notnull
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

        var storageInner = new LsmStorageInner<TKey, TValue>(path, options);

        // The manifest (leveled compaction only) is the authoritative record of which SSTs are live and how
        // they map to levels. It only drives recovery when the store is being opened with leveled
        // compaction: the other strategies never maintain it, so honoring a stale leveled manifest under a
        // different strategy would treat every new (manifest-less) flush as an orphan and delete it. When
        // ignored, recovery falls back to the id-ordered L0 load (where id ordering still encodes recency).
        var manifest = options.CompactionStrategy == CompactionStrategy.Leveled ? Manifest.TryRead(path) : null;

        // Ids of the SSTs whose data is actually present in the recovered state. Used below to decide
        // whether a surviving WAL is stale (its data was already flushed and committed and is loaded) or
        // must be replayed. Only successfully loaded SSTs count: if a referenced SST is missing (fail open),
        // its WAL must still be replayed rather than discarded.
        HashSet<long> committedSstIds;

        var loadParallelism = Math.Max(1, options.MaxReadParallelism);

        // Loads the given SST files concurrently (bounded by MaxReadParallelism) into a position-indexed
        // array, preserving the requested order. On any failure every already-loaded table is disposed so a
        // failed open never leaks file handles.
        async Task<SsTable<TKey, TValue>[]> LoadManyAsync((string filename, long id)[] items)
        {
            var loaded = new SsTable<TKey, TValue>[items.Length];

            try
            {
                await Parallel.ForEachAsync(
                    Enumerable.Range(0, items.Length),
                    new ParallelOptions { MaxDegreeOfParallelism = loadParallelism, CancellationToken = cancellationToken },
                    async (index, ct) =>
                    {
                        var (filename, id) = items[index];
                        var blockBuilder = new BlockBuilder<TKey, TValue>(options.BlockEncoderFactory.Create<TKey, TValue>());
                        loaded[index] = await SsTable<TKey, TValue>.LoadSsTableAsync(filename, options.SsTableEncoderFactory.Create<TKey, TValue>(), blockBuilder, options.BloomFilterFactory, id, ct);
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
            async Task<List<SsTable<TKey, TValue>>> LoadLevelAsync(IEnumerable<long> ids)
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

            var levels = new List<List<SsTable<TKey, TValue>>>();

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
            var recovered = new List<IMemTable<TKey, TValue>>();

            foreach (var (filename, id) in walFiles)
            {
                // If a committed SST with this id exists the memtable was flushed (and, for leveled,
                // committed to the manifest) before the crash and this WAL is stale; remove it and skip
                // replay. This makes the flush/delete sequence idempotent.
                if (committedSstIds.Contains(id!.Value))
                {
                    TryDeleteFile(filename);
                    continue;
                }

                var memTable = new MemTable<TKey, TValue>(id!.Value);
                WriteAheadLog<TKey, TValue>.Replay(filename, memTable);
                recovered.Add(memTable);
            }

            if (recovered.Count > 0)
            {
                storageInner._state.ImmutableMemTables = ImmutableQueue.CreateRange(recovered);
            }
        }

        var compacter = new Compacter<TKey, TValue>(storageInner, TimeProvider.System, options);

        compacter.StartBackgroundFlush();

        return new LsmStorage<TKey, TValue>(storageInner, compacter);
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

public class LsmStorage<TKey, TValue> : IDisposable, IAsyncDisposable where TKey : notnull
{
    internal readonly LsmStorageInner<TKey, TValue> _inner;
    internal readonly Compacter<TKey, TValue> _compacter;

    private bool _disposed;

    internal LsmStorage(LsmStorageInner<TKey, TValue> inner, Compacter<TKey, TValue> compacter)
    {
        _inner = inner;
        _compacter= compacter;
    }

    /// <inheritdoc cref="LsmStorageInner.TryGet(TKey, out TValue)"/>
    /// <remarks>
    /// Zero-copy: the returned key/value is a read-only borrow of engine-owned memory. Do not mutate
    /// or dispose it. If you need an independently owned, mutable copy, copy it yourself (for example
    /// wrap it in a <see cref="Bytes"/>).
    /// </remarks>
    public ValueTask<TValue> GetAsync(TKey key)
    {
        CheckDisposed();

        return _inner.GetAsync(key);
    }

    /// <inheritdoc cref="LsmStorageInner.Put(TKey, TValue)"/>
    /// <remarks>
    /// Zero-copy: ownership of <paramref name="key"/> and <paramref name="value"/> transfers to the
    /// engine. Do not mutate or release them (for example return a pooled buffer) after this call; the
    /// engine keeps and reads them until the owning memtable is flushed and disposed.
    /// </remarks>
    public void Put(TKey key, TValue value)
    {
        CheckDisposed();

        _inner.Put(key, value);
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
    /// Zero-copy: each yielded key/value is a read-only borrow of engine-owned memory. Do not mutate or
    /// dispose it. Copy it yourself if you need independently owned memory.
    /// </remarks>
    public IStorageIterator<TKey, TValue> CreateIterator()
    {
        CheckDisposed();

        return _inner.CreateIterator();
    }

    /// <summary>
    /// Flushes any pending data to disk and stops the compacter background threads.
    /// </summary>
    /// <remarks>
    /// Equivalent to <see cref="DisposeAsync()"/>. Call one or the other.
    /// </remarks>
    public async Task CloseAsync()
    {
        await DisposeAsync();
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

    public async Task DisposeInternalAsync()
    {
        await _compacter.CloseAsync();

        _inner.ForceFreezeMemTable();

        while (!_inner._state.ImmutableMemTables.IsEmpty)
        {
            await _inner.ForceFlushNextImmutableMemTableAsync();
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
