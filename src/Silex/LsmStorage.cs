using Silex.Blocks;
using Silex.Compaction;
using Silex.Tables;
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

        var storageInner = new LsmStorageInner<TKey, TValue>(path, options);

        // SST files are named "{id}.sst" with monotonically increasing ids. Level-0 precedence
        // depends on creation order (the most recent table wins on duplicate keys), so load them
        // ordered by id rather than in arbitrary filesystem enumeration order.
        var sstFiles = Directory.EnumerateFiles(path, "*.sst")
            .Select(filename => (filename, id: TryParseSstId(filename)))
            .Where(x => x.id.HasValue)
            .OrderBy(x => x.id!.Value)
            .ToList();

        var ssTables = new List<SsTable<TKey, TValue>>();

        // TODO: [PERF] Can be parallelized
        foreach (var (filename, id) in sstFiles)
        {
            // Preserve the original id so recency ordering and block-cache keys stay stable, and make
            // sure newly generated ids never collide with one that already exists on disk.
            IdGenerator.EnsureGreaterThan(id!.Value);

            var blockBuilder = new BlockBuilder<TKey, TValue>(options.BlockEncoderFactory.Create<TKey, TValue>());
            var ssTable = await SsTable<TKey, TValue>.LoadSsTableAsync(filename, options.SsTableEncoderFactory.Create<TKey, TValue>(), blockBuilder, options.BloomFilterFactory, id, cancellationToken);
            ssTables.Add(ssTable);
        }

        // TODO: For now we only load l0 SSTs
        storageInner._state.LevelZeroTables = ssTables;

        var compacter = new Compacter<TKey, TValue>(storageInner, TimeProvider.System, options);

        compacter.StartBackgroundFlush();

        return new LsmStorage<TKey, TValue>(storageInner, compacter);
    }

    private static long? TryParseSstId(string filename)
    {
        return long.TryParse(Path.GetFileNameWithoutExtension(filename), out var id) ? id : null;
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
