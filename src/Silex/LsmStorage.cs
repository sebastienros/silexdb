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

        var sstFilenames = Directory.EnumerateFiles(path, "*.sst");

        var ssTables = new List<SsTable<TKey, TValue>>();

        // TODO: [PERF] Can be parallelized
        foreach (var sstFilename in sstFilenames)
        {
            var blockBuilder = new BlockBuilder<TKey, TValue>(options.BlockEncoderFactory.Create<TKey, TValue>());
            var ssTable = await SsTable<TKey, TValue>.LoadSsTableAsync(sstFilename, options.SsTableEncoderFactory.Create<TKey, TValue>(), blockBuilder, options.BloomFilterFactory, cancellationToken);
            ssTables.Add(ssTable);
        }

        // TODO: For now we only load l0 SSTs
        storageInner._state.LevelZeroTables = ssTables;

        var compacter = new Compacter<TKey, TValue>(storageInner, TimeProvider.System, options);

        compacter.StartBackgroundFlush();

        return new LsmStorage<TKey, TValue>(storageInner, compacter);
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
    public ValueTask<TValue> GetAsync(TKey key)
    {
        CheckDisposed();

        return _inner.GetAsync(key);
    }

    /// <inheritdoc cref="LsmStorageInner.Put(TKey, TValue)"/>
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
