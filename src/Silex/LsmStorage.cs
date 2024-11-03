using Silex.Blocks;
using Silex.Tables;

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

        var instance = new LsmStorageInner<TKey, TValue>(path, options);

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
        instance._state.SsTables = [ssTables];

        var compacter = new Compacter<TKey, TValue>(instance, TimeProvider.System, options);

        compacter.StartBackgroundFlush();

        return new LsmStorage<TKey, TValue>(instance, compacter);
    }
}

public class LsmStorage<TKey, TValue> : IAsyncDisposable where TKey : notnull
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
        return _inner.GetAsync(key);
    }

    /// <inheritdoc cref="LsmStorageInner.Put(TKey, TValue)"/>
    public void Put(TKey key, TValue value)
    {
        _inner.Put(key, value);
    }

    /// <inheritdoc cref="LsmStorageInner.Delete(TKey)"/>
    public void Delete(TKey key)
    {
        _inner.Delete(key);
    }

    /// <summary>
    /// Flushes any pending data to disk and stops the compacter background threads.
    /// </summary>
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

    private void Dispose()
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

    ~LsmStorage()
    {
        Dispose();
    }
}
