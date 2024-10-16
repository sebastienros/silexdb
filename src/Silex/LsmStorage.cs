namespace Silex;
using Silex.Blocks;
using Silex.Tables;

public class LsmStorage
{
    internal readonly LsmStorageInner _inner;
    internal readonly Compacter _compacter;

    private LsmStorage(LsmStorageInner inner, Compacter compacter)
    {
        _inner = inner;
        _compacter= compacter;
    }

    public static async Task<LsmStorage> OpenAsync(string path, StorageOptions options, CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(path))
        {
            Directory.CreateDirectory(path);
        }

        var instance = new LsmStorageInner(path, options);

        var sstFilenames = Directory.EnumerateFiles(path, "*.sst");

        var ssTables = new List<SsTable>();

        // TODO: [PERF] Can be parallelized
        foreach (var sstFilename in sstFilenames)
        {
            var blockBuilder = new BlockBuilder(options.BlockEncoder);
            var ssTable = await SsTable.LoadSsTableAsync(sstFilename, options.SsTableEncoder, blockBuilder, cancellationToken);
            ssTables.Add(ssTable);
        }

        // TODO: For now we only load l0 SSTs
        instance._state.SsTables = [ssTables];

        var compacter = new Compacter(instance, TimeProvider.System, options);

        compacter.StartBackgroundFlush();

        return new LsmStorage(instance, compacter);
    }

    /// <inheritdoc cref="LsmStorageInner.TryGet(Bytes, out Bytes)"/>
    public bool TryGet(Bytes key, out Bytes value)
    {
        return _inner.TryGet(key, out value);
    }

    /// <inheritdoc cref="LsmStorageInner.Put(Bytes, Bytes)"/>
    public void Put(Bytes key, Bytes value)
    {
        _inner.Put(key, value);
    }

    /// <summary>
    /// Flushes any pending data to disk and stops the compacter background threads.
    /// </summary>
    public async Task CloseAsync()
    {
        await _compacter.CloseAsync();
        
        _inner.ForceFreezeMemTable();

        while (_inner._state.ImmutableMemTables.Count() > 0)
        {
            await _inner.ForceFlushNextImmutableMemTableAsync();
        }
    }
}
