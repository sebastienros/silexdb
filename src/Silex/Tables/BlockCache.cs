using Silex.Blocks;

namespace Silex.Tables;

internal sealed class BlockCache<TKey> : IDisposable
{
    private readonly object _gate = new();
    private readonly long _sizeLimit;
    private readonly Dictionary<BlockCacheKey, Entry> _entries = [];
    private readonly Dictionary<BlockCacheKey, PendingLoad> _loads = [];
    private readonly LinkedList<Entry> _lru = [];
    private long _size;
    private bool _disposed;

    public BlockCache(long sizeLimit)
    {
        if (sizeLimit < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(sizeLimit), sizeLimit, "Cache size limit must be non-negative.");
        }

        _sizeLimit = sizeLimit;
    }

    public ValueTask<BlockLease<TKey>> GetOrLoadAsync<TLoader>(BlockCacheKey key, TLoader loader, CancellationToken cancellationToken = default)
        where TLoader : struct, IBlockLoader<TKey>
    {
        cancellationToken.ThrowIfCancellationRequested();

        lock (_gate)
        {
            ThrowIfDisposed();

            if (TryAcquireCore(key, out var lease))
            {
                return new ValueTask<BlockLease<TKey>>(lease);
            }
        }

        return new ValueTask<BlockLease<TKey>>(GetOrLoadSlowAsync(key, loader, cancellationToken));
    }

    private async Task<BlockLease<TKey>> GetOrLoadSlowAsync<TLoader>(BlockCacheKey key, TLoader loader, CancellationToken cancellationToken)
        where TLoader : struct, IBlockLoader<TKey>
    {
        cancellationToken.ThrowIfCancellationRequested();
        PendingLoad pendingLoad;

        lock (_gate)
        {
            ThrowIfDisposed();

            if (TryAcquireCore(key, out var lease))
            {
                return lease;
            }

            if (!_loads.TryGetValue(key, out pendingLoad!))
            {
                pendingLoad = new PendingLoad { Waiters = 1 };
                _loads.Add(key, pendingLoad);
                pendingLoad.Task = LoadAndCacheAsync(key, loader, pendingLoad, cancellationToken);
            }
            else
            {
                pendingLoad.Waiters++;
            }
        }

        var entry = await pendingLoad.Task.ConfigureAwait(false);

        return entry == null ? default : new BlockLease<TKey>(this, entry);
    }

    private async Task<Entry?> LoadAndCacheAsync<TLoader>(BlockCacheKey key, TLoader loader, PendingLoad pendingLoad, CancellationToken cancellationToken)
        where TLoader : struct, IBlockLoader<TKey>
    {
        Block<TKey>? block = null;
        Entry? entry = null;

        try
        {
            block = await loader.LoadAsync(cancellationToken).ConfigureAwait(false);
            if (block == null)
            {
                return null;
            }

            lock (_gate)
            {
                if (_disposed)
                {
                    block.Dispose();
                    return null;
                }

                if (_sizeLimit == 0)
                {
                    entry = new Entry(key, block, block.Memory.Length)
                    {
                        RefCount = pendingLoad.Waiters,
                        Evicted = true
                    };
                    block = null;
                    return entry;
                }

                if (_entries.TryGetValue(key, out entry))
                {
                    entry.RefCount += pendingLoad.Waiters;
                    MoveToFront(entry);
                    block.Dispose();
                    block = null;
                    return entry;
                }

                entry = new Entry(key, block, block.Memory.Length)
                {
                    RefCount = pendingLoad.Waiters
                };
                block = null;

                _entries.Add(key, entry);
                entry.Node = _lru.AddFirst(entry);
                _size += entry.Size;

                EvictOverLimit(entry);

                return entry;
            }
        }
        finally
        {
            lock (_gate)
            {
                _loads.Remove(key);
            }

            block?.Dispose();
        }
    }

    private bool TryAcquireCore(BlockCacheKey key, out BlockLease<TKey> lease)
    {
        if (_entries.TryGetValue(key, out var entry))
        {
            entry.RefCount++;
            MoveToFront(entry);
            lease = new BlockLease<TKey>(this, entry);
            return true;
        }

        lease = default;
        return false;
    }

    private void MoveToFront(Entry entry)
    {
        if (entry.Node == null || entry.Node == _lru.First)
        {
            return;
        }

        _lru.Remove(entry.Node);
        _lru.AddFirst(entry.Node);
    }

    private void EvictOverLimit(Entry protectedEntry)
    {
        var node = _lru.Last;

        while (_size > _sizeLimit && node != null)
        {
            var previous = node.Previous;
            var entry = node.Value;

            if (!ReferenceEquals(entry, protectedEntry))
            {
                _lru.Remove(node);
                entry.Node = null;
                entry.Evicted = true;
                _entries.Remove(entry.Key);
                _size -= entry.Size;

                if (entry.RefCount == 0)
                {
                    entry.Block.Dispose();
                }
            }

            node = previous;
        }
    }

    private void Release(Entry entry)
    {
        lock (_gate)
        {
            if (entry.RefCount == 0)
            {
                return;
            }

            entry.RefCount--;

            if (entry.RefCount == 0 && entry.Evicted)
            {
                entry.Block.Dispose();
            }
        }
    }

    public void Dispose()
    {
        GC.SuppressFinalize(this);
        DisposeInternal();
    }

    private void DisposeInternal()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;

            foreach (var entry in _entries.Values)
            {
                entry.Evicted = true;
                if (entry.RefCount == 0)
                {
                    entry.Block.Dispose();
                }
            }

            _entries.Clear();
            _loads.Clear();
            _lru.Clear();
            _size = 0;
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(BlockCache<TKey>));
        }
    }

    ~BlockCache()
    {
        DisposeInternal();
    }

    private sealed class PendingLoad
    {
        public Task<Entry?> Task { get; set; } = null!;
        public int Waiters { get; set; }
    }

    internal sealed class Entry
    {
        public Entry(BlockCacheKey key, Block<TKey> block, int size)
        {
            Key = key;
            Block = block;
            Size = size;
        }

        public BlockCacheKey Key { get; }
        public Block<TKey> Block { get; }
        public int Size { get; }
        public LinkedListNode<Entry>? Node { get; set; }
        public int RefCount { get; set; }
        public bool Evicted { get; set; }
    }

    internal void ReleaseForLease(Entry entry) => Release(entry);
}

internal readonly record struct BlockCacheKey(long TableId, int BlockIndex);

/// <summary>
/// Loads a block on a cache miss. Implemented by a struct so <see cref="BlockCache{TKey}.GetOrLoadAsync"/>
/// can populate a miss without allocating a closure per read.
/// </summary>
internal interface IBlockLoader<TKey>
{
    Task<Block<TKey>?> LoadAsync(CancellationToken cancellationToken = default);
}

internal readonly struct BlockLease<TKey> : IDisposable
{
    private readonly BlockCache<TKey>? _owner;
    private readonly BlockCache<TKey>.Entry? _entry;

    internal BlockLease(BlockCache<TKey> owner, BlockCache<TKey>.Entry entry)
    {
        _owner = owner;
        _entry = entry;
    }

    public Block<TKey>? Block => _entry?.Block;

    public void Dispose()
    {
        if (_owner != null && _entry != null)
        {
            _owner.ReleaseForLease(_entry);
        }
    }
}
