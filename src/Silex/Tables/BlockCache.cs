using Silex.Blocks;
using System.Collections.Concurrent;

namespace Silex.Tables;

internal sealed class BlockCache : IDisposable
{
    private const int RecencySampleMask = 15;

    private readonly object _gate = new();
    private readonly long _sizeLimit;
    private readonly ConcurrentDictionary<BlockCacheKey, Entry> _entries = [];
    private Entry? _lruHead;
    private Entry? _lruTail;
    private long _size;
    [ThreadStatic]
    private static int t_recencyCounter;
    private bool _disposed;

    public BlockCache(long sizeLimit)
    {
        if (sizeLimit < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(sizeLimit), sizeLimit, "Cache size limit must be non-negative.");
        }

        _sizeLimit = sizeLimit;
    }

    public ValueTask<BlockLease> GetOrLoadAsync<TLoader>(BlockCacheKey key, TLoader loader, CancellationToken cancellationToken = default)
        where TLoader : struct, IBlockLoader
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (Volatile.Read(ref _disposed))
        {
            ThrowIfDisposed();
        }

        if (TryAcquire(key, out var lease))
        {
            return new ValueTask<BlockLease>(lease);
        }

        return new ValueTask<BlockLease>(GetOrLoadSlow(key, loader, cancellationToken));
    }

    private BlockLease GetOrLoadSlow<TLoader>(BlockCacheKey key, TLoader loader, CancellationToken cancellationToken)
        where TLoader : struct, IBlockLoader
    {
        cancellationToken.ThrowIfCancellationRequested();
        Block? block = null;

        try
        {
            lock (_gate)
            {
                ThrowIfDisposed();

                if (TryAcquireLocked(key, out var lease))
                {
                    return lease;
                }

                block = loader.Load(cancellationToken);
                if (block == null)
                {
                    return default;
                }

                var entry = new Entry(key, block, block.Memory.Length, refCount: 1);
                block = null;

                if (_sizeLimit == 0)
                {
                    entry.MarkEvicted();
                    return new BlockLease(entry);
                }

                if (!_entries.TryAdd(key, entry))
                {
                    entry.MarkEvicted();
                    throw new InvalidOperationException("A block cache entry was inserted while the cache gate was held.");
                }

                AddFirst(entry);
                _size += entry.Size;

                EvictOverLimit(entry);

                return new BlockLease(entry);
            }
        }
        finally
        {
            block?.Dispose();
        }
    }

    private bool TryAcquire(BlockCacheKey key, out BlockLease lease)
    {
        if (_entries.TryGetValue(key, out var entry) && entry.TryAcquire())
        {
            SampleRecency(entry);
            lease = new BlockLease(entry);
            return true;
        }

        lease = default;
        return false;
    }

    private bool TryAcquireLocked(BlockCacheKey key, out BlockLease lease)
    {
        if (_entries.TryGetValue(key, out var entry) && entry.TryAcquire())
        {
            MoveToFront(entry);
            lease = new BlockLease(entry);
            return true;
        }

        lease = default;
        return false;
    }

    private void SampleRecency(Entry entry)
    {
        if ((++t_recencyCounter & RecencySampleMask) != 0
            || !Monitor.TryEnter(_gate))
        {
            return;
        }

        try
        {
            if (!entry.IsEvicted)
            {
                MoveToFront(entry);
            }
        }
        finally
        {
            Monitor.Exit(_gate);
        }
    }

    private void MoveToFront(Entry entry)
    {
        if (!entry.IsLinked || ReferenceEquals(entry, _lruHead))
        {
            return;
        }

        Remove(entry);
        AddFirst(entry);
    }

    private void EvictOverLimit(Entry protectedEntry)
    {
        var entry = _lruTail;

        while (_size > _sizeLimit && entry != null)
        {
            var previous = entry.Previous;

            if (!ReferenceEquals(entry, protectedEntry))
            {
                Remove(entry);
                _entries.TryRemove(entry.Key, out _);
                _size -= entry.Size;
                entry.MarkEvicted();
            }

            entry = previous;
        }
    }

    private void AddFirst(Entry entry)
    {
        entry.Previous = null;
        entry.Next = _lruHead;
        entry.IsLinked = true;

        if (_lruHead == null)
        {
            _lruTail = entry;
        }
        else
        {
            _lruHead.Previous = entry;
        }

        _lruHead = entry;
    }

    private void Remove(Entry entry)
    {
        if (entry.Previous == null)
        {
            _lruHead = entry.Next;
        }
        else
        {
            entry.Previous.Next = entry.Next;
        }

        if (entry.Next == null)
        {
            _lruTail = entry.Previous;
        }
        else
        {
            entry.Next.Previous = entry.Previous;
        }

        entry.Previous = null;
        entry.Next = null;
        entry.IsLinked = false;
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
                entry.MarkEvicted();
            }

            _entries.Clear();
            _lruHead = null;
            _lruTail = null;
            _size = 0;
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(BlockCache));
        }
    }

    ~BlockCache()
    {
        DisposeInternal();
    }

    internal sealed class Entry
    {
        private int _refCount;
        private int _evicted;
        private int _disposed;

        public Entry(BlockCacheKey key, Block block, int size, int refCount)
        {
            Key = key;
            Block = block;
            Size = size;
            _refCount = refCount;
        }

        public BlockCacheKey Key { get; }
        public Block Block { get; }
        public int Size { get; }
        public Entry? Previous { get; set; }
        public Entry? Next { get; set; }
        public bool IsLinked { get; set; }
        public bool IsEvicted => Volatile.Read(ref _evicted) != 0;

        public bool TryAcquire()
        {
            if (IsEvicted)
            {
                return false;
            }

            Interlocked.Increment(ref _refCount);
            if (!IsEvicted)
            {
                return true;
            }

            Interlocked.Decrement(ref _refCount);
            TryDispose();
            return false;
        }

        public void Release()
        {
            if (Interlocked.Decrement(ref _refCount) < 0)
            {
                Interlocked.Increment(ref _refCount);
                return;
            }

            TryDispose();
        }

        public void MarkEvicted()
        {
            Volatile.Write(ref _evicted, 1);
            TryDispose();
        }

        private void TryDispose()
        {
            if (IsEvicted
                && Volatile.Read(ref _refCount) == 0
                && Interlocked.CompareExchange(ref _disposed, 1, 0) == 0)
            {
                Block.Dispose();
            }
        }
    }
}

internal readonly record struct BlockCacheKey(long TableId, int BlockIndex);

/// <summary>
/// Loads a block on a cache miss. Implemented by a struct so <see cref="BlockCache{ByteSlice, ByteSlice}.GetOrLoadAsync"/>
/// can populate a miss without allocating a closure per read.
/// </summary>
internal interface IBlockLoader
{
    Block? Load(CancellationToken cancellationToken = default);
}

internal readonly struct BlockLease : IDisposable
{
    private readonly BlockCache.Entry? _entry;

    internal BlockLease(BlockCache.Entry entry)
    {
        _entry = entry;
    }

    public Block? Block => _entry?.Block;

    public void Dispose()
    {
        _entry?.Release();
    }
}
