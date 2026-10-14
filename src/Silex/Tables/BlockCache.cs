using Silex.Blocks;
using Silex.Ownership;
using System.Collections.Concurrent;

namespace Silex.Tables;

internal sealed class BlockCache : IDisposable
{
    private const int RecencySampleMask = 15;
    private const int LoadStripeCount = 256;

    private readonly object _gate = new();
    private readonly long _sizeLimit;
    private readonly ConcurrentDictionary<BlockCacheKey, Entry> _entries = [];
    private readonly object[] _loadGates = CreateLoadGates();
    private Entry? _mostRecent;
    [ThreadStatic]
    private static Entry? t_mostRecent;
    private Entry? _lruHead;
    private Entry? _lruTail;
    private long _size;
    [ThreadStatic]
    private static int t_recencyCounter;
    private bool _disposed;
    private readonly StripedCounter _hits = new();
    private long _misses;
    private long _evictions;

    public long SizeLimit => _sizeLimit;
    public long Hits => _hits.Value;
    public long Misses => Interlocked.Read(ref _misses);
    public long Evictions => Interlocked.Read(ref _evictions);
    public long SizeBytes => Interlocked.Read(ref _size);
    public long EntryCount => _entries.Count;

    public BlockCache(long sizeLimit)
    {
        if (sizeLimit < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(sizeLimit), sizeLimit, "Cache size limit must be non-negative.");
        }

        _sizeLimit = sizeLimit;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    internal bool TryAcquireEntry(Entry entry, out BlockLease lease)
    {
        if (!Volatile.Read(ref _disposed) && entry.TryAcquire())
        {
            _hits.Increment();
            SampleRecency(entry);
            lease = new BlockLease(entry);
            return true;
        }

        lease = default;
        return false;
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
            _hits.Increment();
            return new ValueTask<BlockLease>(lease);
        }

        return new ValueTask<BlockLease>(GetOrLoadSlow(key, loader, cancellationToken));
    }

    private BlockLease GetOrLoadSlow<TLoader>(BlockCacheKey key, TLoader loader, CancellationToken cancellationToken)
        where TLoader : struct, IBlockLoader
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_sizeLimit == 0)
        {
            return LoadAndCache(key, loader, cancellationToken);
        }

        lock (GetLoadGate(key))
        {
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfDisposed();

            if (TryAcquire(key, out var lease))
            {
                _hits.Increment();
                return lease;
            }

            return LoadAndCache(key, loader, cancellationToken);
        }
    }

    private object GetLoadGate(BlockCacheKey key)
    {
        var hash = unchecked((int)key.TableId * 397) ^ key.BlockIndex;
        return _loadGates[hash & (LoadStripeCount - 1)];
    }

    private static object[] CreateLoadGates()
    {
        var gates = new object[LoadStripeCount];
        for (var i = 0; i < gates.Length; i++)
        {
            gates[i] = new object();
        }

        return gates;
    }

    private BlockLease LoadAndCache<TLoader>(BlockCacheKey key, TLoader loader, CancellationToken cancellationToken)
        where TLoader : struct, IBlockLoader
    {
        Block? block = null;

        try
        {
            block = loader.Load(cancellationToken);
            if (block == null)
            {
                return default;
            }

            lock (_gate)
            {
                ThrowIfDisposed();

                if (TryAcquireLocked(key, out var lease))
                {
                    _hits.Increment();
                    return lease;
                }

                var entry = new Entry(this, key, block, block.Memory.Length, refCount: 1);
                block = null;
                Interlocked.Increment(ref _misses);

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
                SetMostRecent(entry);

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
        var entry = t_mostRecent;
        if (entry is not null && ReferenceEquals(entry.Owner, this) && entry.Key == key && entry.TryAcquire())
        {
            SampleRecency(entry);
            lease = new BlockLease(entry);
            return true;
        }

        if (_entries.TryGetValue(key, out entry) && entry.TryAcquire())
        {
            t_mostRecent = entry;
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
            SetMostRecent(entry);
            MoveToFront(entry);
            lease = new BlockLease(entry);
            return true;
        }

        lease = default;
        return false;
    }

    private void SampleRecency(Entry entry)
    {
        var mask = Interlocked.Read(ref _size) <= (_sizeLimit >> 1) ? 255 : RecencySampleMask;
        if ((++t_recencyCounter & mask) != 0
            || ReferenceEquals(entry, Volatile.Read(ref _lruHead))
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
                Interlocked.Increment(ref _evictions);
                entry.MarkEvicted();
                Interlocked.CompareExchange(ref _mostRecent, null, entry);
            }

            entry = previous;
        }
    }

    private void SetMostRecent(Entry entry)
    {
        Volatile.Write(ref _mostRecent, entry);
        if (entry.IsEvicted)
        {
            Interlocked.CompareExchange(ref _mostRecent, null, entry);
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

            Volatile.Write(ref _mostRecent, null);
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

        public Entry(BlockCache owner, BlockCacheKey key, Block block, int size, int refCount)
        {
            Owner = owner;
            Key = key;
            Block = block;
            Size = size;
            _refCount = refCount;
        }

        public BlockCache Owner { get; }
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
/// Loads a block on a cache miss. Implemented by a struct so <see cref="BlockCache"/>
/// can populate a miss without allocating a closure per read.
/// </summary>
internal interface IBlockLoader
{
    Block? Load(CancellationToken cancellationToken = default);
}

[CopySensitive]
[MustDispose]
internal readonly struct BlockLease : IDisposable
{
    private readonly BlockCache.Entry? _entry;

    internal BlockLease(BlockCache.Entry entry)
    {
        _entry = entry;
    }

    internal BlockCache.Entry? Entry => _entry;

    public Block? Block => _entry?.Block;

    public void Dispose()
    {
        _entry?.Release();
    }
}
