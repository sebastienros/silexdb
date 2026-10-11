namespace Silex;

/// <summary>A reusable access helper for fully encoded keys beginning with an owned binary prefix.</summary>
/// <remarks>
/// Keys and returned cursor entries include the prefix; this helper does not encode or strip keys.
/// It owns no storage, snapshot, transaction, or batch. Pass the same transaction to multiple keyspaces
/// for cross-keyspace atomicity. Overlapping prefixes intentionally overlap; this is not an access-control
/// boundary against callers using the underlying store directly. Existing ownership, concurrency,
/// resource-limit, conflict, durability and health contracts apply unchanged.
/// </remarks>
public sealed class PrefixKeyspace
{
    private readonly byte[] _prefix;
    private readonly OrderedReadRange _range;

    public PrefixKeyspace(ReadOnlySpan<byte> prefix)
    {
        _prefix = prefix.ToArray();
        byte[]? upper = null;
        for (var i = _prefix.Length - 1; i >= 0; i--)
        {
            if (_prefix[i] == byte.MaxValue) continue;
            upper = _prefix.AsSpan(0, i + 1).ToArray();
            upper[i]++;
            break;
        }
        _range = new OrderedReadRange(_prefix, upper: upper is null ? null : (ReadOnlyMemory<byte>?)upper,
            upperInclusive: false);
    }

    /// <summary>The prefix as a read-only borrow, valid for this helper's lifetime.</summary>
    public ReadOnlySpan<byte> Prefix => _prefix;

    /// <summary>Tests membership in this keyspace, including the prefix key itself.</summary>
    public bool Contains(ReadOnlySpan<byte> key) => key.StartsWith(_prefix);

    /// <summary>
    /// Caps omitted bounds to this keyspace. Explicit bounds must belong to it, except an exclusive upper
    /// bound at its lexicographic successor. Bounds and results remain fully encoded.
    /// </summary>
    /// <remarks>
    /// An empty prefix covers every key. An all-FF prefix has no finite upper bound; all keys at or above
    /// that prefix begin with it. Invalid bounds throw rather than silently widening or clipping a request.
    /// Reversed/equal exclusive endpoints inside the keyspace retain the ordinary empty-range semantics.
    /// </remarks>
    public OrderedReadRange BoundRange(OrderedReadRange? range = null)
    {
        if (range is null) return _range;
        if (range.Lower is { } lower && !Contains(lower.Span))
        {
            throw new ArgumentException("Lower bound is outside the keyspace.", nameof(range));
        }
        if (range.Upper is { } upper && !Contains(upper.Span) &&
            (range.UpperInclusive || _range.Upper is not { } end || !upper.Span.SequenceEqual(end.Span)))
        {
            throw new ArgumentException("Upper bound is outside the keyspace.", nameof(range));
        }
        return new OrderedReadRange(range.Lower ?? _range.Lower,
            range.Lower.HasValue ? range.LowerInclusive : _range.LowerInclusive,
            range.Upper ?? _range.Upper,
            range.Upper.HasValue ? range.UpperInclusive : _range.UpperInclusive);
    }

    public void Put(LsmStorage storage, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ValidateKey(key);
        storage.Put(key, value);
    }

    public void Put(MvccStorage storage, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ValidateKey(key);
        storage.Put(key, value);
    }

    public void Put(MvccTransaction transaction, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ValidateKey(key);
        transaction.Put(key, value);
    }

    /// <summary>Stages a versionstamped put without allowing substitution inside the keyspace prefix.</summary>
    /// <remarks>Offsets address the fully encoded key/value; the prefix is not added or stripped.</remarks>
    public void PutVersionstamped(MvccTransaction transaction, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value,
        int keyOffset = -1, int valueOffset = -1)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ValidateKey(key);
        if (keyOffset >= 0 && keyOffset < _prefix.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(keyOffset), "Versionstamp substitution must not change the keyspace prefix.");
        }
        transaction.PutVersionstamped(key, value, keyOffset, valueOffset);
    }

    /// <summary>Stages a validated key in a caller-owned batch; apply it with LsmStorage.Write.</summary>
    public void Put(LsmWriteBatch batch, ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ValidateKey(key);
        batch.Put(key, value);
    }

    public void Delete(LsmStorage storage, ReadOnlySpan<byte> key)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ValidateKey(key);
        storage.Delete(key);
    }

    public void Delete(MvccStorage storage, ReadOnlySpan<byte> key)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ValidateKey(key);
        storage.Delete(key);
    }

    public void Delete(MvccTransaction transaction, ReadOnlySpan<byte> key)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ValidateKey(key);
        transaction.Delete(key);
    }

    public void Delete(LsmWriteBatch batch, ReadOnlySpan<byte> key)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ValidateKey(key);
        batch.Delete(key);
    }

    public ValueTask<int> GetRawAsync(LsmStorage storage, ReadOnlySpan<byte> key, Memory<byte> destination,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ValidateKey(key);
        return storage.GetRawAsync(key, destination, cancellationToken);
    }

    public ValueTask<int> GetRawAsync(MvccStorage storage, ReadOnlySpan<byte> key, Memory<byte> destination,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ValidateKey(key);
        return storage.GetRawAsync(key, destination, cancellationToken);
    }

    public ValueTask<int> GetRawAsync(MvccSnapshot snapshot, ReadOnlySpan<byte> key, Memory<byte> destination,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ValidateKey(key);
        return snapshot.GetRawAsync(key, destination, cancellationToken);
    }

    public ValueTask<int> GetRawAsync(MvccTransaction transaction, ReadOnlySpan<byte> key, Memory<byte> destination,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ValidateKey(key);
        return transaction.GetRawAsync(key, destination, cancellationToken);
    }

    public ValueTask<int> GetForUpdateRawAsync(MvccTransaction transaction, ReadOnlySpan<byte> key,
        Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ValidateKey(key);
        return transaction.GetForUpdateRawAsync(key, destination, cancellationToken);
    }

    public ValueTask<bool> TryReadRawAsync<TArg>(LsmStorage storage, ReadOnlySpan<byte> key, TArg arg,
        ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ValidateKey(key);
        return storage.TryReadRawAsync(key, arg, reader, cancellationToken);
    }

    public ValueTask<bool> TryReadRawAsync<TArg>(MvccStorage storage, ReadOnlySpan<byte> key, TArg arg,
        ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ValidateKey(key);
        return storage.TryReadRawAsync(key, arg, reader, cancellationToken);
    }

    public ValueTask<bool> TryReadRawAsync<TArg>(MvccSnapshot snapshot, ReadOnlySpan<byte> key, TArg arg,
        ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ValidateKey(key);
        return snapshot.TryReadRawAsync(key, arg, reader, cancellationToken);
    }

    public ValueTask<PointReadBatch> ReadManyRawAsync(MvccStorage storage,
        ReadOnlyMemory<ReadOnlyMemory<byte>> keys, int maxBytes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(storage);
        ValidateKeys(keys);
        return storage.ReadManyRawAsync(keys, maxBytes, cancellationToken);
    }

    public ValueTask<PointReadBatch> ReadManyRawAsync(MvccSnapshot snapshot,
        ReadOnlyMemory<ReadOnlyMemory<byte>> keys, int maxBytes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        ValidateKeys(keys);
        return snapshot.ReadManyRawAsync(keys, maxBytes, cancellationToken);
    }

    public ValueTask<PointReadBatch> ReadManyRawAsync(MvccTransaction transaction,
        ReadOnlyMemory<ReadOnlyMemory<byte>> keys, int maxBytes, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        ValidateKeys(keys);
        return transaction.ReadManyRawAsync(keys, maxBytes, cancellationToken);
    }

    public OrderedReadCursor CreateOrderedReadCursor(LsmStorage storage, OrderedReadRange? range = null,
        OrderedReadDirection direction = OrderedReadDirection.Forward)
    {
        ArgumentNullException.ThrowIfNull(storage);
        return storage.CreateOrderedReadCursor(BoundRange(range), direction);
    }

    public OrderedReadCursor CreateOrderedReadCursor(MvccStorage storage, OrderedReadRange? range = null,
        OrderedReadDirection direction = OrderedReadDirection.Forward)
    {
        ArgumentNullException.ThrowIfNull(storage);
        return storage.CreateOrderedReadCursor(BoundRange(range), direction);
    }

    public OrderedReadCursor CreateOrderedReadCursor(MvccSnapshot snapshot, OrderedReadRange? range = null,
        OrderedReadDirection direction = OrderedReadDirection.Forward)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        return snapshot.CreateOrderedReadCursor(BoundRange(range), direction);
    }

    public OrderedReadCursor CreateOrderedReadCursor(MvccTransaction transaction, OrderedReadRange? range = null,
        OrderedReadDirection direction = OrderedReadDirection.Forward)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return transaction.CreateOrderedReadCursor(BoundRange(range), direction);
    }

    /// <summary>Registers only the bounded interval; cursors and ordinary reads remain untracked.</summary>
    public void TrackReadRange(MvccTransaction transaction, OrderedReadRange? range = null)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        transaction.TrackReadRange(BoundRange(range));
    }

    /// <summary>Stages bounded logical deletion in the supplied transaction, with its usual rollback contract.</summary>
    /// <remarks>Remaining contains fully encoded bounds and can be passed back to this helper unchanged.</remarks>
    public ValueTask<RangeDeleteResult> DeleteRangeAsync(MvccTransaction transaction, int maxEntries,
        OrderedReadRange? range = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(transaction);
        return transaction.DeleteRangeAsync(BoundRange(range), maxEntries, cancellationToken);
    }

    private void ValidateKey(ReadOnlySpan<byte> key)
    {
        if (!Contains(key))
        {
            throw new ArgumentException("Key is outside the keyspace; supply a fully encoded key including its prefix.",
                nameof(key));
        }
    }

    private void ValidateKeys(ReadOnlyMemory<ReadOnlyMemory<byte>> keys)
    {
        foreach (var key in keys.Span)
        {
            if (!Contains(key.Span))
            {
                throw new ArgumentException("Every key must belong to the keyspace.", nameof(keys));
            }
        }
    }
}
