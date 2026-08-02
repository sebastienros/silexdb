using System.Buffers;
using System.Buffers.Binary;
using Silex.Buffers;

namespace Silex;

/// <summary>
/// An opt-in multi-version facade over <see cref="LsmStorage"/>.
/// </summary>
/// <remarks>
/// Opening a database through this type enables versioned keys, snapshots, and optimistic transactions for
/// that database. The regular <see cref="LsmStorage"/> path has no MVCC branches, key expansion, or sequence
/// bookkeeping.
/// </remarks>
public sealed class MvccStorage : IAsyncDisposable
{
    private const byte MetadataPrefix = 0;
    private const byte DataPrefix = 1;
    private const byte TombstoneValue = 0;
    private const byte LiveValue = 1;

    private static readonly byte[] _formatKey = [MetadataPrefix, 1];
    private static readonly byte[] _allocatedSequenceKey = [MetadataPrefix, 2];
    private static readonly byte[] _publishedSequenceKey = [MetadataPrefix, 3];
    private static readonly byte[] _formatValue = "Silex.MVCC.1"u8.ToArray();

    private readonly LsmStorage _storage;
    private readonly SemaphoreSlim _commitGate = new(1, 1);
    private readonly object _snapshotLock = new();
    private readonly Dictionary<long, int> _activeSnapshots = [];
    private long _allocatedSequence;
    private long _publishedSequence;
    private int _disposeState;

    private MvccStorage(LsmStorage storage, long allocatedSequence, long publishedSequence)
    {
        _storage = storage;
        _allocatedSequence = allocatedSequence;
        _publishedSequence = publishedSequence;
    }

    /// <summary>
    /// Gets the sequence visible to new reads.
    /// </summary>
    public long PublishedSequence => Volatile.Read(ref _publishedSequence);

    /// <summary>
    /// Opens or creates an MVCC database.
    /// </summary>
    /// <remarks>
    /// An existing plain <see cref="LsmStorage"/> database cannot be opened as MVCC because its key format is
    /// different. Use a separate path or migrate its entries explicitly.
    /// </remarks>
    public static async Task<MvccStorage> OpenAsync(string path, StorageOptions options, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(options);

        var storage = await LsmStorage.OpenAsync(path, options, cancellationToken).ConfigureAwait(false);

        try
        {
            var format = new byte[_formatValue.Length];
            var formatLength = await storage.GetRawAsync(_formatKey, format, cancellationToken).ConfigureAwait(false);

            if (formatLength < 0)
            {
                var entries = await storage.ScanRawAsync(
                    0,
                    static (_, _, _) => false,
                    maxEntries: 1,
                    cancellationToken).ConfigureAwait(false);

                if (entries != 0)
                {
                    throw new InvalidOperationException("The database contains plain keys and cannot be opened as an MVCC database.");
                }

                storage.Put(_formatKey, _formatValue);
                WriteSequence(storage, _allocatedSequenceKey, 0);
                WriteSequence(storage, _publishedSequenceKey, 0);
                return new MvccStorage(storage, 0, 0);
            }

            if (formatLength != _formatValue.Length || !format.AsSpan().SequenceEqual(_formatValue))
            {
                throw new InvalidDataException("The database uses an unsupported MVCC format.");
            }

            var allocated = await ReadSequenceAsync(storage, _allocatedSequenceKey, cancellationToken).ConfigureAwait(false);
            var published = await ReadSequenceAsync(storage, _publishedSequenceKey, cancellationToken).ConfigureAwait(false);

            if (allocated < published)
            {
                throw new InvalidDataException("The MVCC allocated sequence is older than its published sequence.");
            }

            return new MvccStorage(storage, allocated, published);
        }
        catch
        {
            await storage.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    /// Creates a stable read-only view at the currently published sequence.
    /// </summary>
    public MvccSnapshot CreateSnapshot()
    {
        lock (_snapshotLock)
        {
            CheckDisposed();
            var sequence = Volatile.Read(ref _publishedSequence);
            AddSnapshot(sequence);
            return new MvccSnapshot(this, sequence);
        }
    }

    /// <summary>
    /// Starts an optimistic transaction using snapshot isolation.
    /// </summary>
    public MvccTransaction BeginTransaction()
    {
        lock (_snapshotLock)
        {
            CheckDisposed();
            var sequence = Volatile.Read(ref _publishedSequence);
            AddSnapshot(sequence);
            return new MvccTransaction(this, sequence);
        }
    }

    /// <summary>
    /// Atomically publishes a single value.
    /// </summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        CheckDisposed();
        _commitGate.Wait();

        try
        {
            CheckDisposed();
            CommitSingle(key, value, isTombstone: false);
        }
        finally
        {
            _commitGate.Release();
        }
    }

    /// <summary>
    /// Atomically publishes a single deletion.
    /// </summary>
    public void Delete(ReadOnlySpan<byte> key)
    {
        CheckDisposed();
        _commitGate.Wait();

        try
        {
            CheckDisposed();
            CommitSingle(key, default, isTombstone: true);
        }
        finally
        {
            _commitGate.Release();
        }
    }

    /// <summary>
    /// Reads the latest published value into a caller-owned buffer.
    /// </summary>
    /// <returns>The value length, or <c>-1</c> when the key is missing or deleted.</returns>
    public ValueTask<int> GetRawAsync(ReadOnlySpan<byte> key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        var sequence = RegisterCurrentRead();

        try
        {
            return ReleaseReadAsync(ReadAtAsync(key, sequence, new MemoryCopySink(destination), cancellationToken), sequence);
        }
        catch
        {
            ReleaseSnapshot(sequence);
            throw;
        }
    }

    /// <summary>
    /// Inspects the latest published value without copying it.
    /// </summary>
    public ValueTask<bool> TryReadRawAsync<TArg>(
        ReadOnlySpan<byte> key,
        TArg arg,
        ReadValueAction<TArg> reader,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        CheckDisposed();

        var sequence = RegisterCurrentRead();

        try
        {
            return ReleaseFoundReadAsync(
                ReadAtAsync(key, sequence, new DelegateSink<TArg>(arg, reader), cancellationToken),
                sequence);
        }
        catch
        {
            ReleaseSnapshot(sequence);
            throw;
        }
    }

    /// <summary>
    /// Scans the latest published view in ascending user-key order.
    /// </summary>
    public ValueTask<long> ScanRawAsync<TArg>(
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries = long.MaxValue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentOutOfRangeException.ThrowIfNegative(maxEntries);
        CheckDisposed();

        var sequence = RegisterCurrentRead();
        return ReleaseReadAsync(ScanAtAsync(sequence, default, hasFrom: false, arg, reader, maxEntries, cancellationToken), sequence);
    }

    /// <summary>
    /// Scans the latest published view from <paramref name="from"/> in ascending user-key order.
    /// </summary>
    public ValueTask<long> SeekRawAsync<TArg>(
        ReadOnlySpan<byte> from,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries = long.MaxValue,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentOutOfRangeException.ThrowIfNegative(maxEntries);
        CheckDisposed();

        var sequence = RegisterCurrentRead();

        try
        {
            var encodedFrom = EncodeVersionKey(from, long.MaxValue);
            return ReleaseSeekReadAsync(encodedFrom, sequence, arg, reader, maxEntries, cancellationToken);
        }
        catch
        {
            ReleaseSnapshot(sequence);
            throw;
        }
    }

    /// <summary>
    /// Deletes physical versions that are not visible to the current state or any active snapshot or transaction.
    /// </summary>
    /// <param name="compact">
    /// When <c>true</c>, flushes and compacts after writing the version tombstones so their storage can be reclaimed.
    /// </param>
    /// <returns>The number of obsolete physical versions deleted.</returns>
    public async ValueTask<long> CollectGarbageAsync(bool compact = false, CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        await _commitGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            CheckDisposed();

            long[] requiredSequences;
            lock (_snapshotLock)
            {
                requiredSequences = _activeSnapshots.Keys
                    .Append(Volatile.Read(ref _publishedSequence))
                    .Distinct()
                    .OrderDescending()
                    .ToArray();
            }

            using var state = new GarbageCollectionState(requiredSequences);
            await _storage.ScanRawAsync(
                state,
                static (s, key, _) => s.Accept(key),
                cancellationToken: cancellationToken).ConfigureAwait(false);

            foreach (var key in state.ObsoleteKeys)
            {
                cancellationToken.ThrowIfCancellationRequested();
                _storage.Delete(key);
            }

            if (compact && state.ObsoleteKeys.Count != 0)
            {
                await _storage.FlushAndCompactAsync(cancellationToken).ConfigureAwait(false);
            }

            return state.ObsoleteKeys.Count;
        }
        finally
        {
            _commitGate.Release();
        }
    }

    /// <summary>
    /// Flushes and compacts the underlying LSM tree.
    /// </summary>
    public Task FlushAndCompactAsync(CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        return _storage.FlushAndCompactAsync(cancellationToken);
    }

    /// <summary>
    /// Flushes pending data and closes the database.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeState, 1) != 0)
        {
            return;
        }

        await _commitGate.WaitAsync().ConfigureAwait(false);

        try
        {
            await _storage.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _commitGate.Release();
        }
    }

    internal ValueTask<int> GetRawAtAsync(ReadOnlySpan<byte> key, long sequence, Memory<byte> destination, CancellationToken cancellationToken)
    {
        CheckDisposed();
        return ReadAtAsync(key, sequence, new MemoryCopySink(destination), cancellationToken);
    }

    internal ValueTask<bool> TryReadRawAtAsync<TArg>(
        ReadOnlySpan<byte> key,
        long sequence,
        TArg arg,
        ReadValueAction<TArg> reader,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);
        CheckDisposed();
        return FoundAsync(ReadAtAsync(key, sequence, new DelegateSink<TArg>(arg, reader), cancellationToken));
    }

    internal ValueTask<long> ScanAtAsync<TArg>(
        long sequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        CancellationToken cancellationToken)
    {
        return ScanAtAsync(sequence, default, hasFrom: false, arg, reader, maxEntries, cancellationToken);
    }

    internal ValueTask<long> SeekAtAsync<TArg>(
        ReadOnlySpan<byte> from,
        long sequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        CancellationToken cancellationToken)
    {
        var encodedFrom = EncodeVersionKey(from, long.MaxValue);
        return SeekEncodedAtAsync(encodedFrom, sequence, arg, reader, maxEntries, cancellationToken);
    }

    internal async ValueTask<bool> TryCommitAsync(
        long snapshotSequence,
        IReadOnlyList<MvccMutation> mutations,
        IReadOnlyList<byte[]> trackedReads,
        CancellationToken cancellationToken)
    {
        CheckDisposed();
        await _commitGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            CheckDisposed();
            var published = Volatile.Read(ref _publishedSequence);

            for (var i = 0; i < mutations.Count; i++)
            {
                if (await WasModifiedAfterAsync(mutations[i].Key, snapshotSequence, published, cancellationToken).ConfigureAwait(false))
                {
                    return false;
                }
            }

            for (var i = 0; i < trackedReads.Count; i++)
            {
                var key = trackedReads[i];
                if (ContainsKey(mutations, key))
                {
                    continue;
                }

                if (await WasModifiedAfterAsync(key, snapshotSequence, published, cancellationToken).ConfigureAwait(false))
                {
                    return false;
                }
            }

            if (mutations.Count == 0)
            {
                return true;
            }

            var sequence = AllocateSequence();
            WriteSequence(_storage, _allocatedSequenceKey, sequence);
            _allocatedSequence = sequence;

            for (var i = 0; i < mutations.Count; i++)
            {
                var mutation = mutations[i];
                WriteVersion(mutation.Key, mutation.Value ?? default, mutation.Value is null, sequence);
            }

            Publish(sequence);
            return true;
        }
        finally
        {
            _commitGate.Release();
        }
    }

    internal void ReleaseSnapshot(long sequence)
    {
        lock (_snapshotLock)
        {
            if (!_activeSnapshots.TryGetValue(sequence, out var count))
            {
                return;
            }

            if (count == 1)
            {
                _activeSnapshots.Remove(sequence);
            }
            else
            {
                _activeSnapshots[sequence] = count - 1;
            }
        }
    }

    private void CommitSingle(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone)
    {
        var sequence = AllocateSequence();
        WriteSequence(_storage, _allocatedSequenceKey, sequence);
        _allocatedSequence = sequence;
        WriteVersion(key, value, isTombstone, sequence);
        Publish(sequence);
    }

    private void Publish(long sequence)
    {
        WriteSequence(_storage, _publishedSequenceKey, sequence);
        Volatile.Write(ref _publishedSequence, sequence);
    }

    private long AllocateSequence()
    {
        if (_allocatedSequence == long.MaxValue)
        {
            throw new InvalidOperationException("The MVCC sequence space is exhausted.");
        }

        return _allocatedSequence + 1;
    }

    private void WriteVersion(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone, long sequence)
    {
        using var internalKey = EncodeVersionKey(key, sequence);

        if (isTombstone)
        {
            Span<byte> stored = stackalloc byte[1];
            stored[0] = TombstoneValue;
            _storage.Put(internalKey.Span, stored);
            return;
        }

        byte[]? rented = null;
        Span<byte> encoded = value.Length < 256
            ? stackalloc byte[value.Length + 1]
            : (rented = ArrayPool<byte>.Shared.Rent(value.Length + 1)).AsSpan(0, value.Length + 1);

        try
        {
            encoded[0] = LiveValue;
            value.CopyTo(encoded[1..]);
            _storage.Put(internalKey.Span, encoded);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private async ValueTask<bool> WasModifiedAfterAsync(
        byte[] key,
        long snapshotSequence,
        long publishedSequence,
        CancellationToken cancellationToken)
    {
        var sequence = await FindVisibleSequenceAsync(key, publishedSequence, cancellationToken).ConfigureAwait(false);
        return sequence > snapshotSequence;
    }

    private ValueTask<long> FindVisibleSequenceAsync(ReadOnlySpan<byte> key, long sequence, CancellationToken cancellationToken)
    {
        var encodedKey = EncodeVersionKey(key, sequence);
        var state = new PointReadState<NoOpSink>(encodedKey.Memory[..^sizeof(long)], sequence, default);
        return FindSequenceAsync(encodedKey, state, cancellationToken);

        async ValueTask<long> FindSequenceAsync(
            OwnedByteSlice ownedKey,
            PointReadState<NoOpSink> pointState,
            CancellationToken ct)
        {
            using (ownedKey)
            {
                await _storage.SeekRawAsync(
                    ownedKey.Span,
                    pointState,
                    static (s, internalKey, value) => s.Accept(internalKey, value),
                    maxEntries: 1,
                    ct).ConfigureAwait(false);
                return pointState.Sequence;
            }
        }
    }

    private ValueTask<int> ReadAtAsync<TSink>(
        ReadOnlySpan<byte> key,
        long sequence,
        TSink sink,
        CancellationToken cancellationToken)
        where TSink : struct, IValueSink
    {
        var encodedKey = EncodeVersionKey(key, sequence);
        var state = new PointReadState<TSink>(encodedKey.Memory[..^sizeof(long)], sequence, sink);
        return ReadEncodedAsync(encodedKey, state, cancellationToken);

        async ValueTask<int> ReadEncodedAsync(
            OwnedByteSlice ownedKey,
            PointReadState<TSink> pointState,
            CancellationToken ct)
        {
            using (ownedKey)
            {
                await _storage.SeekRawAsync(
                    ownedKey.Span,
                    pointState,
                    static (s, internalKey, value) => s.Accept(internalKey, value),
                    maxEntries: 1,
                    ct).ConfigureAwait(false);
                return pointState.Length;
            }
        }
    }

    private async ValueTask<long> ScanAtAsync<TArg>(
        long sequence,
        OwnedByteSlice? encodedFrom,
        bool hasFrom,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        CancellationToken cancellationToken)
    {
        CheckDisposed();

        if (maxEntries == 0)
        {
            encodedFrom?.Dispose();
            return 0;
        }

        using (encodedFrom)
        using (var state = new ScanState<TArg>(sequence, arg, reader, maxEntries))
        {
            if (hasFrom)
            {
                await _storage.SeekRawAsync(
                    encodedFrom!.Span,
                    state,
                    static (s, key, value) => s.Accept(key, value),
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _storage.ScanRawAsync(
                    state,
                    static (s, key, value) => s.Accept(key, value),
                    cancellationToken: cancellationToken).ConfigureAwait(false);
            }

            return state.Count;
        }
    }

    private ValueTask<long> SeekEncodedAtAsync<TArg>(
        OwnedByteSlice encodedFrom,
        long sequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        CancellationToken cancellationToken)
    {
        return ScanAtAsync(sequence, encodedFrom, hasFrom: true, arg, reader, maxEntries, cancellationToken);
    }

    private ValueTask<long> ReleaseSeekReadAsync<TArg>(
        OwnedByteSlice encodedFrom,
        long sequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        CancellationToken cancellationToken)
    {
        return ReleaseReadAsync(
            SeekEncodedAtAsync(encodedFrom, sequence, arg, reader, maxEntries, cancellationToken),
            sequence);
    }

    private long RegisterCurrentRead()
    {
        lock (_snapshotLock)
        {
            CheckDisposed();
            var sequence = Volatile.Read(ref _publishedSequence);
            AddSnapshot(sequence);
            return sequence;
        }
    }

    private void AddSnapshot(long sequence)
    {
        _activeSnapshots.TryGetValue(sequence, out var count);
        _activeSnapshots[sequence] = count + 1;
    }

    private static bool ContainsKey(IReadOnlyList<MvccMutation> mutations, ReadOnlySpan<byte> key)
    {
        for (var i = 0; i < mutations.Count; i++)
        {
            if (key.SequenceEqual(mutations[i].Key))
            {
                return true;
            }
        }

        return false;
    }

    private static OwnedByteSlice EncodeVersionKey(ReadOnlySpan<byte> userKey, long sequence)
    {
        var zeroCount = 0;
        for (var i = 0; i < userKey.Length; i++)
        {
            zeroCount += userKey[i] == 0 ? 1 : 0;
        }

        var length = checked(1 + userKey.Length + zeroCount + 2 + sizeof(long));
        var owner = MemoryOwner<byte>.Rent(length);

        try
        {
            var destination = owner.Memory.Span[..length];
            var offset = 0;
            destination[offset++] = DataPrefix;

            for (var i = 0; i < userKey.Length; i++)
            {
                var value = userKey[i];
                destination[offset++] = value;
                if (value == 0)
                {
                    destination[offset++] = byte.MaxValue;
                }
            }

            destination[offset++] = 0;
            destination[offset++] = 0;
            BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], ~(ulong)sequence);
            return OwnedByteSlice.TakeOwnership(owner, length);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    private static bool TryParseVersionKey(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> encodedUserKey, out long sequence)
    {
        if (key.Length < 1 + 2 + sizeof(long) || key[0] != DataPrefix)
        {
            encodedUserKey = default;
            sequence = -1;
            return false;
        }

        var terminator = key.Length - sizeof(long) - 2;
        if (key[terminator] != 0 || key[terminator + 1] != 0)
        {
            throw new InvalidDataException("An MVCC data key has an invalid terminator.");
        }

        var encodedSequence = BinaryPrimitives.ReadUInt64BigEndian(key[^sizeof(long)..]);
        var decodedSequence = ~encodedSequence;
        if (decodedSequence > long.MaxValue)
        {
            throw new InvalidDataException("An MVCC data key has an invalid sequence.");
        }

        encodedUserKey = key.Slice(1, terminator - 1);
        sequence = (long)decodedSequence;
        return true;
    }

    private static bool InvokeUserReader<TArg>(
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        ReadOnlySpan<byte> encodedUserKey,
        ReadOnlySpan<byte> value)
    {
        var firstZero = encodedUserKey.IndexOf((byte)0);
        if (firstZero < 0)
        {
            return reader(arg, encodedUserKey, value);
        }

        var rented = ArrayPool<byte>.Shared.Rent(encodedUserKey.Length);
        var decoded = rented.AsSpan();
        var written = 0;

        try
        {
            for (var i = 0; i < encodedUserKey.Length; i++)
            {
                var current = encodedUserKey[i];
                if (current != 0)
                {
                    decoded[written++] = current;
                    continue;
                }

                if (++i >= encodedUserKey.Length || encodedUserKey[i] != byte.MaxValue)
                {
                    throw new InvalidDataException("An MVCC user key has an invalid escape sequence.");
                }

                decoded[written++] = 0;
            }

            return reader(arg, decoded[..written], value);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static void ValidateStoredValue(ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty || value[0] is not (TombstoneValue or LiveValue))
        {
            throw new InvalidDataException("An MVCC value has an invalid kind marker.");
        }
    }

    private static void WriteSequence(LsmStorage storage, ReadOnlySpan<byte> key, long sequence)
    {
        Span<byte> value = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(value, sequence);
        storage.Put(key, value);
    }

    private static async ValueTask<long> ReadSequenceAsync(
        LsmStorage storage,
        ReadOnlyMemory<byte> key,
        CancellationToken cancellationToken)
    {
        var value = new byte[sizeof(long)];
        var length = await storage.GetRawAsync(key.Span, value, cancellationToken).ConfigureAwait(false);
        if (length != sizeof(long))
        {
            throw new InvalidDataException("The MVCC sequence metadata is missing or invalid.");
        }

        var sequence = BinaryPrimitives.ReadInt64LittleEndian(value);
        if (sequence < 0)
        {
            throw new InvalidDataException("The MVCC sequence metadata is negative.");
        }

        return sequence;
    }

    private static async ValueTask<int> ReleaseReadAsync(ValueTask<int> read, MvccStorage storage, long sequence)
    {
        try
        {
            return await read.ConfigureAwait(false);
        }
        finally
        {
            storage.ReleaseSnapshot(sequence);
        }
    }

    private ValueTask<int> ReleaseReadAsync(ValueTask<int> read, long sequence) => ReleaseReadAsync(read, this, sequence);

    private static async ValueTask<long> ReleaseReadAsync(ValueTask<long> read, MvccStorage storage, long sequence)
    {
        try
        {
            return await read.ConfigureAwait(false);
        }
        finally
        {
            storage.ReleaseSnapshot(sequence);
        }
    }

    private ValueTask<long> ReleaseReadAsync(ValueTask<long> read, long sequence) => ReleaseReadAsync(read, this, sequence);

    private static async ValueTask<bool> ReleaseFoundReadAsync(ValueTask<int> read, MvccStorage storage, long sequence)
    {
        try
        {
            return await read.ConfigureAwait(false) >= 0;
        }
        finally
        {
            storage.ReleaseSnapshot(sequence);
        }
    }

    private ValueTask<bool> ReleaseFoundReadAsync(ValueTask<int> read, long sequence) => ReleaseFoundReadAsync(read, this, sequence);

    private static async ValueTask<bool> FoundAsync(ValueTask<int> read) => await read.ConfigureAwait(false) >= 0;

    private void CheckDisposed()
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposeState) != 0, this);
    }

    private interface IValueSink
    {
        void Accept(ReadOnlySpan<byte> value);
    }

    private readonly struct NoOpSink : IValueSink
    {
        public void Accept(ReadOnlySpan<byte> value)
        {
        }
    }

    private readonly struct MemoryCopySink(Memory<byte> destination) : IValueSink
    {
        public void Accept(ReadOnlySpan<byte> value)
        {
            if (value.Length <= destination.Length)
            {
                value.CopyTo(destination.Span);
            }
        }
    }

    private readonly struct DelegateSink<TArg>(TArg arg, ReadValueAction<TArg> reader) : IValueSink
    {
        public void Accept(ReadOnlySpan<byte> value) => reader(arg, value);
    }

    private sealed class PointReadState<TSink>(ReadOnlyMemory<byte> userPrefix, long snapshotSequence, TSink sink)
        where TSink : struct, IValueSink
    {
        private readonly ReadOnlyMemory<byte> _userPrefix = userPrefix;
        private readonly long _snapshotSequence = snapshotSequence;
        private TSink _sink = sink;

        public int Length { get; private set; } = -1;
        public long Sequence { get; private set; } = -1;

        public bool Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (!TryParseVersionKey(key, out _, out var sequence))
            {
                return false;
            }

            var candidatePrefix = key[..^sizeof(long)];
            if (!candidatePrefix.SequenceEqual(_userPrefix.Span) || sequence > _snapshotSequence)
            {
                return false;
            }

            ValidateStoredValue(value);
            Sequence = sequence;

            if (value[0] == TombstoneValue)
            {
                return false;
            }

            var userValue = value[1..];
            _sink.Accept(userValue);
            Length = userValue.Length;
            return false;
        }
    }

    private sealed class ScanState<TArg>(
        long snapshotSequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries) : IDisposable
    {
        private readonly KeyGroup _group = new();
        private bool _resolved;

        public long Count { get; private set; }

        public bool Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (!TryParseVersionKey(key, out var encodedUserKey, out var sequence))
            {
                return true;
            }

            if (_group.Set(encodedUserKey))
            {
                _resolved = false;
            }

            if (_resolved || sequence > snapshotSequence)
            {
                return true;
            }

            ValidateStoredValue(value);
            _resolved = true;

            if (value[0] == TombstoneValue)
            {
                return true;
            }

            Count++;
            return InvokeUserReader(arg, reader, encodedUserKey, value[1..]) && Count < maxEntries;
        }

        public void Dispose() => _group.Dispose();
    }

    private sealed class GarbageCollectionState(long[] requiredSequences) : IDisposable
    {
        private readonly KeyGroup _group = new();
        private int _nextRequired;

        public List<byte[]> ObsoleteKeys { get; } = [];

        public bool Accept(ReadOnlySpan<byte> key)
        {
            if (!TryParseVersionKey(key, out var encodedUserKey, out var sequence))
            {
                return true;
            }

            if (_group.Set(encodedUserKey))
            {
                _nextRequired = 0;
            }

            var keep = false;
            while (_nextRequired < requiredSequences.Length && sequence <= requiredSequences[_nextRequired])
            {
                keep = true;
                _nextRequired++;
            }

            if (!keep)
            {
                ObsoleteKeys.Add(key.ToArray());
            }

            return true;
        }

        public void Dispose() => _group.Dispose();
    }

    private sealed class KeyGroup : IDisposable
    {
        private byte[]? _buffer;
        private int _length = -1;

        public bool Set(ReadOnlySpan<byte> key)
        {
            if (_length == key.Length && key.SequenceEqual(_buffer.AsSpan(0, _length)))
            {
                return false;
            }

            if (_buffer is null || _buffer.Length < key.Length)
            {
                if (_buffer is not null)
                {
                    ArrayPool<byte>.Shared.Return(_buffer);
                }

                _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(1, key.Length));
            }

            key.CopyTo(_buffer);
            _length = key.Length;
            return true;
        }

        public void Dispose()
        {
            if (_buffer is not null)
            {
                ArrayPool<byte>.Shared.Return(_buffer);
                _buffer = null;
            }
        }
    }
}

internal sealed class MvccMutation(byte[] key, byte[]? value)
{
    public byte[] Key { get; } = key;
    public byte[]? Value { get; set; } = value;
}
