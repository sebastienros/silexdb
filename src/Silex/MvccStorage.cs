using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Numerics;
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
public sealed partial class MvccStorage : IAsyncDisposable
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
    private readonly LsmWriteBatch _commitBatch = new();
    private readonly SemaphoreSlim _garbageCollectionGate = new(1, 1);
    private readonly object _snapshotLock = new();
    private readonly Dictionary<long, SnapshotEntry> _activeSnapshots = [];
    private long _allocatedSequence;
    private long _publishedSequence;
    private int _disposeState;
    private readonly object _disposeLock = new();
    private Task? _disposeTask;
    private readonly CommitCoordinator? _commitCoordinator;
    private readonly TransactionLimits _transactionLimits;

    private MvccStorage(LsmStorage storage, long allocatedSequence, long publishedSequence, int groupSize,
        TransactionLimits transactionLimits)
    {
        _storage = storage;
        _allocatedSequence = allocatedSequence;
        _publishedSequence = publishedSequence;
        _openSequence = publishedSequence;
        _transactionLimits = transactionLimits;
        if (groupSize > 1)
        {
            _commitCoordinator = new CommitCoordinator(this, groupSize);
        }
    }

    /// <summary>
    /// Gets the sequence visible to new reads.
    /// </summary>
    public long PublishedSequence => Volatile.Read(ref _publishedSequence);

    public StorageHealth Health => Volatile.Read(ref _disposeState) != 0 ? StorageHealth.Disposed : _storage.Health;

    public Exception? Fault => _storage.Fault;

    /// <summary>
    /// Opens or creates an MVCC database.
    /// </summary>
    /// <remarks>
    /// An existing plain <see cref="LsmStorage"/> database cannot be opened as MVCC because its key format is
    /// different. Use a separate path or migrate its entries explicitly.
    /// </remarks>
    public static Task<MvccStorage> OpenAsync(string path, StorageOptions options, CancellationToken cancellationToken = default)
        => OpenCoreAsync(path, options, readOnly: false, cancellationToken);

    /// <summary>Opens an existing, validated MVCC database without modifying recovery files under exclusive ownership.</summary>
    public static Task<MvccStorage> OpenReadOnlyAsync(string path, StorageOptions options, CancellationToken cancellationToken = default)
        => OpenCoreAsync(path, options, readOnly: true, cancellationToken);

    /// <summary>Whether this facade was opened for read-only access.</summary>
    public bool IsReadOnly => _storage.IsReadOnly;

    private static async Task<MvccStorage> OpenCoreAsync(string path, StorageOptions options, bool readOnly,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(options);
        var transactionLimits = TransactionLimits.FromOptions(options);
        if (options.MvccCommitGroupSize is < 1 or > 64)
        {
            throw new ArgumentOutOfRangeException(nameof(options), "MvccCommitGroupSize must be between 1 and 64.");
        }
        if (options.MvccCommitGroupSize > 1 && (!options.UseWriteAheadLog || !options.SyncWriteAheadLogToDisk))
        {
            throw new ArgumentException("MVCC group commit requires WAL and SyncWriteAheadLogToDisk.", nameof(options));
        }

        var storage = readOnly
            ? await LsmStorage.OpenReadOnlyAsync(path, options, cancellationToken).ConfigureAwait(false)
            : await LsmStorage.OpenAsync(path, options, cancellationToken).ConfigureAwait(false);

        try
        {
            var format = new byte[_formatValue.Length];
            var formatLength = await storage.GetRawAsync(_formatKey, format, cancellationToken).ConfigureAwait(false);

            if (formatLength < 0)
            {
                if (readOnly)
                {
                    throw new InvalidDataException("The database has no MVCC format metadata.");
                }
                var entries = await storage.ScanRawAsync(
                    0,
                    static (_, _, _) => false,
                    maxEntries: 1,
                    cancellationToken).ConfigureAwait(false);

                if (entries != 0)
                {
                    throw new InvalidOperationException("The database contains plain keys and cannot be opened as an MVCC database.");
                }

                var initialization = new LsmWriteBatch();
                initialization.Put(_formatKey, _formatValue);
                WriteSequence(initialization, _allocatedSequenceKey, 0);
                WriteSequence(initialization, _publishedSequenceKey, 0);
                storage.Write(initialization);
                return new MvccStorage(storage, 0, 0, options.MvccCommitGroupSize, transactionLimits);
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

            return new MvccStorage(storage, allocated, published, options.MvccCommitGroupSize, transactionLimits);
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
            return new MvccTransaction(this, sequence, _transactionLimits);
        }
    }

    /// <summary>
    /// Atomically publishes a single value.
    /// </summary>
    public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        CheckDisposed();
        EnterCommitGate();

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
        EnterCommitGate();

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
    /// <remarks>
    /// The callback borrows the value synchronously and may run while its source is locked.
    /// It must not await, block, retain the span, or reenter storage.
    /// </remarks>
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
    /// Flushes and compacts the underlying LSM tree.
    /// </summary>
    public Task FlushAndCompactAsync(CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        return _storage.FlushAndCompactAsync(cancellationToken);
    }

    /// <summary>Synchronizes recoverable file contents through all previously completed commits.</summary>
    /// <remarks>
    /// Uses the same boundary, cancellation, WAL requirement, and failure contract as
    /// <see cref="LsmStorage.SynchronizeAsync"/>. Serializes with commit publication and shutdown.
    /// Await a successful commit before calling to include that commit; staged transactions are excluded.
    /// </remarks>
    public async Task SynchronizeAsync(CancellationToken cancellationToken = default)
    {
        CheckDisposed();
        await EnterCommitGateAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            CheckDisposed();
            await _storage.SynchronizeAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _commitGate.Release();
        }
    }

    /// <summary>
    /// Flushes pending data and closes the database.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        lock (_disposeLock)
        {
            if (_disposeTask is null)
            {
                Volatile.Write(ref _disposeState, 1);
                _disposeTask = DisposeInternalAsync();
            }
            return new ValueTask(_disposeTask);
        }
    }

    private async Task DisposeInternalAsync()
    {
        if (_commitCoordinator is not null)
        {
            await _commitCoordinator.StopAsync().ConfigureAwait(false);
        }
        // An in-flight collection observes the disposed state at its next chunk boundary and exits promptly.
        await _garbageCollectionGate.WaitAsync().ConfigureAwait(false);
        _garbageCollectionGate.Release();
        await _commitGate.WaitAsync().ConfigureAwait(false);

        try
        {
            await _storage.DisposeAsync().ConfigureAwait(false);
        }
        catch when (_storage.Health == StorageHealth.Faulted)
        {
            // Shutdown itself can fault the store; the facade cannot be disposed a second time.
            await _storage.DisposeAsync().ConfigureAwait(false);
            throw;
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

    internal async ValueTask<MvccConflict?> TryCommitAsync(
        long snapshotSequence,
        MvccWriteBuffer writes,
        CancellationToken cancellationToken)
    {
        CheckDisposed();
        if (_commitCoordinator is not null)
        {
            return await _commitCoordinator.CommitAsync(snapshotSequence, writes, cancellationToken).ConfigureAwait(false);
        }
        await EnterCommitGateAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            CheckDisposed();
            if (writes.HasVersionstamps)
            {
                writes.ResolveVersionstamps(AllocateSequence());
            }
            var conflict = await ValidateAsync(snapshotSequence, writes, cancellationToken).ConfigureAwait(false);
            if (conflict is not null)
            {
                return conflict;
            }

            if (writes.MutationCount == 0)
            {
                return null;
            }

            var sequence = AllocateSequence();
            var batch = _commitBatch;
            PrepareBatch(writes, batch, sequence);
            cancellationToken.ThrowIfCancellationRequested();
            _storage.Write(batch);
            _storage.BeforeMvccPublication();
            _allocatedSequence = sequence;
            Volatile.Write(ref _publishedSequence, sequence);
            writes.CommitSequence = sequence;
            return null;
        }
        finally
        {
            _commitGate.Release();
        }
    }

    private async ValueTask<MvccConflict?> ValidateAsync(long snapshotSequence, MvccWriteBuffer writes, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var published = Volatile.Read(ref _publishedSequence);
        if (published == snapshotSequence)
        {
            return null;
        }
        for (var i = 0; i < writes.MutationCount; i++)
        {
            var key = writes.GetMutationKey(i);
            var sequence = await FindVisibleSequenceAsync(key.Span, published, cancellationToken).ConfigureAwait(false);
            if (sequence > snapshotSequence)
            {
                return new MvccConflict(MvccConflictKind.WrittenKey, key.Span, snapshotSequence, sequence);
            }
        }
        for (var i = 0; i < writes.TrackedReadCount; i++)
        {
            var key = writes.GetTrackedRead(i);
            if (writes.FindMutation(key.Span) >= 0) continue;
            var sequence = await FindVisibleSequenceAsync(key.Span, published, cancellationToken).ConfigureAwait(false);
            if (sequence > snapshotSequence)
            {
                return new MvccConflict(MvccConflictKind.TrackedPointRead, key.Span, snapshotSequence, sequence);
            }
        }
        for (var i = 0; i < writes.ReadRangeCount; i++)
        {
            var conflict = await RangeModifiedAfterAsync(writes.GetReadRange(i), snapshotSequence, published, cancellationToken).ConfigureAwait(false);
            if (conflict is not null)
            {
                return conflict;
            }
        }
        return null;
    }

    // Every user key keeps its newest version through GC (the published sequence is always required), so the
    // newest physical version per key in the interval is exactly the key's latest commit, even after
    // intermediate versions were reclaimed. Tombstones are versions too, so deletions remain visible here.
    private async ValueTask<MvccConflict?> RangeModifiedAfterAsync(
        OrderedReadRange range, long snapshotSequence, long publishedSequence, CancellationToken cancellationToken)
    {
        using var lower = range.Lower is { } low ? EncodeVersionKey(low.Span, range.LowerInclusive ? long.MaxValue : 0) : null;
        using var upper = range.Upper is { } high ? EncodeVersionKey(high.Span, range.UpperInclusive ? 0 : long.MaxValue) : null;
        var physical = new OrderedReadRange(
            lower is null ? new byte[] { DataPrefix } : lower.Slice.Memory, true,
            upper is null ? new byte[] { 2 } : upper.Slice.Memory, upper is not null && range.UpperInclusive);
        using var state = new RangeConflictState(snapshotSequence, publishedSequence, range);
        await _storage.ScanOrderedAsync(physical, OrderedReadDirection.Forward, state,
            static (s, key, _) => s.Accept(key), cancellationToken).ConfigureAwait(false);
        return state.Conflict;
    }

    private sealed class RangeConflictState(long snapshot, long published, OrderedReadRange range) : IDisposable
    {
        private readonly KeyGroup _group = new();
        private bool _resolved;

        public MvccConflict? Conflict { get; private set; }

        public bool Accept(ReadOnlySpan<byte> key)
        {
            if (!TryParseVersionKey(key, out var userKey, out var sequence))
            {
                return true;
            }
            if (_group.Set(userKey))
            {
                _resolved = false;
            }
            if (_resolved || sequence > published)
            {
                return true;
            }
            _resolved = true;
            if (sequence > snapshot)
            {
                return InvokeUserReader((State: this, Sequence: sequence),
                    static (s, decodedKey, _) => s.State.SetConflict(decodedKey, s.Sequence), userKey, default);
            }
            return true;
        }

        private bool SetConflict(ReadOnlySpan<byte> key, long sequence)
        {
            Conflict = new MvccConflict(MvccConflictKind.TrackedRange, key, snapshot, sequence, range);
            return false;
        }

        public void Dispose() => _group.Dispose();
    }

    private static void PrepareBatch(MvccWriteBuffer writes, LsmWriteBatch batch, long sequence)
    {
        batch.Clear();
        WriteSequence(batch, _allocatedSequenceKey, sequence);
        for (var i = 0; i < writes.MutationCount; i++)
        {
            var mutation = writes.GetMutation(i);
            WriteVersion(batch, writes.GetKey(mutation).Span, writes.GetValue(mutation).Span, mutation.IsTombstone, sequence);
        }
        WriteSequence(batch, _publishedSequenceKey, sequence);
    }

    internal void ReleaseSnapshot(long sequence)
    {
        lock (_snapshotLock)
        {
            if (!_activeSnapshots.TryGetValue(sequence, out var entry))
            {
                return;
            }

            if (entry.Count == 1)
            {
                _activeSnapshots.Remove(sequence);
            }
            else
            {
                _activeSnapshots[sequence] = new SnapshotEntry(entry.Count - 1, entry.PinnedAt);
            }
        }
    }

    private void CommitSingle(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone)
    {
        var sequence = AllocateSequence();
        var batch = _commitBatch;
        batch.Clear();
        WriteSequence(batch, _allocatedSequenceKey, sequence);
        WriteVersion(batch, key, value, isTombstone, sequence);
        WriteSequence(batch, _publishedSequenceKey, sequence);
        _storage.Write(batch);
        _storage.BeforeMvccPublication();
        _allocatedSequence = sequence;
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

    private static void WriteVersion(
        LsmWriteBatch batch,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        bool isTombstone,
        long sequence)
    {
        var keyLength = GetVersionKeyLength(key);
        byte[]? rentedKey = null;
        Span<byte> internalKey = keyLength <= 256
            ? stackalloc byte[keyLength]
            : (rentedKey = ArrayPool<byte>.Shared.Rent(keyLength)).AsSpan(0, keyLength);
        EncodeVersionKey(key, sequence, internalKey);

        try
        {
            if (isTombstone)
            {
                Span<byte> stored = stackalloc byte[1];
                stored[0] = TombstoneValue;
                batch.Put(internalKey, stored);
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
                batch.Put(internalKey, encoded);
            }
            finally
            {
                if (rented is not null) ArrayPool<byte>.Shared.Return(rented);
            }
        }
        finally
        {
            if (rentedKey is not null) ArrayPool<byte>.Shared.Return(rentedKey);
        }
    }

    private ValueTask<long> FindVisibleSequenceAsync(ReadOnlySpan<byte> key, long sequence, CancellationToken cancellationToken)
    {
        return SequenceAsync(ReadPointAsync(key, sequence, default(NoOpSink), cancellationToken, 1));

        static async ValueTask<long> SequenceAsync(ValueTask<PointReadState<NoOpSink>> read) =>
            (await read.ConfigureAwait(false)).Sequence;
    }

    private ValueTask<int> ReadAtAsync<TSink>(
        ReadOnlySpan<byte> key,
        long sequence,
        TSink sink,
        CancellationToken cancellationToken)
        where TSink : struct, IValueSink
    {
        return LengthAsync(ReadPointAsync(key, sequence, sink, cancellationToken));

        static async ValueTask<int> LengthAsync(ValueTask<PointReadState<TSink>> read) =>
            (await read.ConfigureAwait(false)).Length;
    }

    private ValueTask<PointReadState<TSink>> ReadPointAsync<TSink>(
        ReadOnlySpan<byte> key, long sequence, TSink sink, CancellationToken cancellationToken,
        int valuePrefixLength = int.MaxValue)
        where TSink : struct, IValueSink
    {
        var length = GetVersionKeyLength(key);
        var encodedKey = ArrayPool<byte>.Shared.Rent(length);
        EncodeVersionKey(key, sequence, encodedKey.AsSpan(0, length));
        return ReadEncodedAsync(this, encodedKey, length, sequence, sink, cancellationToken, valuePrefixLength);

        static async ValueTask<PointReadState<TSink>> ReadEncodedAsync(
            MvccStorage storage, byte[] bytes, int count, long sequence, TSink valueSink, CancellationToken ct,
            int valuePrefixLength)
        {
            try
            {
                var memory = bytes.AsMemory(0, count);
                var state = new PointReadState<TSink>(memory[..^sizeof(long)], sequence, valueSink);
                return await storage._storage.ReadFirstRawAsync(
                    memory, memory[..^sizeof(long)], state, ct, valuePrefixLength).ConfigureAwait(false);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bytes);
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
        _activeSnapshots.TryGetValue(sequence, out var entry);
        _activeSnapshots[sequence] = entry.Count == 0
            ? new SnapshotEntry(1, System.Diagnostics.Stopwatch.GetTimestamp())
            : new SnapshotEntry(entry.Count + 1, entry.PinnedAt);
    }

    private static int GetVersionKeyLength(ReadOnlySpan<byte> userKey)
    {
        var zeroCount = 0;
        for (var i = 0; i < userKey.Length; i++)
        {
            zeroCount += userKey[i] == 0 ? 1 : 0;
        }

        return checked(1 + userKey.Length + zeroCount + 2 + sizeof(long));
    }

    private static OwnedByteSlice EncodeVersionKey(ReadOnlySpan<byte> userKey, long sequence)
    {
        var length = GetVersionKeyLength(userKey);
        var owner = MemoryOwner<byte>.Rent(length);

        try
        {
            EncodeVersionKey(userKey, sequence, owner.Span);
            return OwnedByteSlice.TakeOwnership(owner, length);
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    private static void EncodeVersionKey(ReadOnlySpan<byte> userKey, long sequence, Span<byte> destination)
    {
        var offset = 0;
        destination[offset++] = DataPrefix;
        foreach (var value in userKey)
        {
            destination[offset++] = value;
            if (value == 0)
            {
                destination[offset++] = byte.MaxValue;
            }
        }
        destination[offset++] = 0;
        destination[offset++] = 0;
        BinaryPrimitives.WriteUInt64BigEndian(destination[offset..], ~(ulong)sequence);
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

    private static void WriteSequence(LsmWriteBatch batch, ReadOnlySpan<byte> key, long sequence)
    {
        Span<byte> value = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(value, sequence);
        batch.Put(key, value);
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
        _storage.CheckHealthy();
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

    private struct PointReadState<TSink>(ReadOnlyMemory<byte> userPrefix, long snapshotSequence, TSink sink)
        : IRawEntrySink where TSink : struct, IValueSink
    {
        private readonly ReadOnlyMemory<byte> _userPrefix = userPrefix;
        private readonly long _snapshotSequence = snapshotSequence;
        private TSink _sink = sink;

        public int Length { get; private set; } = -1;
        public long Sequence { get; private set; } = -1;

        public void Accept(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            if (!TryParseVersionKey(key, out _, out var sequence))
            {
                return;
            }

            var candidatePrefix = key[..^sizeof(long)];
            if (!candidatePrefix.SequenceEqual(_userPrefix.Span) || sequence > _snapshotSequence)
            {
                return;
            }

            ValidateStoredValue(value);
            Sequence = sequence;

            if (value[0] == TombstoneValue)
            {
                return;
            }

            var userValue = value[1..];
            _sink.Accept(userValue);
            Length = userValue.Length;
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

internal sealed class MvccWriteBuffer : IDisposable
{
    private const int InitialByteCapacity = 256;
    private const int InitialDescriptorCapacity = 8;
    internal const int DefaultIndexThreshold = 8;
    private readonly int _indexThreshold;
    private readonly TransactionLimits _limits;
    private MvccIndexSlot[] _mutationIndex = [];
    private MvccIndexSlot[] _readIndex = [];
    private int[] _order = [];
    private Comparison<int>? _comparison;
    private bool _orderDirty = true;

    private byte[] _bytes = [];
    private MvccMutation[] _mutations = [];
    private MvccKey[] _trackedReads = [];
    private MvccRange[] _ranges = [];
    private MvccCheckpoint[] _savepoints = [];
    private MvccUndo[] _undo = [];
    private int _savepointCount;
    private int _undoCount;
    private int _nextSavepointId;
    private int _byteCount;
    private bool _disposed;
    private bool _hasVersionstamps;

    public int MutationCount { get; private set; }

    public int TrackedReadCount { get; private set; }

    public int ReadRangeCount { get; private set; }

    public long BufferedBytes => (long)_byteCount + 28L * MutationCount + 8L * TrackedReadCount
        + 32L * ReadRangeCount + 32L * _undoCount + 24L * _savepointCount;

    public long? CommitSequence { get; set; }

    public bool HasVersionstamps => _hasVersionstamps && MutationCount != 0;

    internal MvccWriteBuffer(int indexThreshold = DefaultIndexThreshold, TransactionLimits? limits = null)
    {
        _indexThreshold = indexThreshold;
        _limits = limits ?? TransactionLimits.Unlimited;
    }

    public MvccSavepoint CreateSavepoint()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        CheckCount("savepoints", _limits.Savepoints, (long)_savepointCount + 1);
        CheckBytes(24);
        var id = checked(_nextSavepointId + 1);
        if (_savepointCount == _savepoints.Length)
        {
            var replacement = ArrayPool<MvccCheckpoint>.Shared.Rent(
                Math.Max(InitialDescriptorCapacity, checked(_savepoints.Length * 2)));
            _savepoints.AsSpan(0, _savepointCount).CopyTo(replacement);
            if (_savepoints.Length != 0) ArrayPool<MvccCheckpoint>.Shared.Return(_savepoints);
            _savepoints = replacement;
        }
        _savepoints[_savepointCount++] = new MvccCheckpoint(
            id, MutationCount, TrackedReadCount, _byteCount, _undoCount, ReadRangeCount);
        _nextSavepointId = id;
        return new MvccSavepoint(this, id);
    }

    public void RollbackToSavepoint(MvccSavepoint savepoint)
    {
        var index = FindSavepoint(savepoint);
        var checkpoint = _savepoints[index];
        while (_undoCount > checkpoint.UndoCount)
        {
            var undo = _undo[--_undoCount];
            _mutations[undo.Index] = undo.Mutation;
        }
        var mutationsRemoved = MutationCount != checkpoint.MutationCount;
        var readsRemoved = TrackedReadCount != checkpoint.TrackedReadCount;
        _orderDirty |= mutationsRemoved;
        MutationCount = checkpoint.MutationCount;
        TrackedReadCount = checkpoint.TrackedReadCount;
        ReadRangeCount = checkpoint.RangeCount;
        _byteCount = checkpoint.ByteCount;
        _savepointCount = index + 1;
        if (mutationsRemoved) RebuildIndex(_mutationIndex, reads: false);
        if (readsRemoved) RebuildIndex(_readIndex, reads: true);
    }

    public void ReleaseSavepoint(MvccSavepoint savepoint)
    {
        _savepointCount = FindSavepoint(savepoint);
        if (_savepointCount == 0) _undoCount = 0;
    }

    private int FindSavepoint(MvccSavepoint savepoint)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (ReferenceEquals(savepoint.Owner, this))
        {
            for (var i = _savepointCount - 1; i >= 0; i--)
            {
                if (_savepoints[i].Id == savepoint.Id) return i;
            }
        }
        throw new ArgumentException("The savepoint is not active in this transaction.", nameof(savepoint));
    }

    public int FindMutation(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_mutationIndex.Length != 0) return FindIndexed(key, _mutationIndex, reads: false);
        for (var i = MutationCount - 1; i >= 0; i--)
        {
            if (key.SequenceEqual(GetKey(_mutations[i]).Span))
            {
                return i;
            }
        }

        return -1;
    }

    public void AddMutation(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone,
        int keyVersionstampOffset = -1, int valueVersionstampOffset = -1)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        CheckCount("mutations", _limits.Mutations, (long)MutationCount + 1);
        var additional = (long)key.Length + (isTombstone ? 0 : value.Length);
        CheckBytes(additional + 28);
        EnsureByteCapacity(additional);
        EnsureMutationCapacity();
        EnsureIndexCapacity(ref _mutationIndex, MutationCount, reads: false);
        var keyOffset = Append(key);
        var valueOffset = isTombstone ? 0 : Append(value);
        _mutations[MutationCount++] = new MvccMutation(
            keyOffset,
            key.Length,
            valueOffset,
            isTombstone ? -1 : value.Length,
            isTombstone ? 0 : value.Length, keyVersionstampOffset, valueVersionstampOffset);
        _hasVersionstamps |= keyVersionstampOffset >= 0 || valueVersionstampOffset >= 0;
        if (_mutationIndex.Length != 0) InsertIndex(_mutationIndex, MutationCount - 1, key);
        _orderDirty = true;
    }

    public void SetValue(int index, ReadOnlySpan<byte> value, bool isTombstone,
        int keyVersionstampOffset = -1, int valueVersionstampOffset = -1)
    {
        SetValueCore(index, value, isTombstone);
        ref var mutation = ref _mutations[index];
        mutation.KeyVersionstampOffset = keyVersionstampOffset;
        mutation.ValueVersionstampOffset = valueVersionstampOffset;
        _hasVersionstamps |= keyVersionstampOffset >= 0 || valueVersionstampOffset >= 0;
    }

    private void SetValueCore(int index, ReadOnlySpan<byte> value, bool isTombstone)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentOutOfRangeException.ThrowIfNegative(index);
        if (index >= MutationCount)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }

        ref var mutation = ref _mutations[index];
        if (_savepointCount != 0)
        {
            CheckBytes(32L + (isTombstone ? 0 : value.Length));
            if (_undoCount == _undo.Length)
            {
                var replacement = ArrayPool<MvccUndo>.Shared.Rent(
                    Math.Max(InitialDescriptorCapacity, checked(_undo.Length * 2)));
                _undo.AsSpan(0, _undoCount).CopyTo(replacement);
                if (_undo.Length != 0) ArrayPool<MvccUndo>.Shared.Return(_undo);
                _undo = replacement;
            }

            // Preserve old bytes as well as the descriptor; rollback can then rewind without copying.
            var offset = isTombstone ? 0 : Append(value);
            _undo[_undoCount++] = new MvccUndo(index, mutation);
            mutation.ValueOffset = offset;
            mutation.ValueLength = isTombstone ? -1 : value.Length;
            mutation.ValueCapacity = isTombstone ? 0 : value.Length;
            return;
        }

        if (isTombstone)
        {
            mutation.ValueLength = -1;
            return;
        }

        if (value.Length <= mutation.ValueCapacity)
        {
            value.CopyTo(_bytes.AsSpan(mutation.ValueOffset));
            mutation.ValueLength = value.Length;
            return;
        }

        if (mutation.ValueOffset + mutation.ValueCapacity == _byteCount)
        {
            CheckBytes(value.Length - mutation.ValueCapacity);
            EnsureByteCapacity(value.Length - mutation.ValueCapacity);
            value.CopyTo(_bytes.AsSpan(mutation.ValueOffset));
            _byteCount = mutation.ValueOffset + value.Length;
            mutation.ValueLength = value.Length;
            mutation.ValueCapacity = value.Length;
            return;
        }

        CheckBytes(value.Length);
        mutation.ValueOffset = Append(value);
        mutation.ValueLength = value.Length;
        mutation.ValueCapacity = value.Length;
    }

    public void ResolveVersionstamps(long sequence)
    {
        if (!_hasVersionstamps) return;
        var keysChanged = false;
        for (var i = 0; i < MutationCount; i++)
        {
            var mutation = _mutations[i];
            if (mutation.KeyVersionstampOffset >= 0)
            {
                BinaryPrimitives.WriteInt64BigEndian(
                    _bytes.AsSpan(mutation.KeyOffset + mutation.KeyVersionstampOffset, sizeof(long)), sequence);
                keysChanged = true;
            }
            if (mutation.ValueVersionstampOffset >= 0)
            {
                BinaryPrimitives.WriteInt64BigEndian(
                    _bytes.AsSpan(mutation.ValueOffset + mutation.ValueVersionstampOffset, sizeof(long)), sequence);
            }
        }
        if (!keysChanged) return;
        _orderDirty = true;
        RebuildIndex(_mutationIndex, reads: false);
        for (var i = 0; i < MutationCount; i++)
        {
            if (FindMutation(GetMutationKey(i).Span) != i)
            {
                throw new ArgumentException("Versionstamp substitution produced duplicate transaction keys.");
            }
        }
    }

    public void TrackRead(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (FindTrackedRead(key) >= 0) return;
        CheckCount("tracked reads", _limits.TrackedReads, (long)TrackedReadCount + 1);
        CheckBytes((long)key.Length + 8);
        EnsureByteCapacity(key.Length);
        EnsureTrackedReadCapacity();
        EnsureIndexCapacity(ref _readIndex, TrackedReadCount, reads: true);
        _trackedReads[TrackedReadCount++] = new MvccKey(Append(key), key.Length);
        if (_readIndex.Length != 0) InsertIndex(_readIndex, TrackedReadCount - 1, key);
    }

    public int FindTrackedRead(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_readIndex.Length != 0) return FindIndexed(key, _readIndex, reads: true);
        for (var i = 0; i < TrackedReadCount; i++)
        {
            if (key.SequenceEqual(GetTrackedRead(i).Span))
            {
                return i;
            }
        }

        return -1;
    }

    public void TrackReadRange(OrderedReadRange range)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (range.IsEmpty) return;
        var lower = range.Lower is { } l ? l.Span : default;
        var upper = range.Upper is { } u ? u.Span : default;
        var flags = (range.LowerInclusive ? 1 : 0) | (range.UpperInclusive ? 2 : 0);
        var candidate = new MvccRange(0, range.Lower.HasValue ? lower.Length : -1, 0,
            range.Upper.HasValue ? upper.Length : -1, flags);
        // Registering inside the previous interval adds no information; later rollback removes both or keeps the cover.
        if (ReadRangeCount != 0 && Covers(_ranges[ReadRangeCount - 1], range)) return;
        CheckCount("tracked read ranges", _limits.ReadRanges, (long)ReadRangeCount + 1);
        CheckBytes(32L + lower.Length + upper.Length);
        EnsureByteCapacity((long)lower.Length + upper.Length);
        EnsureRangeCapacity();
        candidate.LowerOffset = Append(lower);
        candidate.UpperOffset = Append(upper);
        _ranges[ReadRangeCount++] = candidate;
    }

    public int FindReadRange(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        for (var i = 0; i < ReadRangeCount; i++)
        {
            var range = _ranges[i];
            if (range.LowerLength >= 0)
            {
                var c = key.SequenceCompareTo(_bytes.AsSpan(range.LowerOffset, range.LowerLength));
                if (c < 0 || (c == 0 && (range.Flags & 1) == 0)) continue;
            }
            if (range.UpperLength >= 0)
            {
                var c = key.SequenceCompareTo(_bytes.AsSpan(range.UpperOffset, range.UpperLength));
                if (c > 0 || (c == 0 && (range.Flags & 2) == 0)) continue;
            }
            return i;
        }
        return -1;
    }

    public OrderedReadRange GetReadRange(int index)
    {
        var range = _ranges[index];
        ReadOnlyMemory<byte>? lower = null, upper = null;
        if (range.LowerLength >= 0) lower = _bytes.AsMemory(range.LowerOffset, range.LowerLength);
        if (range.UpperLength >= 0) upper = _bytes.AsMemory(range.UpperOffset, range.UpperLength);
        return new OrderedReadRange(lower, (range.Flags & 1) != 0, upper, (range.Flags & 2) != 0);
    }

    private bool Covers(MvccRange outer, OrderedReadRange inner)
    {
        if (outer.LowerLength >= 0)
        {
            if (inner.Lower is not { } low) return false;
            var c = low.Span.SequenceCompareTo(_bytes.AsSpan(outer.LowerOffset, outer.LowerLength));
            if (c < 0 || (c == 0 && (outer.Flags & 1) == 0 && inner.LowerInclusive)) return false;
        }
        if (outer.UpperLength >= 0)
        {
            if (inner.Upper is not { } high) return false;
            var c = high.Span.SequenceCompareTo(_bytes.AsSpan(outer.UpperOffset, outer.UpperLength));
            if (c > 0 || (c == 0 && (outer.Flags & 2) == 0 && inner.UpperInclusive)) return false;
        }
        return true;
    }

    public ReadOnlySpan<int> OrderedMutations()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_order.Length < MutationCount)
        {
            var replacement = ArrayPool<int>.Shared.Rent(MutationCount);
            if (_order.Length != 0) ArrayPool<int>.Shared.Return(_order);
            _order = replacement;
        }
        if (_orderDirty)
        {
            for (var i = 0; i < MutationCount; i++) _order[i] = i;
            _order.AsSpan(0, MutationCount).Sort(_comparison ??= Compare);
            _orderDirty = false;
        }
        return _order.AsSpan(0, MutationCount);
    }

    private int Compare(int x, int y) => GetMutationKey(x).Span.SequenceCompareTo(GetMutationKey(y).Span);

    private void CheckBytes(long additional)
    {
        CheckCount("bytes", _limits.Bytes, BufferedBytes + additional);
    }

    private static void CheckCount(string resource, long limit, long requested)
    {
        if (requested > limit) throw new TransactionLimitException(resource, limit, requested);
    }

    private int FindIndexed(ReadOnlySpan<byte> key, MvccIndexSlot[] slots, bool reads)
    {
        var hash = XxHash3.HashToUInt64(key);
        var bucket = (int)(hash & (uint)(slots.Length - 1));
        while (slots[bucket].Position != 0)
        {
            var slot = slots[bucket];
            var position = slot.Position - 1;
            if (slot.Hash == hash && key.SequenceEqual(
                reads ? GetTrackedRead(position).Span : GetMutationKey(position).Span))
                return position;
            bucket = (bucket + 1) & (slots.Length - 1);
        }
        return -1;
    }

    private static void InsertIndex(MvccIndexSlot[] slots, int position, ReadOnlySpan<byte> key)
    {
        var hash = XxHash3.HashToUInt64(key);
        var bucket = (int)(hash & (uint)(slots.Length - 1));
        while (slots[bucket].Position != 0) bucket = (bucket + 1) & (slots.Length - 1);
        slots[bucket] = new MvccIndexSlot(hash, position + 1);
    }

    private void EnsureIndexCapacity(ref MvccIndexSlot[] slots, int count, bool reads)
    {
        if (count < _indexThreshold && slots.Length == 0) return;
        if (slots.Length != 0 && count < slots.Length / 2) return;
        var required = slots.Length == 0 ? ((long)count + 1) * 2 : (long)slots.Length * 2;
        CheckCount("index slots", 1 << 30, required);
        var replacement = ArrayPool<MvccIndexSlot>.Shared.Rent(
            (int)BitOperations.RoundUpToPowerOf2((uint)required));
        RebuildIndex(replacement, reads);
        if (slots.Length != 0) ArrayPool<MvccIndexSlot>.Shared.Return(slots);
        slots = replacement;
    }

    private void RebuildIndex(MvccIndexSlot[] slots, bool reads)
    {
        if (slots.Length == 0) return;
        Array.Clear(slots);
        var count = reads ? TrackedReadCount : MutationCount;
        for (var i = 0; i < count; i++)
            InsertIndex(slots, i, reads ? GetTrackedRead(i).Span : GetMutationKey(i).Span);
    }

    public MvccMutation GetMutation(int index) => _mutations[index];

    public ReadOnlyMemory<byte> GetMutationKey(int index) => GetKey(_mutations[index]);

    public ReadOnlyMemory<byte> GetTrackedRead(int index)
    {
        var key = _trackedReads[index];
        return _bytes.AsMemory(key.Offset, key.Length);
    }

    public ReadOnlyMemory<byte> GetKey(MvccMutation mutation) =>
        _bytes.AsMemory(mutation.KeyOffset, mutation.KeyLength);

    public ReadOnlyMemory<byte> GetValue(MvccMutation mutation) =>
        mutation.IsTombstone
            ? ReadOnlyMemory<byte>.Empty
            : _bytes.AsMemory(mutation.ValueOffset, mutation.ValueLength);

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_mutationIndex.Length != 0) ArrayPool<MvccIndexSlot>.Shared.Return(_mutationIndex);
        if (_readIndex.Length != 0) ArrayPool<MvccIndexSlot>.Shared.Return(_readIndex);
        if (_order.Length != 0) ArrayPool<int>.Shared.Return(_order);
        _mutationIndex = [];
        _readIndex = [];
        _order = [];
        if (_savepoints.Length != 0)
        {
            ArrayPool<MvccCheckpoint>.Shared.Return(_savepoints);
            _savepoints = [];
        }
        if (_undo.Length != 0)
        {
            ArrayPool<MvccUndo>.Shared.Return(_undo);
            _undo = [];
        }
        if (_bytes.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(_bytes);
            _bytes = [];
        }

        if (_mutations.Length != 0)
        {
            ArrayPool<MvccMutation>.Shared.Return(_mutations);
            _mutations = [];
        }

        if (_trackedReads.Length != 0)
        {
            ArrayPool<MvccKey>.Shared.Return(_trackedReads);
            _trackedReads = [];
        }

        if (_ranges.Length != 0)
        {
            ArrayPool<MvccRange>.Shared.Return(_ranges);
            _ranges = [];
        }
    }

    private void EnsureRangeCapacity()
    {
        if (ReadRangeCount < _ranges.Length) return;
        var replacement = ArrayPool<MvccRange>.Shared.Rent(Math.Max(InitialDescriptorCapacity, _ranges.Length * 2));
        _ranges.AsSpan(0, ReadRangeCount).CopyTo(replacement);
        if (_ranges.Length != 0) ArrayPool<MvccRange>.Shared.Return(_ranges);
        _ranges = replacement;
    }

    private int Append(ReadOnlySpan<byte> value)
    {
        var offset = _byteCount;
        if (value.IsEmpty)
        {
            return offset;
        }

        EnsureByteCapacity(value.Length);
        value.CopyTo(_bytes.AsSpan(offset));
        _byteCount += value.Length;
        return offset;
    }

    private void EnsureByteCapacity(long additionalLength)
    {
        CheckCount("arena bytes", Array.MaxLength, _byteCount + additionalLength);
        var required = checked((int)(_byteCount + additionalLength));
        if (required <= _bytes.Length)
        {
            return;
        }

        var newSize = Math.Max(InitialByteCapacity, _bytes.Length);
        while (newSize < required)
        {
            newSize = (int)Math.Min(Array.MaxLength, (long)newSize * 2);
        }

        var replacement = ArrayPool<byte>.Shared.Rent(newSize);
        _bytes.AsSpan(0, _byteCount).CopyTo(replacement);
        if (_bytes.Length != 0)
        {
            ArrayPool<byte>.Shared.Return(_bytes);
        }

        _bytes = replacement;
    }

    private void EnsureMutationCapacity()
    {
        if (MutationCount < _mutations.Length)
        {
            return;
        }

        var replacement = ArrayPool<MvccMutation>.Shared.Rent(
            Math.Max(InitialDescriptorCapacity, _mutations.Length * 2));
        _mutations.AsSpan(0, MutationCount).CopyTo(replacement);
        if (_mutations.Length != 0)
        {
            ArrayPool<MvccMutation>.Shared.Return(_mutations);
        }

        _mutations = replacement;
    }

    private void EnsureTrackedReadCapacity()
    {
        if (TrackedReadCount < _trackedReads.Length)
        {
            return;
        }

        var replacement = ArrayPool<MvccKey>.Shared.Rent(
            Math.Max(InitialDescriptorCapacity, _trackedReads.Length * 2));
        _trackedReads.AsSpan(0, TrackedReadCount).CopyTo(replacement);
        if (_trackedReads.Length != 0)
        {
            ArrayPool<MvccKey>.Shared.Return(_trackedReads);
        }

        _trackedReads = replacement;
    }
}

internal struct MvccMutation(int keyOffset, int keyLength, int valueOffset, int valueLength, int valueCapacity,
    int keyVersionstampOffset = -1, int valueVersionstampOffset = -1)
{
    public int KeyOffset { get; } = keyOffset;
    public int KeyLength { get; } = keyLength;
    public int ValueOffset { get; set; } = valueOffset;
    public int ValueLength { get; set; } = valueLength;
    public int ValueCapacity { get; set; } = valueCapacity;
    public int KeyVersionstampOffset { get; set; } = keyVersionstampOffset;
    public int ValueVersionstampOffset { get; set; } = valueVersionstampOffset;
    public readonly bool IsTombstone => ValueLength < 0;
}

internal readonly record struct MvccKey(int Offset, int Length);

internal struct MvccRange(int lowerOffset, int lowerLength, int upperOffset, int upperLength, int flags)
{
    public int LowerOffset = lowerOffset;
    public int LowerLength = lowerLength;
    public int UpperOffset = upperOffset;
    public int UpperLength = upperLength;
    public int Flags = flags;
}

internal readonly record struct MvccCheckpoint(
    int Id, int MutationCount, int TrackedReadCount, int ByteCount, int UndoCount, int RangeCount);

internal readonly record struct MvccUndo(int Index, MvccMutation Mutation);

internal readonly record struct MvccIndexSlot(ulong Hash, int Position);
