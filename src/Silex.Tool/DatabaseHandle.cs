using System.Buffers;
using System.Runtime.CompilerServices;

namespace Silex.Tool;

internal sealed class DatabaseHandle : IAsyncDisposable
{
    private static readonly byte[] FormatKey = [0, 1];
    private readonly LsmStorage? _lsm;
    private readonly MvccStorage? _mvcc;
    public string Path { get; }
    public string Mode { get; }

    private DatabaseHandle(string path, string mode, LsmStorage? lsm, MvccStorage? mvcc)
    {
        Path = path;
        Mode = mode;
        _lsm = lsm;
        _mvcc = mvcc;
    }

    public static void ValidateMode(string mode)
    {
        if (mode is not ("lsm" or "mvcc"))
            throw new ArgumentException("Mode must be lsm or mvcc.");
    }

    public static async Task<DatabaseHandle> OpenAsync(string path, string mode, bool readOnly,
        bool create, bool wal, CancellationToken cancellationToken)
    {
        ValidateMode(mode);
        path = System.IO.Path.GetFullPath(path);
        if (!create && (!Directory.Exists(path) ||
            !Directory.EnumerateFiles(path).Any(file => System.IO.Path.GetFileName(file) is "manifest" ||
                file.EndsWith(".sst", StringComparison.Ordinal) || file.EndsWith(".wal", StringComparison.Ordinal))))
            throw new DirectoryNotFoundException($"No database exists at '{path}'. Use init or serve --create explicitly.");
        if (create && Directory.Exists(path) && Directory.EnumerateFileSystemEntries(path)
            .Any(file => System.IO.Path.GetFileName(file) != "LOCK"))
            throw new IOException("Creation requires an empty database directory.");
        var options = new StorageOptions { UseWriteAheadLog = wal, SyncWriteAheadLogToDisk = wal };
        if (mode == "mvcc")
        {
            var mvcc = readOnly
                ? await MvccStorage.OpenReadOnlyAsync(path, options, cancellationToken)
                : await MvccStorage.OpenAsync(path, options, cancellationToken);
            return new DatabaseHandle(path, mode, null, mvcc);
        }
        var lsm = readOnly
            ? await LsmStorage.OpenReadOnlyAsync(path, options, cancellationToken)
            : await LsmStorage.OpenAsync(path, options, cancellationToken);
        try
        {
            var marker = new StrongBox<bool>();
            await lsm.TryReadRawAsync(FormatKey, marker,
                static (result, value) => result.Value = value.StartsWith("Silex.MVCC."u8), cancellationToken);
            if (marker.Value)
                throw new InvalidOperationException("This is an MVCC database. Specify --mode mvcc.");
            return new DatabaseHandle(path, mode, lsm, null);
        }
        catch
        {
            await lsm.DisposeAsync();
            throw;
        }
    }

    public ValueTask<bool> ReadAsync(byte[] key, IBufferWriter<byte> writer, CancellationToken cancellationToken)
    {
        ValidateKey(key);
        return _mvcc is null
            ? _lsm!.TryGetRawAsync(key, writer, cancellationToken)
            : _mvcc.TryReadRawAsync(key, writer, static (destination, value) =>
            {
                value.CopyTo(destination.GetSpan(value.Length));
                destination.Advance(value.Length);
            }, cancellationToken);
    }

    public void Put(byte[] key, byte[] value)
    {
        ValidateKey(key);
        if ((long)key.Length + value.Length > ServerHost.MaxPayloadBytes)
            throw new ArgumentException("Key plus value exceeds 16 MiB.");
        if (_mvcc is null) _lsm!.Put(key, value);
        else _mvcc.Put(key, value);
    }

    public void Delete(byte[] key)
    {
        ValidateKey(key);
        if (_mvcc is null) _lsm!.Delete(key);
        else _mvcc.Delete(key);
    }

    public static void ValidateKey(byte[] key)
    {
        if (key is null || key.Length == 0)
            throw new ArgumentException("A nonempty key is required.");
        if (key.Length > ServerHost.MaxPayloadBytes)
            throw new ArgumentException("Key exceeds 16 MiB.");
    }

    public OrderedReadCursor Cursor(ScanRequest request)
    {
        if (request.Limit is < 1 or > 1_000_000)
            throw new ArgumentException("Scan limit must be between 1 and 1000000.");
        if (request.Prefix is not null && (request.From is not null || request.To is not null))
            throw new ArgumentException("Use either a prefix or range bounds, not both.");
        byte[]? lower = request.From, upper = request.To;
        if (request.Prefix is { Length: > 0 } prefix)
        {
            lower = prefix;
            upper = prefix.ToArray();
            var last = upper.Length - 1;
            while (last >= 0 && upper[last] == byte.MaxValue) last--;
            if (last < 0) upper = null;
            else
            {
                upper[last]++;
                Array.Resize(ref upper, last + 1);
            }
        }
        var range = new OrderedReadRange(lower is null ? (ReadOnlyMemory<byte>?)null : lower.AsMemory(),
            upper: upper is null ? (ReadOnlyMemory<byte>?)null : upper.AsMemory(), upperInclusive: false);
        var direction = request.Reverse ? OrderedReadDirection.Reverse : OrderedReadDirection.Forward;
        return _mvcc is null ? _lsm!.CreateOrderedReadCursor(range, direction) : _mvcc.CreateOrderedReadCursor(range, direction);
    }

    public Task ScanAsync(ScanRequest request, Stream output, CancellationToken cancellationToken) =>
        WriteEntriesAsync(request, output, "jsonl", request.Limit, cancellationToken);

    public Task<long> ExportAsync(ExportRequest request, Stream output, CancellationToken cancellationToken) =>
        WriteEntriesAsync(new ScanRequest(request.Prefix, request.From, request.To, Reverse: request.Reverse),
            output, request.Format, long.MaxValue, cancellationToken);

    private async Task<long> WriteEntriesAsync(ScanRequest request, Stream output, string format,
        long limit, CancellationToken cancellationToken)
    {
        using var cursor = Cursor(request);
        using var writer = new DumpWriter(output, format);
        await writer.StartAsync(cancellationToken);
        var remaining = limit;
        while (remaining > 0)
        {
            using var batch = await cursor.ReadBatchAsync((int)Math.Min(remaining, 128), ServerHost.MaxPayloadBytes, cancellationToken);
            for (var i = 0; i < batch.Count; i++)
            {
                var entry = batch[i];
                await writer.WriteAsync(entry.Key, entry.Value, cancellationToken);
            }
            remaining -= batch.Count;
            if (batch.IsExhausted) break;
        }
        await writer.CompleteAsync(cancellationToken);
        return writer.Records;
    }

    public ImportBatch CreateImportBatch() => new(_lsm, _mvcc);

    internal sealed class ImportBatch(LsmStorage? lsm, MvccStorage? mvcc) : IDisposable
    {
        private readonly LsmWriteBatch? _batch = lsm is null ? null : new();
        private MvccTransaction? _transaction;
        public int Count { get; private set; }
        public int Bytes { get; private set; }

        public void Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
        {
            DumpWriter.ValidateLengths(key.Length, value.Length);
            if (mvcc is null) _batch!.Put(key, value);
            else (_transaction ??= mvcc.BeginTransaction()).Put(key, value);
            Count++;
            Bytes = checked(Bytes + key.Length + value.Length);
        }

        public async ValueTask CommitAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (mvcc is null) lsm!.Write(_batch!);
            else await _transaction!.CommitAsync(cancellationToken);
            _batch?.Clear();
            _transaction?.Dispose();
            _transaction = null;
            Count = 0;
            Bytes = 0;
        }

        public void Dispose() => _transaction?.Dispose();
    }

    public async Task<DatabaseStatistics> StatisticsAsync(CancellationToken cancellationToken)
    {
        var mvcc = _mvcc is null ? null : await _mvcc.GetStatisticsAsync(cancellationToken);
        var stats = mvcc?.Engine ?? await _lsm!.GetStatisticsAsync(cancellationToken);
        return new(Path, Mode, stats.Health.ToString(), stats.Fault?.Message, stats.Pressure.ToString(),
            stats.Levels.ToArray(), stats.ActiveMemTableBytes, stats.ImmutableMemTableCount,
            stats.ImmutableMemTableBytes, stats.WriteOperations, stats.UserBytesWritten, stats.Wal.BytesWritten,
            stats.Cache.Hits, stats.Cache.Misses, stats.Cache.HitRatio, stats.WriteAmplification,
            stats.CompactionBacklogBytes, _mvcc?.PublishedSequence);
    }

    public async Task<DatabaseInfo> InfoAsync(CancellationToken cancellationToken)
    {
        var bytes = 0L;
        var sst = 0;
        var wal = 0;
        foreach (var file in Directory.EnumerateFiles(Path))
        {
            bytes += new FileInfo(file).Length;
            if (file.EndsWith(".sst", StringComparison.Ordinal)) sst++;
            if (file.EndsWith(".wal", StringComparison.Ordinal)) wal++;
        }
        return new(await StatisticsAsync(cancellationToken), bytes, sst, wal);
    }

    public Task BackupAsync(string destination, CancellationToken cancellationToken) => _mvcc is null
        ? _lsm!.CreateBackupAsync(destination, cancellationToken) : _mvcc.CreateBackupAsync(destination, cancellationToken);

    public Task<StorageTableStatistics[]> TablesAsync(CancellationToken cancellationToken) => _mvcc is null
        ? _lsm!.GetTablesAsync(cancellationToken) : _mvcc.GetTablesAsync(cancellationToken);

    public Task CompactAsync(CancellationToken cancellationToken) => _mvcc is null
        ? _lsm!.FlushAndCompactAsync(cancellationToken) : _mvcc.FlushAndCompactAsync(cancellationToken);

    public ValueTask DisposeAsync() => _mvcc is null ? _lsm!.DisposeAsync() : _mvcc.DisposeAsync();
}
