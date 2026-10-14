using System.IO.Compression;

namespace Silex.Tool;

internal static class BulkTransfer
{
    internal static readonly TimeSpan Timeout = TimeSpan.FromMinutes(15);
    private static readonly byte[] EmptyGzipMember = [0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 255, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0];

    public static async Task<ImportResult> ImportAsync(DatabaseHandle database, Stream input, string format,
        CancellationToken cancellationToken)
    {
        long committed = 0;
        var batches = 0L;
        using var batch = database.CreateImportBatch();
        try
        {
            using var reader = await DumpReader.OpenAsync(input, format, cancellationToken);
            while (await reader.ReadAsync(cancellationToken) is { } entry)
            {
                if (batch.Count > 0 && batch.Bytes + entry.Key.Length + entry.Value.Length > DumpWriter.BlockTargetBytes)
                    await CommitAsync();
                batch.Put(entry.Key.Span, entry.Value.Span);
                if (batch.Count == 1024 || batch.Bytes >= DumpWriter.BlockTargetBytes)
                    await CommitAsync();
            }
            await CommitAsync();
            return new(committed, batches);
        }
        catch (Exception exception)
        {
            throw new BulkImportException(committed, batches, exception);
        }

        async ValueTask CommitAsync()
        {
            if (batch.Count == 0) return;
            var count = batch.Count;
            await batch.CommitAsync(cancellationToken);
            committed = checked(committed + count);
            batches++;
        }
    }

    public static async Task ExportFileAsync(string destination, bool gzip,
        Func<Stream, CancellationToken, Task> export, CancellationToken cancellationToken)
    {
        if (destination == "-")
        {
            await using var stdout = Console.OpenStandardOutput();
            await WriteExportAsync(stdout, gzip, export, cancellationToken);
            return;
        }
        destination = Path.GetFullPath(destination);
        if (File.Exists(destination) || Directory.Exists(destination) || new FileInfo(destination).LinkTarget is not null)
            throw new IOException("The export destination already exists. Choose a new path.");
        var temporary = destination + "." + Guid.NewGuid().ToString("N") + ".partial";
        try
        {
            await using (var output = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None,
                64 * 1024, FileOptions.Asynchronous | FileOptions.SequentialScan))
                await WriteExportAsync(output, gzip, export, cancellationToken);
            cancellationToken.ThrowIfCancellationRequested();
            File.Move(temporary, destination, overwrite: false);
        }
        finally
        {
            if (File.Exists(temporary)) File.Delete(temporary);
        }
    }

    public static async Task WriteExportAsync(Stream output, bool gzip,
        Func<Stream, CancellationToken, Task> export, CancellationToken cancellationToken)
    {
        if (gzip)
        {
            long bytes;
            await using (var compressed = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
            {
                var destination = new LeaveOpenWriteStream(compressed, flushEmpty: false);
                await ExportBufferedAsync(destination);
                bytes = destination.Bytes;
            }
            // GZipStream emits no complete member without input; encode the empty member explicitly.
            if (bytes == 0) await output.WriteAsync(EmptyGzipMember, cancellationToken);
        }
        else await ExportBufferedAsync(new LeaveOpenWriteStream(output));
        async Task ExportBufferedAsync(LeaveOpenWriteStream destination)
        {
            await using var buffered = new BufferedStream(destination, 64 * 1024);
            await export(buffered, cancellationToken);
            await buffered.FlushAsync(cancellationToken);
        }
    }

    private sealed class LeaveOpenWriteStream(Stream inner, bool flushEmpty = true) : Stream
    {
        public long Bytes { get; private set; }
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Flush() { if (flushEmpty || Bytes > 0) inner.Flush(); }
        public override Task FlushAsync(CancellationToken cancellationToken) =>
            flushEmpty || Bytes > 0 ? inner.FlushAsync(cancellationToken) : Task.CompletedTask;
        public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            inner.Write(buffer);
            Bytes = checked(Bytes + buffer.Length);
        }
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await inner.WriteAsync(buffer, cancellationToken);
            Bytes = checked(Bytes + buffer.Length);
        }
    }
}

internal sealed class BulkImportException(long committedRecords, long committedBatches, Exception inner) : IOException(
    $"Import failed after {committedRecords} confirmed records in {committedBatches} committed batches. " +
    (inner is StorageOperationException operation ? $"Current batch commit outcome: {operation.Outcome}. " : "") +
    inner.Message, inner)
{
    public long CommittedRecords { get; } = committedRecords;
}
