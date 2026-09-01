using System.Buffers.Binary;
using Silex.Buffers;
using Silex.MemTables;
using Silex.Serialization;

namespace Silex.Wal;

/// <summary>
/// Append-only write-ahead log for a single <see cref="MemTable"/>. Every mutation is
/// journaled before it is applied in memory so that the unflushed contents of a memtable can be
/// recovered after a process crash.
/// </summary>
/// <remarks>
/// Version 1 logs start with a header and contain checksum-protected frames. Each frame is
/// <c>[payload length][~payload length][CRC32C][record count][records][payload length][commit marker]</c>,
/// and each record remains
/// <c>[7-bit key length][key bytes][7-bit value length code][value bytes]</c>. A zero length code is a
/// tombstone, <see cref="RecordValueEncoding.EmptyValueLengthCode"/> is a live empty value, and every
/// other code is the value's byte length. Legacy headerless logs remain replayable.
///
/// A frame is written to the operating system before its mutations are applied in memory, which is
/// sufficient to survive a process crash. Enabling <c>syncToDisk</c> additionally <c>fsync</c>s each
/// frame to survive power loss. Multiple mutations may share one frame through <see cref="AppendBatch"/>.
///
/// A single instance is only ever written from one thread at a time: appends happen while the owning
/// <see cref="LsmStorageInner"/> holds the current-memtable write lock, so no internal
/// synchronization is required.
/// </remarks>
internal sealed class WriteAheadLog : IDisposable
{
    private const byte CurrentVersion = 1;
    private const int VersionOffset = 8;
    private const int HeaderCrcOffset = 12;
    private const uint FrameCommitMarker = 0x31584C53;
    internal const int FileHeaderSize = 16;
    internal const int FrameHeaderSize = 12;
    internal const int FrameFooterSize = 8;

    private static readonly IBinaryEncoder<ByteSlice> _keySerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;
    private static readonly IBinaryEncoder<ByteSlice> _valueSerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;
    private static ReadOnlySpan<byte> FileSignature => [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, (byte)'S', (byte)'L', (byte)'X'];

    private readonly FileStream _stream;
    private readonly bool _syncToDisk;
    private readonly PooledArrayBufferWriter<byte> _buffer;
    private bool _disposed;

    internal int RecordWriteCount { get; private set; }
    internal int DiskFlushCount { get; private set; }

    public WriteAheadLog(string path, bool syncToDisk)
    {
        // FileShare.Delete lets the file be deleted while this writer handle is still open (used on
        // clean flush). FileShare.Read lets recovery read the file concurrently if needed.
        _stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read | FileShare.Delete, bufferSize: 1, FileOptions.SequentialScan);
        _syncToDisk = syncToDisk;
        _buffer = new PooledArrayBufferWriter<byte>(256);

        Span<byte> header = stackalloc byte[FileHeaderSize];
        header.Clear();
        FileSignature.CopyTo(header);
        header[VersionOffset] = CurrentVersion;
        BinaryPrimitives.WriteUInt32LittleEndian(header[HeaderCrcOffset..], Crc32C.Compute(header[..HeaderCrcOffset]));
        _stream.Write(header);
    }

    /// <summary>
    /// Appends a single key/value record and flushes it so it survives a process crash.
    /// </summary>
    public void Append(ByteSlice key, ByteSlice value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var writer = StartFrame(recordCount: 1);
        var keyLength = _keySerializer.GetLength(key);
        writer.Write7BitEncodedInt(keyLength);
        _keySerializer.Encode(key, ref writer);
        var valueLength = _valueSerializer.GetLength(value);
        writer.Write7BitEncodedInt(RecordValueEncoding.EncodeLength(valueLength, value.IsTombstone));
        _valueSerializer.Encode(value, ref writer);
        CommitFrame(ref writer);
    }

    public void AppendRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var writer = StartFrame(recordCount: 1);
        WriteRawRecord(ref writer, key, value, isTombstone: false);
        CommitFrame(ref writer);
    }

    public void AppendDeleteRaw(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var writer = StartFrame(recordCount: 1);
        WriteRawRecord(ref writer, key, default, isTombstone: true);
        CommitFrame(ref writer);
    }

    public void AppendBatch(List<LsmWriteBatchEntry> entries)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (entries.Count == 0)
        {
            return;
        }

        var writer = StartFrame(entries.Count);
        for (var i = 0; i < entries.Count; i++)
        {
            var entry = entries[i];
            WriteRawRecord(ref writer, entry.Key, entry.Value ?? default, entry.IsTombstone);
        }

        CommitFrame(ref writer);
    }

    /// <summary>
    /// Flushes any buffered data to the operating system (and to disk when configured to do so).
    /// </summary>
    public void Flush()
    {
        if (_disposed)
        {
            return;
        }

        _stream.Flush(_syncToDisk);
    }

    /// <summary>
    /// Replays the records of the log at <paramref name="path"/> into <paramref name="target"/>.
    /// A torn trailing frame (from a crash in the middle of an append) is tolerated and stops replay.
    /// An intact frame with a bad checksum or malformed contents throws <see cref="InvalidDataException"/>.
    /// </summary>
    public static void Replay(string path, IMemTable target)
    {
        // Windows sharing is symmetric: the reader must grant write sharing to an open WAL writer.
        using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete,
            bufferSize: 1,
            FileOptions.SequentialScan);

        var length = checked((int)stream.Length);
        if (length == 0)
        {
            return;
        }

        var bytes = GC.AllocateUninitializedArray<byte>(length);
        stream.ReadExactly(bytes);

        var content = bytes.AsMemory();
        if (Path.GetExtension(path).Equals(".wal2", StringComparison.OrdinalIgnoreCase)
            || HasFileSignature(bytes))
        {
            ReplayFramed(path, content, target);
            return;
        }

        if (IsTornFileHeader(bytes))
        {
            return;
        }

        ReplayLegacy(content, target);
    }

    private EncoderBinaryWriter StartFrame(int recordCount)
    {
        _buffer.Reset();
        _buffer.Advance(FrameHeaderSize);

        var writer = new EncoderBinaryWriter(_buffer);
        writer.WriteUInt32((uint)recordCount);
        return writer;
    }

    private void CommitFrame(ref EncoderBinaryWriter writer)
    {
        writer.Flush();

        var payloadLength = checked((uint)(_buffer.WrittenCount - FrameHeaderSize));
        var footer = _buffer.GetSpan(FrameFooterSize);
        BinaryPrimitives.WriteUInt32LittleEndian(footer, payloadLength);
        BinaryPrimitives.WriteUInt32LittleEndian(footer[sizeof(uint)..], FrameCommitMarker);
        _buffer.Advance(FrameFooterSize);

        var frame = _buffer.WrittenSpan;
        var payload = frame.Slice(FrameHeaderSize, (int)payloadLength);
        BinaryPrimitives.WriteUInt32LittleEndian(frame, payloadLength);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[sizeof(uint)..], ~payloadLength);
        BinaryPrimitives.WriteUInt32LittleEndian(frame[(2 * sizeof(uint))..], Crc32C.Compute(payload));

        _stream.Write(frame);
        RecordWriteCount++;
        if (_syncToDisk)
        {
            _stream.Flush(flushToDisk: true);
            DiskFlushCount++;
        }
    }

    private static void WriteRawRecord(
        ref EncoderBinaryWriter writer,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        bool isTombstone)
    {
        writer.Write7BitEncodedInt(key.Length);
        writer.WriteRaw(key);
        writer.Write7BitEncodedInt(RecordValueEncoding.EncodeLength(value.Length, isTombstone));
        writer.WriteRaw(value);
    }

    private static bool HasFileSignature(ReadOnlySpan<byte> bytes)
    {
        return bytes.Length >= FileSignature.Length
            && bytes[..FileSignature.Length].SequenceEqual(FileSignature);
    }

    private static bool IsTornFileHeader(ReadOnlySpan<byte> bytes)
    {
        return bytes.Length < FileHeaderSize
            && bytes.SequenceEqual(FileSignature[..Math.Min(bytes.Length, FileSignature.Length)]);
    }

    private static void ReplayFramed(string path, ReadOnlyMemory<byte> content, IMemTable target)
    {
        var bytes = content.Span;
        if (bytes.Length < FileHeaderSize)
        {
            return;
        }

        var fileHeader = bytes[..FileHeaderSize];
        var expectedHeaderCrc = BinaryPrimitives.ReadUInt32LittleEndian(fileHeader[HeaderCrcOffset..]);
        if (!fileHeader[..FileSignature.Length].SequenceEqual(FileSignature)
            || Crc32C.Compute(fileHeader[..HeaderCrcOffset]) != expectedHeaderCrc)
        {
            throw new InvalidDataException($"WAL '{path}' has a corrupt file header.");
        }

        if (fileHeader[VersionOffset] != CurrentVersion)
        {
            throw new InvalidDataException($"WAL '{path}' uses unsupported format version {fileHeader[VersionOffset]}.");
        }

        if (!fileHeader[(VersionOffset + 1)..HeaderCrcOffset].SequenceEqual(stackalloc byte[HeaderCrcOffset - VersionOffset - 1]))
        {
            throw new InvalidDataException($"WAL '{path}' has an invalid file header.");
        }

        var offset = FileHeaderSize;
        while (offset < bytes.Length)
        {
            var remaining = bytes.Length - offset;
            if (remaining < FrameHeaderSize + FrameFooterSize)
            {
                return;
            }

            var header = bytes.Slice(offset, FrameHeaderSize);
            var payloadLength = BinaryPrimitives.ReadUInt32LittleEndian(header);
            var complementedLength = BinaryPrimitives.ReadUInt32LittleEndian(header[sizeof(uint)..]);
            if (complementedLength != ~payloadLength || payloadLength > int.MaxValue)
            {
                throw new InvalidDataException($"WAL '{path}' has a corrupt frame header at offset {offset}.");
            }

            var payloadOffset = offset + FrameHeaderSize;
            if ((int)payloadLength > bytes.Length - payloadOffset - FrameFooterSize)
            {
                return;
            }

            var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(header[(2 * sizeof(uint))..]);
            var payload = content.Slice(payloadOffset, (int)payloadLength);
            var footerOffset = payloadOffset + (int)payloadLength;
            var footer = bytes.Slice(footerOffset, FrameFooterSize);
            var footerLength = BinaryPrimitives.ReadUInt32LittleEndian(footer);
            var commitMarker = BinaryPrimitives.ReadUInt32LittleEndian(footer[sizeof(uint)..]);
            if (commitMarker != FrameCommitMarker)
            {
                if (footerOffset + FrameFooterSize == bytes.Length)
                {
                    return;
                }

                throw new InvalidDataException($"WAL '{path}' has a corrupt frame footer at offset {offset}.");
            }

            if (footerLength != payloadLength)
            {
                throw new InvalidDataException($"WAL '{path}' has a corrupt frame footer length at offset {offset}.");
            }

            if (Crc32C.Compute(payload.Span) != expectedCrc)
            {
                throw new InvalidDataException($"WAL '{path}' has a checksum mismatch at offset {offset}.");
            }

            ReplayFrame(path, offset, payload, target);
            offset = footerOffset + FrameFooterSize;
        }
    }

    private static void ReplayFrame(string path, int frameOffset, ReadOnlyMemory<byte> payload, IMemTable target)
    {
        try
        {
            var reader = new EncoderBinaryReader(payload, 0);
            var recordCount = reader.ReadUInt32();
            if (recordCount > int.MaxValue)
            {
                throw new InvalidDataException($"WAL '{path}' has too many records in the frame at offset {frameOffset}.");
            }

            for (var i = 0; i < (int)recordCount; i++)
            {
                ReplayRecord(ref reader, target: null);
            }

            if (!reader.IsEOF)
            {
                throw new InvalidDataException($"WAL '{path}' has trailing bytes in the frame at offset {frameOffset}.");
            }

            reader = new EncoderBinaryReader(payload, sizeof(uint));
            for (var i = 0; i < (int)recordCount; i++)
            {
                ReplayRecord(ref reader, target);
            }
        }
        catch (InvalidDataException)
        {
            throw;
        }
        catch (Exception exception) when (exception is EndOfStreamException or OverflowException or ArgumentOutOfRangeException)
        {
            throw new InvalidDataException($"WAL '{path}' has a malformed frame at offset {frameOffset}.", exception);
        }
    }

    private static void ReplayLegacy(ReadOnlyMemory<byte> content, IMemTable target)
    {
        var reader = new EncoderBinaryReader(content, 0);

        while (!reader.IsEOF)
        {
            try
            {
                ReplayRecord(ref reader, target);
            }
            catch (EndOfStreamException)
            {
                // The last record was only partially written before the crash; ignore it.
                break;
            }
        }
    }

    private static void ReplayRecord(ref EncoderBinaryReader reader, IMemTable? target)
    {
        var keyLength = reader.Read7BitEncodedInt();
        var keyBytes = reader.ReadBytesSpan(keyLength);

        var valueLength = RecordValueEncoding.DecodeLength(reader.Read7BitEncodedInt(), out var isTombstone);
        var valueBytes = reader.ReadBytesSpan(valueLength);

        if (target is null)
        {
            return;
        }

        if (target is IRawBytesMemTable rawMemTable)
        {
            if (isTombstone)
            {
                rawMemTable.DeleteRaw(keyBytes);
            }
            else
            {
                rawMemTable.PutRaw(keyBytes, valueBytes);
            }

            return;
        }

        var key = _keySerializer.Decode(keyBytes);
        var value = isTombstone ? ByteSlice.Tombstone : _valueSerializer.Decode(valueBytes);
        target.Put(key, value);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        // Close the handle but keep the file: deletion is the caller's responsibility and only happens
        // once the data is durable elsewhere (flushed to an SST) or on a clean shutdown.
        _stream.Flush(_syncToDisk);
        _stream.Dispose();
        _buffer.Dispose();
    }
}
