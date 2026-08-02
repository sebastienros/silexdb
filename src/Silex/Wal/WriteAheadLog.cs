using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
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
/// Each record is <c>[7-bit key length][key bytes][7-bit value length code][value bytes]</c>. A zero
/// length code is a tombstone, <see cref="RecordValueEncoding.EmptyValueLengthCode"/> is a live empty value,
/// and every other code is the value's byte length. Batches use a reserved negative key length followed by
/// their payload length and concatenated records, so recovery applies either the complete batch or none of it.
/// The log is
/// written to the operating system after every append, which is sufficient to survive a process
/// crash; enabling <c>syncToDisk</c> additionally <c>fsync</c>s on every append to survive power loss.
///
/// A single instance is only ever written from one thread at a time: appends happen while the owning
/// <see cref="LsmStorageInner"/> holds the current-memtable write lock, so no internal
/// synchronization is required.
/// </remarks>
internal sealed class WriteAheadLog : IDisposable
{
    private const int BatchMarker = -1;

    private static readonly IBinaryEncoder<ByteSlice> _keySerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;
    private static readonly IBinaryEncoder<ByteSlice> _valueSerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;

    private readonly FileStream _stream;
    private readonly bool _syncToDisk;
    private byte[] _buffer;
    private bool _disposed;

    public WriteAheadLog(string path, bool syncToDisk)
    {
        // FileShare.Delete lets the file be deleted while this writer handle is still open (used on
        // clean flush). FileShare.Read lets recovery read the file concurrently if needed.
        _stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read | FileShare.Delete, bufferSize: 1, FileOptions.SequentialScan);
        _syncToDisk = syncToDisk;
        _buffer = ArrayPool<byte>.Shared.Rent(256);
    }

    /// <summary>
    /// Appends a single key/value record and flushes it so it survives a process crash.
    /// </summary>
    public void Append(ByteSlice key, ByteSlice value)
    {
        if (value.IsTombstone)
        {
            AppendDeleteRaw(key.Span);
        }
        else
        {
            AppendRaw(key.Span, value.Span);
        }
    }

    public void AppendRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        AppendRecord(key, value, isTombstone: false);
    }

    public void AppendDeleteRaw(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        AppendRecord(key, default, isTombstone: true);
    }

    /// <summary>
    /// Appends all records with one operating-system write and, when configured, one disk flush.
    /// </summary>
    public void AppendBatch(ReadOnlySpan<WriteBatchEntry> entries)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (entries.IsEmpty)
        {
            return;
        }

        var recordsLength = 0;
        foreach (ref readonly var entry in entries)
        {
            recordsLength = checked(recordsLength + GetRecordLength(entry.Key.Length, entry.Value.Length, entry.IsDelete));
        }

        var payloadLength = checked(Get7BitEncodedLength(entries.Length) + recordsLength + sizeof(uint));
        var length = checked(Get7BitEncodedLength(BatchMarker) + Get7BitEncodedLength(payloadLength) + payloadLength);
        EnsureCapacity(length);
        var destination = _buffer.AsSpan(0, length);
        var offset = Write7BitEncodedInt(destination, BatchMarker);
        offset += Write7BitEncodedInt(destination[offset..], payloadLength);
        var payloadOffset = offset;
        offset += Write7BitEncodedInt(destination[offset..], entries.Length);

        foreach (ref readonly var entry in entries)
        {
            offset += WriteRecord(destination[offset..], entry.Key.Span, entry.Value.Span, entry.IsDelete);
        }

        var checksum = Crc32.HashToUInt32(destination.Slice(payloadOffset, offset - payloadOffset));
        BinaryPrimitives.WriteUInt32LittleEndian(destination[offset..], checksum);
        WriteBuffer(length);
    }

    private void AppendRecord(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, bool isTombstone)
    {
        var length = GetRecordLength(key.Length, value.Length, isTombstone);
        EnsureCapacity(length);
        WriteRecord(_buffer, key, value, isTombstone);
        WriteBuffer(length);
    }

    private void WriteBuffer(int length)
    {
        _stream.Write(_buffer.AsSpan(0, length));
        if (_syncToDisk)
        {
            _stream.Flush(flushToDisk: true);
        }
    }

    private void EnsureCapacity(int length)
    {
        if (_buffer.Length >= length)
        {
            return;
        }

        var previous = _buffer;
        _buffer = ArrayPool<byte>.Shared.Rent(length);
        ArrayPool<byte>.Shared.Return(previous, clearArray: true);
    }

    private static int GetRecordLength(int keyLength, int valueLength, bool isTombstone) =>
        checked(Get7BitEncodedLength(keyLength) + keyLength +
                RecordValueEncoding.GetEncodedLengthSize(valueLength, isTombstone) +
                (isTombstone ? 0 : valueLength));

    private static int WriteRecord(
        Span<byte> destination,
        ReadOnlySpan<byte> key,
        ReadOnlySpan<byte> value,
        bool isTombstone)
    {
        var offset = Write7BitEncodedInt(destination, key.Length);
        key.CopyTo(destination[offset..]);
        offset += key.Length;
        offset += Write7BitEncodedInt(
            destination[offset..],
            RecordValueEncoding.EncodeLength(value.Length, isTombstone));

        if (!isTombstone)
        {
            value.CopyTo(destination[offset..]);
            offset += value.Length;
        }

        return offset;
    }

    private static int Get7BitEncodedLength(int value)
    {
        var length = 1;
        var remaining = (uint)value;
        while (remaining > 0x7F)
        {
            length++;
            remaining >>= 7;
        }

        return length;
    }

    private static int Write7BitEncodedInt(Span<byte> destination, int value)
    {
        var offset = 0;
        var remaining = (uint)value;
        while (remaining > 0x7F)
        {
            destination[offset++] = (byte)(remaining | 0x80);
            remaining >>= 7;
        }

        destination[offset++] = (byte)remaining;
        return offset;
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
    /// A torn trailing record (from a crash in the middle of an append) is tolerated and stops replay.
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

        var reader = new EncoderBinaryReader(bytes, 0);

        while (!reader.IsEOF)
        {
            try
            {
                var keyLength = reader.Read7BitEncodedInt();
                if (keyLength == BatchMarker)
                {
                    var payloadLength = reader.Read7BitEncodedInt();
                    var payload = reader.ReadBytesMemory(payloadLength);
                    ValidateBatch(payload);
                    ReplayBatch(payload[..^sizeof(uint)], target);

                    continue;
                }

                ReplayRecord(ref reader, target, keyLength);
            }
            catch (EndOfStreamException)
            {
                // The last record was only partially written before the crash; ignore it.
                break;
            }
            catch (InvalidDataException)
            {
                // A complete-length trailing batch can still contain torn sectors after a crash.
                break;
            }
        }
    }

    private static void ValidateBatch(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length < sizeof(uint))
        {
            throw new InvalidDataException("The WAL batch checksum is missing.");
        }

        var records = payload[..^sizeof(uint)];
        var expectedChecksum = BinaryPrimitives.ReadUInt32LittleEndian(payload.Span[^sizeof(uint)..]);
        if (Crc32.HashToUInt32(records.Span) != expectedChecksum)
        {
            throw new InvalidDataException("The WAL batch checksum is invalid.");
        }

        var reader = new EncoderBinaryReader(records, 0);
        var entryCount = reader.Read7BitEncodedInt();
        if (entryCount < 0)
        {
            throw new InvalidDataException($"Invalid WAL batch entry count {entryCount}.");
        }

        for (var i = 0; i < entryCount; i++)
        {
            ValidateRecord(ref reader, reader.Read7BitEncodedInt());
        }

        if (!reader.IsEOF)
        {
            throw new InvalidDataException("The WAL batch contains trailing data.");
        }
    }

    private static void ValidateRecord(ref EncoderBinaryReader reader, int keyLength)
    {
        if (keyLength < 0)
        {
            throw new InvalidDataException($"Invalid WAL key length {keyLength}.");
        }

        reader.Skip(keyLength);
        var valueLength = RecordValueEncoding.DecodeLength(reader.Read7BitEncodedInt(), out _);
        if (valueLength < 0)
        {
            throw new InvalidDataException($"Invalid WAL value length {valueLength}.");
        }

        reader.Skip(valueLength);
    }

    private static void ReplayBatch(ReadOnlyMemory<byte> records, IMemTable target)
    {
        var reader = new EncoderBinaryReader(records, 0);
        var entryCount = reader.Read7BitEncodedInt();
        for (var i = 0; i < entryCount; i++)
        {
            ReplayRecord(ref reader, target, reader.Read7BitEncodedInt());
        }
    }

    private static void ReplayRecord(ref EncoderBinaryReader reader, IMemTable target, int keyLength)
    {
        if (keyLength < 0)
        {
            throw new InvalidDataException($"Invalid WAL key length {keyLength}.");
        }

        var keyBytes = reader.ReadBytesSpan(keyLength);
        var valueLength = RecordValueEncoding.DecodeLength(reader.Read7BitEncodedInt(), out var isTombstone);
        if (valueLength < 0)
        {
            throw new InvalidDataException($"Invalid WAL value length {valueLength}.");
        }

        var valueBytes = reader.ReadBytesSpan(valueLength);

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
        }
        else
        {
            var key = _keySerializer.Decode(keyBytes);
            var value = isTombstone ? ByteSlice.Tombstone : _valueSerializer.Decode(valueBytes);
            target.Put(key, value);
        }
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
        ArrayPool<byte>.Shared.Return(_buffer, clearArray: true);
        _buffer = [];
    }
}
