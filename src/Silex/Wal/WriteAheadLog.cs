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
/// and every other code is the value's byte length. The log is
/// written to the operating system after every append, which is sufficient to survive a process
/// crash; enabling <c>syncToDisk</c> additionally <c>fsync</c>s on every append to survive power loss.
///
/// A single instance is only ever written from one thread at a time: appends happen while the owning
/// <see cref="LsmStorageInner"/> holds the current-memtable write lock, so no internal
/// synchronization is required.
/// </remarks>
internal sealed class WriteAheadLog : IDisposable
{
    private static readonly IBinaryEncoder<ByteSlice> _keySerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;
    private static readonly IBinaryEncoder<ByteSlice> _valueSerializer = BinaryEncoderFactory<ByteSlice>.BinarySerializer;

    private readonly FileStream _stream;
    private readonly bool _syncToDisk;
    private readonly PooledArrayBufferWriter<byte> _buffer;
    private bool _disposed;

    public WriteAheadLog(string path, bool syncToDisk)
    {
        // FileShare.Delete lets the file be deleted while this writer handle is still open (used on
        // clean flush). FileShare.Read lets recovery read the file concurrently if needed.
        _stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read | FileShare.Delete, bufferSize: 1, FileOptions.SequentialScan);
        _syncToDisk = syncToDisk;
        _buffer = new PooledArrayBufferWriter<byte>(256);
    }

    /// <summary>
    /// Appends a single key/value record and flushes it so it survives a process crash.
    /// </summary>
    public void Append(ByteSlice key, ByteSlice value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var keyLength = _keySerializer.GetLength(key);
        var valueLength = _valueSerializer.GetLength(value);

        _buffer.Clear();

        var writer = new EncoderBinaryWriter(_buffer);
        writer.Write7BitEncodedInt(keyLength);
        _keySerializer.Encode(key, ref writer);
        writer.Write7BitEncodedInt(RecordValueEncoding.EncodeLength(valueLength, value.IsTombstone));
        _valueSerializer.Encode(value, ref writer);
        writer.Flush();

        _stream.Write(_buffer.WrittenMemory.Span);
        if (_syncToDisk)
        {
            _stream.Flush(flushToDisk: true);
        }
    }

    public void AppendRaw(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (typeof(ByteSlice) != typeof(ByteSlice) || typeof(ByteSlice) != typeof(ByteSlice))
        {
            throw new InvalidOperationException("Raw WAL appends are only supported by byte-oriented stores.");
        }

        _buffer.Clear();

        var writer = new EncoderBinaryWriter(_buffer);
        writer.Write7BitEncodedInt(key.Length);
        writer.WriteRaw(key);
        writer.Write7BitEncodedInt(RecordValueEncoding.EncodeLength(value.Length, isTombstone: false));
        writer.WriteRaw(value);
        writer.Flush();

        _stream.Write(_buffer.WrittenMemory.Span);
        if (_syncToDisk)
        {
            _stream.Flush(flushToDisk: true);
        }
    }

    public void AppendDeleteRaw(ReadOnlySpan<byte> key)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _buffer.Clear();

        var writer = new EncoderBinaryWriter(_buffer);
        writer.Write7BitEncodedInt(key.Length);
        writer.WriteRaw(key);
        writer.Write7BitEncodedInt(RecordValueEncoding.EncodeLength(0, isTombstone: true));
        writer.Flush();

        _stream.Write(_buffer.WrittenMemory.Span);
        if (_syncToDisk)
        {
            _stream.Flush(flushToDisk: true);
        }
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
        var bytes = File.ReadAllBytes(path);

        if (bytes.Length == 0)
        {
            return;
        }

        var reader = new EncoderBinaryReader(bytes, 0);

        while (!reader.IsEOF)
        {
            try
            {
                var keyLength = reader.Read7BitEncodedInt();
                var keyBytes = reader.ReadBytesSpan(keyLength);

                var valueLength = RecordValueEncoding.DecodeLength(reader.Read7BitEncodedInt(), out var isTombstone);
                var valueBytes = reader.ReadBytesSpan(valueLength);

                if (target is IRawBytesMemTable rawMemTable && typeof(ByteSlice) == typeof(ByteSlice) && typeof(ByteSlice) == typeof(ByteSlice))
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
            catch (EndOfStreamException)
            {
                // The last record was only partially written before the crash; ignore it.
                break;
            }
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
        _buffer.Dispose();
    }
}
