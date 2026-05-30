using Silex.Buffers;
using Silex.MemTables;
using Silex.Serialization;

namespace Silex.Wal;

/// <summary>
/// Append-only write-ahead log for a single <see cref="MemTable{TKey, TValue}"/>. Every mutation is
/// journaled before it is applied in memory so that the unflushed contents of a memtable can be
/// recovered after a process crash.
/// </summary>
/// <remarks>
/// Each record is <c>[7-bit key length][key bytes][7-bit value length][value bytes]</c>. The same
/// zero-length value is used for tombstones (consistent with the rest of the engine). The log is
/// written to the operating system after every append, which is sufficient to survive a process
/// crash; enabling <c>syncToDisk</c> additionally <c>fsync</c>s on every append to survive power loss.
///
/// A single instance is only ever written from one thread at a time: appends happen while the owning
/// <see cref="LsmStorageInner{TKey, TValue}"/> holds the current-memtable write lock, so no internal
/// synchronization is required.
/// </remarks>
internal sealed class WriteAheadLog<TKey, TValue> : IDisposable where TKey : notnull
{
    private static readonly IBinaryEncoder<TKey> _keySerializer = BinaryEncoderFactory<TKey>.BinarySerializer;
    private static readonly IBinaryEncoder<TValue> _valueSerializer = BinaryEncoderFactory<TValue>.BinarySerializer;

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
    public void Append(TKey key, TValue value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var keyLength = _keySerializer.GetLength(key);
        var valueLength = _valueSerializer.GetLength(value);

        _buffer.Clear();

        var writer = new EncoderBinaryWriter(_buffer);
        writer.Write7BitEncodedInt(keyLength);
        _keySerializer.Encode(key, ref writer);
        writer.Write7BitEncodedInt(valueLength);
        _valueSerializer.Encode(value, ref writer);
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
    public static void Replay(string path, IMemTable<TKey, TValue> target)
    {
        var bytes = File.ReadAllBytes(path);

        if (bytes.Length == 0)
        {
            return;
        }

        var reader = new EncoderBinaryReader(bytes, 0);

        while (!reader.IsEOF)
        {
            TKey key;
            TValue value;

            try
            {
                var keyLength = reader.Read7BitEncodedInt();
                key = _keySerializer.Decode(reader.ReadBytesSpan(keyLength));

                var valueLength = reader.Read7BitEncodedInt();
                value = _valueSerializer.Decode(reader.ReadBytesSpan(valueLength));
            }
            catch (EndOfStreamException)
            {
                // The last record was only partially written before the crash; ignore it.
                break;
            }

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
        _buffer.Dispose();
    }
}
