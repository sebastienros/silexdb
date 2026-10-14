using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.IO.Compression;
using System.IO.Hashing;
using System.Text.Json;

namespace Silex.Tool;

internal readonly record struct DumpEntry(ReadOnlyMemory<byte> Key, ReadOnlyMemory<byte> Value);

internal sealed class DumpWriter : IDisposable
{
    internal const int BlockTargetBytes = 1024 * 1024;
    internal const int MaxBlockBytes = ServerHost.MaxPayloadBytes + 8;
    internal const int MaxJsonBytes = ServerHost.MaxPayloadBytes * 4 / 3 + 128;
    internal static ReadOnlySpan<byte> Magic => "SILEXKV1"u8;

    private readonly Stream _output;
    private readonly bool _binary;
    private readonly LimitedBuffer _buffer;
    private readonly Utf8JsonWriter? _json;
    private readonly byte[] _header = new byte[20];
    private uint _blockRecords;
    public long Records { get; private set; }

    public DumpWriter(Stream output, string format)
    {
        ValidateFormat(format);
        _output = output;
        _binary = format == "binary";
        _buffer = new LimitedBuffer(_binary ? MaxBlockBytes : MaxJsonBytes);
        if (!_binary) _json = new Utf8JsonWriter(_buffer);
    }

    public static void ValidateFormat(string format, bool allowAuto = false)
    {
        if (format is not ("jsonl" or "binary") && !(allowAuto && format == "auto"))
            throw new ArgumentException("Format must be jsonl or binary (or auto for import).");
    }

    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        if (_binary)
        {
            Magic.CopyTo(_header);
            await _output.WriteAsync(_header.AsMemory(0, 8), cancellationToken);
        }
    }

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken)
    {
        ValidateLengths(key.Length, value.Length);
        if (_binary)
        {
            var size = checked(8 + key.Length + value.Length);
            if (_blockRecords > 0 && _buffer.WrittenMemory.Length + size > BlockTargetBytes)
                await FlushBlockAsync(cancellationToken);
            AppendBinary(key.Span, value.Span);
            _blockRecords++;
        }
        else
        {
            _buffer.Clear();
            _json!.Reset(_buffer);
            _json.WriteStartObject();
            _json.WriteBase64String("key", key.Span);
            _json.WriteBase64String("value", value.Span);
            _json.WriteEndObject();
            _json.Flush();
            _buffer.GetSpan(1)[0] = (byte)'\n';
            _buffer.Advance(1);
            await _output.WriteAsync(_buffer.WrittenMemory, cancellationToken);
        }
        Records = checked(Records + 1);
    }

    private void AppendBinary(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
    {
        var span = _buffer.GetSpan(8 + key.Length + value.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(span, (uint)key.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(span[4..], (uint)value.Length);
        key.CopyTo(span[8..]);
        value.CopyTo(span[(8 + key.Length)..]);
        _buffer.Advance(8 + key.Length + value.Length);
    }

    private async ValueTask FlushBlockAsync(CancellationToken cancellationToken)
    {
        if (_blockRecords == 0) return;
        BinaryPrimitives.WriteUInt32LittleEndian(_header, (uint)_buffer.WrittenMemory.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(_header.AsSpan(4), _blockRecords);
        BinaryPrimitives.WriteUInt32LittleEndian(_header.AsSpan(8), Crc32.HashToUInt32(_buffer.WrittenMemory.Span));
        await _output.WriteAsync(_header.AsMemory(0, 12), cancellationToken);
        await _output.WriteAsync(_buffer.WrittenMemory, cancellationToken);
        _buffer.Clear();
        _blockRecords = 0;
    }

    public async ValueTask CompleteAsync(CancellationToken cancellationToken)
    {
        if (_binary)
        {
            await FlushBlockAsync(cancellationToken);
            _header.AsSpan().Clear();
            BinaryPrimitives.WriteUInt64LittleEndian(_header.AsSpan(12), (ulong)Records);
            await _output.WriteAsync(_header, cancellationToken);
        }
        await _output.FlushAsync(cancellationToken);
    }

    internal static void ValidateLengths(int key, int value)
    {
        if (key <= 0 || value < 0 || (long)key + value > ServerHost.MaxPayloadBytes)
            throw new InvalidDataException("A record requires a nonempty key and at most 16 MiB of key plus value bytes.");
    }

    public void Dispose()
    {
        _json?.Dispose();
        _buffer.Dispose();
    }
}

internal sealed class DumpReader : IDisposable
{
    private readonly Stream _input;
    private readonly bool _binary;
    private readonly LimitedBuffer _record = new(DumpWriter.MaxJsonBytes);
    private readonly LimitedBuffer _decoded = new(ServerHost.MaxPayloadBytes);
    private readonly LimitedBuffer _escaped = new(DumpWriter.MaxJsonBytes);
    private readonly byte[] _readBuffer = ArrayPool<byte>.Shared.Rent(64 * 1024);
    private readonly byte[] _header = new byte[20];
    private readonly GZipStream? _gzip;
    private readonly TrackingReadStream? _compressed;
    private readonly TrackingReadStream? _uncompressed;
    private int _readPosition, _readLength, _blockPosition;
    private bool _exhausted;
    public long Records { get; private set; }

    private DumpReader(Stream input, bool binary, GZipStream? gzip,
        TrackingReadStream? compressed, TrackingReadStream? uncompressed)
    {
        _input = input;
        _binary = binary;
        _gzip = gzip;
        _compressed = compressed;
        _uncompressed = uncompressed;
    }

    public static async Task<DumpReader> OpenAsync(Stream input, string format, CancellationToken cancellationToken)
    {
        DumpWriter.ValidateFormat(format, allowAuto: true);
        var prefix = new byte[8];
        var count = await input.ReadAtLeastAsync(prefix.AsMemory(0, 2), 2, throwOnEndOfStream: false, cancellationToken);
        Stream payload = new PrefixReadStream(prefix.AsMemory(0, count), input);
        GZipStream? gzip = null;
        TrackingReadStream? compressed = null, uncompressed = null;
        try
        {
            if (count == 2 && prefix[0] == 0x1f && prefix[1] == 0x8b)
            {
                compressed = new TrackingReadStream(payload, checksum: false);
                gzip = new GZipStream(compressed, CompressionMode.Decompress, leaveOpen: true);
                uncompressed = new TrackingReadStream(gzip, checksum: true);
                payload = uncompressed;
            }
            // The second probe needs its own buffer: the first replay still owns the first prefix.
            var header = new byte[8];
            count = await payload.ReadAtLeastAsync(header, 8, throwOnEndOfStream: false, cancellationToken);
            var binary = format == "binary" || (format == "auto" && header.AsSpan(0, count).StartsWith("SILEXKV"u8));
            if (binary && (count != 8 || !header.AsSpan().SequenceEqual(DumpWriter.Magic)))
                throw new InvalidDataException("Invalid or unsupported binary dump header; expected SILEXKV1.");
            return new DumpReader(binary ? payload : new PrefixReadStream(header.AsMemory(0, count), payload),
                binary, gzip, compressed, uncompressed);
        }
        catch
        {
            gzip?.Dispose();
            throw;
        }
    }

    public async ValueTask<DumpEntry?> ReadAsync(CancellationToken cancellationToken)
    {
        if (_exhausted) return null;
        if (!_binary)
        {
            if (!await ReadLineAsync(cancellationToken))
            {
                Finish();
                return null;
            }
            var entry = ParseJson();
            Records = checked(Records + 1);
            return entry;
        }
        if (_blockPosition == _record.WrittenMemory.Length)
        {
            await _input.ReadExactlyAsync(_header.AsMemory(0, 12), cancellationToken);
            var length = BinaryPrimitives.ReadUInt32LittleEndian(_header);
            var count = BinaryPrimitives.ReadUInt32LittleEndian(_header.AsSpan(4));
            var checksum = BinaryPrimitives.ReadUInt32LittleEndian(_header.AsSpan(8));
            if (length == 0)
            {
                await _input.ReadExactlyAsync(_header.AsMemory(12, 8), cancellationToken);
                if (count != 0 || checksum != 0 || BinaryPrimitives.ReadUInt64LittleEndian(_header.AsSpan(12)) != (ulong)Records)
                    throw new InvalidDataException("Invalid binary dump completion footer.");
                if (await _input.ReadAsync(_header.AsMemory(0, 1), cancellationToken) != 0)
                    throw new InvalidDataException("Trailing bytes after the binary dump footer.");
                Finish();
                return null;
            }
            if (length > DumpWriter.MaxBlockBytes || count == 0 || count > length / 9)
                throw new InvalidDataException("Invalid binary block length or record count.");
            _record.Clear();
            await _input.ReadExactlyAsync(_record.GetMemory((int)length)[..(int)length], cancellationToken);
            _record.Advance((int)length);
            if (Crc32.HashToUInt32(_record.WrittenMemory.Span) != checksum)
                throw new InvalidDataException("Binary dump block checksum mismatch.");
            ValidateBlock(count);
            _blockPosition = 0;
        }
        var result = BinaryEntry(_blockPosition, out var next);
        _blockPosition = next;
        Records = checked(Records + 1);
        return result;
    }

    private void ValidateBlock(uint count)
    {
        var position = 0;
        for (uint i = 0; i < count; i++)
            BinaryEntry(position, out position);
        if (position != _record.WrittenMemory.Length)
            throw new InvalidDataException("Binary block record count does not match its payload.");
    }

    private DumpEntry BinaryEntry(int position, out int next)
    {
        var memory = _record.WrittenMemory;
        if (memory.Length - position < 8) throw new InvalidDataException("Truncated binary record header.");
        var key = BinaryPrimitives.ReadUInt32LittleEndian(memory.Span[position..]);
        var value = BinaryPrimitives.ReadUInt32LittleEndian(memory.Span[(position + 4)..]);
        if (key > int.MaxValue || value > int.MaxValue) throw new InvalidDataException("Invalid binary record lengths.");
        DumpWriter.ValidateLengths((int)key, (int)value);
        var size = checked(8 + (int)key + (int)value);
        if (size > memory.Length - position) throw new InvalidDataException("Truncated binary record payload.");
        next = position + size;
        return new(memory.Slice(position + 8, (int)key), memory.Slice(position + 8 + (int)key, (int)value));
    }

    private async ValueTask<bool> ReadLineAsync(CancellationToken cancellationToken)
    {
        _record.Clear();
        while (true)
        {
            if (_readPosition == _readLength)
            {
                _readLength = await _input.ReadAsync(_readBuffer, cancellationToken);
                _readPosition = 0;
                if (_readLength == 0) return _record.WrittenMemory.Length > 0;
            }
            var available = _readBuffer.AsMemory(_readPosition, _readLength - _readPosition);
            var newline = available.Span.IndexOf((byte)'\n');
            var length = newline < 0 ? available.Length : newline;
            if (length > 0)
            {
                available.Span[..length].CopyTo(_record.GetSpan(length));
                _record.Advance(length);
            }
            _readPosition += length + (newline < 0 ? 0 : 1);
            if (newline >= 0) return true;
        }
    }

    private DumpEntry ParseJson()
    {
        _decoded.Clear();
        var reader = new Utf8JsonReader(_record.WrittenMemory.Span, new JsonReaderOptions { MaxDepth = 4 });
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            throw new InvalidDataException($"JSONL record {Records + 1} must be an object.");
        var keyOffset = -1;
        var valueOffset = -1;
        var keyLength = 0;
        var valueLength = 0;
        while (reader.Read() && reader.TokenType != JsonTokenType.EndObject)
        {
            if (reader.TokenType != JsonTokenType.PropertyName)
                throw new InvalidDataException("Expected a key or value property.");
            var isKey = reader.ValueTextEquals("key"u8);
            var isValue = reader.ValueTextEquals("value"u8);
            if ((!isKey && !isValue) || (isKey ? keyOffset >= 0 : valueOffset >= 0))
                throw new InvalidDataException("JSONL records require exactly one key and one value property.");
            if (!reader.Read() || reader.TokenType != JsonTokenType.String)
                throw new InvalidDataException("JSONL keys and values must be base64 strings.");
            var offset = _decoded.WrittenMemory.Length;
            var length = DecodeBase64(ref reader);
            if (isKey) { keyOffset = offset; keyLength = length; }
            else { valueOffset = offset; valueLength = length; }
        }
        if (reader.TokenType != JsonTokenType.EndObject || reader.Read() || keyOffset < 0 || valueOffset < 0)
            throw new InvalidDataException("JSONL records require exactly one key and one value property.");
        DumpWriter.ValidateLengths(keyLength, valueLength);
        return new(_decoded.WrittenMemory.Slice(keyOffset, keyLength), _decoded.WrittenMemory.Slice(valueOffset, valueLength));
    }

    private int DecodeBase64(ref Utf8JsonReader reader)
    {
        var encoded = reader.ValueSpan;
        if (reader.ValueIsEscaped)
        {
            _escaped.Clear();
            _escaped.Advance(reader.CopyString(_escaped.GetSpan(encoded.Length)));
            encoded = _escaped.WrittenMemory.Span;
        }
        if (encoded.IsEmpty) return 0;
        if (encoded.Length % 4 != 0) throw new InvalidDataException("Invalid base64 length.");
        var size = encoded.Length / 4 * 3;
        if (encoded[^1] == '=') size--;
        if (encoded[^2] == '=') size--;
        if (size < 0 || size > ServerHost.MaxPayloadBytes - _decoded.WrittenMemory.Length)
            throw new InvalidDataException("Decoded record exceeds 16 MiB.");
        if (Base64.DecodeFromUtf8(encoded, _decoded.GetSpan(size), out var consumed, out var written) != System.Buffers.OperationStatus.Done ||
            consumed != encoded.Length || written != size)
            throw new InvalidDataException("Invalid base64 key or value.");
        _decoded.Advance(written);
        return written;
    }

    private void Finish()
    {
        if (_gzip is not null)
        {
            var tail = _compressed!.Tail;
            if (_compressed.Bytes < 18 || tail.Length != 8 ||
                BinaryPrimitives.ReadUInt32LittleEndian(tail) != _uncompressed!.Checksum ||
                BinaryPrimitives.ReadUInt32LittleEndian(tail[4..]) != unchecked((uint)_uncompressed.Bytes))
                throw new InvalidDataException("Missing or invalid gzip completion trailer; use a single-member gzip file.");
        }
        _exhausted = true;
    }

    public void Dispose()
    {
        _gzip?.Dispose();
        _uncompressed?.Dispose();
        _compressed?.Dispose();
        _record.Dispose();
        _decoded.Dispose();
        _escaped.Dispose();
        ArrayPool<byte>.Shared.Return(_readBuffer);
    }
}

internal class PrefixReadStream(ReadOnlyMemory<byte> prefix, Stream inner) : Stream
{
    private ReadOnlyMemory<byte> _prefix = prefix;
    protected Stream Inner { get; } = inner;
    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    public override void Flush() => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));
    public override int Read(Span<byte> buffer)
    {
        if (_prefix.IsEmpty) return Inner.Read(buffer);
        var count = Math.Min(buffer.Length, _prefix.Length);
        _prefix.Span[..count].CopyTo(buffer);
        _prefix = _prefix[count..];
        return count;
    }
    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _prefix.IsEmpty ? Inner.ReadAsync(buffer, cancellationToken) : ValueTask.FromResult(Read(buffer.Span));
    }
}

internal sealed class TrackingReadStream(Stream inner, bool checksum) : PrefixReadStream(ReadOnlyMemory<byte>.Empty, inner)
{
    private readonly Crc32? _crc = checksum ? new Crc32() : null;
    private readonly byte[] _tail = new byte[8];
    public long Bytes { get; private set; }
    public uint Checksum => _crc!.GetCurrentHashAsUInt32();
    public ReadOnlySpan<byte> Tail => _tail.AsSpan(0, (int)Math.Min(Bytes, 8));

    private int Track(ReadOnlySpan<byte> bytes)
    {
        if (_crc is not null) _crc.Append(bytes);
        else if (bytes.Length >= 8) bytes[^8..].CopyTo(_tail);
        else
        {
            var retained = (int)Math.Min(Bytes, 8 - bytes.Length);
            _tail.AsSpan((int)Math.Min(Bytes, 8) - retained, retained).CopyTo(_tail);
            bytes.CopyTo(_tail.AsSpan(retained));
        }
        Bytes = checked(Bytes + bytes.Length);
        return bytes.Length;
    }

    public override int Read(Span<byte> buffer) => Track(buffer[..Inner.Read(buffer)]);
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var count = await Inner.ReadAsync(buffer, cancellationToken);
        return Track(buffer.Span[..count]);
    }
}
