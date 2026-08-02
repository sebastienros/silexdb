using Silex.Buffers;
using System.Buffers;
using System.Text.Json;

namespace Silex;

internal sealed class OwnedByteSlice : IDisposable
{
    private IMemoryOwner<byte>? _owner;
    private ByteSlice _slice;
    private bool _disposed;

    private OwnedByteSlice(IMemoryOwner<byte>? owner, ByteSlice slice)
    {
        _owner = owner;
        _slice = slice;
    }

    public ByteSlice Slice
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _slice;
        }
    }

    public ReadOnlySpan<byte> Span => Slice.Span;

    public ReadOnlySpan<byte> AsSpan() => Span;

    public ReadOnlyMemory<byte> Memory => Slice.Memory;

    public int Length => Slice.Length;

    public bool IsEmpty => Slice.IsEmpty;

    public static OwnedByteSlice CopyFrom(ReadOnlySpan<byte> value)
    {
        if (value.IsEmpty)
        {
            return new OwnedByteSlice(null, ByteSlice.Empty);
        }

        var owner = MemoryOwner<byte>.RentCopy(value);
        return new OwnedByteSlice(owner, ByteSlice.CreateView(owner, 0, value.Length));
    }

    internal static OwnedByteSlice Rent(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (length == 0)
        {
            return new OwnedByteSlice(null, ByteSlice.Empty);
        }

        var owner = MemoryOwner<byte>.Rent(length);
        return new OwnedByteSlice(owner, ByteSlice.CreateView(owner, 0, length));
    }

    internal Span<byte> WritableSpan
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _owner is null ? Span<byte>.Empty : _owner.Memory.Span[.._slice.Length];
        }
    }

    public static OwnedByteSlice CopyFrom(ReadOnlySequence<byte> value)
    {
        if (value.IsSingleSegment)
        {
            return CopyFrom(value.FirstSpan);
        }

        var length = ToInt32Length(value.Length);
        if (length == 0)
        {
            return new OwnedByteSlice(null, ByteSlice.Empty);
        }

        var owner = MemoryOwner<byte>.Rent(length);
        try
        {
            value.CopyTo(owner.Memory.Span);
            return new OwnedByteSlice(owner, ByteSlice.CreateView(owner, 0, length));
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    public static OwnedByteSlice CopyFrom(Stream value)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value.CanSeek)
        {
            return CopyFromSeekableStream(value);
        }

        var stream = RecyclableMemoryStreamFactory.Shared.GetStream();
        try
        {
            value.CopyTo(stream);
            return TakeOwnership(stream);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    public static async ValueTask<OwnedByteSlice> CopyFromAsync(Stream value, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(value);

        if (value.CanSeek)
        {
            return await CopyFromSeekableStreamAsync(value, cancellationToken).ConfigureAwait(false);
        }

        var stream = RecyclableMemoryStreamFactory.Shared.GetStream();
        try
        {
            await value.CopyToAsync(stream, cancellationToken).ConfigureAwait(false);
            return TakeOwnership(stream);
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    public static OwnedByteSlice CopyFrom(in Utf8JsonReader value)
    {
        if (value.TokenType is not (JsonTokenType.PropertyName or JsonTokenType.String or JsonTokenType.Number or JsonTokenType.True or JsonTokenType.False or JsonTokenType.Null))
        {
            throw new InvalidOperationException($"Cannot copy bytes from a {nameof(Utf8JsonReader)} token of type '{value.TokenType}'.");
        }

        if (value.TokenType is (JsonTokenType.PropertyName or JsonTokenType.String) && value.ValueIsEscaped)
        {
            var length = ToInt32Length(value.HasValueSequence ? value.ValueSequence.Length : value.ValueSpan.Length);
            if (length == 0)
            {
                return new OwnedByteSlice(null, ByteSlice.Empty);
            }

            var owner = MemoryOwner<byte>.Rent(length);
            try
            {
                var written = value.CopyString(owner.Memory.Span);
                if (written == 0)
                {
                    owner.Dispose();
                    return new OwnedByteSlice(null, ByteSlice.Empty);
                }

                return new OwnedByteSlice(owner, ByteSlice.CreateView(owner, 0, written));
            }
            catch
            {
                owner.Dispose();
                throw;
            }
        }

        return value.HasValueSequence ? CopyFrom(value.ValueSequence) : CopyFrom(value.ValueSpan);
    }

    public static OwnedByteSlice TakeOwnership(IMemoryOwner<byte> owner, int length)
    {
        ArgumentNullException.ThrowIfNull(owner);
        return new OwnedByteSlice(owner, ByteSlice.CreateView(owner, 0, length));
    }

    public static OwnedByteSlice TakeOwnership(byte[] array, int length)
    {
        ArgumentNullException.ThrowIfNull(array);
        return TakeOwnership(new ArrayOwner(array), length);
    }

    public static OwnedByteSlice TakeOwnership(byte[] array, int start, int length)
    {
        ArgumentNullException.ThrowIfNull(array);
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfNegative(length);

        if (start > array.Length || length > array.Length - start)
        {
            throw new ArgumentOutOfRangeException(nameof(length), length, "The requested slice is outside the array.");
        }

        if (start == 0)
        {
            return TakeOwnership(array, length);
        }

        return CopyFrom(array.AsSpan(start, length));
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _slice = ByteSlice.Empty;
        _owner?.Dispose();
        _owner = null;
    }

    private static OwnedByteSlice CopyFromSeekableStream(Stream value)
    {
        var remaining = value.Length - value.Position;
        if (remaining < 0)
        {
            throw new InvalidOperationException("The stream position is beyond the stream length.");
        }

        if (remaining == 0)
        {
            return new OwnedByteSlice(null, ByteSlice.Empty);
        }

        var length = ToInt32Length(remaining);
        var owner = MemoryOwner<byte>.Rent(length);
        var written = 0;

        try
        {
            while (written < length)
            {
                var read = value.Read(owner.Memory.Span[written..length]);
                if (read == 0)
                {
                    break;
                }

                written += read;
            }

            if (written == 0)
            {
                owner.Dispose();
                return new OwnedByteSlice(null, ByteSlice.Empty);
            }

            return new OwnedByteSlice(owner, ByteSlice.CreateView(owner, 0, written));
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    private static async ValueTask<OwnedByteSlice> CopyFromSeekableStreamAsync(Stream value, CancellationToken cancellationToken)
    {
        var remaining = value.Length - value.Position;
        if (remaining < 0)
        {
            throw new InvalidOperationException("The stream position is beyond the stream length.");
        }

        if (remaining == 0)
        {
            return new OwnedByteSlice(null, ByteSlice.Empty);
        }

        var length = ToInt32Length(remaining);
        var owner = MemoryOwner<byte>.Rent(length);
        var written = 0;

        try
        {
            while (written < length)
            {
                var read = await value.ReadAsync(owner.Memory[written..length], cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    break;
                }

                written += read;
            }

            if (written == 0)
            {
                owner.Dispose();
                return new OwnedByteSlice(null, ByteSlice.Empty);
            }

            return new OwnedByteSlice(owner, ByteSlice.CreateView(owner, 0, written));
        }
        catch
        {
            owner.Dispose();
            throw;
        }
    }

    private static OwnedByteSlice TakeOwnership(Microsoft.IO.RecyclableMemoryStream stream)
    {
        var length = ToInt32Length(stream.Length);
        if (length == 0)
        {
            stream.Dispose();
            return new OwnedByteSlice(null, ByteSlice.Empty);
        }

        var owner = new MemoryStreamOwner(stream);
        return new OwnedByteSlice(owner, ByteSlice.CreateView(owner, 0, length));
    }

    private static int ToInt32Length(long length)
    {
        if (length > int.MaxValue)
        {
            throw new InvalidOperationException("Owned byte slices cannot contain more than Int32.MaxValue bytes.");
        }

        return (int)length;
    }

    private sealed class ArrayOwner : IMemoryOwner<byte>
    {
        private byte[]? _array;

        public ArrayOwner(byte[] array)
        {
            _array = array;
        }

        public Memory<byte> Memory
        {
            get
            {
                var array = _array;
                if (array is null)
                {
                    throw new ObjectDisposedException(nameof(OwnedByteSlice), "The byte array has already been disposed.");
                }

                return array;
            }
        }

        public void Dispose()
        {
            _array = null;
        }
    }
}
