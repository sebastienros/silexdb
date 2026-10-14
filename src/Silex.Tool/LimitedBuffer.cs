using System.Buffers;

namespace Silex.Tool;

internal sealed class LimitedBuffer(int limit) : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer = ArrayPool<byte>.Shared.Rent(Math.Min(4096, limit));
    private int _length;
    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _length);
    public void Clear() => _length = 0;

    public void Advance(int count)
    {
        if (count < 0 || count > Math.Min(_buffer.Length, limit) - _length)
            throw new ArgumentOutOfRangeException(nameof(count));
        _length += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        var required = checked(_length + Math.Max(1, sizeHint));
        if (required > limit) throw new ArgumentException($"Value exceeds the {limit}-byte limit.");
        if (required > _buffer.Length)
        {
            var next = ArrayPool<byte>.Shared.Rent(Math.Min(limit, Math.Max(required, _buffer.Length * 2)));
            _buffer.AsSpan(0, _length).CopyTo(next);
            ArrayPool<byte>.Shared.Return(_buffer);
            _buffer = next;
        }
        return _buffer.AsMemory(_length, Math.Min(_buffer.Length, limit) - _length);
    }

    public Span<byte> GetSpan(int sizeHint = 0) => GetMemory(sizeHint).Span;
    public void Dispose() => ArrayPool<byte>.Shared.Return(_buffer);
}
