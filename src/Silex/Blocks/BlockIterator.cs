using System.Runtime.CompilerServices;

namespace Silex.Blocks;

internal sealed class BlockIterator : IStorageIterator
{
    private readonly Block _block;

    public BlockIterator(Block block)
    {
        _block = block;
    }

    public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var cursor = new Cursor(_block, backwards: false);
        while (cursor.MoveNext())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return cursor.Current;
        }
    }

    public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(ByteSlice from, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var cursor = new Cursor(_block, backwards: false, from.Span);
        while (cursor.MoveNext())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return cursor.Current;
        }
    }

    public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var cursor = new Cursor(_block, backwards: true);
        while (cursor.MoveNext())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return cursor.Current;
        }
    }

    public async IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(ByteSlice from, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var cursor = new Cursor(_block, backwards: true, from.Span);
        while (cursor.MoveNext())
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return cursor.Current;
        }
    }

    internal struct Cursor
    {
        private readonly Block _block;
        private readonly int _step;
        private int _index;
        private ReadOnlyMemory<byte> _key;
        private ReadOnlyMemory<byte> _value;
        private bool _isTombstone;

        public Cursor(Block block, bool backwards, ReadOnlySpan<byte> from = default)
        {
            _block = block;
            _step = backwards ? -1 : 1;
            _index = backwards
                ? (from.IsEmpty ? block.Offsets.Count : block.UpperBound(from))
                : (from.IsEmpty ? -1 : block.LowerBound(from) - 1);
            _key = default;
            _value = default;
            _isTombstone = false;
        }

        public readonly ReadOnlySpan<byte> KeySpan => _key.Span;

        public readonly ReadOnlySpan<byte> ValueSpan => _value.Span;

        public readonly bool IsTombstone => _isTombstone;

        public readonly KeyValuePair<ByteSlice, ByteSlice> Current
        {
            get
            {
                var key = ByteSlice.FromMemory(_key);
                var value = _isTombstone ? ByteSlice.Tombstone : ByteSlice.FromMemory(_value);
                return new KeyValuePair<ByteSlice, ByteSlice>(key, value);
            }
        }

        public bool MoveNext()
        {
            _index += _step;
            if ((uint)_index >= (uint)_block.Offsets.Count)
            {
                return false;
            }

            _block.GetRawEntry(_index, out _key, out _value, out _isTombstone);
            return true;
        }
    }
}
