using Silex.Buffers;
using Silex.Serialization;
using System.Buffers;

namespace Silex.Blocks;

internal sealed class Block : IDisposable
{
    private static readonly IComparer<ByteSlice> _keyComparer = BinaryEncoderFactory<ByteSlice>.BinarySerializer.Comparer;

    private readonly IBlockEncoder _encoder;
    private readonly IMemoryOwner<byte>? _memoryOwner;
    private bool _disposed;
    private int _mvccCleanState; // 0 = unknown, 1 = clean & distinct single-version keys, 2 = general MVCC block

    // Key-window index (see BlockKeyWindows); _keyWindowStart is -1 when the block has none.
    private readonly int _keyWindowStart = -1;
    private readonly int _keyPrefixStart;
    private readonly int _keyPrefixLength;

    internal bool ScanMvccSingleVersionRaw<TArg>(
        long snapshotSequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        ref long count)
    {
        if (Volatile.Read(ref _mvccCleanState) != 1)
        {
            ulong lw = 0;
            var ll = -1;
            var res = false;
            byte[]? gb = null;
            try
            {
                return ScanMvccRaw(default, snapshotSequence, arg, reader, maxEntries, ref count, ref lw, ref ll, ref res, ref gb);
            }
            finally
            {
                if (gb is not null) ArrayPool<byte>.Shared.Return(gb);
            }
        }

        var span = Memory.Span;
        var entryCount = Offsets.Count;
        var cursor = 0;

        for (var i = 0; i < entryCount; i++)
        {
            var keyLength = Read7BitLength(span, ref cursor);
            var keyStart = cursor;
            cursor += keyLength;
            var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out _);
            var valueStart = cursor;
            cursor += valueLength;

            if (keyLength < 11 || span[keyStart] != 1)
            {
                continue;
            }

            var termPos = keyStart + keyLength - 10;
            var sequence = (long)~System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(span.Slice(termPos + 2, 8));
            if (sequence > snapshotSequence || span[valueStart] == 0)
            {
                continue;
            }

            count++;
            if (!reader(arg, span.Slice(keyStart + 1, keyLength - 11), span.Slice(valueStart + 1, valueLength - 1)) || count >= maxEntries)
            {
                return false;
            }
        }

        return true;
    }

    public Block(IBlockEncoder encoder, IMemoryOwner<byte> blockData, int length, int count, bool hasKeyWindows = false)
        : this(encoder, blockData, blockData.Memory, length, count, hasKeyWindows)
    {
    }

    public Block(IBlockEncoder encoder, ReadOnlyMemory<byte> blockData, int length, int count, bool hasKeyWindows = false)
        : this(encoder, null, blockData, length, count, hasKeyWindows)
    {
    }

    private Block(IBlockEncoder encoder, IMemoryOwner<byte>? memoryOwner, ReadOnlyMemory<byte> blockData, int length, int count, bool hasKeyWindows)
    {
        _encoder = encoder;
        _memoryOwner = memoryOwner;
        Memory = blockData[..length];
        var offsetsStart = length - (count + 1) * sizeof(ushort);
        Offsets = new BlockOffsets(Memory, offsetsStart, count);
        DataEnd = offsetsStart;

        if (hasKeyWindows)
        {
            var windowStart = offsetsStart - count * BlockKeyWindows.WindowLength;
            var prefixLength = windowStart > 0 ? Memory.Span[windowStart - 1] : -1;
            var prefixStart = windowStart - BlockKeyWindows.GetFixedOverhead(prefixLength);
            if (prefixLength < 0 || prefixStart < 0)
            {
                throw new InvalidDataException("The block key-window index does not fit in the block.");
            }

            _keyWindowStart = windowStart;
            _keyPrefixStart = prefixStart;
            _keyPrefixLength = prefixLength;
            DataEnd = prefixStart;
        }
    }

    public ReadOnlyMemory<byte> Memory { get; }
    public BlockOffsets Offsets { get; }

    /// <summary>The byte position where the data section (the encoded entries) ends.</summary>
    internal int DataEnd { get; }

    internal bool HasKeyWindows => _keyWindowStart >= 0;

    internal ReadOnlySpan<byte> KeyPrefix => HasKeyWindows ? Memory.Span.Slice(_keyPrefixStart, _keyPrefixLength) : default;

    internal ulong GetKeyWindow(int index) =>
        System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(Memory.Span.Slice(_keyWindowStart + index * BlockKeyWindows.WindowLength, BlockKeyWindows.WindowLength));

    /// <summary>
    /// Returns a descriptor of a key/value in a block.
    /// </summary>
    /// <param name="offset"></param>
    /// <returns></returns>
    public RecordLocation GetEntry(int offset)
    {
        return _encoder.DecodeEntry(Memory, offset);
    }

    public ReadOnlySpan<byte> GetValue(ByteSlice key)
    {
        return TryGetValue(key, out var value) ? value : default;
    }

    /// <summary>
    /// Looks up <paramref name="key"/> and reports whether it is present, distinguishing a genuine miss
    /// from a key stored with an empty value.
    /// </summary>
    public bool TryGetValue(ByteSlice key, out ReadOnlySpan<byte> value)
    {
        return TryGetValue(key, out value, out _);
    }

    public bool TryGetValue(ByteSlice key, out ReadOnlySpan<byte> value, out bool isTombstone)
    {
        var start = 0;
        var end = Offsets.Count - 1;

        while (start <= end)
        {
            var m = start + (end - start) / 2;

            var entry = GetEntry(Offsets[m]);

            switch (_keyComparer.Compare(key, entry.Key))
            {
                case 0:
                    value = GetValue(entry);
                    isTombstone = entry.IsTombstone;
                    return true;
                case > 0:
                    start = m + 1;
                    break;
                case < 0:
                    end = m - 1;
                    break;
            }
        }

        value = default;
        isTombstone = false;
        return false;
    }

    /// <summary>
    /// Looks up a key by its already-encoded bytes, doing a binary search directly over the block bytes
    /// without materializing a <typeparamref name="ByteSlice"/> per visited entry. This is the zero-allocation
    /// hot path for point lookups; it is correct because key encoders are order-preserving, so a bytewise
    /// comparison of encoded keys matches the typed key comparison. As with the typed overload, a returned
    /// empty span may be a live empty value; use the three-argument overload when tombstone state matters.
    /// </summary>
    public bool TryGetValue(ReadOnlySpan<byte> encodedKey, out ReadOnlySpan<byte> value)
    {
        return TryGetValue(encodedKey, out value, out _);
    }

    public bool TryGetValue(ReadOnlySpan<byte> encodedKey, out ReadOnlySpan<byte> value, out bool isTombstone)
    {
        var span = Memory.Span;
        var offsets = Offsets;

        if (_keyWindowStart >= 0)
        {
            var index = SearchKeyWindows(span, encodedKey, out var found);
            if (found)
            {
                var entryCursor = (int)offsets[index];
                var entryKeyLength = Read7BitLength(span, ref entryCursor);
                entryCursor += entryKeyLength;
                var entryValueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref entryCursor), out isTombstone);
                value = span.Slice(entryCursor, entryValueLength);
                return true;
            }

            value = default;
            isTombstone = false;
            return false;
        }

        var start = 0;
        var end = offsets.Count - 1;
        var hasPrefix = encodedKey.Length >= 8;
        var targetPrefix = hasPrefix ? System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(encodedKey) : 0UL;

        while (start <= end)
        {
            var m = start + (end - start) / 2;
            var cursor = (int)offsets[m];
            var keyLength = Read7BitLength(span, ref cursor);

            int cmp;
            if (hasPrefix && keyLength >= 8)
            {
                var entryPrefix = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(span.Slice(cursor, 8));
                cmp = targetPrefix != entryPrefix
                    ? (targetPrefix < entryPrefix ? -1 : 1)
                    : encodedKey[8..].SequenceCompareTo(span.Slice(cursor + 8, keyLength - 8));
            }
            else
            {
                cmp = encodedKey.SequenceCompareTo(span.Slice(cursor, keyLength));
            }

            if (cmp == 0)
            {
                cursor += keyLength;
                var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out isTombstone);
                value = span.Slice(cursor, valueLength);
                return true;
            }

            if (cmp > 0)
            {
                start = m + 1;
            }
            else
            {
                end = m - 1;
            }
        }

        value = default;
        isTombstone = false;
        return false;
    }

    /// <summary>
    /// Like <see cref="ForEachRaw{TArg}"/> but starts at the first entry whose encoded key is greater than or
    /// equal to <paramref name="encodedFrom"/> (a lower-bound seek). The block bytes are searched directly,
    /// without materializing a <typeparamref name="ByteSlice"/> per visited entry; this is correct because key
    /// encoders are order-preserving, so a bytewise comparison of encoded keys matches the typed comparison.
    /// When no entry in the block reaches <paramref name="encodedFrom"/> the callback is never invoked and the
    /// method returns <c>true</c> so the caller can continue into the next block.
    /// </summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static int Read7BitLength(ReadOnlySpan<byte> span, ref int cursor)
    {
        var b0 = span[cursor++];
        if (b0 <= 0x7Fu)
        {
            return b0;
        }
        var b1 = span[cursor++];
        var result = (b0 & 0x7Fu) | ((b1 & 0x7Fu) << 7);
        if (b1 <= 0x7Fu)
        {
            return (int)result;
        }
        return Read7BitLengthSlow(span, ref cursor, result);
    }

    private static int Read7BitLengthSlow(ReadOnlySpan<byte> span, ref int cursor, uint result)
    {
        for (var shift = 14; shift < 28; shift += 7)
        {
            var b = span[cursor++];
            result |= (b & 0x7Fu) << shift;
            if (b <= 0x7Fu)
            {
                return (int)result;
            }
        }
        var b4 = span[cursor++];
        if (b4 > 0b_1111u)
        {
            throw new OverflowException();
        }
        return (int)(result | ((uint)b4 << 28));
    }

    internal bool TryReadFirstMatchingPrefix<TSink>(
        ReadOnlySpan<byte> from,
        ReadOnlySpan<byte> prefix,
        ref TSink sink)
        where TSink : struct, IRawEntrySink
    {
        var memory = Memory;
        var span = memory.Span;
        var count = Offsets.Count;
        var start = LowerBound(memory, from);
        if (start >= count)
        {
            return true; // continue to next block
        }

        var cursor = (int)Offsets[start];
        for (var i = start; i < count; i++)
        {
            var keyLength = Read7BitLength(span, ref cursor);
            var key = span.Slice(cursor, keyLength);
            cursor += keyLength;
            var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out var isTombstone);
            var value = span.Slice(cursor, valueLength);
            cursor += valueLength;

            if (!key.StartsWith(prefix))
            {
                return false; // past prefix, stop searching
            }
            if (!isTombstone)
            {
                sink.Accept(key, value);
                return false; // found newest live version, stop searching
            }
        }

        return true; // all matching entries in this block were physical tombstones, check next block
    }

    internal bool ForEachRawFrom<TArg>(ReadOnlySpan<byte> encodedFrom, TArg arg, ReadRawEntryAction<TArg> reader, bool skipTombstones)
    {
        var memory = Memory;
        var span = memory.Span;
        var count = Offsets.Count;
        var start = LowerBound(memory, encodedFrom);
        if (start >= count)
        {
            return true;
        }

        var cursor = (int)Offsets[start];
        for (var i = start; i < count; i++)
        {
            var keyLength = Read7BitLength(span, ref cursor);
            var key = span.Slice(cursor, keyLength);
            cursor += keyLength;
            var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out var isTombstone);
            var value = span.Slice(cursor, valueLength);
            cursor += valueLength;

            if (skipTombstones && isTombstone)
            {
                continue;
            }

            if (!reader(arg, key, value))
            {
                return false;
            }
        }

        return true;
    }

    internal bool TryGetFirstRawFrom(
        ReadOnlySpan<byte> encodedFrom,
        out ReadOnlySpan<byte> key,
        out ReadOnlySpan<byte> value,
        bool skipTombstones)
    {
        var memory = Memory;
        var start = encodedFrom.IsEmpty ? 0 : LowerBound(memory, encodedFrom);

        for (var i = start; i < Offsets.Count; i++)
        {
            var blockReader = new EncoderBinaryReader(memory, Offsets[i]);
            var keyLength = blockReader.Read7BitEncodedInt();
            var candidateKey = blockReader.ReadBytesSpan(keyLength);
            var valueLength = RecordValueEncoding.DecodeLength(blockReader.Read7BitEncodedInt(), out var isTombstone);
            var candidateValue = blockReader.ReadBytesSpan(valueLength);

            if (skipTombstones && isTombstone)
            {
                continue;
            }

            key = candidateKey;
            value = candidateValue;
            return true;
        }

        key = default;
        value = default;
        return false;
    }

    /// <summary>
    /// Returns the index in <see cref="Offsets"/> of the first entry whose encoded key is greater than or
    /// equal to <paramref name="encodedKey"/>, or <see cref="BlockOffsets.Count"/> when every key is smaller.
    /// </summary>
    internal int LowerBound(ReadOnlySpan<byte> encodedKey)
    {
        return LowerBound(Memory, encodedKey);
    }

    internal int UpperBound(ReadOnlySpan<byte> encodedKey)
    {
        var span = Memory.Span;
        var offsets = Offsets;

        if (_keyWindowStart >= 0)
        {
            // Keys are unique within a block, so the upper bound is just past an exact match.
            var index = SearchKeyWindows(span, encodedKey, out var found);
            return found ? index + 1 : index;
        }

        var start = 0;
        var end = offsets.Count;
        var hasPrefix = encodedKey.Length >= 8;
        var targetPrefix = hasPrefix ? System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(encodedKey) : 0UL;

        while (start < end)
        {
            var m = start + (end - start) / 2;
            var cursor = (int)offsets[m];
            var keyLength = Read7BitLength(span, ref cursor);

            int cmp;
            if (hasPrefix && keyLength >= 8)
            {
                var entryPrefix = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(span.Slice(cursor, 8));
                cmp = entryPrefix != targetPrefix
                    ? (entryPrefix < targetPrefix ? -1 : 1)
                    : span.Slice(cursor + 8, keyLength - 8).SequenceCompareTo(encodedKey[8..]);
            }
            else
            {
                cmp = span.Slice(cursor, keyLength).SequenceCompareTo(encodedKey);
            }

            if (cmp <= 0)
            {
                start = m + 1;
            }
            else
            {
                end = m;
            }
        }

        return start;
    }

    internal void GetRawEntry(int index, out ReadOnlyMemory<byte> key, out ReadOnlyMemory<byte> value, out bool isTombstone)
    {
        _encoder.DecodeRawEntry(Memory, Offsets[index], out key, out value, out isTombstone);
    }

    private int LowerBound(ReadOnlyMemory<byte> memory, ReadOnlySpan<byte> encodedKey)
    {
        var span = memory.Span;
        var offsets = Offsets;

        if (_keyWindowStart >= 0)
        {
            return SearchKeyWindows(span, encodedKey, out _);
        }

        var start = 0;
        var end = offsets.Count;
        var hasPrefix = encodedKey.Length >= 8;
        var targetPrefix = hasPrefix ? System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(encodedKey) : 0UL;

        while (start < end)
        {
            var m = start + (end - start) / 2;
            var cursor = (int)offsets[m];
            var keyLength = Read7BitLength(span, ref cursor);

            int cmp;
            if (hasPrefix && keyLength >= 8)
            {
                var entryPrefix = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(span.Slice(cursor, 8));
                cmp = entryPrefix != targetPrefix
                    ? (entryPrefix < targetPrefix ? -1 : 1)
                    : span.Slice(cursor + 8, keyLength - 8).SequenceCompareTo(encodedKey[8..]);
            }
            else
            {
                cmp = span.Slice(cursor, keyLength).SequenceCompareTo(encodedKey);
            }

            if (cmp < 0)
            {
                start = m + 1;
            }
            else
            {
                end = m;
            }
        }

        return start;
    }

    /// <summary>
    /// Lower-bound search over the key-window index: returns the index of the first entry whose key is greater
    /// than or equal to <paramref name="encodedKey"/> (or the entry count), and whether that entry equals it.
    /// Entries are only decoded when their window ties with the key's window.
    /// </summary>
    private int SearchKeyWindows(ReadOnlySpan<byte> span, ReadOnlySpan<byte> encodedKey, out bool found)
    {
        found = false;
        var count = Offsets.Count;
        var prefixLength = _keyPrefixLength;

        if (prefixLength > 0)
        {
            // Every key of the block starts with the shared prefix, so a key that sorts outside it sorts before or
            // after the whole block.
            var compared = Math.Min(prefixLength, encodedKey.Length);
            var prefixCmp = encodedKey[..compared].SequenceCompareTo(span.Slice(_keyPrefixStart, compared));
            if (prefixCmp != 0)
            {
                return prefixCmp < 0 ? 0 : count;
            }

            if (encodedKey.Length < prefixLength)
            {
                return 0;
            }
        }

        var target = BlockKeyWindows.GetWindow(encodedKey, prefixLength);
        var windows = span.Slice(_keyWindowStart, count * BlockKeyWindows.WindowLength);

        // First window >= target.
        var start = 0;
        var length = count;
        while (length > 0)
        {
            var half = length >> 1;
            if (ReadWindow(windows, start + half) < target)
            {
                start += half + 1;
                length -= half + 1;
            }
            else
            {
                length = half;
            }
        }

        if (start == count || ReadWindow(windows, start) != target)
        {
            return start;
        }

        // Windows tie: resolve with full keys, past the shared prefix. Usually a single entry ties.
        var end = start + 1;
        if (end < count && ReadWindow(windows, end) == target)
        {
            // First window > target.
            length = count - end;
            while (length > 0)
            {
                var half = length >> 1;
                if (ReadWindow(windows, end + half) <= target)
                {
                    end += half + 1;
                    length -= half + 1;
                }
                else
                {
                    length = half;
                }
            }
        }

        while (start < end)
        {
            var m = start + (end - start) / 2;
            var cursor = (int)Offsets[m];
            var keyLength = Read7BitLength(span, ref cursor);
            var cmp = span.Slice(cursor + prefixLength, keyLength - prefixLength).SequenceCompareTo(encodedKey[prefixLength..]);
            if (cmp == 0)
            {
                found = true;
                return m;
            }

            if (cmp < 0)
            {
                start = m + 1;
            }
            else
            {
                end = m;
            }
        }

        return start;

        static ulong ReadWindow(ReadOnlySpan<byte> windows, int index) =>
            System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(windows.Slice(index * BlockKeyWindows.WindowLength, BlockKeyWindows.WindowLength));
    }

    internal bool ScanMvccRaw<TArg>(
        ReadOnlySpan<byte> encodedFrom,
        long snapshotSequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        ref long count,
        ref ulong lastWord,
        ref int lastLen,
        ref bool resolved,
        ref byte[]? groupBuf)
    {
        var memory = Memory;
        var span = memory.Span;
        var entryCount = Offsets.Count;
        var start = encodedFrom.IsEmpty ? 0 : LowerBound(memory, encodedFrom);
        if (start >= entryCount)
        {
            return true;
        }

        var cursor = (int)Offsets[start];
        var prevPrefixStart = -1;
        var prevPrefixLen = -1;
        var cleanState = Volatile.Read(ref _mvccCleanState);
        var observedEscapeOrDuplicate = false;

        for (var i = start; i < entryCount; i++)
        {
            var keyLength = Read7BitLength(span, ref cursor);
            var keyStart = cursor;
            cursor += keyLength;
            var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out var isTombstone);
            var valueStart = cursor;
            cursor += valueLength;

            if (isTombstone || keyLength < 11 || span[keyStart] != 1)
            {
                continue;
            }

            var termPos = keyStart + keyLength - 10;
            if (span[termPos] != 0 || span[termPos + 1] != 0)
            {
                throw new InvalidDataException("An MVCC data key has an invalid terminator.");
            }

            var uStart = keyStart + 1;
            var uLen = keyLength - 11;
            var encodedUserKey = span.Slice(uStart, uLen);

            var encodedSequence = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(span.Slice(termPos + 2, 8));
            var decodedSequence = ~encodedSequence;
            if (decodedSequence > long.MaxValue)
            {
                throw new InvalidDataException("An MVCC data key has an invalid sequence.");
            }
            var sequence = (long)decodedSequence;

            var firstWord = uLen >= 8
                ? System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(encodedUserKey)
                : 0UL;

            bool sameGroup;
            if (uLen == lastLen && firstWord == lastWord)
            {
                if (uLen <= 8)
                {
                    sameGroup = true;
                }
                else if (prevPrefixStart >= 0 && uLen == prevPrefixLen)
                {
                    sameGroup = encodedUserKey.SequenceEqual(span.Slice(prevPrefixStart, prevPrefixLen));
                }
                else
                {
                    sameGroup = groupBuf is not null && encodedUserKey.SequenceEqual(groupBuf.AsSpan(0, uLen));
                }
            }
            else
            {
                sameGroup = false;
            }

            if (sameGroup)
            {
                observedEscapeOrDuplicate = true;
                if (resolved)
                {
                    continue;
                }
            }
            else
            {
                resolved = false;
                lastLen = uLen;
                lastWord = firstWord;
                prevPrefixStart = uStart;
                prevPrefixLen = uLen;
            }

            if (sequence > snapshotSequence)
            {
                continue;
            }

            if (valueLength == 0 || span[valueStart] > 1)
            {
                throw new InvalidDataException("An MVCC value has an invalid kind marker.");
            }
            resolved = true;
            if (span[valueStart] == 0)
            {
                continue;
            }

            count++;
            var valueSlice = span.Slice(valueStart + 1, valueLength - 1);
            bool keepGoing;
            if (cleanState == 1)
            {
                keepGoing = reader(arg, encodedUserKey, valueSlice);
            }
            else
            {
                if (cleanState == 0 && !observedEscapeOrDuplicate && encodedUserKey.IndexOf((byte)0) >= 0)
                {
                    observedEscapeOrDuplicate = true;
                }
                keepGoing = !observedEscapeOrDuplicate && cleanState == 0
                    ? reader(arg, encodedUserKey, valueSlice)
                    : MvccStorage.InvokeUserReader(arg, reader, encodedUserKey, valueSlice);
            }
            if (!keepGoing || count >= maxEntries)
            {
                return false;
            }
        }

        if (cleanState == 0 && start == 0)
        {
            Volatile.Write(ref _mvccCleanState, observedEscapeOrDuplicate ? 2 : 1);
        }

        // Persist the last user key across block boundaries only if it is > 8 bytes.
        if (prevPrefixStart >= 0 && prevPrefixLen > 8)
        {
            if (groupBuf is null || groupBuf.Length < prevPrefixLen)
            {
                if (groupBuf is not null)
                {
                    ArrayPool<byte>.Shared.Return(groupBuf);
                }
                groupBuf = ArrayPool<byte>.Shared.Rent(Math.Max(64, prevPrefixLen));
            }
            span.Slice(prevPrefixStart, prevPrefixLen).CopyTo(groupBuf);
        }

        return true;
    }

    internal bool ScanMvccSingleVersionReverseRaw<TArg>(
        long snapshotSequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        ref long count)
    {
        if (Volatile.Read(ref _mvccCleanState) != 1)
        {
            return false;
        }

        var span = Memory.Span;
        var offsets = Offsets;

        for (var i = offsets.Count - 1; i >= 0; i--)
        {
            var cursor = (int)offsets[i];
            var keyLength = Read7BitLength(span, ref cursor);
            var keyStart = cursor;
            cursor += keyLength;
            var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out _);
            var valueStart = cursor;

            if (keyLength < 11 || span[keyStart] != 1)
            {
                continue;
            }

            var termPos = keyStart + keyLength - 10;
            var sequence = (long)~System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(span.Slice(termPos + 2, 8));
            if (sequence > snapshotSequence || span[valueStart] == 0)
            {
                continue;
            }

            count++;
            if (!reader(arg, span.Slice(keyStart + 1, keyLength - 11), span.Slice(valueStart + 1, valueLength - 1)) || count >= maxEntries)
            {
                return true;
            }
        }

        return true;
    }

    internal bool ScanMvccReverseRaw<TArg>(
        long snapshotSequence,
        TArg arg,
        ReadRawEntryAction<TArg> reader,
        long maxEntries,
        ref long count,
        ref ulong lastWord,
        ref int lastLen,
        ref bool hasCandidate,
        ref bool candidateLive,
        ref byte[]? keyBuf,
        ref byte[]? valBuf,
        ref int valLen)
    {
        var span = Memory.Span;
        var offsets = Offsets;
        var cleanState = Volatile.Read(ref _mvccCleanState);

        for (var i = offsets.Count - 1; i >= 0; i--)
        {
            var cursor = (int)offsets[i];
            var keyLength = Read7BitLength(span, ref cursor);
            var keyStart = cursor;
            cursor += keyLength;
            var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out var isTombstone);
            var valueStart = cursor;

            if (isTombstone || keyLength < 11 || span[keyStart] != 1)
            {
                continue;
            }

            var termPos = keyStart + keyLength - 10;
            if (span[termPos] != 0 || span[termPos + 1] != 0)
            {
                throw new InvalidDataException("An MVCC data key has an invalid terminator.");
            }

            var uStart = keyStart + 1;
            var uLen = keyLength - 11;
            var encodedUserKey = span.Slice(uStart, uLen);

            var encodedSequence = System.Buffers.Binary.BinaryPrimitives.ReadUInt64BigEndian(span.Slice(termPos + 2, 8));
            var decodedSequence = ~encodedSequence;
            if (decodedSequence > long.MaxValue)
            {
                throw new InvalidDataException("An MVCC data key has an invalid sequence.");
            }
            var sequence = (long)decodedSequence;

            var firstWord = uLen >= 8
                ? System.Buffers.Binary.BinaryPrimitives.ReadUInt64LittleEndian(encodedUserKey)
                : 0UL;

            var sameGroup = uLen == lastLen &&
                firstWord == lastWord &&
                (uLen <= 8 || (keyBuf is not null && encodedUserKey.SequenceEqual(keyBuf.AsSpan(0, uLen))));

            if (!sameGroup)
            {
                if (hasCandidate && candidateLive)
                {
                    hasCandidate = false;
                    count++;
                    var userKeySpan = keyBuf.AsSpan(0, lastLen);
                    var userValSpan = valBuf.AsSpan(0, valLen);
                    var keepGoing = cleanState == 1
                        ? reader(arg, userKeySpan, userValSpan)
                        : MvccStorage.InvokeUserReader(arg, reader, userKeySpan, userValSpan);
                    if (!keepGoing || count >= maxEntries)
                    {
                        return false;
                    }
                }
                hasCandidate = false;
                candidateLive = false;
                lastLen = uLen;
                lastWord = firstWord;
                if (keyBuf is null || keyBuf.Length < uLen)
                {
                    if (keyBuf is not null) ArrayPool<byte>.Shared.Return(keyBuf);
                    keyBuf = ArrayPool<byte>.Shared.Rent(Math.Max(64, uLen));
                }
                encodedUserKey.CopyTo(keyBuf);
            }

            if (sequence > snapshotSequence)
            {
                continue;
            }

            if (valueLength == 0 || span[valueStart] > 1)
            {
                throw new InvalidDataException("An MVCC value has an invalid kind marker.");
            }
            hasCandidate = true;
            candidateLive = span[valueStart] == 1;
            if (candidateLive)
            {
                valLen = valueLength - 1;
                if (valBuf is null || valBuf.Length < valLen)
                {
                    if (valBuf is not null) ArrayPool<byte>.Shared.Return(valBuf);
                    valBuf = ArrayPool<byte>.Shared.Rent(Math.Max(256, valLen));
                }
                span.Slice(valueStart + 1, valLen).CopyTo(valBuf);
            }
        }

        return true;
    }

    internal bool ForEachReverseRaw<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, bool skipTombstones)
    {
        var span = Memory.Span;
        var offsets = Offsets;

        for (var i = offsets.Count - 1; i >= 0; i--)
        {
            var cursor = (int)offsets[i];
            var keyLength = Read7BitLength(span, ref cursor);
            var key = span.Slice(cursor, keyLength);
            cursor += keyLength;
            var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out var isTombstone);
            var value = span.Slice(cursor, valueLength);

            if (skipTombstones && isTombstone)
            {
                continue;
            }

            if (!reader(arg, key, value))
            {
                return false;
            }
        }

        return true;
    }

    internal bool ForEachRaw<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, bool skipTombstones, long maxEntries, ref long count)
    {
        var span = Memory.Span;
        var entryCount = Offsets.Count;
        var cursor = 0;

        for (var i = 0; i < entryCount; i++)
        {
            var keyLength = Read7BitLength(span, ref cursor);
            var key = span.Slice(cursor, keyLength);
            cursor += keyLength;
            var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out var isTombstone);
            var value = span.Slice(cursor, valueLength);
            cursor += valueLength;

            if (skipTombstones && isTombstone)
            {
                continue;
            }

            count++;
            if (!reader(arg, key, value) || count >= maxEntries)
            {
                return false;
            }
        }

        return true;
    }

    internal bool ForEachRaw<TArg>(TArg arg, ReadRawEntryAction<TArg> reader, bool skipTombstones)
    {
        var span = Memory.Span;
        var count = Offsets.Count;
        var cursor = 0;

        for (var i = 0; i < count; i++)
        {
            var keyLength = Read7BitLength(span, ref cursor);
            var key = span.Slice(cursor, keyLength);
            cursor += keyLength;
            var valueLength = RecordValueEncoding.DecodeLength(Read7BitLength(span, ref cursor), out var isTombstone);
            var value = span.Slice(cursor, valueLength);
            cursor += valueLength;

            if (skipTombstones && isTombstone)
            {
                continue;
            }

            if (!reader(arg, key, value))
            {
                return false;
            }
        }

        return true;
    }

    /// <param name="entry"></param>
    /// <returns></returns>
    public ReadOnlySpan<byte> GetValue(RecordLocation entry)
    {
        return _encoder.DecodeValue(Memory, entry.BlockOffset, entry.StoredValueLength).Span;
    }

    public ReadOnlyMemory<byte> GetValueMemory(RecordLocation entry)
    {
        return _encoder.DecodeValue(Memory, entry.BlockOffset, entry.StoredValueLength);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        GC.SuppressFinalize(this);
        DisposeInternal();

        _disposed = true;
    }

    private void DisposeInternal()
    {
        _memoryOwner?.Dispose();
    }

    ~Block()
    {
        if (_memoryOwner == null)
        {
            return;
        }

        DisposeInternal();
    }
}
