using Silex.Blocks;
using Silex.Buffers;
using Silex.MemTables;
using Silex.Serialization;
using Silex.Tables;
using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace Silex.Test;

internal static class ByteSliceTestExtensions
{
    public static void Put(this LsmStorageInner storage, byte[] key, byte[] value) => storage.PutRaw(key, value);
    public static void Put(this LsmStorageInner storage, byte[] key, int value) => PutEncodedValue(storage, key, value);
    public static void Put(this LsmStorageInner storage, int key, byte[] value) => PutEncodedKey(storage, key, value);
    public static void Put(this LsmStorageInner storage, int key, int value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorageInner storage, int key, string value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorageInner storage, uint key, byte[] value) => PutEncodedKey(storage, key, value);
    public static void Put(this LsmStorageInner storage, long key, byte[] value) => PutEncodedKey(storage, key, value);
    public static void Put(this LsmStorageInner storage, ulong key, byte[] value) => PutEncodedKey(storage, key, value);
    public static void Put(this LsmStorageInner storage, string key, byte[] value) => PutEncodedKey(storage, key, value);

    public static void Delete(this LsmStorageInner storage, byte[] key) => storage.DeleteRaw(key);
    public static void Delete(this LsmStorageInner storage, int key) => DeleteEncoded(storage, key);
    public static void Delete(this LsmStorageInner storage, uint key) => DeleteEncoded(storage, key);
    public static void Delete(this LsmStorageInner storage, long key) => DeleteEncoded(storage, key);
    public static void Delete(this LsmStorageInner storage, ulong key) => DeleteEncoded(storage, key);
    public static void Delete(this LsmStorageInner storage, string key) => DeleteEncoded(storage, key);

    public static ValueTask<OwnedByteSlice?> GetAsync(this LsmStorageInner storage, byte[] key, CancellationToken cancellationToken = default)
    {
        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(storage.GetAsync(ownedKey.Slice, cancellationToken), ownedKey);
    }

    public static ValueTask<OwnedByteSlice?> GetAsync(this LsmStorageInner storage, int key, CancellationToken cancellationToken = default) => GetEncodedAsync(storage, key, cancellationToken);
    public static ValueTask<OwnedByteSlice?> GetAsync(this LsmStorageInner storage, uint key, CancellationToken cancellationToken = default) => GetEncodedAsync(storage, key, cancellationToken);
    public static ValueTask<OwnedByteSlice?> GetAsync(this LsmStorageInner storage, long key, CancellationToken cancellationToken = default) => GetEncodedAsync(storage, key, cancellationToken);
    public static ValueTask<OwnedByteSlice?> GetAsync(this LsmStorageInner storage, ulong key, CancellationToken cancellationToken = default) => GetEncodedAsync(storage, key, cancellationToken);
    public static ValueTask<OwnedByteSlice?> GetAsync(this LsmStorageInner storage, string key, CancellationToken cancellationToken = default) => GetEncodedAsync(storage, key, cancellationToken);

    public static ValueTask<bool> TryGetRawAsync(this LsmStorageInner storage, byte[] key, IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
    {
        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(storage.TryGetRawAsync(ownedKey.Slice, destination, cancellationToken), ownedKey);
    }

    public static ValueTask<int> GetRawAsync(this LsmStorageInner storage, byte[] key, Memory<byte> destination, CancellationToken cancellationToken = default)
    {
        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(storage.GetRawAsync(ownedKey.Slice, destination, cancellationToken), ownedKey);
    }

    public static ValueTask<bool> TryReadRawAsync<TArg>(this LsmStorageInner storage, byte[] key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
    {
        var ownedKey = OwnedByteSlice.CopyFrom(key);
        return DisposeKeyAsync(storage.TryReadRawAsync(ownedKey.Slice, arg, reader, cancellationToken), ownedKey);
    }

    public static ValueTask<long> SeekRawAsync<TArg>(this LsmStorageInner storage, byte[] from, TArg arg, ReadRawEntryAction<TArg> reader, long maxEntries = long.MaxValue, CancellationToken cancellationToken = default)
    {
        var ownedFrom = OwnedByteSlice.CopyFrom(from);
        return DisposeKeyAsync(storage.SeekRawAsync(ownedFrom.Slice, arg, reader, maxEntries, cancellationToken), ownedFrom);
    }

    public static bool TryGet(this IMemTable memTable, int key, [MaybeNullWhen(false)] out ByteSlice result)
    {
        using var ownedKey = Encode(key);
        return memTable.TryGet(ownedKey.Slice, out result);
    }

    public static bool Add(this BlockBuilder builder, int key, int value) => builder.Add(Slice(key), Slice(value));
    public static bool Add(this BlockBuilder builder, int key, string value) => builder.Add(Slice(key), Slice(value));
    public static bool Add(this BlockBuilder builder, int key, byte[] value) => builder.Add(Slice(key), Slice(value));
    public static bool Add(this BlockBuilder builder, ushort key, string value) => builder.Add(Slice(key), Slice(value));
    public static bool Add(this BlockBuilder builder, uint key, byte[] value) => builder.Add(Slice(key), Slice(value));

    public static Task AddAsync(this ISsTableBuilder builder, ushort key, string value, CancellationToken cancellationToken = default) => builder.AddAsync(Slice(key), Slice(value), cancellationToken);
    public static Task AddAsync(this ISsTableBuilder builder, uint key, byte[] value, CancellationToken cancellationToken = default) => builder.AddAsync(Slice(key), Slice(value), cancellationToken);
    public static Task AddAsync(this ISsTableBuilder builder, int key, int value, CancellationToken cancellationToken = default) => builder.AddAsync(Slice(key), Slice(value), cancellationToken);

    public static ReadOnlySpan<byte> GetValue(this Block block, int key) => block.GetValue(Slice(key));
    public static IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(this BlockIterator iterator, int from, CancellationToken cancellationToken = default) => iterator.EnumerateAsync(Slice(from), cancellationToken);
    public static IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(this IStorageIterator iterator, int from, CancellationToken cancellationToken = default) => iterator.EnumerateAsync(Slice(from), cancellationToken);
    public static IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateAsync(this SsTableIterator iterator, uint from, CancellationToken cancellationToken = default) => iterator.EnumerateAsync(Slice(from), cancellationToken);
    public static IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(this BlockIterator iterator, int from, CancellationToken cancellationToken = default) => iterator.EnumerateBackwardsAsync(Slice(from), cancellationToken);
    public static IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(this IStorageIterator iterator, int from, CancellationToken cancellationToken = default) => iterator.EnumerateBackwardsAsync(Slice(from), cancellationToken);
    public static IAsyncEnumerable<KeyValuePair<ByteSlice, ByteSlice>> EnumerateBackwardsAsync(this SsTableIterator iterator, uint from, CancellationToken cancellationToken = default) => iterator.EnumerateBackwardsAsync(Slice(from), cancellationToken);

    public static List<KeyValuePair<ByteSlice, ByteSlice>> SnapshotList(this IEnumerable<KeyValuePair<ByteSlice, ByteSlice>> entries)
    {
        var snapshot = new List<KeyValuePair<ByteSlice, ByteSlice>>();

        foreach (var entry in entries)
        {
            snapshot.Add(new KeyValuePair<ByteSlice, ByteSlice>(
                ByteSlice.FromMemory(entry.Key.Span.ToArray()),
                ByteSlice.FromMemory(entry.Value.Span.ToArray())));
        }

        return snapshot;
    }

    public static ByteSlice Slice(byte[] value) => ByteSlice.FromMemory(value);
    public static ByteSlice Slice(int value) => SliceEncoded(value);
    public static ByteSlice Slice(uint value) => SliceEncoded(value);
    public static ByteSlice Slice(long value) => SliceEncoded(value);
    public static ByteSlice Slice(ulong value) => SliceEncoded(value);
    public static ByteSlice Slice(ushort value) => SliceEncoded(value);
    public static ByteSlice Slice(string value) => SliceEncoded(value);

    private static void PutEncodedKey<TKey>(LsmStorageInner storage, TKey key, ReadOnlySpan<byte> value)
    {
        using var ownedKey = Encode(key);
        storage.PutRaw(ownedKey.Span, value);
    }

    private static void PutEncodedValue<TValue>(LsmStorageInner storage, ReadOnlySpan<byte> key, TValue value)
    {
        using var ownedValue = Encode(value);
        storage.PutRaw(key, ownedValue.Span);
    }

    private static void PutEncoded<TKey, TValue>(LsmStorageInner storage, TKey key, TValue value)
    {
        using var ownedKey = Encode(key);
        using var ownedValue = Encode(value);
        storage.PutRaw(ownedKey.Span, ownedValue.Span);
    }

    private static void DeleteEncoded<TKey>(LsmStorageInner storage, TKey key)
    {
        using var ownedKey = Encode(key);
        storage.DeleteRaw(ownedKey.Span);
    }

    private static ValueTask<OwnedByteSlice?> GetEncodedAsync<TKey>(LsmStorageInner storage, TKey key, CancellationToken cancellationToken)
    {
        var ownedKey = Encode(key);
        return DisposeKeyAsync(storage.GetAsync(ownedKey.Slice, cancellationToken), ownedKey);
    }

    private static async ValueTask<OwnedByteSlice?> DisposeKeyAsync(ValueTask<OwnedByteSlice?> result, OwnedByteSlice ownedKey)
    {
        try
        {
            return await result;
        }
        finally
        {
            ownedKey.Dispose();
        }
    }

    private static async ValueTask<bool> DisposeKeyAsync(ValueTask<bool> result, OwnedByteSlice ownedKey)
    {
        try
        {
            return await result;
        }
        finally
        {
            ownedKey.Dispose();
        }
    }

    private static async ValueTask<int> DisposeKeyAsync(ValueTask<int> result, OwnedByteSlice ownedKey)
    {
        try
        {
            return await result;
        }
        finally
        {
            ownedKey.Dispose();
        }
    }

    private static async ValueTask<long> DisposeKeyAsync(ValueTask<long> result, OwnedByteSlice ownedKey)
    {
        try
        {
            return await result;
        }
        finally
        {
            ownedKey.Dispose();
        }
    }

    private static ByteSlice SliceEncoded<T>(T value)
    {
        using var owned = Encode(value);
        return ByteSlice.FromMemory(owned.Span.ToArray());
    }

    private static OwnedByteSlice Encode<T>(T value)
    {
        var encoder = BinaryEncoderFactory<T>.BinarySerializer;
        using var bufferWriter = new PooledArrayBufferWriter<byte>(Math.Max(1, encoder.GetLength(value)));
        var writer = new EncoderBinaryWriter(bufferWriter);
        encoder.Encode(value, ref writer);
        writer.Flush();
        return OwnedByteSlice.CopyFrom(bufferWriter.WrittenMemory.Span);
    }
}
