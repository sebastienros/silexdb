using System.Buffers;
using System.Text.Json;

namespace Silex;

public static class LsmStorageCopyExtensions
{
    public static void Put(this LsmStorage storage, ReadOnlySpan<byte> key, Stream value)
    {
        using var ownedValue = OwnedByteSlice.CopyFrom(value);
        storage.Put(key, ownedValue.Span);
    }

    public static async ValueTask PutAsync(this LsmStorage storage, ReadOnlyMemory<byte> key, Stream value, CancellationToken cancellationToken = default)
    {
        using var ownedValue = await OwnedByteSlice.CopyFromAsync(value, cancellationToken).ConfigureAwait(false);
        storage.Put(key.Span, ownedValue.Span);
    }

    public static void Put(this LsmStorage storage, ReadOnlySpan<byte> key, ReadOnlySequence<byte> value)
    {
        using var ownedValue = OwnedByteSlice.CopyFrom(value);
        storage.Put(key, ownedValue.Span);
    }

    public static void Put(this LsmStorage storage, ReadOnlySpan<byte> key, in Utf8JsonReader value)
    {
        using var ownedValue = OwnedByteSlice.CopyFrom(in value);
        storage.Put(key, ownedValue.Span);
    }

    public static void Put(this LsmStorage storage, int key, Stream value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);
    public static void Put(this LsmStorage storage, uint key, Stream value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);
    public static void Put(this LsmStorage storage, long key, Stream value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);
    public static void Put(this LsmStorage storage, ulong key, Stream value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);
    public static void Put(this LsmStorage storage, string key, Stream value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);

    public static ValueTask PutAsync(this LsmStorage storage, int key, Stream value, CancellationToken cancellationToken = default) => PutEncodedKeyAsync(storage, LsmStorageTypedExtensions.EncodeKey(key), value, cancellationToken);
    public static ValueTask PutAsync(this LsmStorage storage, uint key, Stream value, CancellationToken cancellationToken = default) => PutEncodedKeyAsync(storage, LsmStorageTypedExtensions.EncodeKey(key), value, cancellationToken);
    public static ValueTask PutAsync(this LsmStorage storage, long key, Stream value, CancellationToken cancellationToken = default) => PutEncodedKeyAsync(storage, LsmStorageTypedExtensions.EncodeKey(key), value, cancellationToken);
    public static ValueTask PutAsync(this LsmStorage storage, ulong key, Stream value, CancellationToken cancellationToken = default) => PutEncodedKeyAsync(storage, LsmStorageTypedExtensions.EncodeKey(key), value, cancellationToken);
    public static ValueTask PutAsync(this LsmStorage storage, string key, Stream value, CancellationToken cancellationToken = default) => PutEncodedKeyAsync(storage, LsmStorageTypedExtensions.EncodeKey(key), value, cancellationToken);

    public static void Put(this LsmStorage storage, int key, ReadOnlySequence<byte> value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);
    public static void Put(this LsmStorage storage, uint key, ReadOnlySequence<byte> value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);
    public static void Put(this LsmStorage storage, long key, ReadOnlySequence<byte> value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);
    public static void Put(this LsmStorage storage, ulong key, ReadOnlySequence<byte> value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);
    public static void Put(this LsmStorage storage, string key, ReadOnlySequence<byte> value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), value);

    public static void Put(this LsmStorage storage, int key, in Utf8JsonReader value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), in value);
    public static void Put(this LsmStorage storage, uint key, in Utf8JsonReader value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), in value);
    public static void Put(this LsmStorage storage, long key, in Utf8JsonReader value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), in value);
    public static void Put(this LsmStorage storage, ulong key, in Utf8JsonReader value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), in value);
    public static void Put(this LsmStorage storage, string key, in Utf8JsonReader value) => PutEncodedKey(storage, LsmStorageTypedExtensions.EncodeKey(key), in value);

    private static void PutEncodedKey(LsmStorage storage, OwnedByteSlice key, Stream value)
    {
        using (key)
        {
            Put(storage, key.Span, value);
        }
    }

    private static async ValueTask PutEncodedKeyAsync(LsmStorage storage, OwnedByteSlice key, Stream value, CancellationToken cancellationToken)
    {
        using (key)
        {
            await PutAsync(storage, key.Memory, value, cancellationToken).ConfigureAwait(false);
        }
    }

    private static void PutEncodedKey(LsmStorage storage, OwnedByteSlice key, ReadOnlySequence<byte> value)
    {
        using (key)
        {
            Put(storage, key.Span, value);
        }
    }

    private static void PutEncodedKey(LsmStorage storage, OwnedByteSlice key, in Utf8JsonReader value)
    {
        using (key)
        {
            Put(storage, key.Span, in value);
        }
    }
}
