using System.Buffers.Binary;

namespace Silex.Test;

/// <summary>
/// Test-only conveniences that bridge the engine's opaque <see cref="ValueBuffer"/> surface to the
/// concrete <see cref="byte"/>[] and <see cref="int"/> values the tests work with. The encoding mirrors
/// the public numeric <c>Put</c> overloads (little-endian fixed width) so inner-engine tests and public
/// API tests agree on the byte layout.
/// </summary>
internal static class StorageTestExtensions
{
    /// <summary>
    /// Stores a byte-array value on the inner engine, taking ownership of the array (zero-copy), exactly
    /// like the public <see cref="LsmStorage{TKey}.Put(TKey, byte[])"/> overload.
    /// </summary>
    public static void Put<TKey>(this LsmStorageInner<TKey> storage, TKey key, byte[] value)
        where TKey : notnull
        => storage.Put(key, new ValueBuffer(value));

    /// <summary>
    /// Stores an <see cref="int"/> value on the inner engine, encoded as four little-endian bytes.
    /// </summary>
    public static void Put<TKey>(this LsmStorageInner<TKey> storage, TKey key, int value)
        where TKey : notnull
        => storage.Put(key, ValueBuffer.FromInt32(value));

    /// <summary>
    /// Reads a byte-array value from the inner engine. Returns <see langword="null"/> for a missing key or
    /// a tombstone, without copying the live value.
    /// </summary>
    public static async ValueTask<byte[]?> GetBytesAsync<TKey>(this LsmStorageInner<TKey> storage, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull
        => (await storage.GetAsync(key, cancellationToken)).ToNullableArray();

    /// <summary>
    /// Reads an <see cref="int"/> value from the inner engine, decoding four little-endian bytes. A missing
    /// key or tombstone reads back as zero, matching the historical default-value semantics of the tests.
    /// </summary>
    public static async ValueTask<int> GetInt32Async<TKey>(this LsmStorageInner<TKey> storage, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull
        => (await storage.GetAsync(key, cancellationToken)).ReadInt32OrZero();

    /// <summary>
    /// Reads an <see cref="int"/> value from the public store, decoding four little-endian bytes. A missing
    /// key or tombstone reads back as zero, matching the historical default-value semantics of the tests.
    /// </summary>
    public static async ValueTask<int> GetInt32Async<TKey>(this LsmStorage<TKey> storage, TKey key, CancellationToken cancellationToken = default)
        where TKey : notnull
    {
        var value = await storage.GetAsync(key, cancellationToken);
        return value is null || value.Length == 0 ? 0 : BinaryPrimitives.ReadInt32LittleEndian(value);
    }

    /// <summary>
    /// Decodes a live four-byte little-endian <see cref="int"/> value. Throws for an empty buffer, so it is
    /// only valid on iterator entries (which never yield tombstones).
    /// </summary>
    public static int ReadInt32(this ValueBuffer value)
        => BinaryPrimitives.ReadInt32LittleEndian(value.Span);

    /// <summary>
    /// Decodes a four-byte little-endian <see cref="int"/> value, treating an empty buffer (miss/tombstone)
    /// as zero.
    /// </summary>
    public static int ReadInt32OrZero(this ValueBuffer value)
        => value.IsEmpty ? 0 : BinaryPrimitives.ReadInt32LittleEndian(value.Span);

    /// <summary>
    /// Returns the single value byte of a live entry. Only valid on iterator entries (which never yield
    /// tombstones) whose value is exactly one byte.
    /// </summary>
    public static byte ReadByte(this ValueBuffer value)
        => value.Span[0];

    // ---------------------------------------------------------------------------------------------
    // int-key conveniences for the uint-keyed stores. The store restricts keys to unsigned types, but
    // the value tests express keys as plain int literals/loop variables. These overloads forward to the
    // uint surface so the tests stay readable without sprinkling `(uint)` casts at every call site.
    // They are more specific than the generic helpers above, so overload resolution prefers them.
    // ---------------------------------------------------------------------------------------------

    public static void Put(this LsmStorageInner<uint> storage, int key, int value)
        => storage.Put((uint)key, ValueBuffer.FromInt32(value));

    public static void Put(this LsmStorageInner<uint> storage, int key, byte[] value)
        => storage.Put((uint)key, new ValueBuffer(value));

    public static void Put(this LsmStorage<uint> storage, int key, int value)
        => storage.Put((uint)key, value);

    public static void Put(this LsmStorage<uint> storage, int key, byte[] value)
        => storage.Put((uint)key, value);

    public static void Delete(this LsmStorageInner<uint> storage, int key)
        => storage.Delete((uint)key);

    public static void Delete(this LsmStorage<uint> storage, int key)
        => storage.Delete((uint)key);

    public static ValueTask<ValueBuffer> GetAsync(this LsmStorageInner<uint> storage, int key, CancellationToken cancellationToken = default)
        => storage.GetAsync((uint)key, cancellationToken);

    public static ValueTask<byte[]?> GetAsync(this LsmStorage<uint> storage, int key, CancellationToken cancellationToken = default)
        => storage.GetAsync((uint)key, cancellationToken);

    public static ValueTask<byte[]?> GetBytesAsync(this LsmStorageInner<uint> storage, int key, CancellationToken cancellationToken = default)
        => storage.GetBytesAsync((uint)key, cancellationToken);

    public static ValueTask<int> GetInt32Async(this LsmStorageInner<uint> storage, int key, CancellationToken cancellationToken = default)
        => storage.GetInt32Async((uint)key, cancellationToken);

    public static ValueTask<int> GetInt32Async(this LsmStorage<uint> storage, int key, CancellationToken cancellationToken = default)
        => storage.GetInt32Async((uint)key, cancellationToken);

    public static ValueTask<int> GetRawAsync(this LsmStorageInner<uint> storage, int key, Memory<byte> destination, CancellationToken cancellationToken = default)
        => storage.GetRawAsync((uint)key, destination, cancellationToken);

    public static ValueTask<int> GetRawAsync(this LsmStorage<uint> storage, int key, Memory<byte> destination, CancellationToken cancellationToken = default)
        => storage.GetRawAsync((uint)key, destination, cancellationToken);

    public static ValueTask<bool> TryGetRawAsync(this LsmStorageInner<uint> storage, int key, System.Buffers.IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
        => storage.TryGetRawAsync((uint)key, destination, cancellationToken);

    public static ValueTask<bool> TryGetRawAsync(this LsmStorage<uint> storage, int key, System.Buffers.IBufferWriter<byte> destination, CancellationToken cancellationToken = default)
        => storage.TryGetRawAsync((uint)key, destination, cancellationToken);
}
