using System.Buffers;
using Silex.Buffers;
using Silex.Serialization;

namespace Silex;

public static class LsmStorageTypedExtensions
{
    public static void Put(this LsmStorage storage, int key, ReadOnlySpan<byte> value) => PutEncodedKey(storage, key, value);
    public static void Put(this LsmStorage storage, uint key, ReadOnlySpan<byte> value) => PutEncodedKey(storage, key, value);
    public static void Put(this LsmStorage storage, long key, ReadOnlySpan<byte> value) => PutEncodedKey(storage, key, value);
    public static void Put(this LsmStorage storage, ulong key, ReadOnlySpan<byte> value) => PutEncodedKey(storage, key, value);
    public static void Put(this LsmStorage storage, string key, ReadOnlySpan<byte> value) => PutEncodedKey(storage, key, value);

    public static void Put(this LsmStorage storage, int key, int value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, int key, uint value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, int key, long value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, int key, ulong value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, int key, string value) => PutEncoded(storage, key, value);

    public static void Put(this LsmStorage storage, uint key, int value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, uint key, uint value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, uint key, long value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, uint key, ulong value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, uint key, string value) => PutEncoded(storage, key, value);

    public static void Put(this LsmStorage storage, long key, int value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, long key, uint value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, long key, long value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, long key, ulong value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, long key, string value) => PutEncoded(storage, key, value);

    public static void Put(this LsmStorage storage, ulong key, int value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, ulong key, uint value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, ulong key, long value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, ulong key, ulong value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, ulong key, string value) => PutEncoded(storage, key, value);

    public static void Put(this LsmStorage storage, string key, int value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, string key, uint value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, string key, long value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, string key, ulong value) => PutEncoded(storage, key, value);
    public static void Put(this LsmStorage storage, string key, string value) => PutEncoded(storage, key, value);

    public static void Delete(this LsmStorage storage, int key) => DeleteEncoded(storage, key);
    public static void Delete(this LsmStorage storage, uint key) => DeleteEncoded(storage, key);
    public static void Delete(this LsmStorage storage, long key) => DeleteEncoded(storage, key);
    public static void Delete(this LsmStorage storage, ulong key) => DeleteEncoded(storage, key);
    public static void Delete(this LsmStorage storage, string key) => DeleteEncoded(storage, key);

    public static ValueTask<bool> TryReadRawAsync<TArg>(this LsmStorage storage, int key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
        => TryReadRawEncodedAsync(storage, Encode(key), arg, reader, cancellationToken);

    public static ValueTask<bool> TryReadRawAsync<TArg>(this LsmStorage storage, uint key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
        => TryReadRawEncodedAsync(storage, Encode(key), arg, reader, cancellationToken);

    public static ValueTask<bool> TryReadRawAsync<TArg>(this LsmStorage storage, long key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
        => TryReadRawEncodedAsync(storage, Encode(key), arg, reader, cancellationToken);

    public static ValueTask<bool> TryReadRawAsync<TArg>(this LsmStorage storage, ulong key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
        => TryReadRawEncodedAsync(storage, Encode(key), arg, reader, cancellationToken);

    public static ValueTask<bool> TryReadRawAsync<TArg>(this LsmStorage storage, string key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken = default)
        => TryReadRawEncodedAsync(storage, Encode(key), arg, reader, cancellationToken);

    public static ValueTask<int> GetInt32Async(this LsmStorage storage, int key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<int>.BinarySerializer, cancellationToken);
    public static ValueTask<int> GetInt32Async(this LsmStorage storage, uint key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<int>.BinarySerializer, cancellationToken);
    public static ValueTask<int> GetInt32Async(this LsmStorage storage, long key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<int>.BinarySerializer, cancellationToken);
    public static ValueTask<int> GetInt32Async(this LsmStorage storage, ulong key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<int>.BinarySerializer, cancellationToken);
    public static ValueTask<int> GetInt32Async(this LsmStorage storage, string key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<int>.BinarySerializer, cancellationToken);

    public static ValueTask<uint> GetUInt32Async(this LsmStorage storage, int key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<uint>.BinarySerializer, cancellationToken);
    public static ValueTask<uint> GetUInt32Async(this LsmStorage storage, uint key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<uint>.BinarySerializer, cancellationToken);
    public static ValueTask<uint> GetUInt32Async(this LsmStorage storage, long key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<uint>.BinarySerializer, cancellationToken);
    public static ValueTask<uint> GetUInt32Async(this LsmStorage storage, ulong key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<uint>.BinarySerializer, cancellationToken);
    public static ValueTask<uint> GetUInt32Async(this LsmStorage storage, string key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<uint>.BinarySerializer, cancellationToken);

    public static ValueTask<long> GetInt64Async(this LsmStorage storage, int key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<long>.BinarySerializer, cancellationToken);
    public static ValueTask<long> GetInt64Async(this LsmStorage storage, uint key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<long>.BinarySerializer, cancellationToken);
    public static ValueTask<long> GetInt64Async(this LsmStorage storage, long key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<long>.BinarySerializer, cancellationToken);
    public static ValueTask<long> GetInt64Async(this LsmStorage storage, ulong key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<long>.BinarySerializer, cancellationToken);
    public static ValueTask<long> GetInt64Async(this LsmStorage storage, string key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<long>.BinarySerializer, cancellationToken);

    public static ValueTask<ulong> GetUInt64Async(this LsmStorage storage, int key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<ulong>.BinarySerializer, cancellationToken);
    public static ValueTask<ulong> GetUInt64Async(this LsmStorage storage, uint key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<ulong>.BinarySerializer, cancellationToken);
    public static ValueTask<ulong> GetUInt64Async(this LsmStorage storage, long key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<ulong>.BinarySerializer, cancellationToken);
    public static ValueTask<ulong> GetUInt64Async(this LsmStorage storage, ulong key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<ulong>.BinarySerializer, cancellationToken);
    public static ValueTask<ulong> GetUInt64Async(this LsmStorage storage, string key, CancellationToken cancellationToken = default)
        => GetAsync(storage, Encode(key), BinaryEncoderFactory<ulong>.BinarySerializer, cancellationToken);

    public static async ValueTask<string?> GetStringAsync(this LsmStorage storage, int key, CancellationToken cancellationToken = default)
        => await GetAsync(storage, Encode(key), BinaryEncoderFactory<string>.BinarySerializer, cancellationToken);
    public static async ValueTask<string?> GetStringAsync(this LsmStorage storage, uint key, CancellationToken cancellationToken = default)
        => await GetAsync(storage, Encode(key), BinaryEncoderFactory<string>.BinarySerializer, cancellationToken);
    public static async ValueTask<string?> GetStringAsync(this LsmStorage storage, long key, CancellationToken cancellationToken = default)
        => await GetAsync(storage, Encode(key), BinaryEncoderFactory<string>.BinarySerializer, cancellationToken);
    public static async ValueTask<string?> GetStringAsync(this LsmStorage storage, ulong key, CancellationToken cancellationToken = default)
        => await GetAsync(storage, Encode(key), BinaryEncoderFactory<string>.BinarySerializer, cancellationToken);
    public static async ValueTask<string?> GetStringAsync(this LsmStorage storage, string key, CancellationToken cancellationToken = default)
        => await GetAsync(storage, Encode(key), BinaryEncoderFactory<string>.BinarySerializer, cancellationToken);

    internal static OwnedByteSlice EncodeKey(int key) => EncodeOwned(BinaryEncoderFactory<int>.BinarySerializer, key);
    internal static OwnedByteSlice EncodeKey(uint key) => EncodeOwned(BinaryEncoderFactory<uint>.BinarySerializer, key);
    internal static OwnedByteSlice EncodeKey(long key) => EncodeOwned(BinaryEncoderFactory<long>.BinarySerializer, key);
    internal static OwnedByteSlice EncodeKey(ulong key) => EncodeOwned(BinaryEncoderFactory<ulong>.BinarySerializer, key);
    internal static OwnedByteSlice EncodeKey(string key) => EncodeOwned(BinaryEncoderFactory<string>.BinarySerializer, key);

    internal static OwnedByteSlice EncodeValue(int value) => EncodeOwned(BinaryEncoderFactory<int>.BinarySerializer, value);
    internal static OwnedByteSlice EncodeValue(uint value) => EncodeOwned(BinaryEncoderFactory<uint>.BinarySerializer, value);
    internal static OwnedByteSlice EncodeValue(long value) => EncodeOwned(BinaryEncoderFactory<long>.BinarySerializer, value);
    internal static OwnedByteSlice EncodeValue(ulong value) => EncodeOwned(BinaryEncoderFactory<ulong>.BinarySerializer, value);
    internal static OwnedByteSlice EncodeValue(string value) => EncodeOwned(BinaryEncoderFactory<string>.BinarySerializer, value);

    private static void PutEncodedKey<TKey>(LsmStorage storage, TKey key, ReadOnlySpan<byte> value)
    {
        var encoder = BinaryEncoderFactory<TKey>.BinarySerializer;
        if (encoder.TryGetRawBytes(key, out var encodedKey))
        {
            storage.Put(encodedKey, value);
            return;
        }

        var length = encoder.GetLength(key);
        var rented = ArrayPool<byte>.Shared.Rent(Math.Max(1, length));
        var buffer = rented.AsSpan(0, length);

        try
        {
            EncodeInto(encoder, key, buffer);
            storage.Put(buffer, value);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static void PutEncoded<TKey, TValue>(LsmStorage storage, TKey key, TValue value)
    {
        var keyEncoder = BinaryEncoderFactory<TKey>.BinarySerializer;
        var valueEncoder = BinaryEncoderFactory<TValue>.BinarySerializer;

        var hasRawKey = keyEncoder.TryGetRawBytes(key, out var encodedKey);
        var hasRawValue = valueEncoder.TryGetRawBytes(value, out var encodedValue);
        var keyLength = hasRawKey ? 0 : keyEncoder.GetLength(key);
        var valueLength = hasRawValue ? 0 : valueEncoder.GetLength(value);

        byte[]? rented = null;
        Span<byte> keyBuffer = default;
        Span<byte> valueBuffer = default;

        try
        {
            var encodedLength = checked(keyLength + valueLength);
            if (encodedLength != 0)
            {
                rented = ArrayPool<byte>.Shared.Rent(encodedLength);
            }

            if (!hasRawKey)
            {
                keyBuffer = rented.AsSpan(0, keyLength);
                EncodeInto(keyEncoder, key, keyBuffer);
                encodedKey = keyBuffer;
            }

            if (!hasRawValue)
            {
                valueBuffer = rented.AsSpan(keyLength, valueLength);
                EncodeInto(valueEncoder, value, valueBuffer);
                encodedValue = valueBuffer;
            }

            storage.Put(encodedKey, encodedValue);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private static void DeleteEncoded<TKey>(LsmStorage storage, TKey key)
    {
        var encoder = BinaryEncoderFactory<TKey>.BinarySerializer;
        if (encoder.TryGetRawBytes(key, out var encodedKey))
        {
            storage.Delete(encodedKey);
            return;
        }

        var length = encoder.GetLength(key);
        var rented = ArrayPool<byte>.Shared.Rent(Math.Max(1, length));
        var buffer = rented.AsSpan(0, length);

        try
        {
            EncodeInto(encoder, key, buffer);
            storage.Delete(buffer);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    internal static OwnedByteSlice EncodeOwned<T>(IBinaryEncoder<T> encoder, T value)
    {
        if (encoder.TryGetRawBytes(value, out var rawBytes))
        {
            return OwnedByteSlice.CopyFrom(rawBytes);
        }

        var encoded = OwnedByteSlice.Rent(encoder.GetLength(value));
        try
        {
            EncodeInto(encoder, value, encoded.WritableSpan);
            return encoded;
        }
        catch
        {
            encoded.Dispose();
            throw;
        }
    }

    private static OwnedByteSlice Encode<T>(T value) =>
        EncodeOwned(BinaryEncoderFactory<T>.BinarySerializer, value);

    private static void EncodeInto<T>(IBinaryEncoder<T> encoder, T value, Span<byte> destination)
    {
        var writer = new EncoderBinaryWriter(destination);
        encoder.Encode(value, ref writer);
        if (writer.BytesWritten != destination.Length)
        {
            throw new InvalidOperationException(
                $"{encoder.GetType().Name} encoded {writer.BytesWritten} bytes; expected {destination.Length}.");
        }
    }

    private static async ValueTask<T> GetAsync<T>(LsmStorage storage, OwnedByteSlice key, IBinaryEncoder<T> encoder, CancellationToken cancellationToken)
    {
        try
        {
            var state = new DecodeState<T>(encoder);
            var found = await storage.TryReadRawAsync(key.Span, state, static (s, value) => s.Set(value), cancellationToken);
            return found ? state.Value : default!;
        }
        finally
        {
            key.Dispose();
        }
    }

    private static async ValueTask<bool> TryReadRawEncodedAsync<TArg>(LsmStorage storage, OwnedByteSlice key, TArg arg, ReadValueAction<TArg> reader, CancellationToken cancellationToken)
    {
        try
        {
            return await storage.TryReadRawAsync(key.Span, arg, reader, cancellationToken);
        }
        finally
        {
            key.Dispose();
        }
    }

    private sealed class DecodeState<T>(IBinaryEncoder<T> encoder)
    {
        public T Value { get; private set; } = default!;

        public void Set(ReadOnlySpan<byte> value)
        {
            Value = encoder.Decode(value);
        }
    }
}
