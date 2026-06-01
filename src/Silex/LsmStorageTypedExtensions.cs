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

    internal static OwnedByteSlice EncodeKey(int key) => Encode(key);
    internal static OwnedByteSlice EncodeKey(uint key) => Encode(key);
    internal static OwnedByteSlice EncodeKey(long key) => Encode(key);
    internal static OwnedByteSlice EncodeKey(ulong key) => Encode(key);
    internal static OwnedByteSlice EncodeKey(string key) => Encode(key);

    internal static OwnedByteSlice EncodeValue(int value) => Encode(value);
    internal static OwnedByteSlice EncodeValue(uint value) => Encode(value);
    internal static OwnedByteSlice EncodeValue(long value) => Encode(value);
    internal static OwnedByteSlice EncodeValue(ulong value) => Encode(value);
    internal static OwnedByteSlice EncodeValue(string value) => Encode(value);

    private static void PutEncodedKey<TKey>(LsmStorage storage, TKey key, ReadOnlySpan<byte> value)
    {
        using var encodedKey = Encode(key);
        storage.Put(encodedKey.Span, value);
    }

    private static void PutEncoded<TKey, TValue>(LsmStorage storage, TKey key, TValue value)
    {
        using var encodedKey = Encode(key);
        using var encodedValue = Encode(value);
        storage.Put(encodedKey.Span, encodedValue.Span);
    }

    private static void DeleteEncoded<TKey>(LsmStorage storage, TKey key)
    {
        using var encodedKey = Encode(key);
        storage.Delete(encodedKey.Span);
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
