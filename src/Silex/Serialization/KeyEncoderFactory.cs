namespace Silex.Serialization;

/// <summary>
/// Resolves the binary encoder for a key type. Keys must be encoded in an order-preserving form so the
/// engine can compare them as raw bytes, which restricts the supported set to types with a well-defined
/// byte ordering: <see cref="Bytes"/>, <see cref="byte"/>[], <see cref="string"/> (UTF-8, code-point
/// order), <see cref="uint"/> and <see cref="ulong"/> (big-endian). Signed integers are intentionally not
/// supported as keys to avoid the sign-flip subtlety; use an unsigned type or a byte buffer instead.
/// </summary>
public static class KeyEncoderFactory<T>
{
    private static readonly IBinaryEncoder<T> _encoder;

    static KeyEncoderFactory()
    {
        var encoder = typeof(T) switch
        {
            Type t when t == typeof(uint) => new UInt32Encoder() as IBinaryEncoder<T>,
            Type t when t == typeof(ulong) => new UInt64Encoder() as IBinaryEncoder<T>,
            Type t when t == typeof(byte[]) => new ByteArrayEncoder() as IBinaryEncoder<T>,
            Type t when t == typeof(string) => new UTF8StringEncoder() as IBinaryEncoder<T>,
            Type t when t == typeof(Bytes) => new BytesEncoder() as IBinaryEncoder<T>,
            _ => null
        };

        if (encoder is null)
        {
            throw new NotSupportedException(
                $"'{typeof(T).Name}' is not a supported key type. Supported key types are: Bytes, byte[], string, uint, ulong.");
        }

        _encoder = encoder;
    }

    public static IBinaryEncoder<T> Encoder => _encoder;
}
