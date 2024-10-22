namespace Silex.Serialization;

public static class BinaryEncoderFactory<T>
{
    private static readonly IBinaryEncoder<T> _binarySerializer;

    static BinaryEncoderFactory()
    {
        var serializer = typeof(T) switch
        {
            Type t when t == typeof(ushort) => new UInt16Serializer() as IBinaryEncoder<T>,
            Type t when t == typeof(int) => new Int32Encoder() as IBinaryEncoder<T>,
            Type t when t == typeof(uint) => new UInt32Encoder() as IBinaryEncoder<T>,
            Type t when t == typeof(long) => new Int64Encoder() as IBinaryEncoder<T>,
            Type t when t == typeof(byte[]) => new ByteArrayEncoder() as IBinaryEncoder<T>,
            Type t when t == typeof(string) => new UTF8StringEncoder() as IBinaryEncoder<T>,
            Type t when t == typeof(char) => new UTF8CharEncoder() as IBinaryEncoder<T>,
            Type t when t == typeof(Bytes) => new BytesEncoder() as IBinaryEncoder<T>,
            _ => null
        };

        if (serializer is null)
        {
            throw new NotSupportedException($"No serializer found for '{typeof(T).Name}'");
        }

        _binarySerializer = serializer;
    }

    public static IBinaryEncoder<T> BinarySerializer => _binarySerializer;
}
