namespace Silex.Test;
public class BytesTests
{
    [Fact]
    public void BytesShouldConvertData()
    {
        byte byteValue = byte.MaxValue;
        short shortValue = short.MaxValue;
        ushort ushortValue = ushort.MaxValue;
        int intValue = int.MaxValue;
        uint uintValue = uint.MaxValue;
        long longValue = long.MaxValue;
        ulong ulongValue = ulong.MaxValue;
        string stringValue = "hello";
        byte[] byteArrayValue = [1, 2, 3, 4, 5];

        Bytes byteBytes = byteValue;
        Bytes shortBytes = shortValue;
        Bytes ushortBytes = ushortValue;
        Bytes intBytes = intValue;
        Bytes uintBytes = uintValue;
        Bytes longBytes = longValue;
        Bytes ulongBytes = ulongValue;
        Bytes stringBytes = stringValue;
        Bytes byteArrayBytes = byteArrayValue;

        Assert.Equal(sizeof(byte), byteBytes.Length);
        Assert.Equal(sizeof(short), shortBytes.Length);
        Assert.Equal(sizeof(ushort), ushortBytes.Length);
        Assert.Equal(sizeof(int), intBytes.Length);
        Assert.Equal(sizeof(uint), uintBytes.Length);
        Assert.Equal(sizeof(long), longBytes.Length);
        Assert.Equal(sizeof(ulong), ulongBytes.Length);
        Assert.Equal(stringValue.Length, stringBytes.Length);
        Assert.Equal(byteArrayValue.Length, byteArrayBytes.Length);
    }

    [Fact]
    public void BytesShouldCopyMemory()
    {
        byte[] array = [1, 2, 3];

        Bytes bytes1 = array;
        Bytes bytes2 = array;

        Assert.Equal(bytes1, bytes2);
        Assert.Equal(bytes1, array);
        Assert.Equal(bytes2, array);

        // Changing the source array should not change the Bytes instances
        array[0] = 0;

        Assert.Equal(bytes1, bytes2);
        Assert.NotEqual(bytes1, array);
        Assert.NotEqual(bytes2, array);
    }

    [Fact]
    public void BytesShouldBeComparable()
    {
        byte[] array123 = [1, 2, 3];
        byte[] array124 = [1, 2, 4];
        byte[] array0123 = [0, 1, 2, 3];
        byte[] array0124 = [0, 1, 2, 4];
        byte[] array1234 = [1, 2, 3, 4];

        Bytes bytesArray123 = array123;
        Bytes bytesArray123b = array123;
        Bytes bytesArray124 = array124;
        Bytes bytesArray0123 = array0123;
        Bytes bytesArray0124 = array0124;
        Bytes bytesArray1234 = array1234;

        Bytes bytesInt123 = 123;
        Bytes bytesInt124 = 124;
        Bytes bytesInt1234 = 1234;

        Assert.Equal(new Bytes(array123), new Bytes(array123));

        Assert.True(bytesArray123 == bytesArray123b);
        Assert.True(bytesArray123 <= bytesArray123b);
        Assert.True(bytesArray123 >= bytesArray123b);
        Assert.True(bytesArray123 < bytesArray124);
        Assert.True(bytesArray124 > bytesArray123);

        // Arrays should be compared sequentially
        Assert.True(bytesArray124 > bytesArray1234);
        Assert.True(bytesArray123 > bytesArray0123);
        Assert.True(bytesArray124 > bytesArray0123);
    }

    [Fact]
    public void BytesShouldRespectSourceLength()
    {
        for (var i = 0; i < 1000000; i++)
        {
            var length = Random.Shared.Next(128);
            byte[] source = new byte[length];
            Random.Shared.NextBytes(source);
            Bytes b = source;
            Assert.Equal(length, b.Length);
        }

        for (var i = 0; i < 1000000; i++)
        {
            Bytes b = Random.Shared.NextInt64();
            Assert.Equal(sizeof(long), b.Length);
        }
    }
}
