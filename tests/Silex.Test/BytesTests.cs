namespace Silex.Test;
public class BytesTests
{
    [Test]
    public async Task BytesShouldConvertData()
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

        await Assert.That(byteBytes.Length).IsEqualTo(sizeof(byte));
        await Assert.That(shortBytes.Length).IsEqualTo(sizeof(short));
        await Assert.That(ushortBytes.Length).IsEqualTo(sizeof(ushort));
        await Assert.That(intBytes.Length).IsEqualTo(sizeof(int));
        await Assert.That(uintBytes.Length).IsEqualTo(sizeof(uint));
        await Assert.That(longBytes.Length).IsEqualTo(sizeof(long));
        await Assert.That(ulongBytes.Length).IsEqualTo(sizeof(ulong));
        await Assert.That(stringBytes.Length).IsEqualTo(stringValue.Length);
        await Assert.That(byteArrayBytes.Length).IsEqualTo(byteArrayValue.Length);
    }

    [Test]
    public async Task BytesShouldCopyMemory()
    {
        byte[] array = [1, 2, 3];

        Bytes bytes1 = array;
        Bytes bytes2 = array;

        await Assert.That(bytes2).IsEqualTo(bytes1);
        await Assert.That((Bytes)array).IsEqualTo(bytes1);
        await Assert.That((Bytes)array).IsEqualTo(bytes2);

        // Changing the source array should not change the Bytes instances
        array[0] = 0;

        await Assert.That(bytes2).IsEqualTo(bytes1);
        await Assert.That((Bytes)array).IsNotEqualTo(bytes1);
        await Assert.That((Bytes)array).IsNotEqualTo(bytes2);
    }

    [Test]
    public async Task BytesShouldBeComparable()
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

        await Assert.That(new Bytes(array123)).IsEqualTo(new Bytes(array123));

        await Assert.That(bytesArray123 == bytesArray123b).IsTrue();
        await Assert.That(bytesArray123 <= bytesArray123b).IsTrue();
        await Assert.That(bytesArray123 >= bytesArray123b).IsTrue();
        await Assert.That(bytesArray123 < bytesArray124).IsTrue();
        await Assert.That(bytesArray124 > bytesArray123).IsTrue();

        // Arrays should be compared sequentially
        await Assert.That(bytesArray124 > bytesArray1234).IsTrue();
        await Assert.That(bytesArray123 > bytesArray0123).IsTrue();
        await Assert.That(bytesArray124 > bytesArray0123).IsTrue();
    }

    [Test]
    public async Task BytesShouldRespectSourceLength()
    {
        for (var i = 0; i < 1000000; i++)
        {
            var length = Random.Shared.Next(128);
            byte[] source = new byte[length];
            Random.Shared.NextBytes(source);
            Bytes b = source;
            await Assert.That(b.Length).IsEqualTo(length);
        }

        for (var i = 0; i < 1000000; i++)
        {
            Bytes b = Random.Shared.NextInt64();
            await Assert.That(b.Length).IsEqualTo(sizeof(long));
        }
    }
}
