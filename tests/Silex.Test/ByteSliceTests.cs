namespace Silex.Test;

public class ByteSliceTests
{
    [Test]
    public async Task ByteSliceShouldViewMemory()
    {
        byte[] array = [1, 2, 3];
        var slice = ByteSlice.FromMemory(array);

        await Assert.That(slice.Length).IsEqualTo(array.Length);
        await Assert.That(slice.Span.ToArray()).IsEquivalentTo(array);

        array[0] = 9;

        await Assert.That(slice.Span[0]).IsEqualTo((byte)9);
    }

    [Test]
    public async Task OwnedByteSliceShouldCopyMemory()
    {
        byte[] array = [1, 2, 3];
        using var owned = OwnedByteSlice.CopyFrom(array);

        array[0] = 9;

        await Assert.That(owned.Span.ToArray()).IsEquivalentTo(new byte[] { 1, 2, 3 });
    }

    [Test]
    public async Task ByteSliceShouldBeComparable()
    {
        byte[] array123 = [1, 2, 3];
        byte[] array124 = [1, 2, 4];
        byte[] array0123 = [0, 1, 2, 3];
        byte[] array1234 = [1, 2, 3, 4];

        var bytesArray123 = ByteSlice.FromMemory(array123);
        var bytesArray123b = ByteSlice.FromMemory(array123);
        var bytesArray124 = ByteSlice.FromMemory(array124);
        var bytesArray0123 = ByteSlice.FromMemory(array0123);
        var bytesArray1234 = ByteSlice.FromMemory(array1234);

        await Assert.That(ByteSlice.FromMemory(array123)).IsEqualTo(ByteSlice.FromMemory(array123));

        await Assert.That(bytesArray123 == bytesArray123b).IsTrue();
        await Assert.That(bytesArray123 <= bytesArray123b).IsTrue();
        await Assert.That(bytesArray123 >= bytesArray123b).IsTrue();
        await Assert.That(bytesArray123 < bytesArray124).IsTrue();
        await Assert.That(bytesArray124 > bytesArray123).IsTrue();

        await Assert.That(bytesArray124 > bytesArray1234).IsTrue();
        await Assert.That(bytesArray123 > bytesArray0123).IsTrue();
        await Assert.That(bytesArray124 > bytesArray0123).IsTrue();
    }

    [Test]
    public async Task ByteSliceShouldRespectSourceLength()
    {
        for (var i = 0; i < 1000000; i++)
        {
            var length = Random.Shared.Next(128);
            byte[] source = new byte[length];
            Random.Shared.NextBytes(source);
            var slice = ByteSlice.FromMemory(source);
            await Assert.That(slice.Length).IsEqualTo(length);
        }
    }
}
