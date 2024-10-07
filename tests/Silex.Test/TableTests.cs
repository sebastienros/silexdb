namespace Silex.Test;

using Silex.Blocks;
using Silex.Tables;
using System.Text;

public class TableTests
{
    [Fact]
    public async Task ShouldCreateTable()
    {
        var tempFilename = Path.GetRandomFileName();

        var builder = new SsTableBuilder(new DefaultSsTableEncoder(), new DefaultBlockEncoder());

        var key = BitConverter.GetBytes((ushort)7);
        var value = Encoding.UTF8.GetBytes($"hello");

        builder.Add(key, value);

        var table = await builder.BuildAsync(tempFilename);

        Assert.Single(table.BlockMetadata);
        using var block = await table.LoadBlockAsync(0);
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block.Memory);

        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldLoadExistingTable()
    {
        var tempFilename = Path.GetRandomFileName();
        var blockBuilder = new BlockBuilder(new DefaultBlockEncoder());

        var builder = new SsTableBuilder(new DefaultSsTableEncoder(), new DefaultBlockEncoder());

        var key = BitConverter.GetBytes((ushort)7);
        var value = Encoding.UTF8.GetBytes($"hello");

        builder.Add(key, value);

        await builder.BuildAsync(tempFilename);

        var table = await SsTable.LoadSsTableAsync(tempFilename, new DefaultSsTableEncoder(), blockBuilder);

        Assert.Single(table.BlockMetadata);
        using var block = await table.LoadBlockAsync(0);
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block.Memory);

        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldIterateAllEntries()
    {
        var tempFilename = Path.GetRandomFileName();

        var builder = new SsTableBuilder(new DefaultSsTableEncoder(), new DefaultBlockEncoder());

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            builder.Add(BitConverter.GetBytes(i), value);
        }

        var table = await builder.BuildAsync(tempFilename);

        Assert.True(table.BlockMetadata.Count > 0);

        var iterator = new SsTableIterator(table);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Equivalent(Enumerable.Range(0, 100), result);

        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldIterateFromKey()
    {
        var tempFilename = Path.GetRandomFileName();

        var builder = new SsTableBuilder(new DefaultSsTableEncoder(), new DefaultBlockEncoder());

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            builder.Add(BitConverter.GetBytes(i), value);
        }

        var table = await builder.BuildAsync(tempFilename);

        // Check we have one table with multiple blocks
        Assert.True(table.BlockMetadata.Count > 0);

        var iterator = new SsTableIterator(table);

        var result = iterator.EnumerateAsync(BitConverter.GetBytes(13)).ToBlockingEnumerable().Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Equivalent(Enumerable.Range(13, 100 - 13), result);

        File.Delete(tempFilename);
    }

    [Fact]
    public async Task ShouldIterateFromUnknownKey()
    {
        var tempFilename = Path.GetRandomFileName();

        var builder = new SsTableBuilder(new DefaultSsTableEncoder(), new DefaultBlockEncoder());

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            builder.Add(BitConverter.GetBytes(i), value);
        }

        var table = await builder.BuildAsync(tempFilename);

        // Check we have one table with multiple blocks
        Assert.True(table.BlockMetadata.Count > 0);

        var iterator = new SsTableIterator(table);

        var result = iterator.EnumerateAsync(BitConverter.GetBytes(101)).ToBlockingEnumerable().Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Empty(result);

        File.Delete(tempFilename);
    }
}

