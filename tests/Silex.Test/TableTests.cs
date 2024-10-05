namespace Silex.Test;

using Silex.Blocks;
using Silex.Tables;
using System.Text;

public class TableTests
{
    [Fact]
    public async Task ShouldCreateTable()
    {
        var tempDirectory = Directory.CreateTempSubdirectory("silex_");

        var builder = new SsTableBuilder(tempDirectory.FullName, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), 2.MiB());

        var key = BitConverter.GetBytes((ushort)7);
        var value = Encoding.UTF8.GetBytes($"hello");

        builder.AddEntry(key, value);

        var tables = await builder.BuildTablesAsync();

        Assert.Single(tempDirectory.GetFiles());
        Assert.Single(tables);
        Assert.Single(tables[0].BlockMetadata);
        using var block = await tables[0].LoadBlockAsync(0);
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block.Memory);

        tempDirectory.Delete(true);
    }

    [Fact]
    public async Task ShouldLoadExistingTable()
    {
        var tempDirectory = Directory.CreateTempSubdirectory("silex_");

        var builder = new SsTableBuilder(tempDirectory.FullName, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), 2.MiB());

        var key = BitConverter.GetBytes((ushort)7);
        var value = Encoding.UTF8.GetBytes($"hello");

        builder.AddEntry(key, value);

        var tables = await builder.BuildTablesAsync();
        var filename = tables[0].Filename;

        var table = await SsTable.LoadSsTableAsync(filename, new DefaultSsTableEncoder(), new DefaultBlockEncoder());

        Assert.Single(table.BlockMetadata);
        using var block = await table.LoadBlockAsync(0);
        Assert.Equal(new byte[] { 2, 7, 0, 5, 104, 101, 108, 108, 111, 0, 0, 1, 0 }, block.Memory);

        tempDirectory.Delete(true);
    }

    [Fact]
    public async Task ShouldCreateNewTablesWhenSstLimitReached()
    {
        var tempDirectory = Directory.CreateTempSubdirectory("silex_");

        // Create 100 KiB SST files
        var builder = new SsTableBuilder(tempDirectory.FullName, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), 100.KiB());

        // Random 1 KiB value
        var value = new byte[1.KiB()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 1000; i++)
        {
            builder.AddEntry(BitConverter.GetBytes(i), value);
        }

        var tables = await builder.BuildTablesAsync();

        Assert.Equal(11, tempDirectory.GetFiles().Length);
        Assert.Equal(11, tables.Count);

        using var block = await tables[0].LoadBlockAsync(0);
        var entry = block.GetEntry(0);
        var data = block.GetValue(entry);

        Assert.Equal(BitConverter.GetBytes((uint)0), entry.Key);
        Assert.Equal(value, data);

        tempDirectory.Delete(true);
    }

    [Fact]
    public async Task ShouldIterateAllEntries()
    {
        var tempDirectory = Directory.CreateTempSubdirectory("silex_");

        // Create 1 MiB SST files
        var builder = new SsTableBuilder(tempDirectory.FullName, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), 1.MiB());

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            builder.AddEntry(BitConverter.GetBytes(i), value);
        }

        var tables = await builder.BuildTablesAsync();

        // Check we have one table with multiple blocks
        Assert.Single(tables);
        Assert.True(tables[0].BlockMetadata.Count > 0);

        var iterator = new SsTableIterator(tables[0]);

        var result = iterator.EnumerateAsync().ToBlockingEnumerable().Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Equivalent(Enumerable.Range(0, 100), result);

        tempDirectory.Delete(true);
    }

    [Fact]
    public async Task ShouldIterateFromKey()
    {
        var tempDirectory = Directory.CreateTempSubdirectory("silex_");

        // Create 1 MiB SST files
        var builder = new SsTableBuilder(tempDirectory.FullName, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), 1.MiB());

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            builder.AddEntry(BitConverter.GetBytes(i), value);
        }

        var tables = await builder.BuildTablesAsync();

        // Check we have one table with multiple blocks
        Assert.Single(tables);
        Assert.True(tables[0].BlockMetadata.Count > 0);

        var iterator = new SsTableIterator(tables[0]);

        var result = iterator.EnumerateAsync(BitConverter.GetBytes(13)).ToBlockingEnumerable().Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Equivalent(Enumerable.Range(13, 100 - 13), result);

        tempDirectory.Delete(true);
    }

    [Fact]
    public async Task ShouldIterateFromUnknownKey()
    {
        var tempDirectory = Directory.CreateTempSubdirectory("silex_");

        // Create 1 MiB SST files
        var builder = new SsTableBuilder(tempDirectory.FullName, new DefaultSsTableEncoder(), new DefaultBlockEncoder(), 1.MiB());

        // Random 100 B values
        var value = new byte[100.B()];
        Random.Shared.NextBytes(value);

        for (uint i = 0; i < 100; i++)
        {
            builder.AddEntry(BitConverter.GetBytes(i), value);
        }

        var tables = await builder.BuildTablesAsync();

        // Check we have one table with multiple blocks
        Assert.Single(tables);
        Assert.True(tables[0].BlockMetadata.Count > 0);

        var iterator = new SsTableIterator(tables[0]);

        var result = iterator.EnumerateAsync(BitConverter.GetBytes(101)).ToBlockingEnumerable().Select(x => BitConverter.ToInt32(x.Key.Span)).ToArray();

        Assert.Empty(result);

        tempDirectory.Delete(true);
    }
}

