using System.Buffers.Binary;
using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Tables;

namespace Silex.Test;

public class VersionPrefixBloomTests
{
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task PrefixFiltersSurviveReloadAndIncludeAllVersionsAndPhysicalTombstones(int format)
    {
        using var folder = TempFolder.Create();
        var path = folder.GetRandomFileName();
        using (var builder = Builder(path, format))
        {
            for (var i = 0; i < 128; i++)
            {
                await builder.AddAsync(ByteSlice.FromMemory(Key(i, 0)), ByteSlice.FromMemory(new byte[] { 1 }));
                await builder.AddAsync(ByteSlice.FromMemory(Key(i, 1)), ByteSlice.Tombstone);
            }
            using var built = await builder.BuildAsync();
            Check(built);
        }
        using var blocks = new BlockBuilder(new DefaultBlockEncoder(128));
        using var loaded = await SsTable.LoadSsTableAsync(path, new DefaultSsTableEncoder(), blocks, new DefaultBloomFilterFactory());
        Check(loaded);
        await Assert.That(loaded.VersionPrefixBloomFilter is not null).IsEqualTo(format == 2);

        void Check(SsTable table)
        {
            for (var i = 0; i < 128; i++)
                if (!table.MayContainVersionPrefix(Key(i, 100).AsSpan(0, 7)))
                    throw new InvalidOperationException("Prefix filter rejected a stored logical key.");
            var rejected = 0;
            for (var i = 128; i < 1128; i++)
                if (!table.MayContainVersionPrefix(Key(i, 0).AsSpan(0, 7))) rejected++;
            if (format == 2 && rejected < 950 || format != 2 && rejected != 0)
                throw new InvalidOperationException($"Unexpected prefix filtering: {rejected} of 1000 misses.");
            if (!table.MayContainVersionPrefix("plain"u8))
                throw new InvalidOperationException("Prefix filter must not apply to an ordinary key.");
        }
    }

    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(5)]
    [Arguments(6)]
    public async Task CorruptPrefixFilterMetadataAndPayloadAreRejected(int corruption)
    {
        using var folder = TempFolder.Create();
        var path = folder.GetRandomFileName();
        using (var builder = Builder(path, 2))
        {
            await builder.AddAsync(ByteSlice.FromMemory(Key(1, 0)), ByteSlice.FromMemory(new byte[] { 1 }));
            using var table = await builder.BuildAsync();
        }
        var bytes = await File.ReadAllBytesAsync(path);
        var trailer = bytes.Length - SsTableFormat.FooterLength - VersionPrefixBloom.TrailerLength;
        var offset = (int)BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(trailer + 8));
        switch (corruption)
        {
            case 0: bytes[offset] ^= 1; break;
            case 1: BinaryPrimitives.WriteUInt64LittleEndian(bytes.AsSpan(trailer + 8), ulong.MaxValue); break;
            case 2: BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(trailer), 0); break;
            case 3: BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(trailer + 4), 255); break;
            case 4: bytes[trailer + 20] ^= 1; break;
            case 5: bytes[trailer + 16] ^= 1; break;
            case 6: BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(trailer), 7); break;
        }
        if (corruption is 2 or 3 or 6)
            BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(trailer + 16),
                VersionPrefixBloom.Checksum(bytes.AsSpan(offset, trailer - offset), bytes.AsSpan(trailer, 16)));
        await File.WriteAllBytesAsync(path, bytes);
        using var blocks = new BlockBuilder(new DefaultBlockEncoder(128));
        await Assert.That(async () =>
        {
            using var table = await SsTable.LoadSsTableAsync(path, new DefaultSsTableEncoder(), blocks, new DefaultBloomFilterFactory());
        }).Throws<InvalidDataException>();
    }

    private static BufferedSsTableBuilder Builder(string path, int format) =>
        new(path, new DefaultSsTableEncoder(), new DefaultBlockEncoder(128),
            new DefaultBloomFilterFactory(), 256, formatVersion: format);

    private static byte[] Key(int user, long version)
    {
        var key = new byte[15];
        key[0] = 1;
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(1), user);
        BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(7), version);
        return key;
    }
}
