using System.Buffers.Binary;
using Silex.Blocks;
using Silex.BloomFilters;
using Silex.Tables;

namespace Silex.Test;

public class BlockKeyWindowTests
{
    public enum KeyShape
    {
        RandomFixed16,
        SharedLongPrefix,
        TieBeyondWindow,
        VariableLengthWithZeros,
        PrefixLongerThanMax,
        Single,
    }

    [Test]
    [MatrixDataSource]
    public async Task WindowedSearchMatchesEntrySearch(
        [Matrix(KeyShape.RandomFixed16, KeyShape.SharedLongPrefix, KeyShape.TieBeyondWindow, KeyShape.VariableLengthWithZeros, KeyShape.PrefixLongerThanMax, KeyShape.Single)] KeyShape shape,
        [Matrix(512, 4096, 16384)] int blockSize)
    {
        var random = new Random(1234 + (int)shape * 31 + blockSize);
        var keys = GenerateKeys(shape, random);

        var plainEncoder = new DefaultBlockEncoder((ushort)blockSize);
        var windowEncoder = new DefaultBlockEncoder((ushort)blockSize, writeKeyWindows: true);
        using var plainBuilder = new BlockBuilder(plainEncoder);
        using var windowBuilder = new BlockBuilder(windowEncoder);

        var added = new List<byte[]>();
        foreach (var key in keys)
        {
            var value = ValueFor(key, added.Count);
            var tombstone = added.Count % 7 == 3;
            if (!windowBuilder.AddRaw(key, value, tombstone))
            {
                break;
            }

            await Assert.That(plainBuilder.AddRaw(key, value, tombstone)).IsTrue();
            added.Add(key);
        }

        using var plain = plainBuilder.BuildBlock();
        using var encoded = windowBuilder.BuildBlock();
        using var windowed = windowEncoder.Decode(encoded.Memory);

        await Assert.That(plain.HasKeyWindows).IsFalse();
        await Assert.That(encoded.HasKeyWindows).IsTrue();
        await Assert.That(windowed.HasKeyWindows).IsTrue();
        await Assert.That(windowed.Offsets.Count).IsEqualTo(added.Count);
        await Assert.That(windowed.DataEnd).IsEqualTo(plain.DataEnd);
        await Assert.That(windowed.KeyPrefix.Length).IsLessThanOrEqualTo(BlockKeyWindows.MaxPrefixLength);

        var mismatches = new List<string>();
        foreach (var probe in Probes(added, random))
        {
            Compare(plain, windowed, probe, mismatches);
            Compare(plain, encoded, probe, mismatches);
        }

        await Assert.That(mismatches).IsEmpty();
    }

    [Test]
    public async Task WindowedBlockLayoutFlagsTheEntryCount()
    {
        var encoder = new DefaultBlockEncoder(4096, writeKeyWindows: true);
        using var builder = new BlockBuilder(encoder);
        builder.AddRaw("prefix-a1"u8, "1"u8, false);
        builder.AddRaw("prefix-b22"u8, "2"u8, false);
        using var block = builder.BuildBlock();

        var span = block.Memory.Span;
        var trailer = BinaryPrimitives.ReadUInt16LittleEndian(span[^2..]);
        await Assert.That(trailer).IsEqualTo((ushort)(2 | BlockKeyWindows.CountFlag));
        await Assert.That(block.KeyPrefix.SequenceEqual("prefix-"u8)).IsTrue();
        await Assert.That(block.GetKeyWindow(0)).IsEqualTo(BinaryPrimitives.ReadUInt64BigEndian("a1\0\0\0\0\0\0"u8));
        await Assert.That(block.GetKeyWindow(1)).IsEqualTo(BinaryPrimitives.ReadUInt64BigEndian("b22\0\0\0\0\0"u8));

        // data | "prefix-" | 7 | 2 windows | 2 offsets | count
        var dataLength = (1 + 9 + 1 + 1) + (1 + 10 + 1 + 1);
        await Assert.That(block.DataEnd).IsEqualTo(dataLength);
        await Assert.That(block.Memory.Length).IsEqualTo(dataLength + 7 + 1 + 2 * 8 + 2 * 2 + 2);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TablesWithKeyWindowsUseNewerFooterVersionsAndStayReadable(bool versionPrefixBloom)
    {
        using var folder = TempFolder.Create();
        var path = folder.GetRandomFileName();
        var keys = Enumerable.Range(0, 300).Select(i => versionPrefixBloom ? VersionKey(i, 7) : BigEndian(i * 3)).ToArray();

        using (var builder = new BufferedSsTableBuilder(
            path, new DefaultSsTableEncoder(), new DefaultBlockEncoder(256, writeKeyWindows: true),
            new DefaultBloomFilterFactory(), keys.Length, formatVersion: versionPrefixBloom ? 2 : 1))
        {
            for (var i = 0; i < keys.Length; i++)
            {
                if (!builder.TryAddRawFast(keys[i], ValueFor(keys[i], i), isTombstone: false))
                {
                    await builder.RolloverBlockForRawAsync(CancellationToken.None);
                    builder.TryAddRawFast(keys[i], ValueFor(keys[i], i), isTombstone: false);
                }
            }

            using var built = await builder.BuildAsync();
        }

        var bytes = await File.ReadAllBytesAsync(path);
        var version = SsTableFormat.TryReadVersion(bytes.AsSpan(bytes.Length - SsTableFormat.FooterLength));
        await Assert.That(version).IsEqualTo(versionPrefixBloom ? 4 : 3);

        // A reader configured without key windows still searches the flagged blocks with the index.
        using var blockBuilder = new BlockBuilder(new DefaultBlockEncoder(256));
        using var table = await SsTable.LoadSsTableAsync(path, new DefaultSsTableEncoder(), blockBuilder, new DefaultBloomFilterFactory());
        await Assert.That(table.BlockMetadata.Count).IsGreaterThan(1);
        await Assert.That(versionPrefixBloom == (table.VersionPrefixBloomFilter is not null)).IsTrue();
        for (var i = 0; i < keys.Length; i++)
        {
            var blockIndex = table.BlockMetadataArray.FindMatchingBlockIndex(keys[i]);
            using var block = table.ReadBlock(blockIndex)!;
            var found = block.TryGetValue(keys[i], out var value, out _);
            var matches = found && block.HasKeyWindows && value.SequenceEqual(ValueFor(keys[i], i));
            await Assert.That(matches).IsTrue();
        }
    }

    [Test]
    public async Task FooterVersionsBeyondKeyWindowsAreRejected()
    {
        var footer = new byte[SsTableFormat.FooterLength];
        BinaryPrimitives.WriteUInt64LittleEndian(footer, 0x54535358454C4953);
        BinaryPrimitives.WriteUInt32LittleEndian(footer.AsSpan(sizeof(ulong)), (uint)SsTableFormat.MaxReadableVersion);
        await Assert.That(SsTableFormat.TryReadVersion(footer)).IsEqualTo(SsTableFormat.MaxReadableVersion);
        BinaryPrimitives.WriteUInt32LittleEndian(footer.AsSpan(sizeof(ulong)), (uint)SsTableFormat.MaxReadableVersion + 1);
        await Assert.That(() => SsTableFormat.TryReadVersion(footer)).Throws<InvalidDataException>();
        await Assert.That(SsTableFormat.GetFooterVersion(false, false)).IsEqualTo(1);
        await Assert.That(SsTableFormat.GetFooterVersion(true, false)).IsEqualTo(2);
        await Assert.That(SsTableFormat.GetFooterVersion(false, true)).IsEqualTo(3);
        await Assert.That(SsTableFormat.GetFooterVersion(true, true)).IsEqualTo(4);
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task StorageReadsTablesWrittenWithAndWithoutKeyWindows(bool startWithKeyWindows)
    {
        using var folder = TempFolder.Create();
        StorageOptions Options(bool keyWindows) => new()
        {
            FlushPeriod = TimeSpan.Zero,
            CompactionStrategy = CompactionStrategy.None,
            BlockSize = 512,
            BlockKeyWindows = keyWindows,
        };

        var expected = new SortedDictionary<int, byte[]>();
        void Write(LsmStorage storage, int from, int count)
        {
            for (var i = from; i < from + count; i++)
            {
                var value = ValueFor(BigEndian(i), i);
                storage.Put(BigEndian(i * 2), value);
                expected[i * 2] = value;
            }
        }

        await using (var storage = await LsmStorage.OpenAsync(folder, Options(startWithKeyWindows)))
        {
            Write(storage, 0, 2_000);
            await storage.FlushAndCompactAsync();
        }

        await using (var storage = await LsmStorage.OpenAsync(folder, Options(!startWithKeyWindows)))
        {
            Write(storage, 1_000, 2_000);
            await storage.FlushAndCompactAsync();
            await AssertContentsAsync(storage, expected);
        }

        foreach (var keyWindows in new[] { false, true })
        {
            var report = await LsmStorage.VerifyIntegrityAsync(folder, Options(keyWindows));
            await Assert.That(report.IsRecoveryComplete).IsTrue();
            await using var storage = await LsmStorage.OpenAsync(folder, Options(keyWindows));
            await AssertContentsAsync(storage, expected);
        }
    }

    private static async Task AssertContentsAsync(LsmStorage storage, SortedDictionary<int, byte[]> expected)
    {
        var buffer = new byte[64];
        var failures = 0;
        for (var i = 0; i <= 6_001; i++)
        {
            var length = await storage.GetRawAsync(BigEndian(i), buffer);
            var hit = expected.TryGetValue(i, out var value);
            if (hit ? length != value!.Length || !buffer.AsSpan(0, length).SequenceEqual(value) : length != -1)
            {
                failures++;
            }

            // Seek to the first key >= i and compare with the expected successor.
            var successor = expected.Keys.FirstOrDefault(k => k >= i, -1);
            var seeked = new List<byte[]>();
            await storage.SeekRawAsync(BigEndian(i), seeked, static (list, key, _) =>
            {
                list.Add(key.ToArray());
                return false;
            }, maxEntries: 1);
            var seekedKey = seeked.Count == 0 ? -1 : BinaryPrimitives.ReadInt32BigEndian(seeked[0]);
            if (seekedKey != successor)
            {
                failures++;
            }
        }

        await Assert.That(failures).IsEqualTo(0);
    }

    private static void Compare(Block expected, Block actual, byte[] probe, List<string> mismatches)
    {
        var expectedFound = expected.TryGetValue(probe, out var expectedValue, out var expectedTombstone);
        var actualFound = actual.TryGetValue(probe, out var actualValue, out var actualTombstone);
        if (expectedFound != actualFound || expectedTombstone != actualTombstone || !expectedValue.SequenceEqual(actualValue))
        {
            mismatches.Add($"TryGetValue {Convert.ToHexString(probe)}");
        }

        if (expected.LowerBound(probe) != actual.LowerBound(probe))
        {
            mismatches.Add($"LowerBound {Convert.ToHexString(probe)}: {expected.LowerBound(probe)} != {actual.LowerBound(probe)}");
        }

        if (expected.UpperBound(probe) != actual.UpperBound(probe))
        {
            mismatches.Add($"UpperBound {Convert.ToHexString(probe)}: {expected.UpperBound(probe)} != {actual.UpperBound(probe)}");
        }
    }

    private static List<byte[]> GenerateKeys(KeyShape shape, Random random)
    {
        var keys = new HashSet<byte[]>(ByteArrayComparer.Instance);
        switch (shape)
        {
            case KeyShape.RandomFixed16:
                while (keys.Count < 4_000)
                {
                    var key = new byte[16];
                    BinaryPrimitives.WriteInt64BigEndian(key, random.Next(0, 1_000_000));
                    keys.Add(key);
                }
                break;
            case KeyShape.SharedLongPrefix:
                while (keys.Count < 4_000)
                {
                    keys.Add([.. "tenant-0042/collection-users/"u8.ToArray(), .. BigEndian(random.Next(0, 1_000_000))]);
                }
                break;
            case KeyShape.TieBeyondWindow:
                // Two distinguishing leading bytes, then twelve constant bytes, then the varying part: every window
                // past the shared prefix ties, so the search must fall back to full keys.
                while (keys.Count < 4_000)
                {
                    var key = new byte[2 + 12 + 4];
                    key[0] = (byte)random.Next(0, 2);
                    key[1] = (byte)random.Next(0, 2);
                    key.AsSpan(2, 12).Fill(0x5A);
                    BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(14), random.Next());
                    keys.Add(key);
                }
                break;
            case KeyShape.VariableLengthWithZeros:
                while (keys.Count < 4_000)
                {
                    var key = new byte[random.Next(1, 14)];
                    for (var i = 0; i < key.Length; i++)
                    {
                        key[i] = (byte)(random.Next(4) == 0 ? 0 : random.Next(0, 3));
                    }
                    keys.Add(key);
                }
                break;
            case KeyShape.PrefixLongerThanMax:
                var prefix = Enumerable.Repeat((byte)'p', 300).ToArray();
                while (keys.Count < 200)
                {
                    keys.Add([.. prefix, .. BigEndian(random.Next(0, 100_000)), (byte)random.Next(0, 3)]);
                }
                break;
            case KeyShape.Single:
                keys.Add("only"u8.ToArray());
                break;
        }

        var sorted = keys.ToList();
        sorted.Sort(ByteArrayComparer.Instance);
        return sorted;
    }

    private static IEnumerable<byte[]> Probes(List<byte[]> keys, Random random)
    {
        yield return [];
        yield return [0];
        yield return [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        foreach (var key in keys)
        {
            yield return key;
            yield return [.. key, 0];
            yield return [.. key, 1];
            yield return key[..^1];
            var bumped = key.ToArray();
            bumped[random.Next(bumped.Length)] ^= (byte)(1 << random.Next(8));
            yield return bumped;
            var decremented = key.ToArray();
            decremented[^1]--;
            yield return decremented;
        }
    }

    private static byte[] ValueFor(byte[] key, int index) => [.. key, (byte)index, (byte)(index >> 8)];

    private static byte[] BigEndian(int value)
    {
        var bytes = new byte[sizeof(int)];
        BinaryPrimitives.WriteInt32BigEndian(bytes, value);
        return bytes;
    }

    private static byte[] VersionKey(int user, long version)
    {
        var key = new byte[15];
        key[0] = 1;
        BinaryPrimitives.WriteInt32BigEndian(key.AsSpan(1), user);
        BinaryPrimitives.WriteInt64BigEndian(key.AsSpan(7), version);
        return key;
    }

    private sealed class ByteArrayComparer : IComparer<byte[]>, IEqualityComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();

        public int Compare(byte[]? x, byte[]? y) => x.AsSpan().SequenceCompareTo(y);

        public bool Equals(byte[]? x, byte[]? y) => x.AsSpan().SequenceEqual(y);

        public int GetHashCode(byte[] obj)
        {
            var hash = new HashCode();
            hash.AddBytes(obj);
            return hash.ToHashCode();
        }
    }
}
