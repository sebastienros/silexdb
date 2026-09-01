using Silex.Tables;

namespace Silex.Test;

public class BlockMetadataTests
{
    [Test]
    public async Task PackedLookupRespectsBlockBoundariesAndGaps()
    {
        using var store = BlockMetadataStore.Create(
        [
            CreateMetadata(0, [10], [19]),
            CreateMetadata(1, [30], [39]),
            CreateMetadata(2, [50], [59]),
        ]);

        await Assert.That(store.FindMatchingBlockIndex([10])).IsEqualTo(0);
        await Assert.That(store.FindMatchingBlockIndex([19])).IsEqualTo(0);
        await Assert.That(store.FindMatchingBlockIndex([20])).IsEqualTo(-1);
        await Assert.That(store.FindMatchingBlockIndex([30])).IsEqualTo(1);
        await Assert.That(store.FindMatchingBlockIndex([59])).IsEqualTo(2);
        await Assert.That(store.FindMatchingBlockIndex([60])).IsEqualTo(-1);
    }

    [Test]
    public async Task StoreCopiesBoundaryKeysIntoOwnedContiguousStorage()
    {
        var firstKey = new byte[] { 1, 2, 3 };
        var lastKey = new byte[] { 4, 5, 6 };
        using var store = BlockMetadataStore.Create([CreateMetadata(0, firstKey, lastKey)]);

        firstKey.AsSpan().Fill(0);
        lastKey.AsSpan().Fill(0);

        await Assert.That(store.GetFirstKeySpan(0).ToArray()).IsEquivalentTo(new byte[] { 1, 2, 3 });
        await Assert.That(store.GetLastKeySpan(0).ToArray()).IsEquivalentTo(new byte[] { 4, 5, 6 });
    }

    private static BlockMetadata CreateMetadata(int index, byte[] firstKey, byte[] lastKey)
    {
        return new BlockMetadata(index, index * 4096L, 4096, SstCompression.None, 0, firstKey, lastKey);
    }
}
