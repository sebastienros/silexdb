namespace Silex.Test;

using Silex.Collections;

public class MemTableTests
{
    [Test]
    public async Task SortedDictionaryShouldReturnRange()
    {
        // SortedDictionary doesn't provide a way to enumerate a range of values (as of .NET 9) so
        // we use a custom extension method to access the private SortedSet which allows it
        // c.f. https://github.com/dotnet/runtime/issues/77645

        var dic = new SortedDictionary<int, int>();

        for (int i = 0; i < 10; i++)
        {
            dic.Add(i * 10, i * 10);
        }

        var items = dic.Enumerate(51, 51, true, false);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 60, 70, 80, 90 });

        items = dic.Enumerate(51, 51, false, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0, 10, 20, 30, 40, 50 });

        items = dic.Enumerate(0, 0, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0 });

        items = dic.Enumerate(0, 0, false, false);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 });

        items = dic.Enumerate(20, 20, false, false);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 });

        items = dic.Enumerate(20, 20, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 20 });

        items = dic.Enumerate(25, 65, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 30, 40, 50, 60 });

        items = dic.Enumerate(-1, 100, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(new int[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 });

        // A lower bound past the last key (from > Max) selects nothing rather than throwing.
        items = dic.Enumerate(95, 95, true, false);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());

        // An upper bound below the first key (to < Min) selects nothing rather than throwing.
        items = dic.Enumerate(-5, -5, false, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());

        // A two-sided range entirely above the data selects nothing.
        items = dic.Enumerate(100, 200, true, true);
        await Assert.That(items.Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());
    }

    [Test]
    public async Task EnumerateOnEmptyDictionaryReturnsEmpty()
    {
        var dic = new SortedDictionary<int, int>();

        await Assert.That(dic.Enumerate(5, 5, true, false).Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());
        await Assert.That(dic.Enumerate(0, 0, false, false).Select(x => x.Key)).IsEquivalentTo(Array.Empty<int>());
    }
}
