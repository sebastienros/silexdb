namespace Silex.Test;

using Silex.Collections;

public class MemTableTests
{
    [Fact]
    public void SortedDictionaryShouldReturnRange()
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
        Assert.Equivalent(new int[] { 60, 70, 80, 90 }, items.Select(x => x.Key));

        items = dic.Enumerate(51, 51, false, true);
        Assert.Equivalent(new int[] { 0, 10, 20, 30, 50 }, items.Select(x => x.Key));

        items = dic.Enumerate(0, 0, true, true);
        Assert.Equivalent(new int[] { 0 }, items.Select(x => x.Key));

        items = dic.Enumerate(0, 0, false, false);
        Assert.Equivalent(Array.Empty<int>(), items.Select(x => x.Key));

        items = dic.Enumerate(20, 20, false, false);
        Assert.Equivalent(Array.Empty<int>(), items.Select(x => x.Key));

        items = dic.Enumerate(20, 20, true, true);
        Assert.Equivalent(new int[] { 20 }, items.Select(x => x.Key));

        items = dic.Enumerate(25, 65, true, true);
        Assert.Equivalent(new int[] { 30, 40, 50 }, items.Select(x => x.Key));

        items = dic.Enumerate(-1, 100, true, true);
        Assert.Equivalent(new int[] { 0, 10, 20, 30, 40, 50, 60, 70, 80, 90 }, items.Select(x => x.Key));
    }
}
