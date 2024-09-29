namespace Silex.Test;

using Silex.Collections;

public class SkipListTests
{
    [Fact]
    public void ShouldAdd()
    {
        var list = new SkipList<int, int>();

        for (var i = 1; i <= 100; i++)
        {
            list[i] = i;
            Assert.Equal(i, list.Count);
        }
    }

    [Fact]
    public void ShouldRemove()
    {
        var list = new SkipList<int, int>();

        list[1] = 1;
        var removed = list.TryRemove(1, out var value);

        Assert.True(removed);
        Assert.Equal(1, value);
        Assert.Empty(list);
    }

    [Fact]
    public void ShouldContainsKey()
    {
        var list = new SkipList<int, int>();

        var key = 123;

        list[key] = 456;
        var containsKey = list.ContainsKey(key);

        Assert.True(containsKey);
    }

    [Fact]
    public void ShouldGetKey()
    {
        var list = new SkipList<int, int>();
        var key = 123;
        var value = 456;

        list[key] = value;
        var result = list[key];

        Assert.Equal(value, result);
    }

    [Theory]
    [InlineData(0, 1)]
    [InlineData(1, 3)]
    [InlineData(2, 3)]
    [InlineData(3, 7)]
    [InlineData(4, 7)]
    [InlineData(5, 7)]
    [InlineData(6, 7)]
    [InlineData(7, 8)]
    [InlineData(8, 13)]
    [InlineData(9, 13)]
    [InlineData(10, 13)]
    [InlineData(11, 13)]
    [InlineData(12, 13)]
    [InlineData(13, 15)]
    [InlineData(14, 15)]
    [InlineData(15, 19)]
    [InlineData(16, 19)]
    [InlineData(17, 19)]
    [InlineData(18, 19)]
    [InlineData(19, 0)]
    [InlineData(20, 0)]
    public void ShouldGetNext(int key, int expected)
    {
        int[] source = [1, 3, 7, 8, 13, 15, 19];
        var list = new SkipList<int, int>();

        foreach (var i in source)
        {
            list.Add(i, i);
        }

        var found = list.TryGetNext(key, out var next);

        Assert.Equal(expected, next);
    }

    [Fact]
    public void ShouldIterateRanges()
    {
        int[] source = [1, 3, 7, 8, 13, 15, 19];

        for (var lower = source.First() - 1; lower <= source.Last() + 1; lower++)
        {
            for (var upper = source.First() - 1; upper <= source.Last() + 1; upper++)
            {
                var list = new SkipList<int, int>();

                foreach (var i in source)
                {
                    list.Add(i, i);
                }

                var expected = source.Where(x => x >= lower && x <= upper).ToArray();

                var enumerator = list.GetEnumerator(lower, upper);

                var result = new List<int>();

                while (enumerator.MoveNext())
                {
                    result.Add(enumerator.Current.Key);
                }

                try
                {
                    Assert.Equivalent(expected, result);
                }
                catch (Exception e)
                {
                    Assert.Fail($"Failed for {lower}, {upper}: {e.Message}");
                }
            }
        }
    }
}
