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
}
