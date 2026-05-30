using System.Collections;

// TUnit parallelises tests by default. Several StorageTests construct LsmStorageInner directly against
// the shared system temp folder and assert on on-disk *.sst / *.wal file counts, which relied on xUnit's
// serial in-class execution. Serialise the whole assembly to preserve that behaviour.
[assembly: TUnit.Core.NotInParallel]

namespace Silex;

/// <summary>
/// Minimal xUnit-compatible assertion surface implemented over plain exceptions so the existing test
/// bodies run unchanged under TUnit (which provides discovery/execution; this provides the asserts).
/// Placed in the <c>Silex</c> namespace so unqualified <c>Assert</c> resolves here from both the
/// <c>Silex.Test</c> and <c>Silex.Tests</c> test namespaces, ahead of TUnit's own imported Assert.
/// </summary>
internal static class Assert
{
    public static void True(bool condition, string? message = null)
    {
        if (!condition)
        {
            throw new AssertException(message ?? "Assert.True() failure");
        }
    }

    public static void False(bool condition, string? message = null)
    {
        if (condition)
        {
            throw new AssertException(message ?? "Assert.False() failure");
        }
    }

    public static void Equal<T>(T expected, T actual)
    {
        if (!AreEqual(expected, actual))
        {
            throw new AssertException($"Assert.Equal() failure\nExpected: {Fmt(expected)}\nActual:   {Fmt(actual)}");
        }
    }

    // Lets Assert.Equal(byte[], ReadOnlyMemory<byte>) bind (byte[] implicitly converts to ReadOnlyMemory<byte>),
    // matching xUnit's dedicated Memory overloads. Compares by content.
    public static void Equal<T>(ReadOnlyMemory<T> expected, ReadOnlyMemory<T> actual)
    {
        if (!SpanEqual(expected.Span, actual.Span))
        {
            throw new AssertException($"Assert.Equal() failure (ReadOnlyMemory)\nExpected: {Fmt(expected.ToArray())}\nActual:   {Fmt(actual.ToArray())}");
        }
    }

    public static void Equal<T>(Memory<T> expected, Memory<T> actual)
        => Equal((ReadOnlyMemory<T>)expected, (ReadOnlyMemory<T>)actual);

    public static void NotEqual<T>(T expected, T actual)
    {
        if (AreEqual(expected, actual))
        {
            throw new AssertException($"Assert.NotEqual() failure\nExpected: not {Fmt(expected)}\nActual:   {Fmt(actual)}");
        }
    }

    public static void Same(object? expected, object? actual)
    {
        if (!ReferenceEquals(expected, actual))
        {
            throw new AssertException("Assert.Same() failure: references differ");
        }
    }

    public static void NotSame(object? expected, object? actual)
    {
        if (ReferenceEquals(expected, actual))
        {
            throw new AssertException("Assert.NotSame() failure: references are the same");
        }
    }

    public static void Single(IEnumerable collection)
    {
        var count = Count(collection);
        if (count != 1)
        {
            throw new AssertException($"Assert.Single() failure: collection contains {count} element(s)");
        }
    }

    public static void Empty(IEnumerable collection)
    {
        var count = Count(collection);
        if (count != 0)
        {
            throw new AssertException($"Assert.Empty() failure: collection contains {count} element(s)");
        }
    }

    public static void NotEmpty(IEnumerable collection)
    {
        if (Count(collection) == 0)
        {
            throw new AssertException("Assert.NotEmpty() failure: collection is empty");
        }
    }

    public static void Contains<T>(T expected, IEnumerable<T> collection)
    {
        foreach (var item in collection)
        {
            if (AreEqual(expected, item))
            {
                return;
            }
        }

        throw new AssertException($"Assert.Contains() failure: {Fmt(expected)} not found in collection");
    }

    public static void All<T>(IEnumerable<T> collection, Action<T> action)
    {
        foreach (var item in collection)
        {
            action(item);
        }
    }

    // Order-independent, count-sensitive comparison (matches xUnit Assert.Equivalent for collections).
    public static void Equivalent(object? expected, object? actual)
    {
        var e = ToObjectList(expected);
        var a = ToObjectList(actual);

        // xUnit's Assert.Equivalent defaults to strict: false, so actual may contain
        // additional items beyond those in expected; every expected item must appear in actual.
        var equivalent = e.Count <= a.Count;
        if (equivalent)
        {
            var remaining = new List<object?>(a);
            foreach (var item in e)
            {
                var idx = remaining.FindIndex(x => AreEqual(item, x));
                if (idx < 0)
                {
                    equivalent = false;
                    break;
                }

                remaining.RemoveAt(idx);
            }
        }

        if (!equivalent)
        {
            throw new AssertException($"Assert.Equivalent() failure\nExpected: {Fmt(expected)}\nActual:   {Fmt(actual)}");
        }
    }

    // Assert.Subset(expectedSuperset, actual): actual must be a subset of expectedSuperset (xUnit arg order).
    public static void Subset<T>(ISet<T> expectedSuperset, ISet<T> actual)
    {
        if (!actual.IsSubsetOf(expectedSuperset))
        {
            throw new AssertException("Assert.Subset() failure: actual is not a subset of the expected superset");
        }
    }

    public static TException Throws<TException>(Action testCode)
        where TException : Exception
    {
        try
        {
            testCode();
        }
        catch (TException ex)
        {
            return ex;
        }
        catch (Exception ex)
        {
            throw new AssertException($"Assert.Throws() failure: expected {typeof(TException).Name}, but {ex.GetType().Name} was thrown");
        }

        throw new AssertException($"Assert.Throws() failure: expected {typeof(TException).Name}, but no exception was thrown");
    }

    private static bool AreEqual(object? expected, object? actual)
    {
        if (ReferenceEquals(expected, actual))
        {
            return true;
        }

        if (expected is null || actual is null)
        {
            return false;
        }

        if (expected is ReadOnlyMemory<byte> em && actual is ReadOnlyMemory<byte> am)
        {
            return em.Span.SequenceEqual(am.Span);
        }

        if (expected is string || actual is string)
        {
            return expected.Equals(actual);
        }

        if (expected is IEnumerable ee && actual is IEnumerable aa)
        {
            return SequenceEqualEnum(ee, aa);
        }

        if (IsNumeric(expected) && IsNumeric(actual))
        {
            return Convert.ToDecimal(expected) == Convert.ToDecimal(actual);
        }

        return expected.Equals(actual);
    }

    private static bool IsNumeric(object value) => value is byte or sbyte or short or ushort
        or int or uint or long or ulong or float or double or decimal;

    private static bool SequenceEqualEnum(IEnumerable a, IEnumerable b)
    {
        IEnumerator ea = a.GetEnumerator();
        IEnumerator eb = b.GetEnumerator();
        try
        {
            while (true)
            {
                var ha = ea.MoveNext();
                var hb = eb.MoveNext();
                if (ha != hb)
                {
                    return false;
                }

                if (!ha)
                {
                    return true;
                }

                if (!AreEqual(ea.Current, eb.Current))
                {
                    return false;
                }
            }
        }
        finally
        {
            (ea as IDisposable)?.Dispose();
            (eb as IDisposable)?.Dispose();
        }
    }

    private static bool SpanEqual<T>(ReadOnlySpan<T> a, ReadOnlySpan<T> b)
    {
        if (a.Length != b.Length)
        {
            return false;
        }

        for (var i = 0; i < a.Length; i++)
        {
            if (!EqualityComparer<T>.Default.Equals(a[i], b[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static int Count(IEnumerable collection)
    {
        if (collection is ICollection col)
        {
            return col.Count;
        }

        var n = 0;
        foreach (var _ in collection)
        {
            n++;
        }

        return n;
    }

    private static List<object?> ToObjectList(object? o)
    {
        var list = new List<object?>();
        if (o is IEnumerable e)
        {
            foreach (var x in e)
            {
                list.Add(x);
            }
        }

        return list;
    }

    private static string Fmt(object? value)
    {
        if (value is null)
        {
            return "null";
        }

        if (value is string s)
        {
            return $"\"{s}\"";
        }

        if (value is IEnumerable e and not string)
        {
            return "[" + string.Join(", ", e.Cast<object?>().Select(x => x?.ToString() ?? "null")) + "]";
        }

        return value.ToString() ?? "null";
    }
}

public sealed class AssertException : Exception
{
    public AssertException(string message)
        : base(message)
    {
    }
}
