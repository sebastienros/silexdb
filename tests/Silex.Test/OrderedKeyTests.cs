using System.Numerics;
using System.Text;
using Silex.Keys;

namespace Silex.Test;

public class OrderedKeyTests
{
    private static void Eq<T>(T actual, T expected)
    {
        if (!EqualityComparer<T>.Default.Equals(actual, expected)) throw new Exception($"Expected {expected}, got {actual}.");
    }

    private static void True(bool condition, [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(condition))] string? expr = null)
    {
        if (!condition) throw new Exception($"Expected true: {expr}");
    }

    private static void False(bool condition, [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(condition))] string? expr = null)
    {
        if (condition) throw new Exception($"Expected false: {expr}");
    }

    private static byte[] Hex(string hex) => Convert.FromHexString(hex);

    private static byte[] Encode(Func<byte[], int> write)
    {
        var buffer = new byte[256];
        var n = write(buffer);
        return buffer.AsSpan(0, n).ToArray();
    }

    private static byte[] Int64Key(long v, OrderedKeyOrder o = OrderedKeyOrder.Ascending) => Encode(b =>
    {
        var w = new OrderedKeyWriter(b);
        w.TryWriteInt64(v, o);
        return w.Written;
    });

    private static byte[] UInt64Key(ulong v, OrderedKeyOrder o = OrderedKeyOrder.Ascending) => Encode(b =>
    {
        var w = new OrderedKeyWriter(b);
        w.TryWriteUInt64(v, o);
        return w.Written;
    });

    private static byte[] DoubleKey(double v, OrderedKeyOrder o = OrderedKeyOrder.Ascending) => Encode(b =>
    {
        var w = new OrderedKeyWriter(b);
        w.TryWriteDouble(v, o);
        return w.Written;
    });

    private static byte[] DecimalKey(decimal v, OrderedKeyOrder o = OrderedKeyOrder.Ascending) => Encode(b =>
    {
        var w = new OrderedKeyWriter(b);
        True(w.TryWriteDecimal(v, o));
        Eq(w.Written, OrderedKey.DecimalLength);
        return w.Written;
    });

    private static byte[] BytesKey(byte[] v, OrderedKeyOrder o = OrderedKeyOrder.Ascending)
    {
        var buffer = new byte[OrderedKey.GetBytesLength(v)];
        var w = new OrderedKeyWriter(buffer);
        if (!w.TryWriteBytes(v, o) || w.Written != buffer.Length) throw new InvalidOperationException();
        return buffer;
    }

    private static byte[] StringKey(string v, OrderedKeyOrder o = OrderedKeyOrder.Ascending)
    {
        var buffer = new byte[OrderedKey.GetStringLength(v)];
        var w = new OrderedKeyWriter(buffer);
        if (!w.TryWriteString(v, o) || w.Written != buffer.Length) throw new InvalidOperationException();
        return buffer;
    }

    private static int Sign(int v) => Math.Sign(v);

    private static int Cmp(byte[] a, byte[] b) => Sign(a.AsSpan().SequenceCompareTo(b));

    private static void AssertOrdered<T>(T[] ascendingValues, Func<T, OrderedKeyOrder, byte[]> encode)
    {
        for (var i = 0; i < ascendingValues.Length; i++)
        {
            for (var j = 0; j < ascendingValues.Length; j++)
            {
                var expected = Sign(i.CompareTo(j));
                Eq(Cmp(encode(ascendingValues[i], OrderedKeyOrder.Ascending),
                    encode(ascendingValues[j], OrderedKeyOrder.Ascending)), expected);
                Eq(Cmp(encode(ascendingValues[i], OrderedKeyOrder.Descending),
                    encode(ascendingValues[j], OrderedKeyOrder.Descending)), -expected);
            }
        }
    }

    [Test]
    public void ExactBinaryVectors()
    {
        True(Int64Key(0).AsSpan().SequenceEqual(Hex("208000000000000000")));
        True(Int64Key(-1).AsSpan().SequenceEqual(Hex("207FFFFFFFFFFFFFFF")));
        True(Int64Key(long.MinValue).AsSpan().SequenceEqual(Hex("200000000000000000")));
        True(Int64Key(long.MaxValue).AsSpan().SequenceEqual(Hex("20FFFFFFFFFFFFFFFF")));
        True(Int64Key(1, OrderedKeyOrder.Descending).AsSpan().SequenceEqual(Hex("DF7FFFFFFFFFFFFFFE")));
        True(UInt64Key(0x0102030405060708).AsSpan().SequenceEqual(Hex("210102030405060708")));
        True(DoubleKey(0.0).AsSpan().SequenceEqual(Hex("228000000000000000")));
        True(DoubleKey(-0.0).AsSpan().SequenceEqual(Hex("227FFFFFFFFFFFFFFF")));
        True(DoubleKey(1.0).AsSpan().SequenceEqual(Hex("22BFF0000000000000")));
        True(DoubleKey(double.NaN).AsSpan().SequenceEqual(Hex("22FFF8000000000000")));
        True(BytesKey([]).AsSpan().SequenceEqual(Hex("100001")));
        True(BytesKey([1, 0, 2, 0]).AsSpan().SequenceEqual(Hex("10" + "01" + "00FF" + "02" + "00FF" + "0001")));
        True(BytesKey([1, 0], OrderedKeyOrder.Descending).AsSpan().SequenceEqual(Hex("EF" + "FE" + "FF00" + "FFFE")));
        True(StringKey("a\0é").AsSpan().SequenceEqual(Hex("11" + "61" + "00FF" + "C3A9" + "0001")));
        var b = new byte[2];
        var w = new OrderedKeyWriter(b);
        w.TryWriteBoolean(false);
        w.TryWriteBoolean(true, OrderedKeyOrder.Descending);
        True(b.AsSpan().SequenceEqual(Hex("04FA")));
    }

    [Test]
    public void Int64AndUInt64OrderWithEdges()
    {
        long[] longs = [long.MinValue, long.MinValue + 1, -65536, -256, -2, -1, 0, 1, 2, 255, 256, 65536, int.MaxValue + 1L, long.MaxValue - 1, long.MaxValue];
        AssertOrdered(longs, Int64Key);
        ulong[] ulongs = [0, 1, 255, 256, 0x7FFFFFFFFFFFFFFF, 0x8000000000000000, ulong.MaxValue - 1, ulong.MaxValue];
        AssertOrdered(ulongs, UInt64Key);
    }

    [Test]
    public void DoubleHasDocumentedTotalOrder()
    {
        double[] values =
        [
            double.NegativeInfinity, -double.MaxValue, -1.5, -1.0, -double.Epsilon * 2, -double.Epsilon, -0.0, 0.0,
            double.Epsilon, double.Epsilon * 2, 1.0, 1.5, double.MaxValue, double.PositiveInfinity, double.NaN,
        ];
        AssertOrdered(values, DoubleKey);
        var otherNaNs = new[] { BitConverter.Int64BitsToDouble(unchecked((long)0xFFF8000000000001)), BitConverter.Int64BitsToDouble(0x7FF0000000000001), -double.NaN };
        foreach (var nan in otherNaNs)
        {
            True(double.IsNaN(nan));
            True(DoubleKey(nan).AsSpan().SequenceEqual(DoubleKey(double.NaN)));
        }
    }

    [Test]
    public void DoubleRoundTripPreservesSignedZeroAndInfinities()
    {
        foreach (var v in new[] { -0.0, 0.0, double.Epsilon, double.NegativeInfinity, double.PositiveInfinity, 1e300, -123.456 })
        {
            foreach (var o in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
            {
                var r = new OrderedKeyReader(DoubleKey(v, o));
                var st = r.TryReadDouble(out var got, o);
                Eq(st, OrderedKeyDecodeStatus.Success);
                Eq(BitConverter.DoubleToInt64Bits(got), BitConverter.DoubleToInt64Bits(v));
                True(r.IsEnd);
            }
        }
    }

    [Test]
    public void BytesOrderLexicographicallyIncludingZerosAndPrefixes()
    {
        byte[][] values =
        [
            [], [0], [0, 0], [0, 1], [1], [1, 0], [1, 0, 0], [1, 1], [1, 255], [2], [255], [255, 0], [255, 255],
        ];
        AssertOrdered(values, BytesKey);
    }

    [Test]
    public void StringsOrderByCodePointNotUtf16()
    {
        string[] values = ["", "\0", "\0\0", "A", "A\0", "AB", "B", "a", "é", "\uFFFD", "\uFFFF", "\U00010000", "\U0010FFFF"];
        AssertOrdered(values, StringKey);
        // UTF-16 ordinal would place U+10000 (D800 DC00) before U+FFFF.
        True(string.CompareOrdinal("\U00010000", "\uFFFF") < 0);
        Eq(Cmp(StringKey("\U00010000"), StringKey("\uFFFF")), 1);
    }

    [Test]
    public void BooleanOrder()
    {
        AssertOrdered(new[] { false, true }, (v, o) => Encode(b =>
        {
            var w = new OrderedKeyWriter(b);
            w.TryWriteBoolean(v, o);
            return w.Written;
        }));
    }

    [Test]
    public void KindsOrderByTagAndDescendingKindsFollowAscending()
    {
        var keys = new[]
        {
            Encode(b => { var w = new OrderedKeyWriter(b); w.TryWriteBoolean(true); return w.Written; }),
            BytesKey([255, 255]),
            StringKey(""),
            Int64Key(long.MaxValue),
            UInt64Key(0),
            DoubleKey(double.NaN),
            DecimalKey(decimal.MinValue),
            DecimalKey(decimal.MaxValue, OrderedKeyOrder.Descending),
            DoubleKey(double.NaN, OrderedKeyOrder.Descending),
            UInt64Key(0, OrderedKeyOrder.Descending),
            Int64Key(0, OrderedKeyOrder.Descending),
            StringKey("", OrderedKeyOrder.Descending),
            BytesKey([], OrderedKeyOrder.Descending),
            Encode(b => { var w = new OrderedKeyWriter(b); w.TryWriteBoolean(false, OrderedKeyOrder.Descending); return w.Written; }),
        };
        for (var i = 0; i + 1 < keys.Length; i++)
        {
            Eq(Cmp(keys[i], keys[i + 1]), -1);
        }
    }

    private static byte[] Tuple(byte[] a, long b, byte[] c, OrderedKeyOrder bo, OrderedKeyOrder co)
    {
        var buffer = new byte[OrderedKey.GetBytesLength(a) + OrderedKey.Int64Length + OrderedKey.GetBytesLength(c)];
        var w = new OrderedKeyWriter(buffer);
        w.TryWriteBytes(a);
        w.TryWriteInt64(b, bo);
        w.TryWriteBytes(c, co);
        if (w.Remaining != 0) throw new InvalidOperationException();
        return buffer;
    }

    [Test]
    public void RandomizedTuplesCompareLikeTheirComponents()
    {
        var rng = new Random(12345);
        byte[] RandomBytes()
        {
            var b = new byte[rng.Next(0, 4)];
            for (var i = 0; i < b.Length; i++) b[i] = (byte)(rng.Next(3) switch { 0 => 0, 1 => 255, _ => rng.Next(256) });
            return b;
        }

        var items = Enumerable.Range(0, 300).Select(_ => (A: RandomBytes(), B: (long)rng.Next(-2, 3) * (rng.Next(2) == 0 ? 1 : long.MaxValue / 2), C: RandomBytes())).ToArray();
        foreach (var bo in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
        {
            foreach (var co in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
            {
                for (var n = 0; n < 3000; n++)
                {
                    var x = items[rng.Next(items.Length)];
                    var y = items[rng.Next(items.Length)];
                    var expected = Sign(x.A.AsSpan().SequenceCompareTo(y.A));
                    if (expected == 0) expected = Sign(x.B.CompareTo(y.B)) * (bo == OrderedKeyOrder.Ascending ? 1 : -1);
                    if (expected == 0) expected = Sign(x.C.AsSpan().SequenceCompareTo(y.C)) * (co == OrderedKeyOrder.Ascending ? 1 : -1);
                    Eq(Cmp(Tuple(x.A, x.B, x.C, bo, co), Tuple(y.A, y.B, y.C, bo, co)), expected);
                }
            }
        }
    }

    [Test]
    public void ComponentBoundariesAreUnambiguous()
    {
        // ("a", "b") must not collide with ("a\0b")-like or ("ab", "") tuples.
        byte[] Pair(string a, string b)
        {
            var buffer = new byte[OrderedKey.GetStringLength(a) + OrderedKey.GetStringLength(b)];
            var w = new OrderedKeyWriter(buffer);
            w.TryWriteString(a);
            w.TryWriteString(b);
            return buffer;
        }

        var tuples = new[] { ("a", "b"), ("ab", ""), ("a\0b", ""), ("a", "\0b"), ("", "ab"), ("a\0", "b"), ("a", ""), ("", "") };
        var keys = tuples.Select(t => Convert.ToHexString(Pair(t.Item1, t.Item2))).ToArray();
        Eq(keys.Distinct().Count(), tuples.Length);

        foreach (var t in tuples)
        {
            var key = Pair(t.Item1, t.Item2);
            var r = new OrderedKeyReader(key);
            var s1 = new char[8];
            var s2 = new char[8];
            Eq(r.TryReadString(s1, out var n1), OrderedKeyDecodeStatus.Success);
            Eq(r.TryReadString(s2, out var n2), OrderedKeyDecodeStatus.Success);
            True(r.IsEnd);
            Eq(new string(s1, 0, n1), t.Item1);
            Eq(new string(s2, 0, n2), t.Item2);
        }

        // A tuple prefix delimits exactly the tuples that start with it, never "ab".
        var prefix = StringKey("a");
        var space = new PrefixKeyspace(prefix);
        True(space.Contains(Pair("a", "z")));
        False(space.Contains(Pair("ab", "z")));
        False(space.Contains(Pair("a\0", "z")));
    }

    [Test]
    public void ReaderWalksMixedTupleWithPeekSkipAndPositions()
    {
        var buffer = new byte[128];
        var w = new OrderedKeyWriter(buffer);
        w.TryWriteString("user");
        w.TryWriteInt64(-7, OrderedKeyOrder.Descending);
        w.TryWriteBytes([0, 1, 0]);
        w.TryWriteBoolean(true);
        w.TryWriteDouble(2.5);
        w.TryWriteUInt64(ulong.MaxValue, OrderedKeyOrder.Descending);
        var key = w.WrittenSpan.ToArray();

        var r = new OrderedKeyReader(key);
        Eq(r.TryPeek(out var kind, out var order), OrderedKeyDecodeStatus.Success);
        Eq(kind, OrderedKeyComponent.String);
        Eq(order, OrderedKeyOrder.Ascending);
        Eq(r.TryPeekLength(out var len), OrderedKeyDecodeStatus.Success);
        Eq(len, OrderedKey.GetStringLength("user"));
        Eq(r.TryPeekPayloadLength(out var payload), OrderedKeyDecodeStatus.Success);
        Eq(payload, 4);
        Eq(r.TrySkip(), OrderedKeyDecodeStatus.Success);
        Eq(r.Position, len);

        Eq(r.TryPeekPayloadLength(out _), OrderedKeyDecodeStatus.UnexpectedComponent);
        Eq(r.TryReadInt64(out _), OrderedKeyDecodeStatus.UnexpectedComponent);
        Eq(r.TryReadUInt64(out _, OrderedKeyOrder.Descending), OrderedKeyDecodeStatus.UnexpectedComponent);
        Eq(r.TryReadInt64(out var i64, OrderedKeyOrder.Descending), OrderedKeyDecodeStatus.Success);
        Eq(i64, -7L);

        var bytes = new byte[3];
        Eq(r.TryReadBytes(bytes, out var bn), OrderedKeyDecodeStatus.Success);
        True(bytes.AsSpan(0, bn).SequenceEqual(new byte[] { 0, 1, 0 }));
        Eq(r.TryReadBoolean(out var flag), OrderedKeyDecodeStatus.Success);
        True(flag);
        Eq(r.TryReadDouble(out var d), OrderedKeyDecodeStatus.Success);
        Eq(d, 2.5);
        False(r.IsEnd);
        Eq(r.TryReadUInt64(out var u, OrderedKeyOrder.Descending), OrderedKeyDecodeStatus.Success);
        Eq(u, ulong.MaxValue);
        True(r.IsEnd);
        Eq(r.TrySkip(), OrderedKeyDecodeStatus.EndOfData);
        Eq(r.TryReadBoolean(out _), OrderedKeyDecodeStatus.EndOfData);
    }

    [Test]
    public void WriterCapacityContractIsExactAndAtomic()
    {
        var key = StringKey("a\0b");
        for (var cap = 0; cap < key.Length; cap++)
        {
            var buffer = new byte[cap];
            var w = new OrderedKeyWriter(buffer);
            False(w.TryWriteString("a\0b"));
            Eq(w.Written, 0);
        }
        var exact = new byte[key.Length];
        var ok = new OrderedKeyWriter(exact);
        True(ok.TryWriteString("a\0b"));
        True(exact.AsSpan().SequenceEqual(key));
        Eq(ok.Remaining, 0);

        var small = new byte[12];
        var w2 = new OrderedKeyWriter(small);
        True(w2.TryWriteInt64(1));
        var before = w2.Written;
        False(w2.TryWriteInt64(2));
        Eq(w2.Written, before);
        True(w2.TryWriteBoolean(true));
        w2.Reset();
        Eq(w2.Written, 0);

        var empty = new OrderedKeyWriter(default);
        False(empty.TryWriteBoolean(false));
    }

    [Test]
    public void UnpairedSurrogatesThrowBeforeWriting()
    {
        var buffer = new byte[64];
        Array.Fill(buffer, (byte)0xAA);
        var threw = 0;
        var written = -1;
        foreach (var bad in new[] { "\uD800", "a\uDC00", "\uD800x" })
        {
            try
            {
                var w = new OrderedKeyWriter(buffer);
                w.TryWriteString(bad);
            }
            catch (ArgumentException)
            {
                threw++;
            }
        }
        var w2 = new OrderedKeyWriter(buffer);
        try { w2.TryWriteString("\uD800"); } catch (ArgumentException) { written = w2.Written; }
        Eq(threw, 3);
        Eq(written, 0);
        True(buffer.All(x => x == 0xAA));
    }

    [Test]
    public void ReaderBufferContractsReportRequiredLengthsWithoutAdvancing()
    {
        var text = string.Concat(Enumerable.Repeat("a\0é\U0001F600", 200));
        foreach (var order in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
        {
            var key = StringKey(text, order);
            var r = new OrderedKeyReader(key);
            var small = new char[text.Length - 1];
            Eq(r.TryReadString(small, out var needed, order), OrderedKeyDecodeStatus.DestinationTooSmall);
            Eq(needed, text.Length);
            Eq(r.Position, 0);
            var exact = new char[needed];
            Eq(r.TryReadString(exact, out var n, order), OrderedKeyDecodeStatus.Success);
            Eq(new string(exact, 0, n), text);
            True(r.IsEnd);

            var data = Enumerable.Range(0, 700).Select(i => (byte)(i % 3 == 0 ? 0 : i)).ToArray();
            var bk = BytesKey(data, order);
            var br = new OrderedKeyReader(bk);
            Eq(br.TryReadBytes(new byte[data.Length - 1], out var need, order), OrderedKeyDecodeStatus.DestinationTooSmall);
            Eq(need, data.Length);
            Eq(br.Position, 0);
            var got = new byte[need];
            Eq(br.TryReadBytes(got, out _, order), OrderedKeyDecodeStatus.Success);
            True(got.AsSpan().SequenceEqual(data));
        }
    }

    [Test]
    public void StringsCrossingDecoderChunkBoundariesRoundTrip()
    {
        foreach (var order in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
        {
            for (var n = 250; n < 270; n++)
            {
                foreach (var unit in new[] { "é", "€", "\U0001F600", "\0", "x" })
                {
                    var text = "x" + string.Concat(Enumerable.Repeat(unit, n));
                    var key = StringKey(text, order);
                    var r = new OrderedKeyReader(key);
                    var dest = new char[text.Length];
                    Eq(r.TryReadString(dest, out var len, order), OrderedKeyDecodeStatus.Success);
                    Eq(new string(dest, 0, len), text);
                }
            }
        }
    }

    private static void AssertMalformed(byte[] key, Func<byte[], OrderedKeyDecodeStatus> read)
    {
        Eq(read(key), OrderedKeyDecodeStatus.Malformed);
    }

    [Test]
    public void DecodingRejectsNonCanonicalOrTruncatedInput()
    {
        OrderedKeyDecodeStatus Skip(byte[] k)
        {
            var r = new OrderedKeyReader(k);
            var s = r.TrySkip();
            if (s != OrderedKeyDecodeStatus.Success && r.Position != 0) throw new InvalidOperationException();
            return s;
        }

        OrderedKeyDecodeStatus Str(byte[] k)
        {
            var r = new OrderedKeyReader(k);
            var s = r.TryReadString(new char[64], out _);
            if (s != OrderedKeyDecodeStatus.Success && r.Position != 0) throw new InvalidOperationException();
            return s;
        }

        OrderedKeyDecodeStatus Dbl(byte[] k)
        {
            var r = new OrderedKeyReader(k);
            var s = r.TryReadDouble(out _);
            if (s != OrderedKeyDecodeStatus.Success && r.Position != 0) throw new InvalidOperationException();
            return s;
        }

        AssertMalformed(Hex("00"), Skip);
        AssertMalformed(Hex("FF"), Skip);
        AssertMalformed(Hex("03"), Skip);
        AssertMalformed(Hex("7F"), Skip);
        AssertMalformed(Hex("2000"), Skip);
        AssertMalformed(Hex("20800000000000"), Skip);
        AssertMalformed(Hex("10"), Skip);
        AssertMalformed(Hex("1001"), Skip);
        AssertMalformed(Hex("100000"), Skip);
        AssertMalformed(Hex("1000"), Skip);
        AssertMalformed(Hex("100002"), Skip);
        AssertMalformed(Hex("1001" + "00FF"), Skip);
        AssertMalformed(Hex("EF01"), Skip);
        AssertMalformed(Hex("EFFFFF"), Skip);
        AssertMalformed(Hex("EFFF01"), Skip);
        AssertMalformed(Hex("11C30001"), Str);
        AssertMalformed(Hex("11C0AF0001"), Str);
        AssertMalformed(Hex("11EDA080" + "0001"), Str);
        AssertMalformed(Hex("11F4908080" + "0001"), Str);
        AssertMalformed(Hex("22FFF8000000000001"), Dbl);
        AssertMalformed(Hex("220007FFFFFFFFFFFF"), Dbl);
        Eq(Skip(Hex("100001")), OrderedKeyDecodeStatus.Success);
        Eq(Skip(Hex("100001" + "00")), OrderedKeyDecodeStatus.Success);
        Eq(Skip(Hex("EFFFFE")), OrderedKeyDecodeStatus.Success);
    }

    [Test]
    public void SizesMatchWriterOutput()
    {
        Eq(OrderedKey.DecimalLength, 25);
        Eq(OrderedKey.GetBytesLength([0, 0, 5]), 3 + 3 + 2);
        Eq(OrderedKey.GetStringLength("é\0"), 3 + 3 + 1);
        Eq(OrderedKey.GetStringLength("\U0001F600"), 3 + 4);
        Eq(OrderedKey.GetStringLength(""), 3);
        Eq(Encoding.UTF8.GetByteCount("é"), 2);
    }

    [Test]
    public void EncodingAndDecodingDoNotAllocate()
    {
        var buffer = new byte[256];
        var chars = new char[32];
        var bytes = new byte[32];
        byte[] payload = [1, 0, 2];
        var text = "héllo\0";
        RoundTrip(buffer, chars, bytes, payload, text);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) RoundTrip(buffer, chars, bytes, payload, text);
        Eq(GC.GetAllocatedBytesForCurrentThread() - before, 0L);
    }

    private static void RoundTrip(byte[] buffer, char[] chars, byte[] bytes, byte[] payload, string text)
    {
        var w = new OrderedKeyWriter(buffer);
        w.TryWriteString(text, OrderedKeyOrder.Descending);
        w.TryWriteBytes(payload);
        w.TryWriteInt64(-5);
        w.TryWriteDouble(1.5, OrderedKeyOrder.Descending);
        w.TryWriteDecimal(decimal.MinValue, OrderedKeyOrder.Descending);
        var r = new OrderedKeyReader(w.WrittenSpan);
        r.TryReadString(chars, out _, OrderedKeyOrder.Descending);
        r.TryReadBytes(bytes, out _);
        r.TryReadInt64(out _);
        r.TryReadDouble(out _, OrderedKeyOrder.Descending);
        if (r.TryReadDecimal(out var dec, OrderedKeyOrder.Descending) != OrderedKeyDecodeStatus.Success
            || dec != decimal.MinValue) throw new InvalidOperationException();
        if (!r.IsEnd) throw new InvalidOperationException();
    }

    [Test]
    public void DecimalGoldenFixtures()
    {
        (decimal Value, string Hex)[] fixtures =
        [
            (0m, "23800000000000000000000000000000000000000000000000"),
            (0.0000000000000000000000000001m, "23800000000000000000000000000000000000000000000001"),
            (-0.0000000000000000000000000001m, "237FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),
            (1m, "23800000000000000000000000204FCE5E3E25026110000000"),
            (-1m, "237FFFFFFFFFFFFFFFFFFFFFFFDFB031A1C1DAFD9EF0000000"),
            (decimal.MaxValue, "23A04FCE5E3E2502610FFFFFFFDFB031A1C1DAFD9EF0000000"),
            (decimal.MinValue, "235FB031A1C1DAFD9EF0000000204FCE5E3E25026110000000"),
            (new decimal(-1, -1, -1, false, 28), "23800000000000000000000000FFFFFFFFFFFFFFFFFFFFFFFF"),
            (new decimal(0, 0, 1, false, 28), "23800000000000000000000000000000010000000000000000"),
            (new decimal(0, 1, 0, false, 0), "238000000000000000204FCE5E3E2502611000000000000000"),
        ];
        foreach (var (value, hex) in fixtures)
        {
            var expected = Hex(hex);
            True(DecimalKey(value).AsSpan().SequenceEqual(expected));
            for (var i = 0; i < expected.Length; i++) expected[i] = (byte)~expected[i];
            Eq(expected[0], (byte)0xDC);
            True(DecimalKey(value, OrderedKeyOrder.Descending).AsSpan().SequenceEqual(expected));
        }
    }

    [Test]
    public void DecimalScalesAndSignedZerosCanonicalize()
    {
        foreach (var order in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
        {
            foreach (var negative in new[] { false, true })
            {
                for (byte scale = 0; scale <= 28; scale++)
                {
                    var zero = new decimal(0, 0, 0, negative, scale);
                    var key = DecimalKey(zero, order);
                    True(key.AsSpan().SequenceEqual(DecimalKey(0m, order)));
                    var reader = new OrderedKeyReader(key);
                    Eq(reader.TryReadDecimal(out var decoded, order), OrderedKeyDecodeStatus.Success);
                    True(decimal.GetBits(decoded).AsSpan().SequenceEqual(new int[4]));
                }
            }
            foreach (var value in new[] { 1m, 1.0m, 1.00m, 1.0000000000000000000000000000m })
            {
                True(DecimalKey(value, order).AsSpan().SequenceEqual(DecimalKey(1m, order)));
                True(DecimalKey(-value, order).AsSpan().SequenceEqual(DecimalKey(-1m, order)));
            }
        }
    }

    private static BigInteger DecimalInteger(decimal value)
    {
        var bits = decimal.GetBits(value);
        var coefficient = (BigInteger)(uint)bits[0] | (BigInteger)(uint)bits[1] << 32 | (BigInteger)(uint)bits[2] << 64;
        var magnitude = coefficient * BigInteger.Pow(10, 28 - ((bits[3] >> 16) & 0xFF));
        return bits[3] < 0 ? -magnitude : magnitude;
    }

    private static byte[] RawDecimalKey(BigInteger integer, OrderedKeyOrder order)
    {
        var payload = integer.ToByteArray(isUnsigned: false, isBigEndian: true);
        True(payload.Length <= 24);
        var key = new byte[25];
        if (integer.Sign < 0) Array.Fill(key, (byte)0xFF);
        payload.CopyTo(key, 25 - payload.Length);
        key[0] = 0x23;
        key[1] ^= 0x80;
        if (order == OrderedKeyOrder.Descending)
            for (var i = 0; i < key.Length; i++) key[i] = (byte)~key[i];
        return key;
    }

    private static bool TryModelDecimal(BigInteger integer, out decimal value)
    {
        value = 0;
        var coefficient = BigInteger.Abs(integer);
        var maximumCoefficient = (BigInteger.One << 96) - 1;
        if (coefficient > maximumCoefficient * BigInteger.Pow(10, 28)) return false;
        if (coefficient.IsZero) return true;
        byte scale = 28;
        while (scale > 0 && coefficient % 10 == 0)
        {
            coefficient /= 10;
            scale--;
        }
        if (coefficient > maximumCoefficient) return false;
        value = new decimal(unchecked((int)(uint)(coefficient & uint.MaxValue)),
            unchecked((int)(uint)((coefficient >> 32) & uint.MaxValue)),
            unchecked((int)(uint)(coefficient >> 64)), integer.Sign < 0, scale);
        return true;
    }

    [Test]
    public void RandomDecimalOrderAndRoundTripMatchIntegerModel()
    {
        var random = new Random(15731);
        decimal NextValue() => new(unchecked((int)random.NextInt64()), unchecked((int)random.NextInt64()),
            unchecked((int)random.NextInt64()), random.Next(2) == 0, (byte)random.Next(29));
        for (var i = 0; i < 10_000; i++)
        {
            var value = NextValue();
            var other = NextValue();
            var integer = DecimalInteger(value);
            True(TryModelDecimal(integer, out var normalized));
            foreach (var order in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
            {
                var key = DecimalKey(value, order);
                True(key.AsSpan().SequenceEqual(RawDecimalKey(integer, order)));
                Eq(Cmp(key, DecimalKey(other, order)), Sign(decimal.Compare(value, other))
                    * (order == OrderedKeyOrder.Ascending ? 1 : -1));
                var reader = new OrderedKeyReader(key);
                Eq(reader.TryReadDecimal(out var decoded, order), OrderedKeyDecodeStatus.Success);
                Eq(decoded, value);
                True(decimal.GetBits(decoded).AsSpan().SequenceEqual(decimal.GetBits(normalized)));
                True(key.AsSpan().SequenceEqual(DecimalKey(decoded, order)));
                True(reader.IsEnd);
            }
        }
    }

    [Test]
    public void DecimalCoefficientAndScaleBoundariesMatchIntegerModel()
    {
        BigInteger[] coefficients =
        [
            0, 1, 10, uint.MaxValue, BigInteger.One << 32, (BigInteger.One << 64) - 1,
            BigInteger.One << 64, BigInteger.One << 95, (BigInteger.One << 96) - 2, (BigInteger.One << 96) - 1,
        ];
        foreach (var coefficient in coefficients)
        {
            for (byte scale = 0; scale <= 28; scale++)
            {
                foreach (var negative in new[] { false, true })
                {
                    var value = new decimal(unchecked((int)(uint)(coefficient & uint.MaxValue)),
                        unchecked((int)(uint)((coefficient >> 32) & uint.MaxValue)),
                        unchecked((int)(uint)(coefficient >> 64)), negative, scale);
                    var integer = DecimalInteger(value);
                    True(TryModelDecimal(integer, out var expected));
                    foreach (var order in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
                    {
                        var key = DecimalKey(value, order);
                        True(key.AsSpan().SequenceEqual(RawDecimalKey(integer, order)));
                        var reader = new OrderedKeyReader(key);
                        Eq(reader.TryReadDecimal(out var decoded, order), OrderedKeyDecodeStatus.Success);
                        True(decimal.GetBits(decoded).AsSpan().SequenceEqual(decimal.GetBits(expected)));
                        True(key.AsSpan().SequenceEqual(DecimalKey(decoded, order)));
                    }
                }
            }
        }
    }

    [Test]
    public void DecimalPayloadValidationMatchesIntegerModel()
    {
        var maximum = ((BigInteger.One << 96) - 1) * BigInteger.Pow(10, 28);
        BigInteger[] boundaries =
        [
            0, 1, -1, maximum, -maximum, maximum - 1, -maximum + 1, maximum + 1, -maximum - 1,
            BigInteger.One << 96, -(BigInteger.One << 96),
            -(BigInteger.One << 191), (BigInteger.One << 191) - 1,
        ];
        var random = new Random(2728);
        var raw = new byte[24];
        for (var i = 0; i < 3000 + boundaries.Length; i++)
        {
            random.NextBytes(raw);
            var integer = i < boundaries.Length ? boundaries[i] : new BigInteger(raw, isUnsigned: false, isBigEndian: true);
            var representable = TryModelDecimal(integer, out var expected);
            foreach (var order in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
            {
                var key = RawDecimalKey(integer, order);
                var reader = new OrderedKeyReader(key);
                var status = reader.TryReadDecimal(out var decoded, order);
                Eq(status, representable ? OrderedKeyDecodeStatus.Success : OrderedKeyDecodeStatus.Malformed);
                Eq(reader.Position, representable ? 25 : 0);
                if (representable)
                {
                    True(decimal.GetBits(decoded).AsSpan().SequenceEqual(decimal.GetBits(expected)));
                    True(key.AsSpan().SequenceEqual(DecimalKey(decoded, order)));
                }
                var peeker = new OrderedKeyReader(key);
                Eq(peeker.TryPeek(out _, out _), status);
                Eq(peeker.TryPeekLength(out _), status);
                Eq(peeker.Position, 0);
                Eq(peeker.TrySkip(), status);
                Eq(peeker.Position, representable ? 25 : 0);
            }
        }
    }

    [Test]
    public void DecimalTruncationCapacityAndWrongKindDoNotAdvance()
    {
        foreach (var order in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
        {
            foreach (var value in new[] { decimal.MinValue, decimal.MaxValue, 0m, 0.0000000000000000000000000001m })
            {
                var key = DecimalKey(value, order);
                for (var length = 0; length < 25; length++)
                {
                    var reader = new OrderedKeyReader(key.AsSpan(0, length));
                    var status = length == 0 ? OrderedKeyDecodeStatus.EndOfData : OrderedKeyDecodeStatus.Malformed;
                    Eq(reader.TryReadDecimal(out _, order), status);
                    Eq(reader.TryPeek(out _, out _), status);
                    Eq(reader.TryPeekLength(out _), status);
                    Eq(reader.TrySkip(), status);
                    Eq(reader.Position, 0);
                    var buffer = new byte[length];
                    var writer = new OrderedKeyWriter(buffer);
                    False(writer.TryWriteDecimal(value, order));
                    Eq(writer.Written, 0);
                }
                var wrongOrder = new OrderedKeyReader(key);
                Eq(wrongOrder.TryReadDecimal(out _, order == OrderedKeyOrder.Ascending
                    ? OrderedKeyOrder.Descending : OrderedKeyOrder.Ascending), OrderedKeyDecodeStatus.UnexpectedComponent);
                Eq(wrongOrder.Position, 0);
                Eq(wrongOrder.TryReadDouble(out _, order), OrderedKeyDecodeStatus.UnexpectedComponent);
                Eq(wrongOrder.Position, 0);
            }
        }
        var otherKind = new OrderedKeyReader(Int64Key(1));
        Eq(otherKind.TryReadDecimal(out _), OrderedKeyDecodeStatus.UnexpectedComponent);
        Eq(otherKind.Position, 0);
        var exact = new OrderedKeyWriter(new byte[25]);
        True(exact.TryWriteDecimal(123.45m));
        Eq(exact.Remaining, 0);
        exact.Reset();
        True(exact.TryWriteBoolean(true));
        False(exact.TryWriteDecimal(123.45m));
        Eq(exact.Written, 1);
        var empty = new OrderedKeyWriter(default);
        False(empty.TryWriteDecimal(0m));
    }

    [Test]
    public void DecimalConsumesExactlyOneComponentInMixedTuples()
    {
        foreach (var order in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
        {
            var writer = new OrderedKeyWriter(new byte[128]);
            True(writer.TryWriteBoolean(true));
            True(writer.TryWriteDecimal(-123.4500m, order));
            True(writer.TryWriteInt64(42));
            True(writer.TryWriteString("tail"));
            var reader = new OrderedKeyReader(writer.WrittenSpan);
            Eq(reader.TryReadBoolean(out _), OrderedKeyDecodeStatus.Success);
            Eq(reader.TryPeek(out var kind, out var direction), OrderedKeyDecodeStatus.Success);
            Eq(kind, OrderedKeyComponent.Decimal);
            Eq(direction, order);
            Eq(reader.TryPeekLength(out var length), OrderedKeyDecodeStatus.Success);
            Eq(length, 25);
            Eq(reader.TryPeekPayloadLength(out _), OrderedKeyDecodeStatus.UnexpectedComponent);
            Eq(reader.Position, 1);
            Eq(reader.TryReadDecimal(out var value, order), OrderedKeyDecodeStatus.Success);
            Eq(value, -123.45m);
            True(decimal.GetBits(value).AsSpan().SequenceEqual(decimal.GetBits(-123.45m)));
            Eq(reader.Position, 26);
            Eq(reader.TryReadInt64(out var integer), OrderedKeyDecodeStatus.Success);
            Eq(integer, 42L);
            var chars = new char[4];
            Eq(reader.TryReadString(chars, out _), OrderedKeyDecodeStatus.Success);
            Eq(new string(chars), "tail");
            True(reader.IsEnd);
            var skipper = new OrderedKeyReader(writer.WrittenSpan);
            Eq(skipper.TrySkip(), OrderedKeyDecodeStatus.Success);
            Eq(skipper.TrySkip(), OrderedKeyDecodeStatus.Success);
            Eq(skipper.Position, 26);
            Eq(skipper.TryReadInt64(out integer), OrderedKeyDecodeStatus.Success);
            Eq(integer, 42L);
        }
    }

    [Test]
    public void MalformedDecimalAfterValidPrefixDoesNotAdvance()
    {
        foreach (var order in new[] { OrderedKeyOrder.Ascending, OrderedKeyOrder.Descending })
        {
            var invalid = RawDecimalKey(BigInteger.One << 96, order);
            var key = new byte[27];
            key[0] = 0x05;
            invalid.CopyTo(key, 1);
            key[26] = 0x04;
            var reader = new OrderedKeyReader(key);
            Eq(reader.TryReadBoolean(out _), OrderedKeyDecodeStatus.Success);
            Eq(reader.TryReadDecimal(out var value, order), OrderedKeyDecodeStatus.Malformed);
            Eq(value, 0m);
            Eq(reader.TryPeek(out _, out _), OrderedKeyDecodeStatus.Malformed);
            Eq(reader.TryPeekLength(out _), OrderedKeyDecodeStatus.Malformed);
            Eq(reader.TrySkip(), OrderedKeyDecodeStatus.Malformed);
            Eq(reader.Position, 1);
        }
    }

    [Test]
    public void DecimalBoundaryEncodingAndDecodingDoNotAllocate()
    {
        decimal[] values = [0m, new decimal(0, 0, 0, true, 28), -1.00m, 1m,
            0.0000000000000000000000000001m, -0.0000000000000000000000000001m, decimal.MinValue, decimal.MaxValue];
        var buffer = new byte[25];
        Check();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++) Check();
        Eq(GC.GetAllocatedBytesForCurrentThread() - before, 0L);

        void Check()
        {
            foreach (var value in values)
            {
                for (var direction = 0; direction < 2; direction++)
                {
                    var order = (OrderedKeyOrder)direction;
                    var writer = new OrderedKeyWriter(buffer);
                    if (!writer.TryWriteDecimal(value, order)) throw new InvalidOperationException();
                    var reader = new OrderedKeyReader(writer.WrittenSpan);
                    if (reader.TryPeek(out _, out _) != OrderedKeyDecodeStatus.Success
                        || reader.TryReadDecimal(out var decoded, order) != OrderedKeyDecodeStatus.Success
                        || decoded != value || !reader.IsEnd) throw new InvalidOperationException();
                }
            }
        }
    }
}
