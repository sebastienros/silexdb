namespace Silex.Jepsen;

/// <summary>Sanity-checks the checkers themselves against histories with known verdicts.</summary>
internal static class SelfTest
{
    public static WorkloadResult Run()
    {
        var failures = new List<string>();
        var checks = 0;

        void Expect(string name, bool actual, bool expected)
        {
            checks++;
            if (actual != expected)
            {
                failures.Add($"{name}: expected {expected} but the checker said {actual}");
            }
        }

        RegOp Read(long call, long ret, int value) =>
            new() { Kind = RegOp.Read, Obs = value, Call = call, Ret = ret };
        RegOp Write(long call, long ret, int value, bool info = false) =>
            new() { Kind = RegOp.Write, A = value, Call = call, Ret = info ? long.MaxValue : ret, Info = info };
        RegOp Cas(long call, long ret, int expected, int next, bool applied, int observed = 0) =>
            new() { Kind = RegOp.Cas, A = expected, B = next, Applied = applied, Obs = observed, Call = call, Ret = ret };

        Expect("sequential write/read", LinearizabilityChecker.Check([Write(1, 2, 1), Read(3, 4, 1)]) == true, true);
        Expect("stale read", LinearizabilityChecker.Check([Write(1, 2, 1), Read(3, 4, -1)]) == true, false);
        Expect("read of the future", LinearizabilityChecker.Check([Read(1, 2, 1), Write(3, 4, 1)]) == true, false);
        Expect("concurrent write/read sees new", LinearizabilityChecker.Check([Write(1, 4, 1), Read(2, 3, 1)]) == true, true);
        Expect("concurrent write/read sees old", LinearizabilityChecker.Check([Write(1, 4, 1), Read(2, 3, -1)]) == true, true);
        Expect("non-monotonic reads", LinearizabilityChecker.Check(
            [Write(1, 10, 1), Read(2, 3, 1), Read(4, 5, -1)]) == true, false);
        Expect("info write explains read", LinearizabilityChecker.Check([Write(1, 0, 7, info: true), Read(2, 3, 7)]) == true, true);
        Expect("info write may not apply", LinearizabilityChecker.Check([Write(1, 0, 7, info: true), Read(2, 3, -1)]) == true, true);
        Expect("cas applies", LinearizabilityChecker.Check([Write(1, 2, 1), Cas(3, 4, 1, 2, true), Read(5, 6, 2)]) == true, true);
        Expect("cas applied on wrong value", LinearizabilityChecker.Check([Write(1, 2, 1), Cas(3, 4, 5, 2, true)]) == true, false);
        Expect("two cas from the same value", LinearizabilityChecker.Check(
            [Write(1, 2, 1), Cas(3, 6, 1, 2, true), Cas(4, 7, 1, 3, true)]) == true, false);
        Expect("failed cas observes", LinearizabilityChecker.Check([Write(1, 2, 1), Cas(3, 4, 9, 2, false, 1)]) == true, true);

        var t = 0L;
        Op Txn(OpStatus status, params Mop[] mops) =>
            new() { Client = 0, F = "txn", Input = mops, Invoke = ++t, Complete = status == OpStatus.Info ? long.MaxValue : ++t, Status = status };
        Mop R(int key, params int[] result) => new() { F = 'r', Key = key, Result = result };
        Mop A(int key, int value) => new() { F = 'a', Key = key, Value = value };
        Dictionary<int, int[]> Final(params (int Key, int[] List)[] lists) => lists.ToDictionary(l => l.Key, l => l.List);

        bool Anomalous(List<Op> history, Dictionary<int, int[]> final, IsolationLevel level, string needle) =>
            ElleChecker.Check(history, final, level, realtime: false).Anomalies.Any(a => a.Contains(needle));

        var clean = new List<Op> { Txn(OpStatus.Ok, A(1, 1)), Txn(OpStatus.Ok, R(1, 1), A(1, 2)) };
        Expect("clean history", ElleChecker.Check(clean, Final((1, [1, 2])), IsolationLevel.StrictSerializable, false).Anomalies.Count == 0, true);

        t = 0;
        var lost = new List<Op> { Txn(OpStatus.Ok, A(1, 1)) };
        Expect("lost write", Anomalous(lost, Final((1, [])), IsolationLevel.StrongSnapshotIsolation, "lost write"), true);

        t = 0;
        var aborted = new List<Op> { Txn(OpStatus.Fail, A(1, 7)), Txn(OpStatus.Ok, R(1, 7)) };
        Expect("aborted read", Anomalous(aborted, Final((1, [7])), IsolationLevel.StrongSnapshotIsolation, "G1a"), true);

        t = 0;
        var intermediate = new List<Op> { Txn(OpStatus.Ok, A(1, 1), A(1, 2)), Txn(OpStatus.Ok, R(1, 1)) };
        Expect("intermediate read", Anomalous(intermediate, Final((1, [1, 2])), IsolationLevel.StrongSnapshotIsolation, "G1b"), true);

        t = 0;
        var incompatible = new List<Op> { Txn(OpStatus.Ok, A(1, 1)), Txn(OpStatus.Ok, A(1, 2)), Txn(OpStatus.Ok, R(1, 2)) };
        Expect("incompatible order", Anomalous(incompatible, Final((1, [1, 2])), IsolationLevel.StrongSnapshotIsolation, "incompatible"), true);

        // T1 writes x and y; T2 sees T1's x but not its y: read skew, forbidden by snapshot isolation.
        t = 0;
        var skew = new List<Op> { Txn(OpStatus.Ok, A(1, 1), A(2, 1)), Txn(OpStatus.Ok, R(1, 1), R(2)) };
        Expect("read skew", Anomalous(skew, Final((1, [1]), (2, [1])), IsolationLevel.StrongSnapshotIsolation, "G-single"), true);

        // Write skew: allowed by snapshot isolation, forbidden by serializability.
        t = 0;
        var writeSkew = new List<Op> { Txn(OpStatus.Ok, R(1), A(2, 1)), Txn(OpStatus.Ok, R(2), A(1, 1)) };
        var finalSkew = Final((1, [1]), (2, [1]));
        Expect("write skew allowed under SI", ElleChecker.Check(writeSkew, finalSkew, IsolationLevel.StrongSnapshotIsolation, false).Anomalies.Count == 0, true);
        Expect("write skew rejected when serializable", Anomalous(writeSkew, finalSkew, IsolationLevel.StrictSerializable, "G2"), true);

        // T2 starts after T1 finished but does not see its write: only a real-time cycle.
        t = 0;
        var stale = new List<Op> { Txn(OpStatus.Ok, A(1, 1)), Txn(OpStatus.Ok, R(1)) };
        Expect("stale read with real time", ElleChecker.Check(stale, Final((1, [1])), IsolationLevel.StrictSerializable, true).Anomalies.Count > 0, true);
        Expect("stale read without real time", ElleChecker.Check(stale, Final((1, [1])), IsolationLevel.StrictSerializable, false).Anomalies.Count == 0, true);

        return new WorkloadResult("selftest", "-", 0, failures.Count == 0, false, $"{checks} checker cases", failures);
    }
}
