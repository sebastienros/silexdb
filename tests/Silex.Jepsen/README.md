# Silex.Jepsen

A Jepsen-style consistency harness for `MvccStorage` and, for the single-key and crash workloads, plain `LsmStorage`. Silex is not distributed, so concurrent clients are
threads in one process, and "nemeses" are background flush/compaction/GC/WAL-sync plus `SIGKILL` of a child
process. It is **not** part of CI: `.github/workflows/build.yml` builds and tests only `Silex.Test`, and this
project is a console app, not a test project.

```bash
dotnet run -c Release --project tests/Silex.Jepsen -- --help
dotnet run -c Release --project tests/Silex.Jepsen -- all --seconds 10 --iterations 5 --seed 42
dotnet run -c Release --project tests/Silex.Jepsen -- append-si --profile tiny-tiered --seed 1234
```

The process exits non-zero when any run fails. Every run is derived from a seed (thread scheduling still
varies), and failing histories are written to `jepsen-results/`.

| Workload | What it checks |
| --- | --- |
| `selftest` | The checkers detect known-bad histories (stale reads, G1a/G1b, G-single, G2, lost writes). |
| `register` | Linearizability (Wing-Gong-Lowe) of `GetRawAsync`, `Put` and transactional CAS per key; indeterminate ops may or may not apply. |
| `register-lsm` | Same linearizability check on plain `LsmStorage` `GetRawAsync`, `Put` and `Delete` (no transactions, so no CAS). |
| `bank` | Constant total, no negative balance, consistent point/transactional/cursor reads, and the same total after close/reopen. |
| `append-si` | Elle-style list-append with untracked reads: no G0, G1a/b/c, G-single, lost writes or incompatible orders, including real-time edges (strong snapshot isolation). Write skew is allowed. |
| `append-ser` | Same with `GetForUpdate` reads, additionally forbidding any dependency cycle (strict serializability). |
| `scan` | Atomic multi-key put/delete vs. forward and reverse snapshot cursors in small batches: no torn scans, scans agree with point reads, snapshots never go back in time, observed generations match committed writers. |
| `crash` | A child process commits multi-key transactions with synchronous WAL and is repeatedly killed; after reopen every acknowledged commit is present, nothing newer than the last started commit appears, and no transaction is torn. Skipped for the no-WAL profile. |
| `crash-lsm` | Same kill/reopen cycle for `LsmStorage`, committing atomic `LsmWriteBatch` writes; checks durability, no phantom writes and batch atomicity. Skipped for the no-WAL profile. |

Profiles combine tiny memtables and a 20 ms flush period with tiered or leveled compaction, optional group
commit, and with or without the WAL, so flush and compaction constantly overlap the workloads.

Known limits: only snapshot-isolation anomalies with a single anti-dependency (G-single) are checked at the
snapshot level, not cycles with several non-adjacent anti-dependencies; I/O fault injection (torn writes,
dropped fsyncs) is not covered because the engine has no I/O abstraction, so crash testing relies on real
process kills, which preserve OS buffers.
