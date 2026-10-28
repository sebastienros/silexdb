#!/usr/bin/env python3
"""Runs the engine comparison suites and renders the README tables.

See README.md in this folder for the adapter contract and workloads.
"""

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent

N = 50_000
READS = 50_000
BOUNDED_SCANS = 5_000
COUNTS = 50
SYNC_WRITES = 50

PAIRS = {
    "lsm": ("silex-lsm", "rocksdb"),
    "mvcc": ("silex-mvcc", "surrealkv"),
}

ADAPTERS = {
    "silex-lsm": [str(HERE / "silex/bin/Release/net10.0/silex-bench")],
    "silex-mvcc": [str(HERE / "silex/bin/Release/net10.0/silex-bench")],
    "rocksdb": [str(HERE / "rocksdb/build/rocksdb_bench")],
    "surrealkv": [str(HERE / "surrealkv/target/release/surrealkv-adapter")],
}

SUITES = ["write", "randomfill", "sync", "read-memtable", "read-sst", "mixed"]

# Phases per suite, with the check each run must report for T threads.
PHASES = {
    "write": {
        "sequential-writes": lambda t: N,
        "overwrites": lambda t: N,
        "random-deletes": lambda t: N,
    },
    "randomfill": {"random-writes": lambda t: N},
    "sync": {"sync-writes": lambda t: SYNC_WRITES},
    "mixed": {"read-write-90-10": lambda t: READS * t},
}
READ_PHASES = {
    "random-reads-first": lambda t: READS * t,
    "random-reads-warm": lambda t: READS * t,
    "random-seeks-first": lambda t: READS * t,
    "random-seeks-warm": lambda t: READS * t,
    "forward-scan": lambda t: N * t,
    "reverse-scan": lambda t: N * t,
    "scan-100": lambda t: BOUNDED_SCANS * 100 * t,
    "skip-5000-scan-100": lambda t: BOUNDED_SCANS * 100 * t,
    "count-all": lambda t: COUNTS * N * t,
}
PHASES["read-memtable"] = READ_PHASES
PHASES["read-sst"] = READ_PHASES

# README rows: (label, residency, suite, phase).
THROUGHPUT_ROWS = [
    ("Sequential writes", "Memtable + WAL", "write", "sequential-writes"),
    ("Random writes", "Memtable + WAL", "randomfill", "random-writes"),
    ("Overwrites", "Memtable + WAL", "write", "overwrites"),
    ("Random deletes", "Memtable + WAL", "write", "random-deletes"),
    ("Synchronized writes", "WAL sync", "sync", "sync-writes"),
    ("90% reads, 10% overwrites", "Memtable + WAL", "mixed", "read-write-90-10"),
]
READ_LABELS = [
    ("Random reads, first pass", "random-reads-first"),
    ("Random reads, warm", "random-reads-warm"),
    ("Random seeks, first pass", "random-seeks-first"),
    ("Random seeks, warm", "random-seeks-warm"),
    ("Forward scan", "forward-scan"),
    ("Reverse scan", "reverse-scan"),
    ("Scan 100 entries", "scan-100"),
    ("Skip 5,000, scan 100", "skip-5000-scan-100"),
    ("Count all 50,000 keys", "count-all"),
]
for residency, suite in (("Memtable", "read-memtable"), ("SST", "read-sst")):
    THROUGHPUT_ROWS += [(label, residency, suite, phase) for label, phase in READ_LABELS]

MEMORY_ROWS = [
    ("Read-suite memory", "Memtable", "read-memtable"),
    ("Read-suite memory", "SST", "read-sst"),
    ("Write-suite memory", "Sequential fill + overwrite + delete", "write"),
    ("Random-fill memory", "Memtable + WAL", "randomfill"),
    ("Synchronized-write memory", "WAL sync", "sync"),
    ("Mixed-suite memory", "Memtable + WAL", "mixed"),
]

UNITS = {}


def cpu_idle_percent():
    """Idle CPU over the last second on macOS (`top`), or None elsewhere."""
    if sys.platform != "darwin":
        return None
    sample = subprocess.run(["top", "-l", "2", "-s", "1", "-n", "0"], capture_output=True, text=True).stdout
    lines = [line for line in sample.splitlines() if line.startswith("CPU usage:")]
    return float(lines[-1].split(",")[2].split("%")[0]) if lines else None


def wait_for_idle(min_idle):
    """Waits until the machine is at least min_idle percent idle, so other work does not skew a run."""
    waited = False
    while True:
        idle = cpu_idle_percent()
        if idle is None or idle >= min_idle:
            if waited:
                print(f"  resumed at {idle:.0f}% idle", flush=True)
            return idle
        if not waited:
            print(f"  waiting: {idle:.0f}% idle < {min_idle}%", flush=True)
            waited = True
        time.sleep(5)


def parse_cpu_time(text):
    """Parses a `ps` cumulative CPU time: [[dd-]hh:]mm:ss.ss."""
    days, _, text = text.rpartition("-")
    seconds = 0.0
    for part in text.split(":"):
        seconds = seconds * 60 + float(part)
    return (int(days) if days else 0) * 86400 + seconds


def cpu_times():
    """Cumulative CPU seconds of every process, by pid."""
    out = subprocess.run(["ps", "-A", "-o", "pid=,time="], capture_output=True, text=True).stdout
    return {int(pid): parse_cpu_time(t) for pid, t in (line.split() for line in out.splitlines())}


def measured_run(engine, suite, threads, command, work_root, min_idle, max_other_cores, tries=5):
    """Runs one suite; reruns it when other processes used more than max_other_cores on average meanwhile."""
    for _ in range(tries):
        idle = wait_for_idle(min_idle) if min_idle else None
        load = os.getloadavg()[0]
        before, start = cpu_times(), time.monotonic()
        records = run_one(engine, suite, threads, command, work_root)
        wall = time.monotonic() - start
        after = cpu_times()
        # The adapter has exited by now, so its CPU time is not in either snapshot.
        others = sum(t - before.get(pid, 0.0) for pid, t in after.items() if pid not in (0, os.getpid())) / wall
        if not max_other_cores or others <= max_other_cores:
            for record in records:
                record.update(load1=load, idle_before=idle, other_cores=others)
            return records
        print(f"  rerun {engine} {suite} t{threads}: other processes used {others:.2f} cores", flush=True)
    raise RuntimeError(f"{engine} {suite} t{threads}: other processes kept using more than {max_other_cores} cores")


def run_one(engine, suite, threads, command, work_root):
    directory = tempfile.mkdtemp(prefix=f"{engine}-{suite}-{threads}-", dir=work_root)
    try:
        args = command + ["--engine", engine, "--suite", suite, "--threads", str(threads), "--dir", os.path.join(directory, "db")]
        completed = subprocess.run(args, capture_output=True, text=True, timeout=1800)
        if completed.returncode != 0:
            raise RuntimeError(f"{engine} {suite} t{threads} exited {completed.returncode}:\n{completed.stderr[-4000:]}")
        records = [json.loads(line) for line in completed.stdout.splitlines() if line.startswith("{")]
    finally:
        shutil.rmtree(directory, ignore_errors=True)

    expected = PHASES[suite]
    seen = {r["phase"]: r for r in records if r["type"] == "result"}
    if set(seen) != set(expected):
        raise RuntimeError(f"{engine} {suite} t{threads}: phases {sorted(seen)} != {sorted(expected)}")
    for phase, check in expected.items():
        if seen[phase]["check"] != check(threads):
            raise RuntimeError(f"{engine} {suite} t{threads} {phase}: check {seen[phase]['check']} != {check(threads)}")
    if sum(1 for r in records if r["type"] == "memory") != 1:
        raise RuntimeError(f"{engine} {suite} t{threads}: expected one memory line")
    return records


def run(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    work_root = out / "work"
    work_root.mkdir(exist_ok=True)
    adapters = dict(ADAPTERS)
    for override in args.adapter:
        name, _, command = override.partition("=")
        adapters[name] = command.split()
    engines_filter = set(args.engines.split(",")) if args.engines else None

    log = open(out / "runs.jsonl", "a")
    for rep in range(args.reps):
        for threads in [int(t) for t in args.threads.split(",")]:
            for suite in args.suites.split(","):
                for pair in args.pairs.split(","):
                    order = PAIRS[pair] if rep % 2 == 0 else tuple(reversed(PAIRS[pair]))
                    for engine in order:
                        if engines_filter and engine not in engines_filter:
                            continue
                        records = measured_run(engine, suite, threads, adapters[engine], work_root,
                                               args.min_idle, args.max_other_cores)
                        for record in records:
                            record["rep"] = rep
                            record["label"] = engine + args.label_suffix
                            log.write(json.dumps(record) + "\n")
                        log.flush()
                        summary = ", ".join(
                            f"{r['phase']} {r['work'] / r['seconds']:,.0f}" for r in records if r["type"] == "result")
                        print(f"rep {rep} t{threads} {suite} {engine}: {summary}", flush=True)
    shutil.rmtree(work_root, ignore_errors=True)


def load(directory):
    throughput, rss, peak = {}, {}, {}
    with open(Path(directory) / "runs.jsonl") as log:
        for line in log:
            r = json.loads(line)
            key = (r["label"], r["threads"], r["suite"])
            if r["type"] == "result":
                throughput.setdefault(key + (r["phase"],), []).append(r["work"] / r["seconds"])
                UNITS[(r["suite"], r["phase"])] = r["unit"]
            else:
                rss.setdefault(key, []).append(r["rss_bytes"] / 2**20)
                peak.setdefault(key, []).append(r["peak_rss_bytes"] / 2**20)
    median = lambda d: {k: statistics.median(v) for k, v in d.items()}
    return median(throughput), median(rss), median(peak)


def render(args):
    throughput, rss, peak = {}, {}, {}
    for directory in args.render:
        t, r, p = load(directory)
        throughput.update(t)
        rss.update(r)
        peak.update(p)

    labels = sorted({k[0] for k in throughput})
    pairs = [p for p in PAIRS.values() if all(e in labels for e in p)]
    if args.columns:
        pairs = [tuple(args.columns.split(","))]
    for pair in pairs:
        columns = [(engine, threads) for threads in (1, 8) for engine in pair]
        print("| Workload | Residency | Unit | " + " | ".join(f"{e}, {t} thread{'s' if t > 1 else ''}" for e, t in columns) + " |")
        print("|---|---|---|" + "---:|" * len(columns))
        for label, residency, suite, phase in THROUGHPUT_ROWS:
            cells = [throughput.get((e, t, suite, phase)) for e, t in columns]
            if all(c is None for c in cells):
                continue
            unit = UNITS.get((suite, phase), "")
            print(f"| {label} | {residency} | {unit} | " + " | ".join("" if c is None else f"{c:,.0f}" for c in cells) + " |")
        for label, residency, suite in MEMORY_ROWS:
            cells = [(rss.get((e, t, suite)), peak.get((e, t, suite))) for e, t in columns]
            if all(c[0] is None for c in cells):
                continue
            print(f"| {label} | {residency} | MiB RSS / peak | " +
                  " | ".join("" if c[0] is None else f"{c[0]:.1f} / {c[1]:.1f}" for c in cells) + " |")
        print()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", help="results folder; runs are appended to runs.jsonl")
    parser.add_argument("--render", nargs="+", metavar="DIR", help="print tables from one or more results folders")
    parser.add_argument("--columns", help="with --render: comma-separated labels to show instead of the pairs")
    parser.add_argument("--pairs", default="lsm,mvcc")
    parser.add_argument("--engines", help="only run these engines of the pairs (comma-separated)")
    parser.add_argument("--threads", default="1,8")
    parser.add_argument("--suites", default=",".join(SUITES))
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--adapter", action="append", default=[], metavar="ENGINE=COMMAND",
                        help="run ENGINE with another adapter build, e.g. silex-lsm=/path/to/silex-bench")
    parser.add_argument("--min-idle", type=float, default=85,
                        help="before each run, wait until the CPU is at least this percent idle (macOS; 0 disables)")
    parser.add_argument("--max-other-cores", type=float, default=3,
                        help="rerun a suite when other processes averaged more than this many cores during it (0 disables)")
    parser.add_argument("--label-suffix", default="", help="record runs as ENGINE + suffix, e.g. silex-lsm@baseline")
    args = parser.parse_args()
    if args.render:
        render(args)
    elif args.out:
        run(args)
    else:
        parser.error("pass --out to run or --render to print tables")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        print(error, file=sys.stderr)
        sys.exit(1)
