# Silex across the memtable index work — 2026-10-27

Every suite of the [engine comparison harness](../../tests/Silex.EngineComparison/README.md), Silex LSM and
Silex MVCC only, at each merge commit of the work that followed the October comparison:

| Column | Commit | Change |
|---|---|---|
| before | `43345cb` | the commit the October README tables were measured at |
| #53 | `7f35c72` | keep memtable indexes current after writes, prefetch records in leaf scans |
| #54 | `b869d29` | memtable index invariant checks, fix index state after a failed update |
| #57 | `e4efeb7` | opt-in SST key-window index (off by default, so no effect expected here) |
| #55 | `49a93df` | memtable index node redesign: inline windows, prefix stripping, vector search |
| #56 | `0c12718` | read the active memtable without waiting for its writer |

## Method

- Apple M4 Pro (10 performance and 4 efficiency cores, 48 GiB RAM), macOS 15.8, .NET 10.0.11.
- The same adapter source (`tests/Silex.EngineComparison/silex/Program.cs` from `b0d2058`) built against each commit.
- 3 repetitions of every engine, thread count, suite and commit, 432 fresh processes. The commit order rotates and
  reverses every repetition. Cells are medians of 3.
- Each run waited for the CPU to be at least 85% idle, and the CPU time every other process used while it ran was
  measured. A run where other processes averaged more than 3 cores was discarded and rerun (macOS background work
  alone uses about 1.7 cores on this machine). 5 runs were rerun.

## Findings

- **#56** is the large win for concurrent readers and writers: the 90% read / 10% overwrite workload at 8 threads goes
  from about 0.97M to 2.71M ops/s on LSM (2.8×) and from 0.60M to 2.91M on MVCC (4.9×).
- **#53** removes the MVCC index rebuilds after writes: the same mixed workload goes from 25k to 1.18M ops/s at 1
  thread and from 9.7k to 0.57M at 8 threads.
- **#55** speeds up LSM memtable seeks (+58% warm at 1 thread, +40% at 8) and costs memtable forward scans (−26% LSM
  and −17% MVCC at 1 thread, −20% LSM at 8). MVCC memtable seeks gain less (+16% warm at 1 thread) with these 50,000
  16-byte keys than #55 measured at 400,000 entries.
- **#57** changes nothing measurable, as expected with the option off.
- Writes, SST reads and memory do not move by more than about 10% at any commit.

### Bimodal 8-thread memtable scans before #55

Before #55, a process running 8 threads of LSM memtable scans lands in one of two modes, and stays there for the whole
process: scan-100 at 4.2–4.8M scans/s and forward scans at 425–458M entries/s, or 0.6–0.7M scans/s and
100–105M entries/s. Per-thread throughput in the fast mode is about 3× a single thread's, so the fast mode is not
plain scaling. The fast mode appeared in 0 of 3 runs at `43345cb`, 2 of 3 at #53 and #54 and 1 of 3 at #57 here
(3 of 3 at `43345cb` in an earlier run), and in none from #55 on. The median columns below can therefore swing by 6×
between neighbouring commits on those rows. PR #58 described this as a 4–7× regression from #55; it is not a
regression of the normal mode, where scan-100 is unchanged and forward scans lose about 20%.

| 8 threads, LSM memtable | before | #53 | #54 | #57 | #55 | #56 |
|---|---|---|---|---|---|---|
| Forward scan, M entries/s, per run | 103, 102, 104 | **458**, 104, 100 | **425**, 99, 106 | 104, 103, 106 | 83, 83, 86 | 93, 92, 95 |
| Scan 100, M scans/s, per run | 0.66, 0.71, 0.71 | **4.32**, 0.71, **4.18** | **4.79**, **4.31**, 0.60 | **4.83**, 0.66, 0.66 | 0.70, 0.71, 0.70 | 0.65, 0.66, 0.66 |

Throughput rows mark changes of ±10% or more in bold, and the last section lists every step of ±15% or more.

## Results

Result lines: 2160, other processes' CPU per run: median 1.66 cores, max 2.91.
Median spread (max/min of 3) per cell: 1.05x.

### Silex LSM, 1 thread

| Workload | Residency | Unit | before `43345cb` | #53 `7f35c72` | #54 `b869d29` | #57 `e4efeb7` | #55 `49a93df` | #56 `0c12718` | before → #56 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sequential writes | Memtable + WAL | ops/s | 641,722 | 651,525 | 637,994 | 602,882 | 644,307 | 637,392 | -1% |
| Random writes | Memtable + WAL | ops/s | 647,589 | 653,561 | 652,408 | 636,921 | 648,385 | 642,479 | -1% |
| Overwrites | Memtable + WAL | ops/s | 658,813 | 671,840 | 626,875 | 643,500 | 662,576 | 668,387 | +1% |
| Random deletes | Memtable + WAL | ops/s | 798,616 | 807,370 | 822,194 | 801,443 | 803,672 | 803,781 | +1% |
| Synchronized writes | WAL sync | ops/s | 244 | 245 | 244 | 239 | 244 | 245 | +0% |
| 90% reads, 10% overwrites | Memtable + WAL | ops/s | 2,237,637 | 2,274,454 | 2,202,478 | 2,215,958 | 2,229,883 | 2,307,284 | +3% |
| Random reads, first pass | Memtable | ops/s | 3,720,598 | 3,596,450 | 3,623,924 | 3,663,460 | 3,637,051 | 3,729,173 | +0% |
| Random reads, warm | Memtable | ops/s | 5,367,514 | 5,332,423 | 5,327,878 | 5,407,861 | 5,335,041 | 5,468,724 | +2% |
| Random seeks, first pass | Memtable | ops/s | 874,753 | 857,158 | 847,659 | 850,661 | 1,114,802 | 1,162,083 | **+33%** |
| Random seeks, warm | Memtable | ops/s | 1,050,195 | 1,006,964 | 1,003,768 | 1,017,470 | 1,612,227 | 1,770,482 | **+69%** |
| Forward scan | Memtable | entries/s | 19,073,777 | 18,932,940 | 18,935,091 | 20,024,029 | 14,876,082 | 16,802,204 | **-12%** |
| Reverse scan | Memtable | entries/s | 22,451,729 | 22,104,332 | 22,084,806 | 21,777,952 | 21,828,342 | 22,167,051 | -1% |
| Scan 100 entries | Memtable | scans/s | 194,939 | 185,227 | 184,515 | 186,672 | 201,422 | 204,650 | +5% |
| Skip 5,000, scan 100 | Memtable | scans/s | 18,210 | 17,599 | 17,464 | 17,403 | 17,116 | 18,107 | -1% |
| Count all 50,000 keys | Memtable | scans/s | 4,326 | 4,264 | 4,269 | 4,170 | 4,225 | 4,419 | +2% |
| Random reads, first pass | SST | ops/s | 1,083,271 | 1,119,455 | 1,120,770 | 1,099,999 | 1,097,765 | 1,099,677 | +2% |
| Random reads, warm | SST | ops/s | 1,792,295 | 1,793,117 | 1,811,443 | 1,800,012 | 1,783,434 | 1,822,031 | +2% |
| Random seeks, first pass | SST | ops/s | 1,293,671 | 1,268,553 | 1,281,545 | 1,285,605 | 1,248,970 | 1,282,939 | -1% |
| Random seeks, warm | SST | ops/s | 1,478,188 | 1,479,106 | 1,479,194 | 1,477,978 | 1,493,830 | 1,496,177 | +1% |
| Forward scan | SST | entries/s | 29,592,803 | 28,757,117 | 28,527,415 | 29,795,602 | 27,443,877 | 28,420,394 | -4% |
| Reverse scan | SST | entries/s | 16,802,204 | 16,544,787 | 16,604,676 | 17,030,553 | 16,598,612 | 16,610,744 | -1% |
| Scan 100 entries | SST | scans/s | 240,327 | 240,176 | 241,559 | 238,235 | 240,105 | 242,754 | +1% |
| Skip 5,000, scan 100 | SST | scans/s | 13,772 | 14,136 | 13,897 | 13,654 | 14,125 | 14,949 | +9% |
| Count all 50,000 keys | SST | scans/s | 1,425 | 1,432 | 1,413 | 1,435 | 1,412 | 1,428 | +0% |
| Read-suite memory | Memtable | MiB RSS / peak | 62.6 / 70.2 | 62.7 / 70.0 | 62.7 / 70.0 | 62.6 / 70.0 | 63.2 / 70.4 | 63.4 / 70.7 | |
| Read-suite memory | SST | MiB RSS / peak | 78.8 / 79.8 | 78.7 / 79.7 | 78.9 / 80.0 | 78.9 / 79.9 | 79.1 / 80.2 | 79.0 / 80.1 | |
| Write-suite memory | Sequential fill + overwrite + delete | MiB RSS / peak | 67.0 / 73.5 | 67.0 / 73.7 | 67.1 / 73.6 | 67.0 / 73.4 | 67.0 / 73.7 | 67.0 / 73.7 | |
| Random-fill memory | Memtable + WAL | MiB RSS / peak | 61.7 / 69.9 | 61.8 / 70.0 | 61.7 / 69.8 | 61.8 / 69.9 | 61.7 / 69.8 | 61.7 / 70.1 | |
| Synchronized-write memory | WAL sync | MiB RSS / peak | 49.9 / 55.1 | 49.7 / 55.0 | 49.7 / 54.9 | 49.7 / 54.9 | 49.7 / 55.0 | 49.8 / 55.1 | |
| Mixed-suite memory | Memtable + WAL | MiB RSS / peak | 62.3 / 70.2 | 62.2 / 69.9 | 62.2 / 70.0 | 62.3 / 70.1 | 62.2 / 70.2 | 62.2 / 70.3 | |

### Silex LSM, 8 threads

| Workload | Residency | Unit | before `43345cb` | #53 `7f35c72` | #54 `b869d29` | #57 `e4efeb7` | #55 `49a93df` | #56 `0c12718` | before → #56 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sequential writes | Memtable + WAL | ops/s | 313,527 | 310,310 | 329,507 | 315,706 | 319,635 | 321,304 | +2% |
| Random writes | Memtable + WAL | ops/s | 315,331 | 313,413 | 315,491 | 313,642 | 314,138 | 316,460 | +0% |
| Overwrites | Memtable + WAL | ops/s | 318,161 | 313,187 | 326,407 | 333,881 | 325,588 | 311,713 | -2% |
| Random deletes | Memtable + WAL | ops/s | 364,096 | 381,374 | 409,715 | 402,496 | 328,458 | 387,398 | +6% |
| Synchronized writes | WAL sync | ops/s | 717 | 683 | 673 | 679 | 694 | 733 | +2% |
| 90% reads, 10% overwrites | Memtable + WAL | ops/s | 970,539 | 933,733 | 956,189 | 953,499 | 965,713 | 2,708,572 | **2.79×** |
| Random reads, first pass | Memtable | ops/s | 17,243,386 | 16,009,926 | 15,085,402 | 14,828,930 | 14,917,080 | 15,094,397 | **-12%** |
| Random reads, warm | Memtable | ops/s | 21,708,455 | 18,874,157 | 21,512,779 | 20,888,601 | 20,437,986 | 18,604,997 | **-14%** |
| Random seeks, first pass | Memtable | ops/s | 4,627,921 | 4,575,774 | 4,476,551 | 4,313,357 | 5,474,802 | 5,845,082 | **+26%** |
| Random seeks, warm | Memtable | ops/s | 5,732,244 | 5,460,684 | 5,277,156 | 5,376,799 | 7,538,422 | 8,416,162 | **+47%** |
| Forward scan | Memtable | entries/s | 103,015,787 | 103,640,368 | 105,499,143 | 103,739,821 | 83,113,429 | 93,187,960 | -10% |
| Reverse scan | Memtable | entries/s | 123,160,293 | 117,078,882 | 118,011,506 | 115,400,150 | 113,846,592 | 113,597,637 | -8% |
| Scan 100 entries | Memtable | scans/s | 710,076 | 4,176,586 | 4,311,599 | 656,867 | 704,976 | 656,380 | -8% |
| Skip 5,000, scan 100 | Memtable | scans/s | 119,129 | 294,358 | 284,338 | 121,196 | 114,347 | 112,895 | -5% |
| Count all 50,000 keys | Memtable | scans/s | 34,162 | 34,297 | 32,020 | 33,778 | 34,187 | 38,853 | **+14%** |
| Random reads, first pass | SST | ops/s | 6,450,833 | 6,464,521 | 6,586,368 | 6,439,067 | 6,374,299 | 6,070,061 | -6% |
| Random reads, warm | SST | ops/s | 8,669,831 | 8,804,115 | 8,685,664 | 8,155,659 | 8,382,423 | 8,255,082 | -5% |
| Random seeks, first pass | SST | ops/s | 6,322,401 | 6,304,902 | 6,079,683 | 6,103,171 | 5,888,397 | 6,175,727 | -2% |
| Random seeks, warm | SST | ops/s | 7,042,055 | 7,150,339 | 7,509,650 | 7,282,473 | 7,494,524 | 7,562,895 | +7% |
| Forward scan | SST | entries/s | 56,322,163 | 56,273,828 | 55,721,947 | 50,725,373 | 55,714,962 | 61,294,228 | +9% |
| Reverse scan | SST | entries/s | 42,453,831 | 41,687,510 | 41,628,507 | 42,602,139 | 41,826,566 | 44,477,556 | +5% |
| Scan 100 entries | SST | scans/s | 851,073 | 860,865 | 856,157 | 807,454 | 832,395 | 826,224 | -3% |
| Skip 5,000, scan 100 | SST | scans/s | 106,356 | 99,829 | 97,849 | 103,219 | 109,002 | 108,516 | +2% |
| Count all 50,000 keys | SST | scans/s | 3,362 | 3,418 | 3,414 | 3,223 | 3,182 | 4,288 | **+28%** |
| Read-suite memory | Memtable | MiB RSS / peak | 66.5 / 73.9 | 63.9 / 71.5 | 66.6 / 73.9 | 66.5 / 73.8 | 67.3 / 74.5 | 65.4 / 72.5 | |
| Read-suite memory | SST | MiB RSS / peak | 83.2 / 84.2 | 83.1 / 84.2 | 83.3 / 84.3 | 83.2 / 84.3 | 83.5 / 84.5 | 83.4 / 84.4 | |
| Write-suite memory | Sequential fill + overwrite + delete | MiB RSS / peak | 68.0 / 74.5 | 68.3 / 74.6 | 68.1 / 74.6 | 68.2 / 74.7 | 68.1 / 74.9 | 68.1 / 74.8 | |
| Random-fill memory | Memtable + WAL | MiB RSS / peak | 62.2 / 70.2 | 62.2 / 70.1 | 62.1 / 70.1 | 62.2 / 70.3 | 62.2 / 70.3 | 62.2 / 70.3 | |
| Synchronized-write memory | WAL sync | MiB RSS / peak | 49.5 / 54.9 | 49.6 / 55.0 | 49.6 / 55.0 | 49.7 / 55.0 | 49.6 / 55.0 | 49.6 / 54.9 | |
| Mixed-suite memory | Memtable + WAL | MiB RSS / peak | 66.6 / 74.3 | 66.5 / 74.5 | 66.5 / 74.3 | 66.5 / 74.3 | 66.7 / 74.7 | 65.9 / 73.8 | |

### Silex MVCC, 1 thread

| Workload | Residency | Unit | before `43345cb` | #53 `7f35c72` | #54 `b869d29` | #57 `e4efeb7` | #55 `49a93df` | #56 `0c12718` | before → #56 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sequential writes | Memtable + WAL | ops/s | 388,167 | 387,128 | 384,003 | 394,731 | 387,805 | 382,596 | -1% |
| Random writes | Memtable + WAL | ops/s | 393,496 | 392,015 | 384,213 | 392,795 | 392,327 | 383,753 | -2% |
| Overwrites | Memtable + WAL | ops/s | 412,229 | 411,844 | 407,845 | 423,503 | 414,008 | 411,562 | -0% |
| Random deletes | Memtable + WAL | ops/s | 446,782 | 443,528 | 431,129 | 453,208 | 456,019 | 447,654 | +0% |
| Synchronized writes | WAL sync | ops/s | 234 | 243 | 242 | 244 | 241 | 243 | +4% |
| 90% reads, 10% overwrites | Memtable + WAL | ops/s | 24,906 | 1,184,764 | 1,199,392 | 1,209,090 | 1,228,139 | 1,220,745 | **49.01×** |
| Random reads, first pass | Memtable | ops/s | 2,051,450 | 2,012,891 | 1,983,844 | 2,032,000 | 2,034,331 | 2,005,817 | -2% |
| Random reads, warm | Memtable | ops/s | 3,066,488 | 3,030,468 | 2,981,052 | 2,984,754 | 2,975,216 | 3,045,623 | -1% |
| Random seeks, first pass | Memtable | ops/s | 731,116 | 725,920 | 724,999 | 724,922 | 758,512 | 746,670 | +2% |
| Random seeks, warm | Memtable | ops/s | 853,130 | 819,075 | 809,369 | 814,084 | 943,539 | 915,287 | +7% |
| Forward scan | Memtable | entries/s | 13,951,671 | 13,700,132 | 13,878,865 | 13,846,580 | 11,541,746 | 11,400,169 | **-18%** |
| Reverse scan | Memtable | entries/s | 16,105,134 | 15,443,538 | 15,857,913 | 15,484,191 | 15,599,651 | 15,267,176 | -5% |
| Scan 100 entries | Memtable | scans/s | 133,767 | 125,590 | 124,542 | 122,858 | 128,127 | 127,720 | -5% |
| Skip 5,000, scan 100 | Memtable | scans/s | 13,987 | 13,777 | 13,877 | 13,767 | 13,101 | 13,264 | -5% |
| Count all 50,000 keys | Memtable | scans/s | 3,869 | 3,837 | 3,824 | 3,799 | 3,774 | 3,992 | +3% |
| Random reads, first pass | SST | ops/s | 912,380 | 889,126 | 915,969 | 901,032 | 875,815 | 917,648 | +1% |
| Random reads, warm | SST | ops/s | 1,323,182 | 1,360,637 | 1,367,005 | 1,354,977 | 1,276,438 | 1,386,436 | +5% |
| Random seeks, first pass | SST | ops/s | 1,328,049 | 1,313,895 | 1,328,032 | 1,315,388 | 1,287,309 | 1,331,437 | +0% |
| Random seeks, warm | SST | ops/s | 1,431,270 | 1,394,778 | 1,422,799 | 1,434,263 | 1,410,079 | 1,438,779 | +1% |
| Forward scan | SST | entries/s | 15,410,695 | 14,482,259 | 15,438,293 | 15,164,842 | 15,421,627 | 15,409,270 | -0% |
| Reverse scan | SST | entries/s | 16,366,076 | 15,624,512 | 16,570,557 | 15,908,368 | 16,215,340 | 16,136,840 | -1% |
| Scan 100 entries | SST | scans/s | 209,727 | 208,406 | 210,504 | 210,501 | 211,340 | 212,132 | +1% |
| Skip 5,000, scan 100 | SST | scans/s | 12,880 | 12,938 | 13,312 | 13,002 | 13,526 | 13,284 | +3% |
| Count all 50,000 keys | SST | scans/s | 1,083 | 1,080 | 1,076 | 1,082 | 1,097 | 1,097 | +1% |
| Read-suite memory | Memtable | MiB RSS / peak | 69.8 / 77.4 | 69.6 / 77.3 | 69.6 / 77.4 | 69.7 / 77.4 | 70.7 / 78.3 | 66.8 / 74.4 | |
| Read-suite memory | SST | MiB RSS / peak | 83.6 / 84.7 | 83.6 / 84.7 | 83.6 / 84.7 | 84.2 / 85.3 | 84.2 / 85.3 | 84.0 / 85.0 | |
| Write-suite memory | Sequential fill + overwrite + delete | MiB RSS / peak | 97.6 / 111.1 | 97.8 / 111.3 | 97.7 / 111.4 | 97.6 / 111.1 | 97.7 / 112.0 | 97.6 / 111.7 | |
| Random-fill memory | Memtable + WAL | MiB RSS / peak | 66.7 / 75.3 | 66.8 / 75.3 | 66.7 / 75.1 | 66.8 / 75.2 | 66.6 / 75.4 | 66.9 / 75.5 | |
| Synchronized-write memory | WAL sync | MiB RSS / peak | 50.0 / 55.3 | 50.0 / 55.3 | 50.3 / 55.3 | 50.0 / 55.3 | 50.1 / 55.4 | 50.1 / 55.4 | |
| Mixed-suite memory | Memtable + WAL | MiB RSS / peak | 67.6 / 76.1 | 65.0 / 73.9 | 65.2 / 74.0 | 65.1 / 73.9 | 65.1 / 74.2 | 65.1 / 74.3 | |

### Silex MVCC, 8 threads

| Workload | Residency | Unit | before `43345cb` | #53 `7f35c72` | #54 `b869d29` | #57 `e4efeb7` | #55 `49a93df` | #56 `0c12718` | before → #56 |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| Sequential writes | Memtable + WAL | ops/s | 446,610 | 443,637 | 445,324 | 442,814 | 440,003 | 439,352 | -2% |
| Random writes | Memtable + WAL | ops/s | 428,129 | 445,958 | 443,322 | 445,742 | 443,590 | 440,207 | +3% |
| Overwrites | Memtable + WAL | ops/s | 512,109 | 508,094 | 506,918 | 518,596 | 513,218 | 520,297 | +2% |
| Random deletes | Memtable + WAL | ops/s | 513,131 | 541,075 | 543,864 | 557,100 | 541,454 | 546,247 | +6% |
| Synchronized writes | WAL sync | ops/s | 801 | 813 | 797 | 810 | 843 | 813 | +1% |
| 90% reads, 10% overwrites | Memtable + WAL | ops/s | 9,687 | 569,557 | 572,662 | 647,348 | 597,559 | 2,906,487 | **300.04×** |
| Random reads, first pass | Memtable | ops/s | 8,140,803 | 8,236,469 | 8,062,289 | 9,039,589 | 8,571,184 | 8,059,706 | -1% |
| Random reads, warm | Memtable | ops/s | 14,998,013 | 13,539,495 | 13,866,548 | 13,607,294 | 14,825,028 | 12,432,785 | **-17%** |
| Random seeks, first pass | Memtable | ops/s | 3,711,026 | 3,919,064 | 3,805,541 | 3,739,125 | 3,917,678 | 3,799,096 | +2% |
| Random seeks, warm | Memtable | ops/s | 4,998,107 | 4,587,966 | 4,651,601 | 4,636,687 | 5,194,650 | 5,205,676 | +4% |
| Forward scan | Memtable | entries/s | 83,263,947 | 83,251,816 | 82,039,502 | 82,172,645 | 70,099,190 | 62,889,126 | **-24%** |
| Reverse scan | Memtable | entries/s | 100,250,627 | 92,751,472 | 93,514,752 | 92,790,201 | 96,639,366 | 93,392,482 | -7% |
| Scan 100 entries | Memtable | scans/s | 560,795 | 568,527 | 585,502 | 584,653 | 568,868 | 540,163 | -4% |
| Skip 5,000, scan 100 | Memtable | scans/s | 127,457 | 121,539 | 128,000 | 127,466 | 119,735 | 138,335 | +9% |
| Count all 50,000 keys | Memtable | scans/s | 25,505 | 25,013 | 25,595 | 24,598 | 24,208 | 26,657 | +5% |
| Random reads, first pass | SST | ops/s | 4,942,781 | 5,043,354 | 4,897,525 | 5,228,068 | 4,991,215 | 5,034,100 | +2% |
| Random reads, warm | SST | ops/s | 6,669,268 | 6,576,406 | 6,631,069 | 6,250,117 | 6,791,806 | 7,038,164 | +6% |
| Random seeks, first pass | SST | ops/s | 6,001,248 | 5,989,593 | 6,031,236 | 5,962,825 | 6,062,186 | 6,223,656 | +4% |
| Random seeks, warm | SST | ops/s | 6,290,970 | 6,018,920 | 6,635,711 | 6,676,013 | 6,136,390 | 6,767,445 | +8% |
| Forward scan | SST | entries/s | 48,435,532 | 48,385,146 | 47,880,681 | 48,589,094 | 43,915,024 | 46,768,857 | -3% |
| Reverse scan | SST | entries/s | 40,945,011 | 41,083,368 | 46,132,909 | 46,210,187 | 45,083,122 | 45,221,245 | **+10%** |
| Scan 100 entries | SST | scans/s | 742,836 | 740,558 | 688,818 | 737,741 | 810,082 | 779,282 | +5% |
| Skip 5,000, scan 100 | SST | scans/s | 88,962 | 90,411 | 90,146 | 91,221 | 90,316 | 90,654 | +2% |
| Count all 50,000 keys | SST | scans/s | 2,002 | 2,010 | 2,016 | 2,041 | 2,252 | 2,243 | **+12%** |
| Read-suite memory | Memtable | MiB RSS / peak | 71.2 / 79.1 | 71.3 / 79.1 | 71.2 / 79.1 | 71.4 / 79.2 | 72.0 / 79.8 | 68.0 / 75.8 | |
| Read-suite memory | SST | MiB RSS / peak | 84.8 / 85.9 | 84.8 / 85.9 | 84.8 / 85.8 | 84.9 / 86.0 | 85.1 / 86.2 | 85.1 / 86.2 | |
| Write-suite memory | Sequential fill + overwrite + delete | MiB RSS / peak | 162.4 / 176.0 | 162.5 / 175.8 | 162.4 / 175.9 | 162.2 / 176.1 | 162.3 / 176.5 | 162.4 / 176.4 | |
| Random-fill memory | Memtable + WAL | MiB RSS / peak | 88.9 / 97.2 | 89.0 / 97.3 | 89.0 / 97.2 | 88.9 / 97.4 | 89.1 / 97.6 | 89.0 / 97.3 | |
| Synchronized-write memory | WAL sync | MiB RSS / peak | 50.3 / 55.7 | 50.3 / 55.6 | 50.3 / 55.5 | 50.4 / 55.7 | 50.3 / 55.8 | 50.2 / 55.7 | |
| Mixed-suite memory | Memtable + WAL | MiB RSS / peak | 99.5 / 111.7 | 94.8 / 105.8 | 95.0 / 106.0 | 94.4 / 105.4 | 95.3 / 106.8 | 94.9 / 106.4 | |

### Changes of 15% or more from one commit to the next

| Engine | Threads | Workload | Residency | Commit | Before | After | Change |
|---|---|---|---|---|---:|---:|---:|
| LSM | 1 | Random seeks, first pass | Memtable | #55 | 850,661 | 1,114,802 | **+31%** |
| LSM | 1 | Random seeks, warm | Memtable | #55 | 1,017,470 | 1,612,227 | **+58%** |
| LSM | 1 | Forward scan | Memtable | #55 | 20,024,029 | 14,876,082 | **-26%** |
| LSM | 8 | Random deletes | Memtable + WAL | #55 | 402,496 | 328,458 | **-18%** |
| LSM | 8 | Random deletes | Memtable + WAL | #56 | 328,458 | 387,398 | **+18%** |
| LSM | 8 | 90% reads, 10% overwrites | Memtable + WAL | #56 | 965,713 | 2,708,572 | **2.80×** |
| LSM | 8 | Random seeks, first pass | Memtable | #55 | 4,313,357 | 5,474,802 | **+27%** |
| LSM | 8 | Random seeks, warm | Memtable | #55 | 5,376,799 | 7,538,422 | **+40%** |
| LSM | 8 | Forward scan | Memtable | #55 | 103,739,821 | 83,113,429 | **-20%** |
| LSM | 8 | Scan 100 entries | Memtable | #53 | 710,076 | 4,176,586 | **5.88×** (spread 6.1×) |
| LSM | 8 | Scan 100 entries | Memtable | #57 | 4,311,599 | 656,867 | **0.15×** (spread 7.9×) |
| LSM | 8 | Skip 5,000, scan 100 | Memtable | #53 | 119,129 | 294,358 | **2.47×** (spread 2.5×) |
| LSM | 8 | Skip 5,000, scan 100 | Memtable | #57 | 284,338 | 121,196 | **0.43×** (spread 2.4×) |
| LSM | 8 | Count all 50,000 keys | SST | #56 | 3,182 | 4,288 | **+35%** |
| MVCC | 1 | 90% reads, 10% overwrites | Memtable + WAL | #53 | 24,906 | 1,184,764 | **47.57×** |
| MVCC | 1 | Random seeks, warm | Memtable | #55 | 814,084 | 943,539 | **+16%** |
| MVCC | 1 | Forward scan | Memtable | #55 | 13,846,580 | 11,541,746 | **-17%** |
| MVCC | 8 | 90% reads, 10% overwrites | Memtable + WAL | #53 | 9,687 | 569,557 | **58.80×** |
| MVCC | 8 | 90% reads, 10% overwrites | Memtable + WAL | #56 | 597,559 | 2,906,487 | **4.86×** |
| MVCC | 8 | Random reads, warm | Memtable | #56 | 14,825,028 | 12,432,785 | **-16%** |
| MVCC | 8 | Skip 5,000, scan 100 | Memtable | #56 | 119,735 | 138,335 | **+16%** |
