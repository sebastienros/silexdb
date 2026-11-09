//! SlateDB adapter for the engine comparison harness.
//!
//! Implements the adapter contract of `tests/Silex.EngineComparison/README.md`:
//!
//! ```text
//! slatedb-adapter --engine slatedb --suite <suite> --threads <T> --dir <path>
//! ```
//!
//! One JSON line per timed phase and one memory line go to standard output; diagnostics go to
//! standard error. Any error ends the process with a non-zero exit code.
//!
//! # Storage
//!
//! SlateDB writes every file (WAL segments, SSTs, manifest) to an `object_store::ObjectStore`.
//! The adapter gives it the local-filesystem store rooted at `--dir`, with `fsync` enabled so that
//! a write SlateDB reports durable has reached the disk, as it has for the other engines.
//! SlateDB's own local disk cache for remote objects is not configured: with a local store it
//! would only add a second copy of each file.
//!
//! # Writes and durability
//!
//! `Db::put` and `Db::delete` return once the write is in SlateDB's in-memory WAL buffer and
//! memtable; a background task writes the buffered WAL to the object store every
//! `flush_interval` (100 ms by default). Those are the unsynchronized writes of every suite but
//! `sync`. A synchronized write is a put followed by `Db::flush`, which writes the WAL buffer to
//! the object store (with `fsync`) before it returns, so no write waits for the 100 ms timer.
//! `SLATEDB_ADAPTER_SYNC=await-durable` instead awaits the put's `WriteHandle::await_durable`,
//! which waits for the next timed WAL flush (diagnostics only).
//!
//! # How workers are driven
//!
//! SlateDB's API is async and its background work (WAL flusher, memtable flusher, compactor,
//! garbage collector) runs as tokio tasks. As in the SurrealKV adapter, each worker is a tokio task
//! on a multi-threaded runtime with `T` worker threads, the database is opened inside that
//! runtime, and workers start on a blocking `std::sync::Barrier` so each holds its own runtime
//! thread when it is released. Reads are awaited in the task.
//!
//! # Reads
//!
//! Point reads and scans read the latest version, including writes not yet flushed to the object
//! store (`DurabilityLevel::Memory`, the default). Scans set `cache_blocks` (off by default for
//! scans, on for point reads) so scanned SST blocks go through the block cache, as they do in the
//! other engines. SlateDB's iterator always yields key and value, so a count also receives the
//! value handle (a reference-counted `Bytes`, no copy); it does not read its bytes.

use std::future::Future;
use std::hint::black_box;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use slatedb::config::{FlushOptions, FlushType, PutOptions, ScanOptions, Settings, WriteOptions};
use slatedb::db_cache::foyer::{FoyerCache, FoyerCacheOptions};
use slatedb::db_cache::{DbCache, SplitCache};
use slatedb::object_store::local::LocalFileSystem;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, IterationOrder};
use tokio::runtime::Runtime;

const ENGINE: &str = "slatedb";

/// Number of keys.
const N: u64 = 50_000;
const KEY_LEN: usize = 16;
const VALUE_LEN: usize = 100;

const MEMTABLE_SIZE: usize = 64 << 20;
const BLOCK_CACHE_SIZE: u64 = 8 << 20;

const READS_PER_THREAD: u64 = 50_000;
const SCANS_PER_THREAD: u64 = 5_000;
const SCAN_LENGTH: u64 = 100;
const SKIP_LENGTH: u64 = 5_000;
const COUNTS_PER_THREAD: u64 = 50;
const SYNC_WRITES: u64 = 50;
const MIXED_OPS_PER_THREAD: u64 = 50_000;

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Res<T> = Result<T, BoxError>;

// ---------------------------------------------------------------------------------------------
// Data generation
// ---------------------------------------------------------------------------------------------

struct SplitMix64 {
	state: u64,
}

impl SplitMix64 {
	fn new(state: u64) -> Self {
		Self {
			state,
		}
	}

	/// The per-thread stream of phase `phase` and thread `thread`.
	fn stream(phase: u64, thread: u64) -> Self {
		Self::new(phase.wrapping_mul(1_000_003).wrapping_add(thread))
	}

	#[inline]
	fn next(&mut self) -> u64 {
		self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
		let mut z = self.state;
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		z ^ (z >> 31)
	}
}

/// Fisher-Yates shuffle of `0..N-1` with the stream starting at `state = phase`.
fn permutation(phase: u64) -> Vec<u64> {
	let mut perm: Vec<u64> = (0..N).collect();
	let mut rng = SplitMix64::new(phase);
	for i in (1..N as usize).rev() {
		let j = (rng.next() % (i as u64 + 1)) as usize;
		perm.swap(i, j);
	}
	perm
}

/// Key `i`: its 16 ASCII decimal digits, zero-padded.
#[inline]
fn key(i: u64) -> [u8; KEY_LEN] {
	let mut k = [b'0'; KEY_LEN];
	let mut v = i;
	let mut pos = KEY_LEN;
	while v > 0 {
		pos -= 1;
		k[pos] = b'0' + (v % 10) as u8;
		v /= 10;
	}
	k
}

/// Value `i` of generation `g`: byte `j` is `(i + j + 31 * g) mod 256`.
#[inline]
fn value(i: u64, generation: u64) -> [u8; VALUE_LEN] {
	let base = i.wrapping_add(31u64.wrapping_mul(generation));
	let mut v = [0u8; VALUE_LEN];
	for (j, b) in v.iter_mut().enumerate() {
		*b = base.wrapping_add(j as u64) as u8;
	}
	v
}

/// Thread `t`'s share of `m` operations over `threads` threads.
fn partition(m: u64, t: u64, threads: u64) -> Range<u64> {
	(t * m / threads)..((t + 1) * m / threads)
}

// ---------------------------------------------------------------------------------------------
// Engine operations
// ---------------------------------------------------------------------------------------------

/// How a synchronized write waits for durability.
#[derive(Clone, Copy, PartialEq, Debug)]
enum SyncMode {
	/// `Db::flush` after the write: the WAL buffer is written to the object store immediately.
	Flush,
	/// `WriteHandle::await_durable`: waits for the next timed WAL flush.
	AwaitDurable,
}

#[derive(Clone)]
struct Ctx {
	db: Arc<Db>,
	sync_mode: SyncMode,
}

impl Ctx {
	/// One put (or delete when `val` is `None`). A synchronized write returns once it is durable
	/// in the object store.
	async fn write(&self, k: &[u8; KEY_LEN], val: Option<&[u8]>, sync: bool) -> Res<()> {
		let handle = match val {
			Some(v) => self.db.put_with_options(k, v, &PutOptions::default(), &WriteOptions::default()).await?,
			None => self.db.delete_with_options(k, &WriteOptions::default()).await?,
		};
		if sync {
			match self.sync_mode {
				SyncMode::Flush => self.db.flush().await?,
				SyncMode::AwaitDurable => handle.await_durable().await?,
			}
		}
		Ok(())
	}

	/// One point read. Returns whether the key was found.
	#[inline]
	async fn point_read(&self, k: &[u8; KEY_LEN]) -> Res<bool> {
		match self.db.get(k).await? {
			Some(v) => {
				black_box(v.len());
				Ok(true)
			}
			None => Ok(false),
		}
	}

	fn scan_options(order: IterationOrder) -> ScanOptions {
		ScanOptions::default().with_cache_blocks(true).with_order(order)
	}

	/// One seek: a scan from the target with no upper bound, whose first entry's key and value are
	/// read. Returns whether that key equals the target.
	#[inline]
	async fn seek(&self, k: &[u8; KEY_LEN]) -> Res<bool> {
		let mut it = self.db.scan_with_options(&k[..].., &Self::scan_options(IterationOrder::Ascending)).await?;
		match it.next().await? {
			Some(kv) => {
				black_box(kv.value.len());
				Ok(kv.key.as_ref() == &k[..])
			}
			None => Ok(false),
		}
	}

	/// One full scan, reading key and value of every entry. Returns the number of entries.
	async fn full_scan(&self, reverse: bool) -> Res<u64> {
		let order = if reverse { IterationOrder::Descending } else { IterationOrder::Ascending };
		let mut it = self.db.scan_with_options(.., &Self::scan_options(order)).await?;
		let mut entries = 0u64;
		let mut bytes = 0usize;
		while let Some(kv) = it.next().await? {
			bytes += kv.key.len() + kv.value.len();
			entries += 1;
		}
		black_box(bytes);
		Ok(entries)
	}

	/// One range scan: from `start`, advance over `skip` entries without reading them, then read
	/// key and value of up to `take` entries. Returns the number of entries read after skipping.
	#[inline]
	async fn range_scan(&self, start: u64, skip: u64, take: u64) -> Res<u64> {
		let k = key(start);
		let mut it = self.db.scan_with_options(&k[..].., &Self::scan_options(IterationOrder::Ascending)).await?;
		for _ in 0..skip {
			if it.next().await?.is_none() {
				return Ok(0);
			}
		}
		let mut read = 0u64;
		let mut bytes = 0usize;
		while read < take {
			match it.next().await? {
				Some(kv) => {
					bytes += kv.key.len() + kv.value.len();
					read += 1;
				}
				None => break,
			}
		}
		black_box(bytes);
		Ok(read)
	}

	/// One count of every entry; values are not read.
	async fn count_all(&self) -> Res<u64> {
		let mut it = self.db.scan_with_options(.., &Self::scan_options(IterationOrder::Ascending)).await?;
		let mut entries = 0u64;
		while it.next().await?.is_some() {
			entries += 1;
		}
		Ok(entries)
	}
}

// ---------------------------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------------------------

/// What a worker returns: its `check` contribution.
#[derive(Default, Clone, Copy)]
struct WorkerResult {
	check: u64,
}

struct PhaseOutcome {
	seconds: f64,
	check: u64,
}

struct Runner {
	runtime: Runtime,
}

impl Runner {
	/// Runs `work(t)` for `t` in `0..threads` as tokio tasks, every worker released together by a
	/// blocking barrier (the runtime has exactly `threads` worker threads, so each released task
	/// holds its own thread). The time runs from the earliest release to the last worker finishing.
	fn run_phase<F, Fut>(&self, threads: u64, work: F) -> Res<PhaseOutcome>
	where
		F: Fn(u64) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Res<WorkerResult>> + Send + 'static,
	{
		let barrier = Arc::new(Barrier::new(threads as usize));
		let work = Arc::new(work);
		let handles: Vec<_> = (0..threads)
			.map(|t| {
				let barrier = Arc::clone(&barrier);
				let work = Arc::clone(&work);
				self.runtime.spawn(async move {
					barrier.wait();
					let start = Instant::now();
					let result = work(t).await;
					let end = Instant::now();
					result.map(|r| (start, end, r))
				})
			})
			.collect();
		let results: Vec<Res<(Instant, Instant, WorkerResult)>> = self.runtime.block_on(async move {
			let mut results = Vec::with_capacity(handles.len());
			for h in handles {
				results.push(h.await.unwrap_or_else(|e| Err(format!("worker task failed: {e}").into())));
			}
			results
		});

		let mut first_start: Option<Instant> = None;
		let mut last_end: Option<Instant> = None;
		let mut check = 0;
		for r in results {
			let (start, end, w) = r?;
			first_start = Some(first_start.map_or(start, |s| s.min(start)));
			last_end = Some(last_end.map_or(end, |e| e.max(end)));
			check += w.check;
		}
		let seconds = match (first_start, last_end) {
			(Some(s), Some(e)) => e.duration_since(s).as_secs_f64(),
			_ => 0.0,
		};
		Ok(PhaseOutcome {
			seconds,
			check,
		})
	}
}

struct Reporter {
	suite: String,
	threads: u64,
}

impl Reporter {
	fn result(&self, phase: &str, unit: &str, work: u64, outcome: &PhaseOutcome) {
		println!(
			"{{\"type\":\"result\",\"engine\":\"{ENGINE}\",\"suite\":\"{}\",\"threads\":{},\"phase\":\"{}\",\"unit\":\"{}\",\"work\":{},\"seconds\":{},\"check\":{}}}",
			self.suite, self.threads, phase, unit, work, outcome.seconds, outcome.check
		);
	}

	fn memory(&self, rss: u64, peak: u64) {
		println!(
			"{{\"type\":\"memory\",\"engine\":\"{ENGINE}\",\"suite\":\"{}\",\"threads\":{},\"rss_bytes\":{},\"peak_rss_bytes\":{}}}",
			self.suite, self.threads, rss, peak
		);
	}
}

/// Writes (or deletes) the keys `order[i]` for `i` in each worker's partition of `0..len`.
/// `order` is `None` for ascending keys.
fn write_phase(
	runner: &Runner,
	ctx: &Ctx,
	threads: u64,
	len: u64,
	order: Option<Arc<Vec<u64>>>,
	generation: Option<u64>,
	sync: bool,
) -> Res<PhaseOutcome> {
	let ctx = ctx.clone();
	runner.run_phase(threads, move |t| {
		let ctx = ctx.clone();
		let order = order.clone();
		async move {
			let mut w = WorkerResult::default();
			for i in partition(len, t, threads) {
				let k = order.as_ref().map_or(i, |o| o[i as usize]);
				let v = generation.map(|g| value(k, g));
				ctx.write(&key(k), v.as_ref().map(|v| &v[..]), sync).await?;
				w.check += 1;
			}
			Ok(w)
		}
	})
}

/// Writes keys `0..N-1` ascending with generation-0 values from one worker, outside any timing.
fn load(runner: &Runner, ctx: &Ctx) -> Res<()> {
	let o = write_phase(runner, ctx, 1, N, None, Some(0), false)?;
	if o.check != N {
		return Err(format!("load wrote {} keys, not {N}", o.check).into());
	}
	eprintln!("[{ENGINE}] load: {N} keys in {:.3}s", o.seconds);
	Ok(())
}

fn suite_write(runner: &Runner, ctx: &Ctx, rep: &Reporter, threads: u64) -> Res<()> {
	let perm2 = Arc::new(permutation(2));
	let perm3 = Arc::new(permutation(3));

	let o = write_phase(runner, ctx, threads, N, None, Some(0), false)?;
	rep.result("sequential-writes", "ops/s", N, &o);

	let o = write_phase(runner, ctx, threads, N, Some(perm2), Some(1), false)?;
	rep.result("overwrites", "ops/s", N, &o);

	let o = write_phase(runner, ctx, threads, N, Some(perm3), None, false)?;
	rep.result("random-deletes", "ops/s", N, &o);
	Ok(())
}

fn suite_randomfill(runner: &Runner, ctx: &Ctx, rep: &Reporter, threads: u64) -> Res<()> {
	let perm4 = Arc::new(permutation(4));
	let o = write_phase(runner, ctx, threads, N, Some(perm4), Some(0), false)?;
	rep.result("random-writes", "ops/s", N, &o);
	Ok(())
}

fn suite_sync(runner: &Runner, ctx: &Ctx, rep: &Reporter, threads: u64) -> Res<()> {
	let o = write_phase(runner, ctx, threads, SYNC_WRITES, None, Some(0), true)?;
	rep.result("sync-writes", "ops/s", SYNC_WRITES, &o);
	Ok(())
}

fn suite_reads(runner: &Runner, ctx: &Ctx, rep: &Reporter, threads: u64) -> Res<()> {
	for (phase, name) in [(10, "random-reads-first"), (11, "random-reads-warm")] {
		let ctx = ctx.clone();
		let o = runner.run_phase(threads, move |t| {
			let ctx = ctx.clone();
			async move {
				let mut rng = SplitMix64::stream(phase, t);
				let mut w = WorkerResult::default();
				for _ in 0..READS_PER_THREAD {
					if ctx.point_read(&key(rng.next() % N)).await? {
						w.check += 1;
					}
				}
				Ok(w)
			}
		})?;
		rep.result(name, "ops/s", READS_PER_THREAD * threads, &o);
	}

	for (phase, name) in [(12, "random-seeks-first"), (13, "random-seeks-warm")] {
		let ctx = ctx.clone();
		let o = runner.run_phase(threads, move |t| {
			let ctx = ctx.clone();
			async move {
				let mut rng = SplitMix64::stream(phase, t);
				let mut w = WorkerResult::default();
				for _ in 0..READS_PER_THREAD {
					if ctx.seek(&key(rng.next() % N)).await? {
						w.check += 1;
					}
				}
				Ok(w)
			}
		})?;
		rep.result(name, "ops/s", READS_PER_THREAD * threads, &o);
	}

	for (reverse, name) in [(false, "forward-scan"), (true, "reverse-scan")] {
		let ctx = ctx.clone();
		let o = runner.run_phase(threads, move |_| {
			let ctx = ctx.clone();
			async move {
				Ok(WorkerResult {
					check: ctx.full_scan(reverse).await?,
				})
			}
		})?;
		rep.result(name, "entries/s", N * threads, &o);
	}

	for (phase, name, skip) in [(16, "scan-100", 0), (17, "skip-5000-scan-100", SKIP_LENGTH)] {
		let ctx = ctx.clone();
		let o = runner.run_phase(threads, move |t| {
			let ctx = ctx.clone();
			async move {
				let mut rng = SplitMix64::stream(phase, t);
				let mut w = WorkerResult::default();
				for _ in 0..SCANS_PER_THREAD {
					let start = rng.next() % (N - skip - SCAN_LENGTH);
					w.check += ctx.range_scan(start, skip, SCAN_LENGTH).await?;
				}
				Ok(w)
			}
		})?;
		rep.result(name, "scans/s", SCANS_PER_THREAD * threads, &o);
	}

	let ctx = ctx.clone();
	let o = runner.run_phase(threads, move |_| {
		let ctx = ctx.clone();
		async move {
			let mut w = WorkerResult::default();
			for _ in 0..COUNTS_PER_THREAD {
				w.check += ctx.count_all().await?;
			}
			Ok(w)
		}
	})?;
	rep.result("count-all", "scans/s", COUNTS_PER_THREAD * threads, &o);
	Ok(())
}

fn suite_mixed(runner: &Runner, ctx: &Ctx, rep: &Reporter, threads: u64) -> Res<()> {
	let ctx = ctx.clone();
	let o = runner.run_phase(threads, move |t| {
		let ctx = ctx.clone();
		async move {
			let mut rng = SplitMix64::stream(20, t);
			let mut w = WorkerResult::default();
			for _ in 0..MIXED_OPS_PER_THREAD {
				let read = rng.next() % 100 < 90;
				let k = rng.next() % N;
				if read {
					if ctx.point_read(&key(k)).await? {
						w.check += 1;
					}
				} else {
					ctx.write(&key(k), Some(&value(k, 1)), false).await?;
					w.check += 1;
				}
			}
			Ok(w)
		}
	})?;
	rep.result("read-write-90-10", "ops/s", MIXED_OPS_PER_THREAD * threads, &o);
	Ok(())
}

/// Counts the files with `extension` under `dir`, recursively, and their total size.
fn files_with_extension(dir: &Path, extension: &str) -> Res<(u64, u64)> {
	let mut count = 0;
	let mut bytes = 0;
	if !dir.exists() {
		return Ok((0, 0));
	}
	for entry in std::fs::read_dir(dir)? {
		let entry = entry?;
		let path = entry.path();
		if path.is_dir() {
			let (c, b) = files_with_extension(&path, extension)?;
			count += c;
			bytes += b;
		} else if path.extension().is_some_and(|e| e == extension) {
			count += 1;
			bytes += entry.metadata()?.len();
		}
	}
	Ok((count, bytes))
}

/// Moves the loaded data from the memtable to an SST: `Db::flush_with_options` with
/// `FlushType::MemTable` freezes the memtable and waits until it is written as an L0 SST and
/// recorded in the manifest. The load fits in one memtable, so this writes one SST. SlateDB's
/// compactor only starts merging L0 SSTs into sorted runs once enough of them exist, and SlateDB
/// has no public call that forces or awaits a compaction, so the SST stays in L0 (as SurrealKV's
/// does).
fn flush_to_ssts(runner: &Runner, db: &Db, db_dir: &Path) -> Res<()> {
	let started = Instant::now();
	runner.runtime.block_on(db.flush_with_options(FlushOptions {
		flush_type: FlushType::MemTable,
	}))?;
	let elapsed = started.elapsed().as_secs_f64();
	let (tables, bytes) = files_with_extension(&db_dir.join("compacted"), "sst")?;
	eprintln!("[{ENGINE}] flush: memtable flushed in {elapsed:.3}s; {tables} SST file(s), {bytes} bytes");
	if tables != 1 {
		return Err(format!("expected one SST after the flush, found {tables}").into());
	}
	Ok(())
}

// ---------------------------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------------------------

#[cfg(target_os = "macos")]
fn resident_bytes() -> Res<u64> {
	// SAFETY: `proc_pidinfo` writes at most `size` bytes into the zeroed `proc_taskinfo`.
	unsafe {
		let mut info: libc::proc_taskinfo = std::mem::zeroed();
		let size = std::mem::size_of::<libc::proc_taskinfo>() as libc::c_int;
		let written = libc::proc_pidinfo(
			libc::getpid(),
			libc::PROC_PIDTASKINFO,
			0,
			&mut info as *mut libc::proc_taskinfo as *mut libc::c_void,
			size,
		);
		if written != size {
			return Err("proc_pidinfo(PROC_PIDTASKINFO) failed".into());
		}
		Ok(info.pti_resident_size)
	}
}

#[cfg(not(target_os = "macos"))]
fn resident_bytes() -> Res<u64> {
	let statm = std::fs::read_to_string("/proc/self/statm")?;
	let pages: u64 = statm.split_whitespace().nth(1).ok_or("bad /proc/self/statm")?.parse()?;
	// SAFETY: sysconf has no preconditions.
	let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;
	Ok(pages * page)
}

fn peak_resident_bytes() -> Res<u64> {
	// SAFETY: `getrusage` fills the zeroed `rusage`.
	let usage = unsafe {
		let mut usage: libc::rusage = std::mem::zeroed();
		if libc::getrusage(libc::RUSAGE_SELF, &mut usage) != 0 {
			return Err("getrusage failed".into());
		}
		usage
	};
	let maxrss = usage.ru_maxrss as u64;
	// Bytes on macOS, kilobytes elsewhere.
	Ok(if cfg!(target_os = "macos") {
		maxrss
	} else {
		maxrss * 1024
	})
}

// ---------------------------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------------------------

struct Args {
	suite: String,
	threads: u64,
	dir: PathBuf,
}

fn parse_args() -> Res<Args> {
	let mut engine = None;
	let mut suite = None;
	let mut threads = None;
	let mut dir = None;
	let mut args = std::env::args().skip(1);
	while let Some(arg) = args.next() {
		let mut take = || args.next().ok_or_else(|| format!("missing value for {arg}"));
		match arg.as_str() {
			"--engine" => engine = Some(take()?),
			"--suite" => suite = Some(take()?),
			"--threads" => threads = Some(take()?.parse::<u64>()?),
			"--dir" => dir = Some(PathBuf::from(take()?)),
			other => return Err(format!("unknown argument {other}").into()),
		}
	}
	const USAGE: &str = "usage: slatedb-adapter --engine slatedb --suite <suite> --threads <T> --dir <path>";
	let engine = engine.ok_or(USAGE)?;
	if engine != ENGINE {
		return Err(format!("this adapter only runs --engine {ENGINE}, not {engine}").into());
	}
	let threads = threads.ok_or(USAGE)?;
	if threads == 0 {
		return Err("--threads must be at least 1".into());
	}
	Ok(Args {
		suite: suite.ok_or(USAGE)?,
		threads,
		dir: dir.ok_or(USAGE)?,
	})
}

fn run() -> Res<()> {
	let args = parse_args()?;
	let suite = args.suite.as_str();
	if !matches!(suite, "write" | "randomfill" | "sync" | "read-memtable" | "read-sst" | "mixed") {
		return Err(format!("unknown suite {suite}").into());
	}
	let threads = args.threads;
	let sync_mode = match std::env::var("SLATEDB_ADAPTER_SYNC").as_deref() {
		Err(_) | Ok("flush") => SyncMode::Flush,
		Ok("await-durable") => SyncMode::AwaitDurable,
		Ok(other) => return Err(format!("SLATEDB_ADAPTER_SYNC={other} is not flush or await-durable").into()),
	};

	std::fs::create_dir_all(&args.dir)?;
	let db_dir = args.dir.join("db");
	std::fs::create_dir_all(&args.dir)?;

	let runtime = tokio::runtime::Builder::new_multi_thread().worker_threads(threads as usize).enable_all().build()?;

	let object_store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&args.dir)?.with_fsync(true));

	let mut settings = Settings::default();
	// Defaults, repeated so the configuration is explicit: freeze the memtable at 64 MiB, no
	// compression. The WAL is on and written to the object store every 100 ms.
	settings.l0_sst_size_bytes = MEMTABLE_SIZE;
	settings.compression_codec = None;

	// An 8 MiB block cache. Index and filter blocks go to SlateDB's separate metadata cache
	// (default size), as RocksDB keeps them outside its block cache by default.
	let block_cache: Arc<dyn DbCache> = Arc::new(FoyerCache::new_with_opts(FoyerCacheOptions {
		max_capacity: BLOCK_CACHE_SIZE,
		..FoyerCacheOptions::default()
	}));
	let meta_cache: Arc<dyn DbCache> = Arc::new(FoyerCache::new());
	let db_cache: Arc<dyn DbCache> =
		Arc::new(SplitCache::new().with_block_cache(Some(block_cache)).with_meta_cache(Some(meta_cache)).build());

	let db = Arc::new(runtime.block_on(
		Db::builder("db", object_store).with_settings(settings).with_db_cache(Arc::clone(&db_cache), 0).build(),
	)?);
	eprintln!(
		"[{ENGINE}] opened {} (memtable {MEMTABLE_SIZE} B, block cache {BLOCK_CACHE_SIZE} B, no compression, WAL on, fsync on); {threads} runtime threads, sync mode {sync_mode:?}",
		db_dir.display()
	);

	let ctx = Ctx {
		db: Arc::clone(&db),
		sync_mode,
	};
	let runner = Runner {
		runtime,
	};
	let rep = Reporter {
		suite: args.suite.clone(),
		threads,
	};

	match suite {
		"write" => suite_write(&runner, &ctx, &rep, threads)?,
		"randomfill" => suite_randomfill(&runner, &ctx, &rep, threads)?,
		"sync" => suite_sync(&runner, &ctx, &rep, threads)?,
		"read-memtable" => {
			load(&runner, &ctx)?;
			suite_reads(&runner, &ctx, &rep, threads)?;
		}
		"read-sst" => {
			load(&runner, &ctx)?;
			flush_to_ssts(&runner, &db, &db_dir)?;
			suite_reads(&runner, &ctx, &rep, threads)?;
		}
		"mixed" => {
			load(&runner, &ctx)?;
			suite_mixed(&runner, &ctx, &rep, threads)?;
		}
		_ => unreachable!(),
	}

	let rss = resident_bytes()?;

	drop(ctx);
	let closed = Instant::now();
	runner.runtime.block_on(db.close())?;
	drop(db);
	runner.runtime.block_on(db_cache.close())?;
	eprintln!("[{ENGINE}] closed in {:.3}s", closed.elapsed().as_secs_f64());
	runner.runtime.shutdown_background();

	let peak = peak_resident_bytes()?;
	rep.memory(rss, peak);
	Ok(())
}

fn main() {
	if let Err(e) = run() {
		eprintln!("[{ENGINE}] error: {e}");
		std::process::exit(1);
	}
}
