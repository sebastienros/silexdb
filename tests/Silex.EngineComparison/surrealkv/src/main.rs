//! SurrealKV adapter for the engine comparison harness.
//!
//! Implements the adapter contract of `tests/Silex.EngineComparison/README.md`:
//!
//! ```text
//! surrealkv-adapter --engine surrealkv --suite <suite> --threads <T> --dir <path>
//! ```
//!
//! One JSON line per timed phase and one memory line go to standard output; diagnostics go to
//! standard error. Any error ends the process with a non-zero exit code.
//!
//! # How workers are driven
//!
//! A SurrealKV commit does not write anything itself. `Transaction::commit` validates the
//! transaction, puts it into the commit ring, wakes the pipeline's flusher (a tokio task spawned
//! when the tree is built) and awaits a oneshot channel. The flusher appends the group to the WAL
//! inline (SurrealKV's `affinitypool` hand-off runs inline because no global pool is built),
//! applies it to the memtable, publishes it and completes the channels. With
//! `Durability::Eventual` there is no other commit path, and there is no batch API besides a
//! transaction, so every commit costs a hand-off to the flusher task and back.
//!
//! The adapter makes that hand-off as cheap as SurrealKV allows while keeping one transaction per
//! mutation, each commit awaited before the next:
//!
//! - Each worker is a tokio task on a multi-threaded runtime with `T` worker threads, one per
//!   worker. The tree is built inside that runtime, so the flusher and the background memtable
//!   flush and compaction tasks share those threads with the workers.
//! - A committing task wakes the flusher from a runtime thread, which puts it in that thread's
//!   LIFO slot; when the task then awaits, the flusher runs on the same thread and completes the
//!   channel the same way. At `T = 1` the commit never leaves the thread. At `T > 1` the flusher
//!   wakes the committers of a group on their own threads, which is the cost of group commit.
//! - Spare runtime threads make commits slower, not faster: an idle runtime thread steals the
//!   flusher (or the committer) once tokio stops polling the LIFO slot, and every steal is a park
//!   and unpark between threads. Measured on a 14-core Mac (randomfill, median of 3):
//!   `T` runtime threads 411k ops/s at `T = 1` and 512k at `T = 8`; 14 runtime threads 314k and
//!   491k; OS threads outside the runtime calling `Handle::block_on` per commit 89k and 305k,
//!   because then both hand-offs cross threads.
//! - The writes of these suites never fill the 64 MiB memtable, so no background flush or
//!   compaction competes with the workers for the `T` threads during a timed phase.
//!
//! Workers start on a blocking `std::sync::Barrier`: each holds its own runtime thread when it is
//! released, rather than being woken onto the queue of the thread that arrived last. Reads are
//! synchronous in SurrealKV and run directly in the task, which never yields during a read phase;
//! they measure the same as from plain OS threads.
//!
//! Diagnostics, for comparing strategies only: `SURREALKV_ADAPTER_DRIVER=threads` runs the workers
//! as OS threads outside the runtime with `Handle::block_on`, `SURREALKV_ADAPTER_RUNTIME_THREADS`
//! sets the runtime size (at least `T`), and `SURREALKV_ADAPTER_WRITE_MODE=write-only` begins write
//! transactions in `Mode::WriteOnly` (no snapshot) instead of the default `Mode::ReadWrite`.

use std::future::Future;
use std::hint::black_box;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use surrealkv::{Durability, Error as SkvError, LSMIterator, Mode, Tree, TreeBuilder};
use tokio::runtime::Runtime;

const ENGINE: &str = "surrealkv";

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

#[derive(Clone)]
struct Ctx {
	tree: Arc<Tree>,
	/// The mode of write transactions.
	write_mode: Mode,
}

impl Ctx {
	/// One write transaction: begin, one set (or delete when `val` is `None`), commit. A commit
	/// that fails with a write conflict is retried from a new transaction. Returns the number of
	/// retries.
	async fn write(&self, k: &[u8; KEY_LEN], val: Option<&[u8]>, sync: bool) -> Res<u64> {
		let mut retries = 0;
		loop {
			let mut tx = self.tree.begin_with_mode(self.write_mode)?;
			if sync {
				tx.set_durability(Durability::Immediate);
			}
			match val {
				Some(v) => tx.set(k, v)?,
				None => tx.delete(k)?,
			}
			match tx.commit().await {
				Ok(()) => return Ok(retries),
				Err(SkvError::TransactionWriteConflict) => retries += 1,
				Err(e) => return Err(e.into()),
			}
		}
	}

	/// One point read in its own read-only transaction. Returns whether the key was found.
	#[inline]
	fn point_read(&self, k: &[u8; KEY_LEN]) -> Res<bool> {
		let tx = self.tree.begin_with_mode(Mode::ReadOnly)?;
		match tx.get(k)? {
			Some(v) => {
				black_box(v.len());
				Ok(true)
			}
			None => Ok(false),
		}
	}

	/// One seek in its own read-only transaction: a range from the target with no upper bound,
	/// positioned on its first entry, whose key and value are read. Returns whether that key
	/// equals the target.
	#[inline]
	fn seek(&self, k: &[u8; KEY_LEN]) -> Res<bool> {
		let tx = self.tree.begin_with_mode(Mode::ReadOnly)?;
		let mut it = tx.scan(&k[..]..)?;
		if !it.seek_first()? {
			return Ok(false);
		}
		let found = it.key().user_key() == &k[..];
		black_box(it.value()?.len());
		Ok(found)
	}

	/// One full scan in one read-only transaction, reading key and value of every entry.
	/// Returns the number of entries.
	fn full_scan(&self, reverse: bool) -> Res<u64> {
		let tx = self.tree.begin_with_mode(Mode::ReadOnly)?;
		let mut it = tx.iter()?;
		if reverse {
			it.seek_last()?;
		} else {
			it.seek_first()?;
		}
		let mut entries = 0u64;
		let mut bytes = 0usize;
		while it.valid() {
			bytes += it.key().user_key().len() + it.value()?.len();
			entries += 1;
			if reverse {
				it.prev()?;
			} else {
				it.next()?;
			}
		}
		black_box(bytes);
		Ok(entries)
	}

	/// One range scan in one read-only transaction: seek to `start`, advance over `skip` entries
	/// without reading them, then read key and value of up to `take` entries. Returns the
	/// number of entries read after skipping.
	#[inline]
	fn range_scan(&self, start: u64, skip: u64, take: u64) -> Res<u64> {
		let k = key(start);
		let tx = self.tree.begin_with_mode(Mode::ReadOnly)?;
		let mut it = tx.scan(&k[..]..)?;
		if !it.seek_first()? {
			return Ok(0);
		}
		for _ in 0..skip {
			if !it.next()? {
				return Ok(0);
			}
		}
		let mut read = 0u64;
		let mut bytes = 0usize;
		while it.valid() {
			bytes += it.key().user_key().len() + it.value()?.len();
			read += 1;
			if read == take {
				break;
			}
			it.next()?;
		}
		black_box(bytes);
		Ok(read)
	}

	/// One count of every entry in one read-only transaction; values are not read.
	fn count_all(&self) -> Res<u64> {
		let tx = self.tree.begin_with_mode(Mode::ReadOnly)?;
		let mut it = tx.iter()?;
		it.seek_first()?;
		let mut entries = 0u64;
		while it.valid() {
			entries += 1;
			it.next()?;
		}
		Ok(entries)
	}
}

// ---------------------------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------------------------

/// How workers are run.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Driver {
	/// Each worker is a tokio task on the multi-threaded runtime and awaits its commits.
	Tasks,
	/// Each worker is an OS thread outside the runtime that drives its work with
	/// `Handle::block_on`.
	Threads,
}

/// What a worker returns: its `check` contribution and its conflict retries.
#[derive(Default, Clone, Copy)]
struct WorkerResult {
	check: u64,
	retries: u64,
}

struct PhaseOutcome {
	seconds: f64,
	check: u64,
	retries: u64,
}

struct Runner {
	runtime: Runtime,
	driver: Driver,
}

impl Runner {
	/// Runs `work(t)` for `t` in `0..threads`, every worker released together by a barrier. The
	/// time runs from the earliest release to the last worker finishing.
	///
	/// The barrier is a blocking `std::sync::Barrier` in both drivers. With tasks, every worker
	/// then holds its own runtime thread when it is released (the runtime has at least as many
	/// threads as workers), rather than being woken onto the queue of the worker that arrived
	/// last.
	fn run_phase<F, Fut>(&self, threads: u64, work: F) -> Res<PhaseOutcome>
	where
		F: Fn(u64) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Res<WorkerResult>> + Send + 'static,
	{
		let barrier = Arc::new(Barrier::new(threads as usize));
		let work = Arc::new(work);
		let results: Vec<Res<(Instant, Instant, WorkerResult)>> = match self.driver {
			Driver::Tasks => {
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
				self.runtime.block_on(async move {
					let mut results = Vec::with_capacity(handles.len());
					for h in handles {
						results.push(h.await.unwrap_or_else(|e| Err(format!("worker task failed: {e}").into())));
					}
					results
				})
			}
			Driver::Threads => {
				let handle = self.runtime.handle();
				std::thread::scope(|s| {
					let handles: Vec<_> = (0..threads)
						.map(|t| {
							let barrier = &barrier;
							let work = &work;
							s.spawn(move || {
								barrier.wait();
								let start = Instant::now();
								let result = handle.block_on(work(t));
								let end = Instant::now();
								result.map(|r| (start, end, r))
							})
						})
						.collect();
					handles
						.into_iter()
						.map(|h| h.join().unwrap_or_else(|_| Err("a worker thread panicked".into())))
						.collect()
				})
			}
		};

		let mut first_start: Option<Instant> = None;
		let mut last_end: Option<Instant> = None;
		let mut check = 0;
		let mut retries = 0;
		for r in results {
			let (start, end, w) = r?;
			first_start = Some(first_start.map_or(start, |s| s.min(start)));
			last_end = Some(last_end.map_or(end, |e| e.max(end)));
			check += w.check;
			retries += w.retries;
		}
		let seconds = match (first_start, last_end) {
			(Some(s), Some(e)) => e.duration_since(s).as_secs_f64(),
			_ => 0.0,
		};
		Ok(PhaseOutcome {
			seconds,
			check,
			retries,
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

	fn write_result(&self, phase: &str, work: u64, outcome: &PhaseOutcome) {
		eprintln!("[{ENGINE}] {phase}: {} conflict retries", outcome.retries);
		self.result(phase, "ops/s", work, outcome);
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
				w.retries += ctx.write(&key(k), v.as_ref().map(|v| &v[..]), sync).await?;
				w.check += 1;
			}
			Ok(w)
		}
	})
}

/// Writes keys `0..N-1` ascending with generation-0 values from one worker, one transaction per
/// key, outside any timing.
fn load(runner: &Runner, ctx: &Ctx) -> Res<()> {
	let o = write_phase(runner, ctx, 1, N, None, Some(0), false)?;
	if o.check != N {
		return Err(format!("load wrote {} keys, not {N}", o.check).into());
	}
	eprintln!("[{ENGINE}] load: {N} keys in {:.3}s ({} conflict retries)", o.seconds, o.retries);
	Ok(())
}

fn suite_write(runner: &Runner, ctx: &Ctx, rep: &Reporter, threads: u64) -> Res<()> {
	let perm2 = Arc::new(permutation(2));
	let perm3 = Arc::new(permutation(3));

	let o = write_phase(runner, ctx, threads, N, None, Some(0), false)?;
	rep.write_result("sequential-writes", N, &o);

	let o = write_phase(runner, ctx, threads, N, Some(perm2), Some(1), false)?;
	rep.write_result("overwrites", N, &o);

	let o = write_phase(runner, ctx, threads, N, Some(perm3), None, false)?;
	rep.write_result("random-deletes", N, &o);
	Ok(())
}

fn suite_randomfill(runner: &Runner, ctx: &Ctx, rep: &Reporter, threads: u64) -> Res<()> {
	let perm4 = Arc::new(permutation(4));
	let o = write_phase(runner, ctx, threads, N, Some(perm4), Some(0), false)?;
	rep.write_result("random-writes", N, &o);
	Ok(())
}

fn suite_sync(runner: &Runner, ctx: &Ctx, rep: &Reporter, threads: u64) -> Res<()> {
	let o = write_phase(runner, ctx, threads, SYNC_WRITES, None, Some(0), true)?;
	rep.write_result("sync-writes", SYNC_WRITES, &o);
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
					if ctx.point_read(&key(rng.next() % N))? {
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
					if ctx.seek(&key(rng.next() % N))? {
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
					check: ctx.full_scan(reverse)?,
					retries: 0,
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
					w.check += ctx.range_scan(start, skip, SCAN_LENGTH)?;
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
				w.check += ctx.count_all()?;
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
					if ctx.point_read(&key(k))? {
						w.check += 1;
					}
				} else {
					w.retries += ctx.write(&key(k), Some(&value(k, 1)), false).await?;
					w.check += 1;
				}
			}
			Ok(w)
		}
	})?;
	eprintln!("[{ENGINE}] read-write-90-10: {} conflict retries", o.retries);
	rep.result("read-write-90-10", "ops/s", MIXED_OPS_PER_THREAD * threads, &o);
	Ok(())
}

/// Moves the loaded data from the memtable to SSTs.
///
/// SurrealKV 1.0.0 has no public flush or compact call (`Tree::flush` and `Tree::compact` exist
/// only under `#[cfg(test)]`). `Tree::create_checkpoint` is the public call that does it: it
/// rotates the active memtable and flushes every immutable memtable to a level-0 SST
/// synchronously on the calling thread before it copies the files, then wakes the level
/// compaction task. The checkpoint copy is a side effect and is deleted afterwards. Level
/// compaction only starts once level 0 holds `level0_max_files` (4) tables, and the load fits in
/// one memtable, so there is no compaction to wait for; SurrealKV has no public call that waits
/// for one either.
fn flush_to_ssts(tree: &Tree, dir: &Path, db_dir: &Path) -> Res<()> {
	let checkpoint_dir = dir.join("flush-checkpoint");
	let started = Instant::now();
	tree.create_checkpoint(&checkpoint_dir)?;
	let elapsed = started.elapsed().as_secs_f64();
	std::fs::remove_dir_all(&checkpoint_dir)?;

	let mut tables = 0;
	let mut bytes = 0;
	for entry in std::fs::read_dir(db_dir.join("sstables"))? {
		let entry = entry?;
		if entry.path().extension().is_some_and(|e| e == "sst") {
			tables += 1;
			bytes += entry.metadata()?.len();
		}
	}
	eprintln!(
		"[{ENGINE}] flush: memtables flushed through create_checkpoint in {elapsed:.3}s; {tables} SST file(s), {bytes} bytes"
	);

	// Optional diagnostic: the level of every table, read back from the manifest, with options of
	// their own so that the tables it opens do not touch the tree's block cache.
	if std::env::var_os("SURREALKV_ADAPTER_DUMP_LEVELS").is_some() {
		let opts = surrealkv::Options::new().with_block_cache_capacity(1 << 20);
		let manifest = surrealkv::inspect::dump_manifest_with_options(db_dir, &opts)?;
		for level in &manifest.levels {
			if !level.tables.is_empty() {
				let level_bytes: u64 = level.tables.iter().map(|t| t.file_size).sum();
				eprintln!(
					"[{ENGINE}] level {}: {} table(s), {} bytes",
					level.level,
					level.tables.len(),
					level_bytes
				);
			}
		}
	}

	if let Some(e) = tree.background_error() {
		return Err(format!("background error after flush: {e:?}").into());
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
	const USAGE: &str =
		"usage: surrealkv-adapter --engine surrealkv --suite <suite> --threads <T> --dir <path>";
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

/// Reads an optional choice from an environment variable.
fn env_choice<T: Copy>(name: &str, default: T, choices: &[(&str, T)]) -> Res<T> {
	match std::env::var(name) {
		Err(_) => Ok(default),
		Ok(v) => choices
			.iter()
			.find(|(n, _)| *n == v)
			.map(|(_, c)| *c)
			.ok_or_else(|| format!("{name}={v} is not one of {:?}", choices.iter().map(|c| c.0).collect::<Vec<_>>()).into()),
	}
}

fn run() -> Res<()> {
	let args = parse_args()?;
	let suite = args.suite.as_str();
	if !matches!(suite, "write" | "randomfill" | "sync" | "read-memtable" | "read-sst" | "mixed") {
		return Err(format!("unknown suite {suite}").into());
	}
	let threads = args.threads;

	let driver = env_choice(
		"SURREALKV_ADAPTER_DRIVER",
		Driver::Tasks,
		&[("tasks", Driver::Tasks), ("threads", Driver::Threads)],
	)?;
	let write_mode = env_choice(
		"SURREALKV_ADAPTER_WRITE_MODE",
		Mode::ReadWrite,
		&[("read-write", Mode::ReadWrite), ("write-only", Mode::WriteOnly)],
	)?;
	// One runtime thread per worker by default: the workers, the commit flusher and the
	// background flush and compaction tasks share them (see the module documentation). At least
	// `threads`, so that the blocking start barrier can release every worker task.
	let runtime_threads = match std::env::var("SURREALKV_ADAPTER_RUNTIME_THREADS") {
		Ok(v) => v.parse::<usize>()?.max(threads as usize),
		Err(_) => threads as usize,
	};

	std::fs::create_dir_all(&args.dir)?;
	let db_dir = args.dir.join("db");

	let runtime = tokio::runtime::Builder::new_multi_thread()
		.worker_threads(runtime_threads)
		.enable_all()
		.build()?;

	// The tree spawns its background tasks on the runtime it is built in.
	let tree = {
		let _guard = runtime.enter();
		Arc::new(
			TreeBuilder::new()
				.with_path(db_dir.clone())
				.with_max_memtable_size(MEMTABLE_SIZE)
				.with_block_cache_capacity(BLOCK_CACHE_SIZE)
				.without_compression()
				.with_versioning(false, 0)
				.build()?,
		)
	};
	eprintln!(
		"[{ENGINE}] opened {} (memtable {MEMTABLE_SIZE} B, block cache {BLOCK_CACHE_SIZE} B, no compression, versioning off); driver {driver:?}, {runtime_threads} runtime threads, write transactions {write_mode:?}",
		db_dir.display()
	);

	let ctx = Ctx {
		tree: Arc::clone(&tree),
		write_mode,
	};
	let runner = Runner {
		runtime,
		driver,
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
			flush_to_ssts(&tree, &args.dir, &db_dir)?;
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
	runner.runtime.block_on(tree.close())?;
	drop(tree);
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
