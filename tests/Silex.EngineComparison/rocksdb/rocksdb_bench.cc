// RocksDB adapter for the engine comparison harness (see ../README.md for the contract).
//
//   rocksdb_bench --engine rocksdb --suite <suite> --threads <T> --dir <path>
//
// One process runs one suite against a fresh plain rocksdb::DB in --dir, prints one JSON result line
// per timed phase and one memory line to stdout, and exits non-zero on any RocksDB error.

#include <libproc.h>
#include <sys/resource.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <latch>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "rocksdb/version.h"

static_assert(ROCKSDB_MAJOR == 11 && ROCKSDB_MINOR == 4 && ROCKSDB_PATCH == 0,
              "the comparison is defined against RocksDB 11.4.0");

namespace {

using Clock = std::chrono::steady_clock;

constexpr uint64_t kN = 50'000;
constexpr size_t kKeySize = 16;
constexpr size_t kValueSize = 100;
constexpr uint64_t kReadOps = 50'000;
constexpr uint64_t kShortScans = 5'000;
constexpr uint64_t kShortScanLength = 100;
constexpr uint64_t kSkip = 5'000;
constexpr uint64_t kCounts = 50;
constexpr uint64_t kSyncKeys = 50;

// ---------------------------------------------------------------------------------------------
// Data

struct SplitMix64 {
  uint64_t state;
  explicit SplitMix64(uint64_t seed) : state(seed) {}
  uint64_t next() {
    state += 0x9E3779B97F4A7C15ULL;
    uint64_t z = state;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31);
  }
};

SplitMix64 thread_stream(uint64_t phase, uint64_t thread) { return SplitMix64(phase * 1'000'003ULL + thread); }

std::vector<uint64_t> permutation(uint64_t phase) {
  std::vector<uint64_t> perm(kN);
  for (uint64_t i = 0; i < kN; ++i) perm[i] = i;
  SplitMix64 rng(phase);
  for (uint64_t i = kN - 1; i >= 1; --i) {
    uint64_t j = rng.next() % (i + 1);
    std::swap(perm[i], perm[j]);
  }
  return perm;
}

struct Key {
  char bytes[kKeySize];
  explicit Key(uint64_t i) {
    for (int k = static_cast<int>(kKeySize) - 1; k >= 0; --k) {
      bytes[k] = static_cast<char>('0' + i % 10);
      i /= 10;
    }
  }
  rocksdb::Slice slice() const { return rocksdb::Slice(bytes, kKeySize); }
};

struct Value {
  char bytes[kValueSize];
  Value(uint64_t i, uint64_t generation) {
    for (uint64_t j = 0; j < kValueSize; ++j) bytes[j] = static_cast<char>((i + j + 31 * generation) & 0xFF);
  }
  rocksdb::Slice slice() const { return rocksdb::Slice(bytes, kValueSize); }
};

struct Range {
  uint64_t begin, end;
};

Range partition(uint64_t m, uint64_t threads, uint64_t t) { return {t * m / threads, (t + 1) * m / threads}; }

// ---------------------------------------------------------------------------------------------
// Errors

[[noreturn]] void fail(const std::string& what) {
  std::fflush(stdout);
  std::fprintf(stderr, "rocksdb_bench: %s\n", what.c_str());
  std::exit(1);
}

void check_status(const rocksdb::Status& s, const char* what) {
  if (!s.ok()) fail(std::string(what) + ": " + s.ToString());
}

// ---------------------------------------------------------------------------------------------
// Phase runner

struct alignas(64) ThreadResult {
  uint64_t check = 0;
  uint64_t sink = 0;  // bytes touched, so reads cannot be optimized away
  std::string error;  // first RocksDB error seen by the thread
  Clock::time_point end;
};

struct Config {
  std::string suite;
  uint64_t threads = 0;
  std::string dir;
};

volatile uint64_t g_sink = 0;

// Runs `body(t, result)` on T threads released together; time runs from the release to the last thread
// finishing. Prints the phase's result line.
template <typename Body>
void run_phase(const Config& cfg, const char* phase, const char* unit, uint64_t work, Body body) {
  const uint64_t T = cfg.threads;
  std::vector<ThreadResult> results(T);
  std::latch ready(static_cast<std::ptrdiff_t>(T));
  std::latch go(1);
  std::vector<std::thread> workers;
  workers.reserve(T);
  for (uint64_t t = 0; t < T; ++t) {
    workers.emplace_back([&, t] {
      ready.count_down();
      go.wait();
      body(t, results[t]);
      results[t].end = Clock::now();
    });
  }
  ready.wait();
  const Clock::time_point start = Clock::now();
  go.count_down();
  for (auto& w : workers) w.join();

  Clock::time_point end = start;
  uint64_t check = 0;
  uint64_t sink = 0;
  for (auto& r : results) {
    if (!r.error.empty()) fail(std::string(phase) + ": " + r.error);
    end = std::max(end, r.end);
    check += r.check;
    sink += r.sink;
  }
  g_sink = g_sink + sink;
  const double seconds = std::chrono::duration<double>(end - start).count();
  std::printf(
      "{\"type\":\"result\",\"engine\":\"rocksdb\",\"suite\":\"%s\",\"threads\":%llu,\"phase\":\"%s\","
      "\"unit\":\"%s\",\"work\":%llu,\"seconds\":%.9f,\"check\":%llu}\n",
      cfg.suite.c_str(), static_cast<unsigned long long>(T), phase, unit, static_cast<unsigned long long>(work),
      seconds, static_cast<unsigned long long>(check));
  std::fprintf(stderr, "%s: %.0f %s (check %llu)\n", phase, static_cast<double>(work) / seconds, unit,
               static_cast<unsigned long long>(check));
}

// Records a non-OK status in the thread's result; returns false so the worker can stop.
bool ok(ThreadResult& r, const rocksdb::Status& s, const char* what) {
  if (s.ok()) return true;
  r.error = std::string(what) + ": " + s.ToString();
  return false;
}

// ---------------------------------------------------------------------------------------------
// Engine

std::unique_ptr<rocksdb::DB> open_db(const std::string& dir) {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 64ULL << 20;
  options.compression = rocksdb::kNoCompression;
  options.bottommost_compression = rocksdb::kNoCompression;

  rocksdb::BlockBasedTableOptions table;
  table.block_cache = rocksdb::NewLRUCache(8ULL << 20);
  table.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
  options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table));

  std::unique_ptr<rocksdb::DB> db;
  check_status(rocksdb::DB::Open(options, dir, &db), "open");
  return db;
}

void load(rocksdb::DB* db) {
  rocksdb::WriteOptions wo;
  for (uint64_t i = 0; i < kN; ++i) {
    check_status(db->Put(wo, Key(i).slice(), Value(i, 0).slice()), "load put");
  }
}

// ---------------------------------------------------------------------------------------------
// Write phases

// Each thread writes (or deletes) its partition of `order`, or of 0..m-1 when `order` is null.
void write_phase(const Config& cfg, rocksdb::DB* db, const char* phase, uint64_t m, const std::vector<uint64_t>* order,
                 bool remove, uint64_t generation, bool sync) {
  rocksdb::WriteOptions wo;
  wo.sync = sync;
  run_phase(cfg, phase, "ops/s", m, [&](uint64_t t, ThreadResult& r) {
    const Range range = partition(m, cfg.threads, t);
    for (uint64_t x = range.begin; x < range.end; ++x) {
      const uint64_t i = order ? (*order)[x] : x;
      const Key key(i);
      if (remove) {
        if (!ok(r, db->Delete(wo, key.slice()), "delete")) return;
      } else {
        const Value value(i, generation);
        if (!ok(r, db->Put(wo, key.slice(), value.slice()), "put")) return;
      }
      ++r.check;
    }
  });
}

// ---------------------------------------------------------------------------------------------
// Read phases

void point_reads(const Config& cfg, rocksdb::DB* db, const char* phase, uint64_t p) {
  run_phase(cfg, phase, "ops/s", kReadOps * cfg.threads, [&](uint64_t t, ThreadResult& r) {
    SplitMix64 rng = thread_stream(p, t);
    rocksdb::ReadOptions ro;
    rocksdb::ColumnFamilyHandle* cf = db->DefaultColumnFamily();
    rocksdb::PinnableSlice value;
    for (uint64_t op = 0; op < kReadOps; ++op) {
      const Key key(rng.next() % kN);
      rocksdb::Status s = db->Get(ro, cf, key.slice(), &value);
      if (s.ok()) {
        ++r.check;
        r.sink += value.size();
      } else if (!s.IsNotFound()) {
        ok(r, s, "get");
        return;
      }
      value.Reset();
    }
  });
}

void seeks(const Config& cfg, rocksdb::DB* db, const char* phase, uint64_t p) {
  run_phase(cfg, phase, "ops/s", kReadOps * cfg.threads, [&](uint64_t t, ThreadResult& r) {
    SplitMix64 rng = thread_stream(p, t);
    rocksdb::ReadOptions ro;
    for (uint64_t op = 0; op < kReadOps; ++op) {
      const Key target(rng.next() % kN);
      std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro));
      it->Seek(target.slice());
      if (it->Valid()) {
        const rocksdb::Slice k = it->key();
        const rocksdb::Slice v = it->value();
        r.sink += k.size() + v.size();
        if (k == target.slice()) ++r.check;
      }
      if (!ok(r, it->status(), "seek")) return;
    }
  });
}

void full_scan(const Config& cfg, rocksdb::DB* db, const char* phase, bool reverse) {
  run_phase(cfg, phase, "entries/s", kN * cfg.threads, [&](uint64_t, ThreadResult& r) {
    rocksdb::ReadOptions ro;
    std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro));
    if (reverse) {
      for (it->SeekToLast(); it->Valid(); it->Prev()) {
        r.sink += it->key().size() + it->value().size();
        ++r.check;
      }
    } else {
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        r.sink += it->key().size() + it->value().size();
        ++r.check;
      }
    }
    ok(r, it->status(), "scan");
  });
}

// `kShortScans` scans per thread: seek to `next() mod (N - skip - length)`, advance `skip` entries without
// reading them, then read up to `length` entries.
void short_scans(const Config& cfg, rocksdb::DB* db, const char* phase, uint64_t p, uint64_t skip) {
  const uint64_t range = kN - skip - kShortScanLength;
  run_phase(cfg, phase, "scans/s", kShortScans * cfg.threads, [&](uint64_t t, ThreadResult& r) {
    SplitMix64 rng = thread_stream(p, t);
    rocksdb::ReadOptions ro;
    for (uint64_t op = 0; op < kShortScans; ++op) {
      const Key target(rng.next() % range);
      std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro));
      it->Seek(target.slice());
      for (uint64_t s = 0; s < skip && it->Valid(); ++s) it->Next();
      for (uint64_t n = 0; n < kShortScanLength && it->Valid(); ++n, it->Next()) {
        r.sink += it->key().size() + it->value().size();
        ++r.check;
      }
      if (!ok(r, it->status(), "scan")) return;
    }
  });
}

void counts(const Config& cfg, rocksdb::DB* db, const char* phase) {
  run_phase(cfg, phase, "scans/s", kCounts * cfg.threads, [&](uint64_t, ThreadResult& r) {
    rocksdb::ReadOptions ro;
    for (uint64_t c = 0; c < kCounts; ++c) {
      std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(ro));
      for (it->SeekToFirst(); it->Valid(); it->Next()) ++r.check;
      if (!ok(r, it->status(), "count")) return;
    }
  });
}

void read_phases(const Config& cfg, rocksdb::DB* db) {
  point_reads(cfg, db, "random-reads-first", 10);
  point_reads(cfg, db, "random-reads-warm", 11);
  seeks(cfg, db, "random-seeks-first", 12);
  seeks(cfg, db, "random-seeks-warm", 13);
  full_scan(cfg, db, "forward-scan", /*reverse=*/false);
  full_scan(cfg, db, "reverse-scan", /*reverse=*/true);
  short_scans(cfg, db, "scan-100", 16, /*skip=*/0);
  short_scans(cfg, db, "skip-5000-scan-100", 17, /*skip=*/kSkip);
  counts(cfg, db, "count-all");
}

void mixed_phase(const Config& cfg, rocksdb::DB* db) {
  const uint64_t p = 20;
  run_phase(cfg, "read-write-90-10", "ops/s", kReadOps * cfg.threads, [&](uint64_t t, ThreadResult& r) {
    SplitMix64 rng = thread_stream(p, t);
    rocksdb::ReadOptions ro;
    rocksdb::WriteOptions wo;
    rocksdb::ColumnFamilyHandle* cf = db->DefaultColumnFamily();
    rocksdb::PinnableSlice value;
    for (uint64_t op = 0; op < kReadOps; ++op) {
      if (rng.next() % 100 < 90) {
        const Key key(rng.next() % kN);
        rocksdb::Status s = db->Get(ro, cf, key.slice(), &value);
        if (s.ok()) {
          ++r.check;
          r.sink += value.size();
        } else if (!s.IsNotFound()) {
          ok(r, s, "get");
          return;
        }
        value.Reset();
      } else {
        const uint64_t i = rng.next() % kN;
        if (!ok(r, db->Put(wo, Key(i).slice(), Value(i, 1).slice()), "put")) return;
        ++r.check;
      }
    }
  });
}

// ---------------------------------------------------------------------------------------------
// Main

uint64_t resident_bytes() {
  proc_taskinfo info{};
  const int n = proc_pidinfo(getpid(), PROC_PIDTASKINFO, 0, &info, sizeof(info));
  if (n != static_cast<int>(sizeof(info))) fail("proc_pidinfo(PROC_PIDTASKINFO) failed");
  return info.pti_resident_size;
}

[[noreturn]] void usage(const std::string& error) {
  std::fprintf(stderr,
               "rocksdb_bench: %s\nusage: rocksdb_bench --engine rocksdb --suite "
               "<write|randomfill|sync|read-memtable|read-sst|mixed> --threads <T> --dir <path>\n",
               error.c_str());
  std::exit(2);
}

Config parse(int argc, char** argv) {
  Config cfg;
  std::string engine;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    std::string value;
    if (const auto eq = arg.find('='); arg.rfind("--", 0) == 0 && eq != std::string::npos) {
      value = arg.substr(eq + 1);
      arg = arg.substr(0, eq);
    } else {
      if (i + 1 >= argc) usage("missing value for " + arg);
      value = argv[++i];
    }
    if (arg == "--engine") {
      engine = value;
    } else if (arg == "--suite") {
      cfg.suite = value;
    } else if (arg == "--threads") {
      char* end = nullptr;
      const unsigned long long t = std::strtoull(value.c_str(), &end, 10);
      if (end == value.c_str() || *end != '\0' || t == 0) usage("invalid --threads " + value);
      cfg.threads = t;
    } else if (arg == "--dir") {
      cfg.dir = value;
    } else {
      usage("unknown argument " + arg);
    }
  }
  if (engine != "rocksdb") usage("--engine must be rocksdb");
  if (cfg.suite.empty()) usage("missing --suite");
  if (cfg.threads == 0) usage("missing --threads");
  if (cfg.dir.empty()) usage("missing --dir");
  return cfg;
}

}  // namespace

int main(int argc, char** argv) {
  const Config cfg = parse(argc, argv);
  const std::string& suite = cfg.suite;
  if (suite != "write" && suite != "randomfill" && suite != "sync" && suite != "read-memtable" &&
      suite != "read-sst" && suite != "mixed") {
    usage("unknown suite " + suite);
  }

  std::unique_ptr<rocksdb::DB> owner = open_db(cfg.dir);
  rocksdb::DB* db = owner.get();

  if (suite == "write") {
    const auto perm2 = permutation(2);
    const auto perm3 = permutation(3);
    write_phase(cfg, db, "sequential-writes", kN, nullptr, /*remove=*/false, 0, /*sync=*/false);
    write_phase(cfg, db, "overwrites", kN, &perm2, /*remove=*/false, 1, /*sync=*/false);
    write_phase(cfg, db, "random-deletes", kN, &perm3, /*remove=*/true, 0, /*sync=*/false);
  } else if (suite == "randomfill") {
    const auto perm4 = permutation(4);
    write_phase(cfg, db, "random-writes", kN, &perm4, /*remove=*/false, 0, /*sync=*/false);
  } else if (suite == "sync") {
    write_phase(cfg, db, "sync-writes", kSyncKeys, nullptr, /*remove=*/false, 0, /*sync=*/true);
  } else if (suite == "read-memtable") {
    load(db);
    read_phases(cfg, db);
  } else if (suite == "read-sst") {
    load(db);
    rocksdb::FlushOptions flush;
    flush.wait = true;
    check_status(db->Flush(flush), "flush");
    check_status(db->CompactRange(rocksdb::CompactRangeOptions(), nullptr, nullptr), "compact range");
    read_phases(cfg, db);
  } else {
    load(db);
    mixed_phase(cfg, db);
  }

  const uint64_t rss = resident_bytes();
  check_status(db->Close(), "close");
  owner.reset();

  rusage usage_info{};
  if (getrusage(RUSAGE_SELF, &usage_info) != 0) fail("getrusage failed");
  const uint64_t peak = static_cast<uint64_t>(usage_info.ru_maxrss);  // bytes on macOS

  std::printf(
      "{\"type\":\"memory\",\"engine\":\"rocksdb\",\"suite\":\"%s\",\"threads\":%llu,\"rss_bytes\":%llu,"
      "\"peak_rss_bytes\":%llu}\n",
      suite.c_str(), static_cast<unsigned long long>(cfg.threads), static_cast<unsigned long long>(rss),
      static_cast<unsigned long long>(peak));
  std::fflush(stdout);
  return 0;
}
