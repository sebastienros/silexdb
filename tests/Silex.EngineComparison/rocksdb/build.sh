#!/usr/bin/env bash
# Builds the RocksDB adapter against a RocksDB 11.4.0 source checkout on which `make static_lib` was run.
#   ROCKSDB_DIR=~/github/rocksdb tests/Silex.EngineComparison/rocksdb/build.sh
set -euo pipefail

ROCKSDB_DIR="${ROCKSDB_DIR:-$HOME/github/rocksdb}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

version_h="$ROCKSDB_DIR/include/rocksdb/version.h"
lib="$ROCKSDB_DIR/librocksdb.a"
[[ -f "$version_h" ]] || { echo "build.sh: $version_h not found (set ROCKSDB_DIR)" >&2; exit 1; }
[[ -f "$lib" ]] || { echo "build.sh: $lib not found (run 'make static_lib' in $ROCKSDB_DIR)" >&2; exit 1; }

version="$(awk '/#define ROCKSDB_(MAJOR|MINOR|PATCH) /{v = v (v ? "." : "") $3} END{print v}' "$version_h")"
if [[ "$version" != "11.4.0" ]]; then
  echo "build.sh: RocksDB $version found in $ROCKSDB_DIR, 11.4.0 required" >&2
  exit 1
fi

mkdir -p "$HERE/build"
set -x
clang++ -O2 -std=c++20 -DNDEBUG \
  -I"$ROCKSDB_DIR/include" \
  "$HERE/rocksdb_bench.cc" \
  "$lib" \
  -lz -lbz2 -lpthread \
  -o "$HERE/build/rocksdb_bench"
