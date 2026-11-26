#!/usr/bin/env bash
# Region coverage of src/ from all test binaries (integration tests included).
# Needs llvm-profdata and llvm-cov matching rustc's LLVM; point LLVM_BIN at their directory.
set -euo pipefail

LLVM_BIN=${LLVM_BIN:-$(dirname "$(command -v llvm-cov)")}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-target/cov}
export RUSTFLAGS="-C instrument-coverage"
export LLVM_PROFILE_FILE="$CARGO_TARGET_DIR/prof/%p-%m.profraw"
rm -rf "$CARGO_TARGET_DIR/prof"

cargo test --no-fail-fast >/dev/null 2>&1 || echo "note: some tests failed under instrumentation" >&2
objects=$(cargo test --no-run --message-format=json 2>/dev/null \
  | python3 -c '
import json, sys
paths = set()
for line in sys.stdin:
    try:
        m = json.loads(line)
    except ValueError:
        continue
    if m.get("reason") == "compiler-artifact" and m.get("executable"):
        paths.add(m["executable"])
print(" ".join("-object " + p for p in sorted(paths)))
')
"$LLVM_BIN/llvm-profdata" merge -sparse "$CARGO_TARGET_DIR"/prof/*.profraw -o "$CARGO_TARGET_DIR/all.profdata"
# shellcheck disable=SC2086
"$LLVM_BIN/llvm-cov" report $objects -instr-profile="$CARGO_TARGET_DIR/all.profdata" \
  --ignore-filename-regex='/.cargo/|/rustc/|/tests/|/benches/|src/bin/'
