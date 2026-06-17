#!/usr/bin/env bash
# WASM-001 R7 / CN3: enforce the <2MB-gzipped WASM binary budget.
#
# Builds the release wasm artifact, applies `wasm-opt -Oz` if available, gzips
# it, and fails if the result exceeds 2 MiB. Run in CI on every change to the
# crate. Falls back to the raw `cargo build` artifact when `wasm-pack`/`wasm-opt`
# are not installed (a conservative over-estimate, since wasm-opt only shrinks).
#
# Last local measurement (raw cargo build, no wasm-opt): 166 KiB gzipped — well
# under budget.
set -euo pipefail

BUDGET=$((2 * 1024 * 1024)) # 2 MiB
CRATE_DIR="crates/haematite"

if command -v wasm-pack >/dev/null 2>&1; then
    wasm-pack build "$CRATE_DIR" --release --target web --features wasm
    WASM=$(ls "$CRATE_DIR"/pkg/*_bg.wasm | head -1)
else
    echo "wasm-pack not found; falling back to raw cargo build (over-estimate)" >&2
    cargo build -p haematite --release --target wasm32-unknown-unknown --features wasm
    WASM="target/wasm32-unknown-unknown/release/haematite.wasm"
fi

if command -v wasm-opt >/dev/null 2>&1; then
    wasm-opt -Oz "$WASM" -o "$WASM.opt"
    WASM="$WASM.opt"
else
    echo "wasm-opt not found; measuring unoptimised artifact (over-estimate)" >&2
fi

GZIPPED=$(gzip -9 -c "$WASM" | wc -c | tr -d ' ')
echo "gzipped wasm size: ${GZIPPED} bytes (budget ${BUDGET})"

if [ "$GZIPPED" -gt "$BUDGET" ]; then
    echo "FAIL: wasm binary exceeds 2 MiB gzipped budget" >&2
    exit 1
fi
echo "OK: within budget"
