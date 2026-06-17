#!/usr/bin/env bash
# All Haematite gates in one place. Run from the repository root.
#
# Native gates always run. The WASM build and lint gates always run (they need
# only the wasm32 target). The browser test gate runs when a wasm-bindgen test
# runner and a WebDriver are available, and is otherwise reported as SKIPPED so a
# developer without browser tooling still gets a clear signal rather than a
# silent pass. CI installs the full toolchain so nothing is skipped there.
#
# Mirrors the WASM-001 required gates.
set -euo pipefail

PKG="haematite"
WASM_TARGET="wasm32-unknown-unknown"

step() { printf '\n=== %s ===\n' "$1"; }

step "fmt --check"
cargo fmt --all --check

step "clippy (native, all targets)"
cargo clippy -p "$PKG" --all-targets -- -D warnings

step "test (native)"
cargo test -p "$PKG" --all-targets

step "check (wasm, --features wasm)"
cargo check -p "$PKG" --target "$WASM_TARGET" --features wasm

step "clippy (wasm, all targets)"
cargo clippy -p "$PKG" --target "$WASM_TARGET" --features wasm --all-targets -- -D warnings

step "git diff --check"
git diff --check

step "wasm-bindgen browser tests"
if command -v wasm-bindgen-test-runner >/dev/null 2>&1 \
    && { [ -n "${CHROMEDRIVER:-}" ] || [ -n "${GECKODRIVER:-}" ] || [ -n "${SAFARIDRIVER:-}" ] \
        || command -v chromedriver >/dev/null 2>&1 || command -v geckodriver >/dev/null 2>&1; }; then
    CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER="wasm-bindgen-test-runner" \
        cargo test -p "$PKG" --target "$WASM_TARGET" --features wasm --test wasm
else
    echo "SKIPPED: no wasm-bindgen-test-runner + WebDriver found." >&2
    echo "  Install wasm-bindgen-cli and a driver, then export CHROMEDRIVER/GECKODRIVER." >&2
fi

step "wasm size budget"
bash scripts/wasm-size-check.sh

printf '\nAll gates passed.\n'
