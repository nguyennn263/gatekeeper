#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --all -- --check
cargo check --all-targets
cargo test --all --locked
cargo clippy --all-targets -- -D warnings
node --check web/app.js
node --check web/dashboard.js
