#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGINS_DIR="${SCRIPT_DIR}/../plugins/wasm"

mkdir -p "${PLUGINS_DIR}"

echo "Building slow-request-filter..."

cd "${SCRIPT_DIR}/slow-request-filter"

if ! command -v cargo &> /dev/null; then
    echo "ERROR: Rust/Cargo not installed. Install from https://rustup.rs"
    exit 1
fi

if ! rustup target list | grep -q "wasm32-unknown-unknown"; then
    echo "Adding wasm32-unknown-unknown target..."
    rustup target add wasm32-unknown-unknown
fi

cargo build --release --target wasm32-unknown-unknown

cp target/wasm32-unknown-unknown/release/slow_request_filter.wasm "${PLUGINS_DIR}/slow-request-filter.wasm"

echo "Plugin built: ${PLUGINS_DIR}/slow-request-filter.wasm"
ls -lh "${PLUGINS_DIR}/"

echo "Done."
