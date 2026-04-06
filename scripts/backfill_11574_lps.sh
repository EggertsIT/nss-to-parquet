#!/usr/bin/env bash
set -euo pipefail

BIN="${BIN:-./target/release/nss-ingestor}"
CONFIG="${CONFIG:-/etc/nss-ingestor/config.toml}"
TOTAL_ROWS="${TOTAL_ROWS:-1000000}"
DAYS="${DAYS:-1}"
WORKERS="${WORKERS:-16}"
SEED="${SEED:-20260406}"
TARGET_LPS="${TARGET_LPS:-11574}"
PROGRESS_EVERY="${PROGRESS_EVERY:-100000}"
USER_COUNT="${USER_COUNT:-42000}"
MIN_DEVICES_PER_USER="${MIN_DEVICES_PER_USER:-2}"
MAX_DEVICES_PER_USER="${MAX_DEVICES_PER_USER:-4}"

if [[ ! -x "$BIN" ]]; then
  echo "ERROR: binary not found or not executable: $BIN" >&2
  echo "Build first: cargo build --release" >&2
  exit 1
fi

exec "$BIN" backfill-direct \
  --config "$CONFIG" \
  --total-rows "$TOTAL_ROWS" \
  --days "$DAYS" \
  --workers "$WORKERS" \
  --seed "$SEED" \
  --target-lps "$TARGET_LPS" \
  --progress-every "$PROGRESS_EVERY" \
  --user-count "$USER_COUNT" \
  --min-devices-per-user "$MIN_DEVICES_PER_USER" \
  --max-devices-per-user "$MAX_DEVICES_PER_USER"
