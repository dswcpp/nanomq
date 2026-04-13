#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SESSIONS=(nanomq nanomq-taos)
BIN_CANDIDATES=(
    "$ROOT_DIR/build-deb-taos/nanomq/nanomq"
    "$ROOT_DIR/build-deb-notests/nanomq/nanomq"
    "$ROOT_DIR/build/nanomq/nanomq"
)

for session in "${SESSIONS[@]}"; do
    if tmux has-session -t "$session" 2>/dev/null; then
        tmux kill-session -t "$session"
    fi
done

for bin in "${BIN_CANDIDATES[@]}"; do
    if [[ -x "$bin" ]]; then
        "$bin" stop >/dev/null 2>&1 || true
    fi
done

mapfile -t pids < <(pgrep -f "$ROOT_DIR/.*/nanomq/nanomq start --conf " || true)
if ((${#pids[@]} > 0)); then
    kill -TERM "${pids[@]}" || true
    sleep 1
fi

mapfile -t remaining < <(pgrep -f "$ROOT_DIR/.*/nanomq/nanomq start --conf " || true)
if ((${#remaining[@]} > 0)); then
    echo "NanoMQ is still running: ${remaining[*]}" >&2
    exit 1
fi

echo "NanoMQ stopped."
