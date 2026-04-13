#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CONF="$ROOT_DIR/etc/nanomq_weld_taos.conf"
SESSION="nanomq-taos"
MODE="${1:---tmux}"
BROKER_PATTERN="$ROOT_DIR/.*/nanomq/nanomq start --conf "

BIN_CANDIDATES=(
    "$ROOT_DIR/build-deb-taos/nanomq/nanomq"
    "$ROOT_DIR/build-deb-notests/nanomq/nanomq"
    "$ROOT_DIR/build/nanomq/nanomq"
)

BIN=""
for candidate in "${BIN_CANDIDATES[@]}"; do
    if [[ -x "$candidate" ]]; then
        BIN="$candidate"
        break
    fi
done

if [[ ! -x "$BIN" ]]; then
    echo "NanoMQ binary not found in known build directories." >&2
    exit 1
fi

if [[ ! -f "$CONF" ]]; then
    echo "TAOS config not found: $CONF" >&2
    exit 1
fi

if pgrep -f "$BIN start --conf $CONF" >/dev/null 2>&1; then
    echo "NanoMQ is already running with TAOS config: $CONF"
    exit 0
fi

if pgrep -f "$BROKER_PATTERN" >/dev/null 2>&1; then
    echo "Stopping existing NanoMQ broker instance before starting TAOS config..."
    bash "$ROOT_DIR/scripts/stop-nanomq.sh"
fi

case "$MODE" in
    --foreground)
        exec "$BIN" start --conf "$CONF"
        ;;
    --tmux)
        if ! command -v tmux >/dev/null 2>&1; then
            echo "tmux is not available; rerun with --foreground." >&2
            exit 1
        fi

        if tmux has-session -t "$SESSION" 2>/dev/null; then
            tmux kill-session -t "$SESSION"
        fi

        printf -v launch_cmd 'cd %q && exec %q start --conf %q' \
            "$ROOT_DIR" "$BIN" "$CONF"
        tmux new-session -d -s "$SESSION" "$launch_cmd"
        sleep 1

        if ! pgrep -f "$BIN start --conf $CONF" >/dev/null 2>&1; then
            echo "NanoMQ failed to start with TAOS config." >&2
            tmux capture-pane -pt "$SESSION" || true
            exit 1
        fi

        echo "NanoMQ started in tmux session: $SESSION"
        echo "Attach with: tmux attach -t $SESSION"
        ss -ltnp '( sport = :1883 or sport = :8083 )' 2>/dev/null || true
        ;;
    *)
        echo "Usage: $0 [--tmux|--foreground]" >&2
        exit 1
        ;;
esac
