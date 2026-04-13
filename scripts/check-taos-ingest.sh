#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TAOS_URL="http://127.0.0.1:6041/rest/sql/mqtt_rule"
TAOS_AUTH="${TAOS_AUTH:-root:taosdata}"
STABLES=(
    weld_env_point
    weld_flow_point
    weld_current_raw
    weld_voltage_raw
)

query() {
    curl -sS -m 5 -u "$TAOS_AUTH" "$TAOS_URL" -d "$1"
}

extract_count() {
    sed -n 's/.*"data":\[\[\([^]]*\)\]\].*/\1/p'
}

extract_latest() {
    sed -n 's/.*"data":\[\["\([^"]*\)","\([^"]*\)",\("[^"]*"\|null\)\]\].*/\1\t\2\t\3/p'
}

extract_latest_raw() {
    sed -n 's/.*"data":\[\["\([^"]*\)","\([^"]*\)","\([^"]*\)",\("[^"]*"\|null\),\([^]]*\),"\([^"]*\)"\]\].*/\1\t\2\t\3\t\4\t\5\t\6/p'
}

echo "NanoMQ process:"
pgrep -a nanomq || echo "  not running"
echo

echo "TDengine listener:"
ss -ltnp '( sport = :6041 )' 2>/dev/null || true
echo

echo "TDengine stables:"
query "show stables"
echo

for stable in "${STABLES[@]}"; do
    count_resp=$(query "select count(*) from $stable")
    count=$(printf '%s\n' "$count_resp" | extract_count)
    if [[ "$stable" == "weld_current_raw" || "$stable" == "weld_voltage_raw" ]]; then
        latest_resp=$(query "select ts, recv_ts, topic_name, task_id, point_count, encoding from $stable order by ts desc limit 1")
        latest=$(printf '%s\n' "$latest_resp" | extract_latest_raw)
    else
        latest_resp=$(query "select ts, topic_name, task_id from $stable order by ts desc limit 1")
        latest=$(printf '%s\n' "$latest_resp" | extract_latest)
    fi
    if [[ -z "$count" ]]; then
        count="unknown"
    fi
    if [[ -z "$latest" ]]; then
        latest="unknown"
    fi
    printf '%-20s count=%s latest=%s\n' "$stable" "$count" "$latest"
done
