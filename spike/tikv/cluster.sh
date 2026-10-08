#!/usr/bin/env bash
# Starts or stops the spike's TiKV cluster: one PD and one or three TiKV nodes.
#
#   ./cluster.sh up [1|3]   start PD and N TiKV nodes (default 1)
#   ./cluster.sh down       stop and remove them, and their data
#   ./cluster.sh kill N     kill TiKV node N (1-based) without a clean stop
#   ./cluster.sh start N    start a killed TiKV node N again
#
# Host networking, so the addresses PD hands out (127.0.0.1:2016x) are the ones
# the test client dials. That works on Linux only; on macOS run it inside a
# Linux VM.
set -euo pipefail

VERSION="${TIKV_VERSION:-v8.5.8}"
RUNTIME="${CONTAINER_RUNTIME:-$(command -v docker || command -v podman)}"
# On disk, not under /tmp: /tmp is a RAM disk on many Linux hosts, which hides
# the cost of every commit's sync.
DATA="${SPIKE_DATA:-$HOME/.cache/dittofs-tikv-spike}"

pd_up() {
    mkdir -p "$DATA/pd"
    "$RUNTIME" run -d --name spike-pd --network host \
        -v "$DATA/pd:/data" "docker.io/pingcap/pd:$VERSION" \
        --name=pd --data-dir=/data \
        --client-urls=http://127.0.0.1:2379 --peer-urls=http://127.0.0.1:2380 \
        --initial-cluster=pd=http://127.0.0.1:2380 >/dev/null
}

tikv_up() {
    local n=$1 port=$((20159 + $1)) status=$((20179 + $1))
    mkdir -p "$DATA/tikv$n"
    "$RUNTIME" run -d --name "spike-tikv$n" --network host \
        -v "$DATA/tikv$n:/data" "docker.io/pingcap/tikv:$VERSION" \
        --addr="127.0.0.1:$port" --status-addr="127.0.0.1:$status" \
        --pd=127.0.0.1:2379 --data-dir=/data >/dev/null
}

wait_ready() {
    local want=$1
    for _ in $(seq 1 60); do
        local up
        up=$(curl -fsS http://127.0.0.1:2379/pd/api/v1/stores 2>/dev/null |
            grep -c '"state_name": "Up"' || true)
        if [[ "$up" -ge "$want" ]]; then
            echo "PD and $up TiKV node(s) up"
            return 0
        fi
        sleep 2
    done
    echo "cluster not ready after 120s" >&2
    "$RUNTIME" logs --tail 30 spike-tikv1 >&2 || true
    return 1
}

# Created here, as the user: a bind mount of a missing directory would have the
# container runtime create it as root, and nothing after could write to it.
mkdir -p "$DATA"

case "${1:-}" in
up)
    n="${2:-1}"
    pd_up
    for i in $(seq 1 "$n"); do tikv_up "$i"; done
    wait_ready "$n"
    ;;
down)
    for c in spike-pd spike-tikv1 spike-tikv2 spike-tikv3; do
        "$RUNTIME" rm -f "$c" >/dev/null 2>&1 || true
    done
    # The containers write as root; remove the data from inside one.
    "$RUNTIME" run --rm -v "$DATA:/d" docker.io/library/busybox:1.37 rm -rf /d/pd /d/tikv1 /d/tikv2 /d/tikv3 >/dev/null 2>&1 || true
    ;;
kill)
    "$RUNTIME" kill -s KILL "spike-tikv${2:?node number}" >/dev/null
    ;;
start)
    "$RUNTIME" start "spike-tikv${2:?node number}" >/dev/null
    ;;
*)
    sed -n '2,10p' "$0"
    exit 2
    ;;
esac
