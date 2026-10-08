#!/usr/bin/env bash
# The spike's FoundationDB: the client library, a cluster, and the tests.
#
#   ./cluster.sh client      download libfdb_c and its headers (outside the repo)
#   ./cluster.sh up [1|3]    start N fdbserver processes (default 1); three run
#                            double replication with three coordinators
#   ./cluster.sh down        stop and remove them, and their data
#   ./cluster.sh kill N      kill process N (1-based) without a clean stop
#   ./cluster.sh start N     start a killed process N again
#   ./cluster.sh test ...    go test with the client library on the cgo paths
#
# Host networking, so the cluster file's 127.0.0.1 addresses are the ones the
# test client dials. That works on Linux only; on macOS run it inside a Linux
# VM. The client library is x86_64 only here.
set -euo pipefail

VERSION="${FDB_VERSION:-7.3.77}"
RUNTIME="${CONTAINER_RUNTIME:-$(command -v docker || command -v podman)}"
DATA="${SPIKE_DATA:-${TMPDIR:-/tmp}/dittofs-fdb-spike}"
HERE="$(cd "$(dirname "$0")" && pwd)"
CLIENT="${FDB_CLIENT_DIR:-${TMPDIR:-/tmp}/dittofs-fdb-client}"
IMAGE="docker.io/foundationdb/foundationdb:$VERSION"

client() {
    local base="https://github.com/apple/foundationdb/releases/download/$VERSION"
    mkdir -p "$CLIENT/lib" "$CLIENT/include/foundationdb"
    curl -fsSL -o "$CLIENT/lib/libfdb_c.so" "$base/libfdb_c.x86_64.so"
    curl -fsSL "$base/fdb-headers-$VERSION.tar.gz" | tar -xz -C "$CLIENT/include/foundationdb"
    # fdb_c.h includes fdb_c_types.h, which the release's header tarball leaves out.
    curl -fsSL -o "$CLIENT/include/foundationdb/fdb_c_types.h" \
        "https://raw.githubusercontent.com/apple/foundationdb/$VERSION/bindings/c/foundationdb/fdb_c_types.h"
    echo "client $VERSION in $CLIENT"
}

process_up() {
    local n=$1 port=$((4499 + $1))
    mkdir -p "$DATA/p$n"
    "$RUNTIME" run -d --name "spike-fdb$n" --network host \
        -v "$DATA:/cluster" --entrypoint /usr/bin/fdbserver "$IMAGE" \
        --cluster-file /cluster/fdb.cluster --public-address "127.0.0.1:$port" \
        --listen-address "127.0.0.1:$port" --datadir "/cluster/p$n" --logdir "/cluster/p$n" \
        --locality-zoneid "z$n" --locality-machineid "m$n" >/dev/null
}

fdbcli() {
    "$RUNTIME" exec spike-fdb1 /usr/bin/fdbcli -C /cluster/fdb.cluster --exec "$1"
}

up() {
    local n=$1 coords="" mode=single
    for i in $(seq 1 "$n"); do coords+="${coords:+,}127.0.0.1:$((4499 + i))"; done
    # double, not triple: triple places logs in three zones, so three processes
    # cannot lose one. double keeps two durable copies and survives one loss.
    [[ "$n" -ge 3 ]] && mode=double
    mkdir -p "$DATA"
    echo "spike:spike@$coords" >"$DATA/fdb.cluster"
    for i in $(seq 1 "$n"); do process_up "$i"; done
    sleep 2
    fdbcli "configure new $mode ssd" >/dev/null
    for _ in $(seq 1 60); do
        if fdbcli "status minimal" 2>/dev/null | grep -q "available"; then
            echo "FoundationDB $VERSION up: $n process(es), $mode replication"
            return 0
        fi
        sleep 1
    done
    fdbcli "status" >&2 || true
    return 1
}

# Created here, as the user: a bind mount of a missing directory would have the
# container runtime create it as root, and nothing after could write to it.
mkdir -p "$DATA"

case "${1:-}" in
client) client ;;
up) up "${2:-1}" ;;
down)
    for i in 1 2 3; do "$RUNTIME" rm -f "spike-fdb$i" >/dev/null 2>&1 || true; done
    # The processes write as root; remove the data from inside a container.
    "$RUNTIME" run --rm -v "$DATA:/d" docker.io/library/busybox:1.37 sh -c 'rm -rf /d/*' >/dev/null 2>&1 || true
    ;;
kill) "$RUNTIME" kill -s KILL "spike-fdb${2:?process number}" >/dev/null ;;
start) "$RUNTIME" start "spike-fdb${2:?process number}" >/dev/null ;;
test)
    shift
    [[ -f "$CLIENT/lib/libfdb_c.so" ]] || client
    export CGO_CFLAGS="-I$CLIENT/include" CGO_LDFLAGS="-L$CLIENT/lib"
    export LD_LIBRARY_PATH="$CLIENT/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
    export SPIKE_CLUSTER_FILE="${SPIKE_CLUSTER_FILE:-$DATA/fdb.cluster}"
    cd "$HERE" && exec go test "$@"
    ;;
*)
    sed -n '2,13p' "$0"
    exit 2
    ;;
esac
