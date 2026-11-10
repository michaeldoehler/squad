#!/usr/bin/env bash
# Starts a single-node Garage (https://garagehq.deuxfleurs.fr) for tests
# against a real S3 implementation, creates a bucket and an access key, and
# prints the environment the S3 integration tests read.
#
# Usage: eval "$(scripts/garage-dev.sh start)"   # exports SQUAD_TEST_S3_URL
#        scripts/garage-dev.sh stop
set -euo pipefail
dir="${GARAGE_DEV_DIR:-${TMPDIR:-/tmp}/squad-garage-dev}"
port_s3="${GARAGE_DEV_PORT:-3900}"
cmd="${1:-start}"

garage_cli() { garage -c "$dir/garage.toml" "$@"; }

case "$cmd" in
start)
    mkdir -p "$dir/meta" "$dir/data"
    if [ ! -f "$dir/garage.toml" ]; then
        cat > "$dir/garage.toml" <<CFG
metadata_dir = "$dir/meta"
data_dir = "$dir/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "127.0.0.1:$((port_s3 + 1))"
rpc_public_addr = "127.0.0.1:$((port_s3 + 1))"
rpc_secret = "$(openssl rand -hex 32)"
[s3_api]
s3_region = "garage"
api_bind_addr = "127.0.0.1:$port_s3"
root_domain = ".s3.garage.localhost"
[admin]
api_bind_addr = "127.0.0.1:$((port_s3 + 3))"
CFG
    fi
    if ! [ -f "$dir/pid" ] || ! kill -0 "$(cat "$dir/pid")" 2>/dev/null; then
        garage -c "$dir/garage.toml" server > "$dir/log" 2>&1 &
        echo $! > "$dir/pid"
        for _ in $(seq 50); do garage_cli status > /dev/null 2>&1 && break; sleep 0.2; done
    fi
    if [ ! -f "$dir/env" ]; then
        node=$(garage_cli status 2>/dev/null | awk '/^[0-9a-f]{16}/ {print $1; exit}')
        garage_cli layout assign -z dc1 -c 1G "$node" > /dev/null
        garage_cli layout apply --version 1 > /dev/null
        garage_cli bucket create squad-test > /dev/null
        out=$(garage_cli key create squad-test-key)
        key=$(echo "$out" | awk '/Key ID/ {print $NF}')
        secret=$(echo "$out" | awk '/Secret key/ {print $NF}')
        garage_cli bucket allow --read --write --owner squad-test --key squad-test-key > /dev/null
        echo "export SQUAD_TEST_S3_URL='s3://squad-test?endpoint=http://127.0.0.1:$port_s3&region=garage&path_style=true&access_key=$key&secret_key=$secret'" > "$dir/env"
    fi
    cat "$dir/env"
    ;;
stop)
    [ -f "$dir/pid" ] && kill "$(cat "$dir/pid")" 2>/dev/null || true
    rm -f "$dir/pid"
    ;;
*) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
