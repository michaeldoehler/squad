#!/usr/bin/env bash
# Runs the public sqllogictest corpus (written for SQLite, about 5.7
# million records) against squad.
#
# The corpus is fetched into target/sqllogictest-corpus on first use; it
# is not part of this repository. Results are printed per directory.
#
# Usage: scripts/sqllogictest-corpus.sh [runner options] [subdirectory ...]
#
#   scripts/sqllogictest-corpus.sh                    # everything (~10 min on 4 cores)
#   scripts/sqllogictest-corpus.sh select1.test evidence
#   scripts/sqllogictest-corpus.sh --group-depth 2 index
#
# To see which failures are differences from PostgreSQL rather than
# SQLite-specific expectations, pass `--verdicts FILE` and feed the file
# to scripts/sqllogictest-pg-compare.py.
set -euo pipefail
cd "$(dirname "$0")/.."

corpus=target/sqllogictest-corpus/src
repo="${SQLLOGICTEST_CORPUS_URL:-https://github.com/gregrahn/sqllogictest.git}"
if [ ! -d "$corpus/test" ]; then
    mkdir -p "$(dirname "$corpus")"
    git clone --depth 1 "$repo" "$corpus"
fi
echo "corpus revision: $(git -C "$corpus" rev-parse HEAD)" >&2

options=()
paths=()
while [ $# -gt 0 ]; do
    case "$1" in
        --jobs | --failures | --group-depth | --dump | --verdicts)
            options+=("$1" "$2")
            shift 2
            ;;
        *)
            paths+=("$corpus/test/$1")
            shift
            ;;
    esac
done
if [ ${#paths[@]} -eq 0 ]; then
    paths=("$corpus/test")
fi

cargo build --release -p squad-sqllogictest
# The exit code reports failed records; the summary is what matters here.
target/release/squad-sqllogictest "${options[@]}" "${paths[@]}" || true
