#!/usr/bin/env bash
# Regenerates the expected output of the SQL regression suite from a real
# PostgreSQL server.
#
# Every crates/squad/tests/regress/*.sql file is run through psql against
# a scratch database, and the normalized output is stored next to it as
# *.out. The Rust test `regress` then requires squad to produce exactly
# the same output.
#
# Usage: PGHOST=127.0.0.1 PGPORT=5432 PGUSER=postgres scripts/regress-expected.sh [name ...]
#
# The server must use the C locale (initdb --locale=C): squad sorts text
# bytewise.
set -euo pipefail
cd "$(dirname "$0")/../crates/squad/tests/regress"

files=("$@")
if [ ${#files[@]} -eq 0 ]; then
    files=(*.sql)
fi
for f in "${files[@]}"; do
    name="${f%.sql}"
    psql -X -q -d postgres -c "DROP DATABASE IF EXISTS squad_regress" -c "CREATE DATABASE squad_regress TEMPLATE template0" >/dev/null
    psql -X -q -a -A -F '|' -P null=NULL -v VERBOSITY=sqlstate -d squad_regress -f "$name.sql" 2>&1 |
        sed -E 's/^psql:[^ ]+: (ERROR|WARNING|NOTICE)/\1/' | grep -Ev '^(NOTICE|WARNING)' >"$name.out" || true
    echo "wrote $name.out"
done
psql -X -q -d postgres -c "DROP DATABASE IF EXISTS squad_regress" >/dev/null
