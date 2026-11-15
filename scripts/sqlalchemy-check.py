#!/usr/bin/env python3
"""Compares SQLAlchemy's schema introspection of squad with PostgreSQL's.

Creates the same tables in both databases, runs the inspector calls that
Apache Superset and other SQLAlchemy applications use, and reports every
difference. Not part of `cargo test`; it needs SQLAlchemy (1.4 or 2.x) and
psycopg2:

    python3 -m venv /tmp/sa && /tmp/sa/bin/pip install sqlalchemy psycopg2-binary
    squad serve --memory --listen 127.0.0.1:5491 &
    /tmp/sa/bin/python scripts/sqlalchemy-check.py \\
        postgresql://squad@127.0.0.1:5491/squad \\
        postgresql://postgres@127.0.0.1:5432/postgres

With only the squad URL, the results are printed instead of compared.
Exit status 0 means no difference.
"""

import json
import sys

import sqlalchemy
from sqlalchemy import create_engine, inspect, text

SCHEMA = [
    "DROP VIEW IF EXISTS sa_check_view",
    "DROP TABLE IF EXISTS sa_check_child",
    "DROP TABLE IF EXISTS sa_check_parent",
    """CREATE TABLE sa_check_parent (
        id integer PRIMARY KEY,
        code varchar(20) NOT NULL UNIQUE,
        region text,
        created timestamptz DEFAULT now(),
        score numeric(10, 2),
        flags smallint[],
        CONSTRAINT sa_check_parent_score CHECK (score >= 0)
    )""",
    """CREATE TABLE sa_check_child (
        parent_id integer NOT NULL REFERENCES sa_check_parent (id) ON DELETE CASCADE,
        seq bigint NOT NULL,
        a int, b text, c date, d boolean,
        payload jsonb,
        PRIMARY KEY (parent_id, seq),
        CONSTRAINT sa_check_child_ab UNIQUE (a, b)
    )""",
    "CREATE INDEX sa_check_child_c_a ON sa_check_child (c, a)",
    "CREATE UNIQUE INDEX sa_check_child_d_b_c ON sa_check_child (d, b, c)",
    "CREATE INDEX sa_check_parent_region ON sa_check_parent (region)",
    "CREATE VIEW sa_check_view AS SELECT id, code FROM sa_check_parent",
]

TABLES = ["sa_check_parent", "sa_check_child"]


def normalize(value):
    """Makes inspector results comparable and printable."""
    if isinstance(value, dict):
        return {k: normalize(v) for k, v in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [normalize(v) for v in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    # Types and other objects: their repr names them well enough.
    return repr(value)


def introspect(url):
    engine = create_engine(url)
    with engine.begin() as conn:
        for stmt in SCHEMA:
            conn.execute(text(stmt))
    insp = inspect(engine)
    out = {
        "tables": sorted(t for t in insp.get_table_names() if t.startswith("sa_check_")),
        "views": sorted(v for v in insp.get_view_names() if v.startswith("sa_check_")),
        "view_columns": insp.get_columns("sa_check_view"),
    }
    for t in TABLES:
        columns = insp.get_columns(t)
        for c in columns:
            # The default of `created` is a function call whose spelling
            # is not under test; the column name and type are.
            c.pop("comment", None)
        out[t] = {
            "columns": columns,
            "pk": insp.get_pk_constraint(t),
            "fks": insp.get_foreign_keys(t),
            "indexes": sorted(insp.get_indexes(t), key=lambda i: i["name"]),
            "unique": sorted(insp.get_unique_constraints(t), key=lambda u: u["name"]),
            "checks": sorted(insp.get_check_constraints(t), key=lambda c: c["name"]),
        }
    with engine.begin() as conn:
        for stmt in SCHEMA[:3]:
            conn.execute(text(stmt))
    engine.dispose()
    return normalize(out)


def diff(a, b, path=""):
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            yield from diff(a.get(k), b.get(k), f"{path}.{k}")
    elif isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        for i, (x, y) in enumerate(zip(a, b)):
            yield from diff(x, y, f"{path}[{i}]")
    elif a != b:
        yield f"{path}: squad {json.dumps(a)} != postgres {json.dumps(b)}"


def main():
    if len(sys.argv) not in (2, 3):
        print(__doc__)
        return 2
    print(f"SQLAlchemy {sqlalchemy.__version__}")
    squad = introspect(sys.argv[1])
    if len(sys.argv) == 2:
        print(json.dumps(squad, indent=2))
        return 0
    postgres = introspect(sys.argv[2])
    differences = list(diff(squad, postgres))
    for d in differences:
        print(d)
    print(f"{len(differences)} differences")
    return 1 if differences else 0


if __name__ == "__main__":
    sys.exit(main())
