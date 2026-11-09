#!/usr/bin/env python3
"""Fills in the expected results of the .slt files from a real PostgreSQL.

Every record of every given file (default: slt/*.slt) is run through psql
against a scratch database. Statements become `statement ok` or
`statement error (SQLSTATE)`, queries get their result rows (or
`query error (SQLSTATE)`), rendered exactly as the squad runner renders
them. Existing expectations are discarded, so a file only needs the record
headers and the SQL:

    statement
    CREATE TABLE t (a int)

    query IT rowsort
    SELECT a, a::text FROM t

Connection settings come from the usual PGHOST / PGPORT / PGUSER
variables. The server must use the C locale.

Usage: ./generate.py [file.slt ...]
"""
import glob
import os
import re
import subprocess
import sys

SEP = "\x1f"
NULL = "\x01NULL\x01"
DB = "slt_generate"


def parse(path):
    """Splits a file into records: (kind, header words, sql) or ("raw", text)."""
    records = []
    lines = open(path).read().split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        word = line.split(" ")[0] if line else ""
        if word in ("statement", "query"):
            header = line.split()
            i += 1
            sql = []
            while i < len(lines) and lines[i] != "" and lines[i] != "----":
                sql.append(lines[i])
                i += 1
            # Skip old results.
            if i < len(lines) and lines[i] == "----":
                while i < len(lines) and lines[i] != "":
                    i += 1
            records.append((word, header, "\n".join(sql)))
        elif line.startswith("#") or line == "":
            records.append(("raw", line))
            i += 1
        else:
            records.append(("raw", line))
            i += 1
    return records


def psql(args, **kw):
    return subprocess.run(["psql", "-X", "-q"] + args, capture_output=True, text=True, **kw)


def run(records):
    script = []
    n = 0
    for r in records:
        if r[0] == "raw":
            continue
        script.append(f"\\echo @@@{n}")
        sql = r[2].rstrip().rstrip(";")
        script.append(sql + ";")
        n += 1
    script.append(f"\\echo @@@{n}")
    # stderr is merged into stdout so errors land in the right segment.
    psql(["-d", "postgres", "-c", f"DROP DATABASE IF EXISTS {DB}", "-c", f"CREATE DATABASE {DB} TEMPLATE template0"])
    merged = subprocess.run(
        ["psql", "-X", "-q", "-A", "-t", "-F", SEP, "-P", f"null={NULL}", "-v", "VERBOSITY=sqlstate", "-d", DB, "-f", "-"],
        input="\n".join(script) + "\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    ).stdout
    psql(["-d", "postgres", "-c", f"DROP DATABASE IF EXISTS {DB}"])
    segments = re.split(r"^@@@\d+\n", merged, flags=re.M)[1:]
    return [s.rstrip("\n").split("\n") if s.strip("\n") else [] for s in segments]


def render(value, ty):
    if value == NULL:
        return "NULL"
    if ty == "I":
        return str(int(float(value)))
    if ty == "R":
        return "%.3f" % float(value)
    if value == "":
        return "(empty)"
    return "".join(c if " " <= c <= "~" else "@" for c in value)


def generate(path):
    records = parse(path)
    results = run(records)
    out = []
    k = 0
    for r in records:
        if r[0] == "raw":
            out.append(r[1])
            continue
        kind, header, sql = r
        lines = results[k]
        k += 1
        error = next((re.search(r"ERROR:\s+(\w{5})", l).group(1) for l in lines if re.search(r"ERROR:\s+\w{5}", l)), None)
        if kind == "statement":
            out.append(f"statement error ({error})" if error else "statement ok")
            out.append(sql)
        else:
            types = header[1] if len(header) > 1 and header[1] not in ("error", "ok") else "T"
            sort = [w for w in header[2:] if w in ("rowsort", "valuesort", "nosort")]
            if error:
                out.append(f"query error ({error})")
                out.append(sql)
            else:
                rows = [[render(v, types[i] if i < len(types) else "T") for i, v in enumerate(l.split(SEP))] for l in lines]
                for row in rows:
                    if len(row) != len(types):
                        sys.exit(f"{path}: query returns {len(row)} columns but is declared '{types}':\n{sql}")
                if "rowsort" in sort:
                    rows.sort()
                out.append(" ".join(["query", types] + sort))
                out.append(sql)
                out.append("----")
                out.extend(" ".join(row) for row in rows)
        out.append("")
    text = "\n".join(out)
    text = re.sub(r"\n{3,}", "\n\n", text).rstrip("\n") + "\n"
    open(path, "w").write(text)
    print(f"{path}: {k} records")


if __name__ == "__main__":
    here = os.path.dirname(os.path.abspath(__file__))
    files = sys.argv[1:] or sorted(glob.glob(os.path.join(here, "slt", "*.slt")))
    for f in files:
        generate(f)
