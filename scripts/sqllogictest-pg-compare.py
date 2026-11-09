#!/usr/bin/env python3
"""Compares squad's sqllogictest verdicts with PostgreSQL's.

The public sqllogictest corpus was written for SQLite, so some of its
records cannot pass on a PostgreSQL-compatible engine. This script replays
corpus files against a real PostgreSQL (through psql) and, for every
record, checks whether PostgreSQL meets the recorded expectation. It then
reads squad's verdicts for the same records (from
`squad-sqllogictest --verdicts FILE`) and reports where the two disagree:
those records are the ones that matter for compatibility.

Usage:
    squad-sqllogictest --verdicts verdicts.tsv FILES...
    scripts/sqllogictest-pg-compare.py verdicts.tsv FILES...

PostgreSQL is reached through PGHOST / PGPORT / PGUSER and must use the C
locale. A scratch database `slt_compare_<pid>` is created and dropped.
"""
import hashlib
import re
import subprocess
import sys

SEP = "\x1f"
NULL = "\x01N\x01"
DB = f"slt_compare_{__import__("os").getpid()}"
ENGINE = "postgresql"


def parse(path):
    """Yields (line, kind, header words, sql, expected lines) per record."""
    lines = open(path, errors="replace").read().split("\n")
    i = 0
    skip = False
    out = []
    while i < len(lines):
        line = re.sub(r"\s+#.*$", "", lines[i]) if lines[i].startswith(("skipif", "onlyif")) else lines[i]
        words = line.split()
        if not words or line.startswith("#"):
            i += 1
            continue
        if words[0] == "skipif":
            skip |= words[1] == ENGINE
            i += 1
        elif words[0] == "onlyif":
            skip |= words[1] != ENGINE
            i += 1
        elif words[0] == "halt":
            if not skip:
                break
            skip = False
            i += 1
        elif words[0] == "hash-threshold":
            out.append((i + 1, "threshold", words, "", []))
            i += 1
        elif words[0] in ("statement", "query"):
            start = i + 1
            i += 1
            sql = []
            while i < len(lines) and lines[i].strip() != "" and lines[i] != "----":
                sql.append(lines[i])
                i += 1
            expected = []
            if i < len(lines) and lines[i] == "----":
                i += 1
                while i < len(lines) and lines[i].strip() != "":
                    expected.append(lines[i])
                    i += 1
            if not skip:
                out.append((start, words[0], words, "\n".join(sql), expected))
            else:
                out.append((start, "skipped", words, "", []))
            skip = False
        else:
            i += 1
    return out


def atoi(s):
    m = re.match(r"\s*[-+]?\d+", s)
    return int(m.group(0)) if m else 0


def render(value, ty):
    if value == NULL:
        return "NULL"
    if ty == "I":
        try:
            return str(int(float(value)))
        except ValueError:
            if value in ("t", "f"):
                return "1" if value == "t" else "0"
            return str(atoi(value))
    if ty == "R":
        try:
            return "%.3f" % float(value)
        except ValueError:
            return "0.000"
    if value == "":
        return "(empty)"
    return "".join(c if " " <= c <= "~" else "@" for c in value)


def postgres_verdicts(path):
    records = parse(path)
    script = []
    runnable = [r for r in records if r[1] in ("statement", "query")]
    for n, r in enumerate(runnable):
        script.append(f"\\echo @@@{n}")
        script.append(r[3].rstrip().rstrip(";") + ";")
    script.append(f"\\echo @@@{len(runnable)}")
    admin = ["psql", "-X", "-q", "-d", "postgres"]
    subprocess.run(admin + ["-c", f"DROP DATABASE IF EXISTS {DB}", "-c", f"CREATE DATABASE {DB} TEMPLATE template0"], capture_output=True)
    merged = subprocess.run(
        ["psql", "-X", "-q", "-A", "-t", "-F", SEP, "-P", f"null={NULL}", "-v", "VERBOSITY=sqlstate", "-d", DB, "-f", "-"],
        input="\n".join(script) + "\n",
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
    ).stdout
    subprocess.run(admin + ["-c", f"DROP DATABASE IF EXISTS {DB}"], capture_output=True)
    segments = re.split(r"^@@@\d+\n", merged, flags=re.M)[1:]
    verdicts = {}
    threshold = 8
    k = 0
    for line, kind, words, sql, expected in records:
        if kind == "threshold":
            threshold = int(words[1])
            continue
        if kind == "skipped":
            verdicts[line] = "S"
            continue
        seg = segments[k].rstrip("\n") if k < len(segments) else ""
        k += 1
        rows = seg.split("\n") if seg != "" else []
        failed = any(re.search(r"ERROR:\s+\w{5}", l) for l in rows)
        if kind == "statement":
            want_error = len(words) > 1 and words[1] == "error"
            verdicts[line] = "P" if failed == want_error else "F"
            continue
        if len(words) > 1 and words[1] == "error":
            verdicts[line] = "P" if failed else "F"
            continue
        if failed:
            verdicts[line] = "F"
            continue
        types = words[1] if len(words) > 1 else ""
        sort = words[2] if len(words) > 2 else "nosort"
        table = []
        ok = True
        for l in rows:
            cells = l.split(SEP)
            if len(cells) != len(types):
                ok = False
                break
            table.append([render(v, types[i]) for i, v in enumerate(cells)])
        if not ok:
            verdicts[line] = "F"
            continue
        if sort == "rowsort":
            table.sort()
        values = [v for row in table for v in row]
        if sort == "valuesort":
            values.sort()
        if threshold and len(values) > threshold:
            digest = hashlib.md5("".join(v + "\n" for v in values).encode()).hexdigest()
            actual = [f"{len(values)} values hashing to {digest}"]
        else:
            actual = values
        norm = lambda s: " ".join(s.split())
        want = [norm(e) for e in expected]
        by_row = [norm(" ".join(row)) for row in table] if not (threshold and len(values) > threshold) else None
        verdicts[line] = "P" if [norm(a) for a in actual] == want or by_row == want else "F"
    return verdicts, {r[0]: r[3] for r in records}


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    squad = {}
    for l in open(sys.argv[1]):
        f, line, v = l.rstrip("\n").split("\t")
        squad[(f, int(line))] = v
    totals = {"both pass": 0, "both fail": 0, "only squad fails": 0, "only postgres fails": 0}
    examples = []
    for path in sys.argv[2:]:
        pg, sqls = postgres_verdicts(path)
        for line, v in pg.items():
            s = squad.get((path, line))
            if s is None or v == "S" or s == "S":
                continue
            key = {("P", "P"): "both pass", ("F", "F"): "both fail", ("P", "F"): "only squad fails", ("F", "P"): "only postgres fails"}[(v, s)]
            totals[key] += 1
            if v != s and len(examples) < 40:
                examples.append(f"{path}:{line} postgres={v} squad={s}\n    {sqls[line][:300]}")
        print(f"{path}: done", file=sys.stderr)
    n = sum(totals.values())
    for k, v in totals.items():
        print(f"{k:<22} {v:>9}  {v * 100.0 / max(n, 1):6.2f}%")
    agree = totals["both pass"] + totals["both fail"]
    print(f"{'agreement':<22} {agree:>9}  {agree * 100.0 / max(n, 1):6.2f}%")
    for e in examples:
        print(e)


if __name__ == "__main__":
    main()
