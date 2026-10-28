"""
nih-reporter-harvest, tools -- writes docs/data_model.md from the database's
own catalog: a Mermaid ER diagram of the key and join columns, a data
dictionary of every column of every table and view, and the result of
checking every documented relationship against the data.

Reads data/nih_harvester.db read-only (immutable=1, no locks, nothing
copied): sqlite_master and PRAGMA table_info for what exists, and the data
itself for the checks. tools/relationships.yml has the joins (SQLite has
foreign keys, but this database declares none) and each object's key.
Every object and column it names must be in the catalog, or this exits 2
before writing anything.

Without locks it can't tell when the weekly job is writing, so it stops if
a -journal file or a non-empty -wal file is next to the database. Run it
when the weekly refresh isn't running (its log ends in SUCCESS or FAILED).

THE CHECKS: each key is checked for duplicates and NULLs, and each
relationship for orphans (rows whose join columns have no match on the
other side) and, where the cardinality says a side is unique, for
duplicates there. A relationship that doesn't hold cleanly is documented
with its counts, not dropped; this does not fail the run. About 3 minutes
at ~3M records, most of it one scan of nih_raw_projects for its key.

Regenerate after any change to a table, a view or relationships.yml.
Needs PyYAML (requirements.txt).

Usage:
    .venv/bin/python3 tools/make_data_model.py [--db PATH]
"""

import argparse
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

BASE = Path(__file__).resolve().parent.parent
DB_PATH = BASE / "data" / "nih_harvester.db"
RELATIONSHIPS = BASE / "tools" / "relationships.yml"
OUT = BASE / "docs" / "data_model.md"

CARDINALITIES = {"1:1": "||--||", "N:1": "}o--||", "1:N": "||--o{", "N:N": "}o--o{"}

# Identifiers, codes, sequences and fiscal years are text in the child
# tables. Columns whose names say they are one of those and whose declared
# type is numeric are listed in the model.
ID_LIKE = re.compile(r"(^|_)(id|seq|code|num|fy|fiscal_year)$|^pmid$")
NUMERIC_TYPES = ("INT", "INTEGER", "REAL", "NUMERIC", "FLOAT", "DOUBLE")


# --- Catalog --------------------------------------------------------------------

def connect(path):
    for suffix, what in (("-journal", "a rollback journal"), ("-wal", "a write-ahead log")):
        side = Path(f"{path}{suffix}")
        if side.exists() and side.stat().st_size > 0:
            raise SystemExit(f"{side.name} is next to the database ({what}): the weekly job may be "
                             "writing. Run this once it has finished.")
    return sqlite3.connect(f"file:{path}?mode=ro&immutable=1", uri=True)


def catalog(conn):
    """{name: {"kind", "columns": [(name, declared type)], "reads"}}, in name order."""
    objects = conn.execute("SELECT name, type, sql FROM sqlite_master WHERE type IN ('table', 'view') "
                           "AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()
    names = [n for n, _, _ in objects]
    result = {}
    for name, kind, sql in objects:
        columns = [(c[1], c[2] or "") for c in conn.execute(f'PRAGMA table_info("{name}")')]
        reads = [n for n in names if n != name and kind == "view" and re.search(rf"\b{n}\b", sql or "")]
        result[name] = {"kind": kind, "columns": columns, "reads": reads}
    return result


# --- relationships.yml ----------------------------------------------------------

def parse_ref(ref):
    """'object.col' or 'object.(col1, col2)' -> (object, [col, ...])."""
    m = re.fullmatch(r"\s*(\w+)\.(?:\((.+)\)|(\w+))\s*", ref)
    if not m:
        raise ValueError(f"not object.column or object.(col1, col2): {ref!r}")
    cols = m.group(2).split(",") if m.group(2) else [m.group(3)]
    return m.group(1), [c.strip() for c in cols]


def load_spec(cat):
    spec = yaml.safe_load(RELATIONSHIPS.read_text())
    errors = []

    def check(where, obj, cols):
        if obj not in cat:
            errors.append(f"{where}: no object {obj}")
            return
        have = {c for c, _ in cat[obj]["columns"]}
        errors.extend(f"{where}: {obj} has no column {c}" for c in cols if c not in have)

    keys, key_notes = {}, {}
    for obj, val in (spec.get("keys") or {}).items():
        cols, note = (val.get("columns", []), val.get("note")) if isinstance(val, dict) else (val, None)
        check(f"keys.{obj}", obj, cols)
        keys[obj] = cols
        if note:
            key_notes[obj] = note

    rels = []
    for i, r in enumerate(spec.get("relationships") or []):
        where = f"relationships[{i}]"
        try:
            f_obj, f_cols = parse_ref(r["from"])
            t_obj, t_cols = parse_ref(r["to"])
        except (KeyError, ValueError) as e:
            errors.append(f"{where}: {e}")
            continue
        check(f"{where} from", f_obj, f_cols)
        check(f"{where} to", t_obj, t_cols)
        if len(f_cols) != len(t_cols):
            errors.append(f"{where}: {len(f_cols)} from-column(s) but {len(t_cols)} to-column(s)")
        if r.get("cardinality") not in CARDINALITIES:
            errors.append(f"{where}: cardinality must be one of {', '.join(CARDINALITIES)}")
        if not r.get("meaning"):
            errors.append(f"{where}: no meaning")
        rels.append({**r, "from_obj": f_obj, "from_cols": f_cols, "to_obj": t_obj, "to_cols": t_cols})

    if errors:
        print(f"{RELATIONSHIPS.relative_to(BASE)} doesn't match the catalog:", file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        raise SystemExit(2)
    return keys, key_notes, rels


# --- Checks against the data ----------------------------------------------------
#
# Comparisons rely on SQLite's affinity rules: a text appl_id in a child
# table compared with an INTEGER appl_id column is compared as a number.

def scalar(conn, sql, verbose):
    started = time.monotonic()
    value = conn.execute(sql).fetchone()[0]
    if verbose:
        print(f"    {time.monotonic() - started:6.1f}s  {' '.join(sql.split())[:150]}")
    return value


def check_key(conn, obj, cols, verbose):
    keys = ", ".join(cols)
    nulls = " OR ".join(f"{c} IS NULL" for c in cols)
    return {
        "rows": scalar(conn, f"SELECT count(*) FROM {obj}", verbose),
        "null_keys": scalar(conn, f"SELECT count(*) FROM {obj} WHERE {nulls}", verbose),
        "dup_keys": scalar(conn, f"SELECT count(*) FROM (SELECT {keys} FROM {obj} "
                                 f"GROUP BY {keys} HAVING count(*) > 1)", verbose),
    }


def check_relationship(conn, r, verbose):
    f_obj, t_obj, f_cols, t_cols = r["from_obj"], r["to_obj"], r["from_cols"], r["to_cols"]
    where = f"({r['where']})" if r.get("where") else "1 = 1"
    not_null = " AND ".join(f"f.{c} IS NOT NULL" for c in f_cols)
    join = " AND ".join(f"t.{tc} = f.{fc}" for fc, tc in zip(f_cols, t_cols))
    orphan = f"{where} AND {not_null} AND NOT EXISTS (SELECT 1 FROM {t_obj} t WHERE {join})"
    f_keys = ", ".join(f"f.{c}" for c in f_cols)
    result = {
        "rows": scalar(conn, f"SELECT count(*) FROM {f_obj} f WHERE {where}", verbose),
        "null_rows": scalar(conn, f"SELECT count(*) FROM {f_obj} f WHERE {where} AND NOT ({not_null})", verbose),
        "orphan_rows": scalar(conn, f"SELECT count(*) FROM {f_obj} f WHERE {orphan}", verbose),
        "orphan_keys": scalar(conn, f"SELECT count(*) FROM (SELECT DISTINCT {f_keys} FROM {f_obj} f "
                                    f"WHERE {orphan})", verbose),
    }
    # The side the cardinality says is unique ("1"): check that it is.
    if r["cardinality"] in ("N:1", "1:1"):
        t_keys = ", ".join(t_cols)
        result["to_dup_keys"] = scalar(conn, f"SELECT count(*) FROM (SELECT {t_keys} FROM {t_obj} "
                                             f"GROUP BY {t_keys} HAVING count(*) > 1)", verbose)
    if r["cardinality"] in ("1:1", "1:N"):
        result["from_dup_keys"] = scalar(conn, f"SELECT count(*) FROM (SELECT {f_keys} FROM {f_obj} f "
                                               f"WHERE {where} GROUP BY {f_keys} HAVING count(*) > 1)", verbose)
    result["holds"] = not (result["orphan_rows"] or result.get("to_dup_keys") or result.get("from_dup_keys"))
    return result


# --- Markdown -------------------------------------------------------------------

def mermaid_type(t):
    return re.sub(r"\W", "_", t.split("(")[0].strip()) or "untyped"


def mermaid(cat, keys, rels):
    """erDiagram of every object's key and join columns."""
    join_cols, fk_cols = {}, {}
    for r in rels:
        join_cols.setdefault(r["from_obj"], set()).update(r["from_cols"])
        join_cols.setdefault(r["to_obj"], set()).update(r["to_cols"])
        fk_cols.setdefault(r["from_obj"], set()).update(r["from_cols"])

    lines = ["```mermaid", "erDiagram"]
    for name, obj in cat.items():
        shown = set(keys.get(name, [])) | join_cols.get(name, set())
        label = f"{name} (view)" if obj["kind"] == "view" else name
        lines.append(f'    {name}["{label}"] {{')
        for col, typ in obj["columns"]:
            if col not in shown:
                continue
            marks = (["PK"] if col in keys.get(name, []) else []) + (["FK"] if col in fk_cols.get(name, set()) else [])
            lines.append(f"        {mermaid_type(typ)} {col} {', '.join(marks)}".rstrip())
        lines.append("    }")
    for r in rels:
        label = ", ".join(r["from_cols"])
        if r.get("where"):
            label += " where " + r["where"].replace('"', "").replace("'", "")
        lines.append(f'    {r["from_obj"]} {CARDINALITIES[r["cardinality"]]} {r["to_obj"]} : "{label}"')
    lines.append("```")
    return lines


def fmt(n):
    return f"{n:,}"


def ref(obj, cols):
    return f"`{obj}.{cols[0]}`" if len(cols) == 1 else f"`{obj}.({', '.join(cols)})`"


def relationship_table(rels, results):
    lines = ["| From | To | Card. | Meaning | Rows | NULL join | Orphan rows (keys) | Unique-side dups | Holds |",
             "| --- | --- | --- | --- | ---: | ---: | ---: | ---: | --- |"]
    for r, res in zip(rels, results):
        frm = ref(r["from_obj"], r["from_cols"]) + (f" where `{r['where']}`" if r.get("where") else "")
        checked = "to_dup_keys" in res or "from_dup_keys" in res
        dups = res.get("to_dup_keys", 0) + res.get("from_dup_keys", 0)
        lines.append(f"| {frm} | {ref(r['to_obj'], r['to_cols'])} | {r['cardinality']} | {r['meaning'].strip()} "
                     f"| {fmt(res['rows'])} | {fmt(res['null_rows'])} "
                     f"| {fmt(res['orphan_rows'])} ({fmt(res['orphan_keys'])}) | {fmt(dups) if checked else 'n/a'} "
                     f"| {'yes' if res['holds'] else '**no**'} |")
    noted = [(r, res) for r, res in zip(rels, results) if not res["holds"] or r.get("note")]
    if noted:
        lines += ["", "Notes:", ""]
        for r, res in noted:
            what = "doesn't hold cleanly" if not res["holds"] else "holds"
            counts = f"{fmt(res['orphan_rows'])} orphan rows ({fmt(res['orphan_keys'])} distinct keys)"
            if res.get("to_dup_keys"):
                counts += f", {fmt(res['to_dup_keys'])} duplicate keys on the `to` side"
            if res.get("from_dup_keys"):
                counts += f", {fmt(res['from_dup_keys'])} duplicate keys on the `from` side"
            note = f" {' '.join(r['note'].split())}" if r.get("note") else ""
            lines.append(f"- {ref(r['from_obj'], r['from_cols'])} -> {ref(r['to_obj'], r['to_cols'])} "
                         f"{what}: {counts}.{note}")
    return lines


def key_table(keys, results, notes):
    def flag(n):
        return f"**{fmt(n)}**" if n else "0"
    lines = ["| Object | Key (grain) | Rows | NULL keys | Duplicate keys |", "| --- | --- | ---: | ---: | ---: |"]
    for (obj, cols), res in zip(keys.items(), results):
        lines.append(f"| `{obj}` | {', '.join(cols)} | {fmt(res['rows'])} | {flag(res['null_keys'])} "
                     f"| {flag(res['dup_keys'])} |")
    noted = [(obj, res) for (obj, _), res in zip(keys.items(), results)
             if res["null_keys"] or res["dup_keys"] or obj in notes]
    if noted:
        lines += ["", "Notes:", ""]
        for obj, res in noted:
            unique = not (res["null_keys"] or res["dup_keys"])
            counts = f"{fmt(res['null_keys'])} rows with a NULL key column, {fmt(res['dup_keys'])} duplicate keys"
            note = f" {' '.join(notes[obj].split())}" if obj in notes else ""
            lines.append(f"- `{obj}` {'is unique' if unique else 'is not unique'}: {counts}.{note}")
    return lines


def write_model(cat, keys, key_notes, rels, rel_results, key_results, db_path, generated):
    numeric_ids = [(name, col, typ) for name, obj in cat.items() for col, typ in obj["columns"]
                   if ID_LIKE.search(col) and typ.split("(")[0].strip().upper() in NUMERIC_TYPES]
    try:
        source = db_path.resolve().relative_to(BASE)
    except ValueError:
        source = db_path
    lines = [
        "# Data model: NIH RePORTER harvest (nationwide)",
        "",
        f"Generated by `.venv/bin/python3 tools/make_data_model.py` at {generated} from the SQLite catalog of "
        f"`{source}` and `tools/relationships.yml`. Don't edit by hand: change `relationships.yml` or the "
        f"database and regenerate.",
        "",
        "Every record is keyed by `appl_id`: one per fiscal year a project is active. `project_num` and "
        "`core_project_num` group those years into a project, and collapsing to the latest year per project "
        "happens in views, never at load time. `nih_raw_projects` keeps every version of every record; "
        "`nih_raw_projects_current` is the current one. The database declares no foreign keys, so the joins "
        "below come from `tools/relationships.yml` and are checked against the data each time this is "
        "generated.",
        "",
        "In the diagram, PK marks the columns that identify a row (the object's grain; only "
        "`nih_raw_projects.version_id`, `nih_change_log.change_id` and `nih_refresh_runs.run_id` are declared "
        "keys) and FK marks a join column on the `from` side. Views are labelled `(view)`. Only key and join "
        "columns are drawn; every column is in the data dictionary below. `untyped` means the column has no "
        "declared type: a view's computed column, or a child-table column built from one.",
        "",
        *mermaid(cat, keys, rels),
        "",
        "## Relationships, checked against the data",
        "",
        "Orphan rows: rows whose join columns are all set but match nothing on the `to` side. Unique-side "
        "dups: key values that repeat on a side the cardinality says is unique.",
        "",
        *relationship_table(rels, rel_results),
        "",
        "## Keys",
        "",
        *key_table(keys, key_results, key_notes),
        "",
        "## Identifiers declared as numbers",
        "",
        "Ids, codes, sequences and fiscal years are text in the child tables. These columns have such a name "
        "and a numeric declared type:",
        "",
        "| Object | Column | Type |",
        "| --- | --- | --- |",
        *[f"| `{o}` | {c} | {t} |" for o, c, t in numeric_ids],
        "",
        "## Data dictionary",
        "",
    ]
    for name, obj in cat.items():
        lines += [f"### `{name}` ({obj['kind']})", ""]
        if obj["reads"]:
            lines += ["Reads: " + ", ".join(f"`{r}`" for r in obj["reads"]) + ".", ""]
        lines += ["| Column | Type |", "| --- | --- |"]
        lines += [f"| {col} | {typ or '(none)'} |" for col, typ in obj["columns"]]
        lines.append("")
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text("\n".join(lines).rstrip() + "\n")
    print(f"Wrote {OUT.relative_to(BASE)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DB_PATH, help="another copy of the database")
    parser.add_argument("--verbose", action="store_true", help="print each check query and its time")
    args = parser.parse_args()
    if not args.db.exists():
        raise SystemExit(f"{args.db} not found.")

    conn = connect(args.db)
    try:
        cat = catalog(conn)
        keys, key_notes, rels = load_spec(cat)
        rel_results = [check_relationship(conn, r, args.verbose) for r in rels]
        key_results = [check_key(conn, o, c, args.verbose) for o, c in keys.items()]
    finally:
        conn.close()
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    write_model(cat, keys, key_notes, rels, rel_results, key_results, args.db, generated)
    for r, res in zip(rels, rel_results):
        if not res["holds"]:
            print(f"  doesn't hold cleanly: {r['from']} -> {r['to']} (orphan rows {res['orphan_rows']}, "
                  f"dup keys {res.get('to_dup_keys', 0) + res.get('from_dup_keys', 0)})")
    for (o, c), res in zip(keys.items(), key_results):
        if res["dup_keys"] or res["null_keys"]:
            print(f"  key not unique: {o} ({', '.join(c)}): {res['dup_keys']} duplicate, {res['null_keys']} NULL")


if __name__ == "__main__":
    main()
