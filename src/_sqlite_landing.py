"""
Shared SQLite landing-table helpers for nih-reporter-harvest with a schema-on-read
design (raw_json verbatim, hash-based change detection) adapted from an
earlier Oracle-based pipeline. Raw JSON is stored whole rather than
flattened at load time so nothing is dropped or restricted before it's
even queried.

VERSIONED (Type-2): nih_raw_projects keeps every version of every record.
A changed record's current row is closed out (valid_to set, is_current = 0)
and the new version is inserted; a record NIH stops returning is closed out
the same way and logged as REMOVED. Nothing is ever deleted or overwritten.
nih_raw_projects_current (is_current = 1) is what the views read. Every
difference goes to nih_change_log: one row per NEW or REMOVED appl_id and
one row per changed top-level field.

The key is appl_id, not project_num: NIH issues one appl_id per fiscal year
of a project. A partial unique index allows at most one current row per
appl_id.
"""

import hashlib
import json
import sqlite3

TABLE = "nih_raw_projects"
CHANGE_LOG_TABLE = "nih_change_log"
RUNS_TABLE = "nih_refresh_runs"   # one row per weekly load, even one with no changes
KEY_COL = "appl_id"

SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    version_id       INTEGER PRIMARY KEY,   -- one per version of a record
    appl_id          INTEGER NOT NULL,
    project_num      TEXT,
    core_project_num TEXT,
    fiscal_year      INTEGER,
    raw_json         TEXT NOT NULL,         -- the full API record, verbatim
    row_hash         TEXT NOT NULL,         -- sha256 of raw_json's canonical form
    valid_from       TEXT NOT NULL,         -- when this version was first loaded
    valid_to         TEXT,                  -- when it was superseded or removed (NULL = current)
    is_current       INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS {CHANGE_LOG_TABLE} (
    change_id    INTEGER PRIMARY KEY,
    run_id       TEXT NOT NULL,
    detected_at  TEXT NOT NULL,             -- load time, not when NIH made the change
    appl_id      INTEGER NOT NULL,
    project_num  TEXT,
    fiscal_year  INTEGER,
    change_type  TEXT NOT NULL,             -- NEW / CHANGED / REMOVED
    field_name   TEXT,                      -- top-level field (CHANGED only)
    old_value    TEXT,                      -- field JSON before; whole record for REMOVED
    new_value    TEXT,                      -- field JSON after
    found_by     TEXT                       -- full_repull (NULL for the migration baseline)
);
CREATE TABLE IF NOT EXISTS {RUNS_TABLE} (
    run_id         TEXT PRIMARY KEY,        -- UTC, sorts in run order
    loaded_at      TEXT NOT NULL,
    records_pulled INTEGER NOT NULL,
    new_count      INTEGER NOT NULL,
    changed_count  INTEGER NOT NULL,
    removed_count  INTEGER NOT NULL,
    delta_file     TEXT,
    delta_sha256   TEXT
);
CREATE VIEW IF NOT EXISTS {TABLE}_current AS
    SELECT * FROM {TABLE} WHERE is_current = 1;
"""

# Created after a bulk copy rather than before, which is much faster.
INDEX_SQL = f"""
CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE}_one_current ON {TABLE}({KEY_COL}) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS idx_{TABLE}_key ON {TABLE}({KEY_COL});
CREATE INDEX IF NOT EXISTS idx_{TABLE}_fy_cur ON {TABLE}(fiscal_year, is_current);
CREATE INDEX IF NOT EXISTS idx_{CHANGE_LOG_TABLE}_appl ON {CHANGE_LOG_TABLE}({KEY_COL});
CREATE INDEX IF NOT EXISTS idx_{CHANGE_LOG_TABLE}_run ON {CHANGE_LOG_TABLE}(run_id);
"""


def connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")  # better concurrent read/write behavior for a multi-GB db
    return conn


def is_versioned(conn):
    """True once the table has the versioned layout (a version_id column)."""
    cols = [r[1] for r in conn.execute(f"PRAGMA table_info({TABLE})")]
    return not cols or "version_id" in cols


def create_schema(conn):
    if not is_versioned(conn):
        raise SystemExit(f"{TABLE} still has the pre-versioning layout -- run migrate_to_versions.py first.")
    conn.executescript(SCHEMA_SQL)
    conn.executescript(INDEX_SQL)
    conn.commit()


def canonical_json(record):
    return json.dumps(record, sort_keys=True, separators=(",", ":"))


def row_hash(canonical_text):
    return hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()


def existing_hashes(conn):
    """{appl_id: row_hash} for every current row -- a couple hundred MB as a
    Python dict at ~3M rows."""
    cur = conn.execute(f"SELECT {KEY_COL}, row_hash FROM {TABLE} WHERE is_current = 1")
    return dict(cur.fetchall())


def current_ids_by_year(conn):
    """{fiscal_year: set(appl_id)} for every current row."""
    by_year = {}
    for appl_id, fy in conn.execute(f"SELECT {KEY_COL}, fiscal_year FROM {TABLE} WHERE is_current = 1"):
        by_year.setdefault(fy, set()).add(appl_id)
    return by_year


def current_record(conn, appl_id):
    row = conn.execute(
        f"SELECT raw_json FROM {TABLE} WHERE {KEY_COL} = ? AND is_current = 1", (appl_id,)
    ).fetchone()
    return json.loads(row[0]) if row else None


def insert_version(conn, record, canonical, h, now_iso):
    conn.execute(
        f"""INSERT INTO {TABLE}
            ({KEY_COL}, project_num, core_project_num, fiscal_year, raw_json, row_hash, valid_from, is_current)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
        (record.get(KEY_COL), record.get("project_num"), record.get("core_project_num"),
         record.get("fiscal_year"), canonical, h, now_iso),
    )


def close_out(conn, appl_id, now_iso):
    conn.execute(
        f"UPDATE {TABLE} SET valid_to = ?, is_current = 0 WHERE {KEY_COL} = ? AND is_current = 1",
        (now_iso, appl_id),
    )


def insert_new_only(conn, records, existing, now_iso):
    """Initial-build load (2_load_to_sqlite.py): insert appl_ids the table
    has never had a current row for; never touch existing ones. Returns
    (new_count, skipped_count)."""
    new_count = skipped = 0
    for record in records:
        appl_id = record.get(KEY_COL)
        if appl_id in existing:
            skipped += 1
            continue
        canonical = canonical_json(record)
        h = row_hash(canonical)
        insert_version(conn, record, canonical, h, now_iso)
        existing[appl_id] = h
        new_count += 1
    return new_count, skipped


def _json_text(value):
    return None if value is None else json.dumps(value, sort_keys=True, separators=(",", ":"))


def field_diffs(old, new):
    """[(field, old_json_text, new_json_text)] for each top-level field that differs."""
    return [
        (k, _json_text(old.get(k)), _json_text(new.get(k)))
        for k in sorted(set(old) | set(new))
        if old.get(k) != new.get(k)
    ]


def write_change_log(conn, rows):
    """rows: (run_id, detected_at, appl_id, project_num, fiscal_year,
    change_type, field_name, old_value, new_value, found_by)."""
    conn.executemany(
        f"""INSERT INTO {CHANGE_LOG_TABLE}
            (run_id, detected_at, {KEY_COL}, project_num, fiscal_year, change_type,
             field_name, old_value, new_value, found_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        rows,
    )
