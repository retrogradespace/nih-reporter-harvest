"""
Shared SQLite landing-table helpers for nih-reporter-harvest with a schema-on-read
design (raw_json verbatim, hash-based change detection) adapted from an
earlier Oracle-based pipeline. Raw JSON is stored whole rather than
flattened at load time so nothing is dropped or restricted before it's
even queried.
"""

import hashlib
import json
import sqlite3

TABLE = "nih_raw_projects"
KEY_COL = "appl_id"
INDEX_FIELDS = ["project_num", "core_project_num", "fiscal_year"]


def connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")  # better concurrent read/write behavior for a multi-GB db
    return conn


def create_schema(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            {KEY_COL} INTEGER PRIMARY KEY,
            project_num TEXT,
            core_project_num TEXT,
            fiscal_year INTEGER,
            raw_json TEXT NOT NULL,
            row_hash TEXT NOT NULL,
            valid_from TEXT,
            valid_to TEXT,
            is_current INTEGER NOT NULL DEFAULT 1
        )
    """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_key_cur ON {TABLE}({KEY_COL}, is_current)")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE}_fy ON {TABLE}(fiscal_year)")
    conn.execute(f"""
        CREATE VIEW IF NOT EXISTS {TABLE}_current AS
        SELECT * FROM {TABLE} WHERE is_current = 1
    """)
    conn.commit()


def canonical_json(record):
    return json.dumps(record, sort_keys=True, separators=(",", ":"))


def row_hash(canonical_text):
    return hashlib.sha256(canonical_text.encode("utf-8")).hexdigest()


def existing_hashes(conn):
    """{appl_id: row_hash} for every current row cheap even at a few
    million rows (a couple hundred MB as a Python dict), used to make loads
    idempotent: rerunning after an interruption only touches new/changed
    rows, same principle as a typical incremental warehouse loader."""
    cur = conn.execute(f"SELECT {KEY_COL}, row_hash FROM {TABLE} WHERE is_current = 1")
    return dict(cur.fetchall())


def upsert_batch(conn, records, existing, now_iso):
    """Insert new / update changed / skip unchanged, for a batch of raw
    records (dicts). No history tracking yet -- this overwrites in place
    rather than closing out old versions; nothing yet reads
    valid_from/valid_to as real history.
    Returns (new_count, changed_count, unchanged_count)."""
    insert_rows = []
    update_rows = []
    new_count = changed_count = unchanged_count = 0

    for record in records:
        appl_id = record.get(KEY_COL)
        canonical = canonical_json(record)
        h = row_hash(canonical)

        if appl_id in existing:
            if existing[appl_id] == h:
                unchanged_count += 1
                continue
            changed_count += 1
            update_rows.append((
                record.get("project_num"), record.get("core_project_num"),
                record.get("fiscal_year"), canonical, h, now_iso, appl_id,
            ))
        else:
            new_count += 1
            insert_rows.append((
                appl_id, record.get("project_num"), record.get("core_project_num"),
                record.get("fiscal_year"), canonical, h, now_iso,
            ))

    if insert_rows:
        conn.executemany(
            f"""INSERT INTO {TABLE}
                (appl_id, project_num, core_project_num, fiscal_year, raw_json, row_hash, valid_from, is_current)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
            insert_rows,
        )
    if update_rows:
        conn.executemany(
            f"""UPDATE {TABLE}
                SET project_num = ?, core_project_num = ?, fiscal_year = ?,
                    raw_json = ?, row_hash = ?, valid_from = ?
                WHERE appl_id = ?""",
            update_rows,
        )

    return new_count, changed_count, unchanged_count
