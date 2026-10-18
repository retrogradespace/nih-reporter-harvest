"""
nih-reporter-harvest, one-time migration of data/nih_harvester.db to the
versioned layout (see _sqlite_landing.py).

The old nih_raw_projects has appl_id as its primary key, so it can hold only
one version of a record. SQLite can't change a primary key in place, and
copying within the same file would leave it at twice the size until a
VACUUM (another full rewrite). So this builds a NEW file next to it:

1. data/nih_harvester.versioning.tmp.db gets the versioned schema, then
   every row of the old table as version 1 (valid_from kept as it was,
   valid_to NULL, is_current 1), in appl_id order.
2. The views (nih_parsed, nih_indicators) are copied over from the old
   file's own definitions, then the indexes are built.
3. Checks, all of which must pass or nothing is swapped: same row count,
   one row per appl_id, no (appl_id, row_hash) pair in the old table that
   isn't in the new one, the same total raw_json length, and quick_check.
4. Swap: the old file becomes data/nih_harvester.pre_versioning.db and the
   new one becomes data/nih_harvester.db. The old file is not deleted.
   Remove it by hand once you're satisfied (it's ~24 GB).

The change log starts empty. There is no earlier history to backfill: until
now the table was overwritten in place. The first weekly refresh logs the
first changes.

Needs free disk space about the size of the database. Refuses to run if the
database is already versioned or has a -wal file (open elsewhere, or not
checkpointed).

Usage:
    ../.venv/bin/python3 migrate_to_versions.py
"""

import os
import sqlite3
import time
from pathlib import Path

import _sqlite_landing as db

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE.parent / "data"
DB_PATH = DATA_DIR / "nih_harvester.db"
TMP_PATH = DATA_DIR / "nih_harvester.versioning.tmp.db"
OLD_PATH = DATA_DIR / "nih_harvester.pre_versioning.db"

COPY_COLS = "appl_id, project_num, core_project_num, fiscal_year, raw_json, row_hash, valid_from"


def step(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def main():
    if not DB_PATH.exists():
        raise SystemExit(f"No {DB_PATH}")
    if OLD_PATH.exists():
        raise SystemExit(f"{OLD_PATH.name} already exists -- a migration already ran. Nothing done.")
    for suffix in ("-wal", "-shm"):
        if DB_PATH.with_name(DB_PATH.name + suffix).exists():
            raise SystemExit(f"{DB_PATH.name}{suffix} exists -- close anything using the db, "
                             f"or run `PRAGMA wal_checkpoint(TRUNCATE)` on it, then rerun.")

    check = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    if db.is_versioned(check):
        raise SystemExit(f"{DB_PATH.name} is already versioned. Nothing done.")
    views = check.execute(
        "SELECT name, sql FROM sqlite_master WHERE type = 'view' AND name != ?", (f"{db.TABLE}_current",)
    ).fetchall()
    check.close()

    free = os.statvfs(DATA_DIR)
    free_bytes = free.f_bavail * free.f_frsize
    need = DB_PATH.stat().st_size * 1.1
    if free_bytes < need:
        raise SystemExit(f"Need ~{need / 1e9:.1f} GB free, have {free_bytes / 1e9:.1f} GB.")

    TMP_PATH.unlink(missing_ok=True)
    conn = sqlite3.connect(TMP_PATH, uri=True)
    # A scratch file until the swap: if anything fails it is simply deleted,
    # so no journal is needed while building it.
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.executescript(db.SCHEMA_SQL)
    conn.execute(f"ATTACH DATABASE 'file:{DB_PATH}?mode=ro' AS src")

    try:
        step("Copying every row as version 1...")
        conn.execute(f"""
            INSERT INTO main.{db.TABLE} ({COPY_COLS}, valid_to, is_current)
            SELECT {COPY_COLS}, NULL, 1 FROM src.{db.TABLE} ORDER BY appl_id
        """)
        conn.commit()

        step(f"Copying {len(views)} view(s): {[n for n, _ in views]}")
        for _, sql in views:
            conn.execute(sql)

        step("Building indexes...")
        conn.executescript(db.INDEX_SQL)
        conn.commit()

        step("Checking the copy...")
        old_n, old_len = conn.execute(f"SELECT count(*), sum(length(raw_json)) FROM src.{db.TABLE}").fetchone()
        new_n, new_ids, new_len = conn.execute(
            f"SELECT count(*), count(DISTINCT appl_id), sum(length(raw_json)) FROM main.{db.TABLE}"
        ).fetchone()
        missing = conn.execute(f"""
            SELECT count(*) FROM (
                SELECT appl_id, row_hash FROM src.{db.TABLE}
                EXCEPT SELECT appl_id, row_hash FROM main.{db.TABLE})
        """).fetchone()[0]
        print(f"  rows: old {old_n}, new {new_n}; distinct appl_ids in new {new_ids}")
        print(f"  raw_json total length: old {old_len}, new {new_len}")
        print(f"  (appl_id, row_hash) pairs missing from new: {missing}")
        if not (old_n == new_n == new_ids and old_len == new_len and missing == 0):
            raise SystemExit("Checks FAILED -- nothing swapped.")
        for name, _ in views:
            conn.execute(f"SELECT * FROM main.{name} LIMIT 1").fetchall()

        conn.execute("DETACH DATABASE src")
        step("quick_check...")
        result = conn.execute("PRAGMA quick_check").fetchone()[0]
        if result != "ok":
            raise SystemExit(f"quick_check failed: {result} -- nothing swapped.")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.close()
    except BaseException:
        conn.close()
        TMP_PATH.unlink(missing_ok=True)
        raise

    os.replace(DB_PATH, OLD_PATH)
    os.replace(TMP_PATH, DB_PATH)
    step(f"Done. {DB_PATH.name} is versioned; the old file is {OLD_PATH.name} (not deleted).")


if __name__ == "__main__":
    main()
