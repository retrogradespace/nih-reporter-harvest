"""
nih-reporter-harvest, stage 2 loads every data/raw/fy*.jsonl file produced by
1_harvest_raw.py into data/nih_harvester.db, a single SQLite landing table
(nih_raw_projects) covering all fiscal years, all institutions.

Idempotent and resumable: hashes the current DB contents once up front
(existing_hashes), then only inserts new / updates changed / skips
unchanged records as it streams through the JSONL files rerunning after
an interruption (or after 1_harvest_raw.py adds more years later) doesn't
redo work already done. Same principle as a typical incremental warehouse
loader, see _sqlite_landing.py.

Streams line-by-line in batches (BATCH_SIZE), not one giant read since the
full harvest will eventually be millions of records across possibly tens
of GB of JSONL, too large to hold in memory at once.

Usage:
    ../.venv/bin/python3 2_load_to_sqlite.py
"""

import json
import time
from datetime import datetime
from pathlib import Path

import _sqlite_landing as db

BASE = Path(__file__).resolve().parent
RAW_DIR = BASE.parent / "data" / "raw"
DB_PATH = BASE.parent / "data" / "nih_harvester.db"
BATCH_SIZE = 5000


def read_batches(paths, batch_size):
    batch = []
    for path in paths:
        with path.open() as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                batch.append(json.loads(line))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
    if batch:
        yield batch


def main():
    # Only years with a .done marker (see 1_harvest_raw.py) may still be
    # actively writing the in-progress year's .jsonl when this runs
    # (loading it here could pick up a partial file, ahead of its own
    # coverage-check warnings and before the manifest is even written).
    done_years = sorted(p.stem.removeprefix("fy") for p in RAW_DIR.glob("fy*.done"))
    jsonl_paths = [RAW_DIR / f"fy{year}.jsonl" for year in done_years]
    jsonl_paths = [p for p in jsonl_paths if p.exists()]

    skipped = sorted(RAW_DIR.glob("fy*.jsonl"))
    skipped = [p for p in skipped if p not in jsonl_paths]
    if skipped:
        print(f"Skipping {len(skipped)} in-progress year(s) (no .done marker yet): "
              f"{[p.name for p in skipped]}")

    if not jsonl_paths:
        raise SystemExit(f"No completed fy*.jsonl files found in {RAW_DIR} -- run 1_harvest_raw.py first.")

    print(f"Loading {len(jsonl_paths)} fiscal-year file(s) into {DB_PATH}")

    conn = db.connect(DB_PATH)
    db.create_schema(conn)

    print("Loading existing row hashes for idempotent diffing...")
    existing = db.existing_hashes(conn)
    print(f"  {len(existing)} rows already in the DB")

    now_iso = datetime.now().isoformat(timespec="seconds")
    total_new = total_changed = total_unchanged = total_seen = 0
    start = time.time()

    for batch in read_batches(jsonl_paths, BATCH_SIZE):
        new, changed, unchanged = db.upsert_batch(conn, batch, existing, now_iso)
        conn.commit()
        total_new += new
        total_changed += changed
        total_unchanged += unchanged
        total_seen += len(batch)
        elapsed = time.time() - start
        print(f"  {total_seen} records processed "
              f"(new={total_new} changed={total_changed} unchanged={total_unchanged}) "
              f"-- {elapsed:.0f}s elapsed")

    conn.close()
    print(f"\nDone. new={total_new} changed={total_changed} unchanged={total_unchanged} "
          f"total_processed={total_seen}")


if __name__ == "__main__":
    main()
