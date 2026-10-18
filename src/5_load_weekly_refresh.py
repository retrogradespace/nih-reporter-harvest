"""
nih-reporter-harvest, stage 5 -- loads a weekly pull (4_weekly_refresh_pull.py)
into the versioned nih_raw_projects table and records every difference in
nih_change_log.

1. **Upsert everything pulled.** Each record is hash-compared with its
   current version. New appl_ids are inserted (NEW); a changed one has its
   current row closed out and the new version inserted, with one CHANGED
   row per top-level field that differs; unchanged ones are skipped.

2. **Removals, within the years the pull fully covered.** An appl_id that
   is current in the database for one of those years but wasn't returned by
   this pull (in any year -- NIH sometimes moves a record to another fiscal
   year) has been dropped by NIH. It is closed out, never deleted, and
   logged as REMOVED with the whole last-known record in old_value.

   Guard: more than --max-removals (default 100) and nothing is committed.
   A genuine NIH removal is rare and small; a large count is far more likely
   a bad pull. Check the listed appl_ids, then rerun with --run-id and a
   higher --max-removals only if they really are gone.

Everything commits in one transaction, together with a nih_refresh_runs
row. A run older than the newest one already loaded is refused.

AFTER THE COMMIT the week's changes are kept and the full snapshot is not:
data/weekly/delta_<run_id>.jsonl.gz holds every NEW and CHANGED record in
full and every REMOVED appl_id, and its sha256 goes in nih_refresh_runs.
Then the snapshot folder data/weekly/<run_id>/ is deleted (~5.5 GB a week;
the pull's manifest stays). The original harvest in data/raw plus every
delta, in order, rebuild the current data; the database holds every
version.

Usage:
    ../.venv/bin/python3 5_load_weekly_refresh.py [--run-id ID] [--max-removals N]
With no --run-id, loads the newest complete pull.
"""

import argparse
import gzip
import hashlib
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import _sqlite_landing as db

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE.parent / "data"
WEEKLY_DIR = DATA_DIR / "weekly"
DB_PATH = DATA_DIR / "nih_harvester.db"


def sha256_of(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def find_manifest(run_id):
    if run_id:
        path = WEEKLY_DIR / f"{run_id}.manifest.json"
        if not path.exists():
            raise SystemExit(f"No complete pull for run {run_id} ({path.name} missing).")
        return path
    manifests = sorted(WEEKLY_DIR.glob("*.manifest.json"))
    if not manifests:
        raise SystemExit("No complete weekly pull found -- run 4_weekly_refresh_pull.py first.")
    return manifests[-1]


def change_row(run_id, now, record, change_type, field=None, old=None, new=None):
    return (run_id, now, record.get(db.KEY_COL), record.get("project_num"), record.get("fiscal_year"),
            change_type, field, old, new, "full_repull")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id")
    parser.add_argument("--max-removals", type=int, default=100)
    parser.add_argument("--db", type=Path, default=DB_PATH, help="for testing against a copy")
    parser.add_argument("--keep-snapshot", action="store_true", help="for testing: don't delete the snapshot")
    args = parser.parse_args()

    manifest_path = find_manifest(args.run_id)
    manifest = json.loads(manifest_path.read_text())
    run_id = manifest["run_id"]
    run_dir = WEEKLY_DIR / run_id
    years = manifest["full_coverage_fiscal_years"]

    print(f"Loading pull {run_id}: {manifest['records']} records, FY{years[0]}-FY{years[-1]}")
    print("Checking snapshot files against the manifest...")
    for y, info in manifest["years"].items():
        path = run_dir / info["data_file"]
        if not path.exists():
            raise SystemExit(f"{path} is missing -- was this run already loaded?")
        if sha256_of(path) != info["data_file_sha256"]:
            raise SystemExit(f"{path.name}: sha256 doesn't match the manifest -- refusing to load.")

    conn = db.connect(args.db)
    db.create_schema(conn)
    last = conn.execute(f"SELECT max(run_id) FROM {db.RUNS_TABLE}").fetchone()[0]
    if last and last >= run_id:
        raise SystemExit(f"Run {last} is already loaded; refusing to load the same or an older run ({run_id}).")

    print("Loading current hashes...")
    existing = db.existing_hashes(conn)
    print(f"  {len(existing)} current rows")

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    delta_path = WEEKLY_DIR / f"delta_{run_id}.jsonl.gz"
    delta_tmp = delta_path.with_suffix(".gz.partial")
    pulled = set()
    new_n = changed_n = 0
    field_counts = {}

    try:
        with gzip.open(delta_tmp, "wt", encoding="utf-8") as delta:
            for y in sorted(manifest["years"], key=int):
                path = run_dir / manifest["years"][y]["data_file"]
                with gzip.open(path, "rt", encoding="utf-8") as f:
                    for line in f:
                        record = json.loads(line)
                        appl_id = record.get(db.KEY_COL)
                        pulled.add(appl_id)
                        canonical = db.canonical_json(record)
                        h = db.row_hash(canonical)
                        old_h = existing.get(appl_id)
                        if old_h == h:
                            continue
                        if old_h is None:
                            rows = [change_row(run_id, now, record, "NEW")]
                            change_type = "NEW"
                            new_n += 1
                        else:
                            old = db.current_record(conn, appl_id)
                            rows = [change_row(run_id, now, record, "CHANGED", fld, o, n)
                                    for fld, o, n in db.field_diffs(old, record)]
                            for r in rows:
                                field_counts[r[6]] = field_counts.get(r[6], 0) + 1
                            db.close_out(conn, appl_id, now)
                            change_type = "CHANGED"
                            changed_n += 1
                        db.insert_version(conn, record, canonical, h, now)
                        db.write_change_log(conn, rows)
                        existing[appl_id] = h
                        delta.write(json.dumps({"change_type": change_type, "record": record}) + "\n")
                print(f"  FY{y}: done (new so far {new_n}, changed {changed_n})", flush=True)

            removed = []
            for y in years:
                current = {r[0] for r in conn.execute(
                    f"SELECT {db.KEY_COL} FROM {db.TABLE} WHERE is_current = 1 AND fiscal_year = ?", (y,))}
                removed += sorted(current - pulled)

            if len(removed) > args.max_removals:
                conn.rollback()
                raise SystemExit(
                    f"ABORTED, nothing committed: {len(removed)} appl_ids would be removed "
                    f"(limit {args.max_removals}). First 50: {removed[:50]}\n"
                    f"Check these against NIH RePORTER. If they really are gone, rerun with "
                    f"--run-id {run_id} --max-removals {len(removed)}.")

            for appl_id in removed:
                old = db.current_record(conn, appl_id)
                db.write_change_log(conn, [change_row(run_id, now, old, "REMOVED", old=db.canonical_json(old))])
                db.close_out(conn, appl_id, now)
                delta.write(json.dumps({"change_type": "REMOVED", "appl_id": appl_id,
                                        "fiscal_year": old.get("fiscal_year")}) + "\n")

        delta_tmp.replace(delta_path)
        delta_sha = sha256_of(delta_path)
        conn.execute(
            f"INSERT INTO {db.RUNS_TABLE} VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (run_id, now, len(pulled), new_n, changed_n, len(removed), delta_path.name, delta_sha),
        )
        conn.commit()
    except BaseException:
        conn.rollback()
        delta_tmp.unlink(missing_ok=True)
        if delta_path.exists() and not conn.execute(
                f"SELECT 1 FROM {db.RUNS_TABLE} WHERE run_id = ?", (run_id,)).fetchone():
            delta_path.unlink()
        raise
    finally:
        conn.close()

    print(f"\n[{db.TABLE}] new={new_n} changed={changed_n} removed={len(removed)} "
          f"(of {len(pulled)} pulled)")
    if removed:
        print(f"[{db.TABLE}] removed appl_ids: {removed}")
    if field_counts:
        print("changed fields: " + ", ".join(f"{k}={v}" for k, v in sorted(field_counts.items(), key=lambda x: -x[1])))
    print(f"Delta: {delta_path.relative_to(DATA_DIR.parent)} (sha256 {delta_sha[:12]}...)")

    if args.keep_snapshot:
        return
    shutil.rmtree(run_dir)
    print(f"Deleted the full snapshot {run_dir.relative_to(DATA_DIR.parent)}/ (the delta and manifest are kept).")


if __name__ == "__main__":
    main()
