"""
nih-reporter-harvest, stage 7 -- rebuilds the five child tables
(ddl/nih_child_tables.sql: nih_principal_investigators, nih_program_officers,
nih_agency_ic_fundings, nih_spending_categories, nih_covid_response) from
nih_raw_projects_current, then runs ddl/nih_spending_categories_check.sql.

6_run_weekly_refresh.py runs this after every successful load (5_), so the
tables are never older than the last load. The rebuild is one transaction:
if it fails, the previous tables are left exactly as they were. A few
minutes at ~3M records; the tables take about 5 GB on top of the raw data,
and the WAL briefly needs about as much again while the rebuild runs.

THE CHECK IS A WARNING, NOT A GATE: rows it returns are printed as a
WARNING (spending_category_name may be wrong for those projects / ids) and
this script still exits 0. A failed rebuild exits non-zero.

nih_parsed_view.sql is run first (create view if not exists), since the
tables take their project columns from nih_parsed.

Usage:
    ../.venv/bin/python3 7_build_child_tables.py [--db PATH]
"""

import argparse
import sqlite3
import time
from pathlib import Path

import _sqlite_landing as db

BASE = Path(__file__).resolve().parent
DDL_DIR = BASE.parent / "ddl"
DB_PATH = BASE.parent / "data" / "nih_harvester.db"
CHILD_TABLES = ["nih_principal_investigators", "nih_program_officers", "nih_agency_ic_fundings",
                "nih_spending_categories", "nih_covid_response"]
MAX_ROWS_PRINTED = 50  # the log gets the count either way


def spending_category_check(conn):
    sql = (DDL_DIR / "nih_spending_categories_check.sql").read_text()
    rows = conn.execute(sql).fetchall()
    if not rows:
        print("Spending category check: OK -- every project's ids and names pair up.")
        return
    print(f"WARNING: spending category check returned {len(rows)} problem row(s). "
          "spending_category_name may be wrong for these until they are looked at:")
    print("  ISSUE            APPL_ID      CATEGORY_ID  DETAIL")
    for issue, appl_id, category_id, detail in rows[:MAX_ROWS_PRINTED]:
        print(f"  {issue:<16} {appl_id or '':<12} {category_id or '':<12} {detail}")
    if len(rows) > MAX_ROWS_PRINTED:
        print(f"  ... and {len(rows) - MAX_ROWS_PRINTED} more -- run nih_spending_categories_check.sql "
              "for the full list.")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", type=Path, default=DB_PATH, help="for testing against a copy")
    args = parser.parse_args()
    if not args.db.exists():
        raise SystemExit(f"{args.db} not found.")

    conn = db.connect(args.db)
    try:
        conn.executescript((DDL_DIR / "nih_parsed_view.sql").read_text())
        print("Rebuilding the child tables...")
        started = time.monotonic()
        try:
            conn.executescript((DDL_DIR / "nih_child_tables.sql").read_text())
        except sqlite3.Error:
            if conn.in_transaction:
                conn.rollback()
            print("Rebuild failed; the previous child tables are unchanged.")
            raise
        print(f"  done in {time.monotonic() - started:.0f}s")
        for table in CHILD_TABLES:
            rows, appl_ids = conn.execute(f"SELECT count(*), count(DISTINCT appl_id) FROM {table}").fetchone()
            print(f"  {table:<28} {rows:>10} rows  {appl_ids:>9} appl_ids")
        spending_category_check(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
