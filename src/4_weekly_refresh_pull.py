"""
nih-reporter-harvest, stage 4 -- weekly full re-pull of every fiscal year
(FY1985 through next fiscal year), for 5_load_weekly_refresh.py to compare
against the database.

Every year, every week: changes turn up in old years too (the University of
Utah pipeline this was adapted from found changed records in FY2020-FY2024
that a two-year window missed), and only a full re-pull can show that NIH
dropped a record. About three hours of API calls.

Each year comes from _complete_pull.YearPull, which either returns exactly
NIH's total or raises. Any year that raises fails the whole pull: no
manifest is written, so 5_ has nothing to load and a short pull can never
look like removals.

OUTPUT, under data/weekly/:
    <run_id>/fy<year>.jsonl.gz   this run's snapshot, one record per line.
                                 Temporary: 5_ deletes the folder once it
                                 has loaded it and written the week's delta.
    <run_id>/fy<year>.done       year finished; a rerun with the same
                                 --run-id skips it (resume after a failure)
    <run_id>.manifest.json       written last, only when every year is
                                 complete: NIH's total for every query
                                 (api_meta_total_by_query), and each year's
                                 record count, file size and sha256

data/state/pull_hints.json keeps each year's activity codes and agencies
from the last complete pull, so the next one knows which buckets to probe.
The first run builds it from data/raw/fy*.jsonl.

Usage:
    ../.venv/bin/python3 4_weekly_refresh_pull.py [--run-id ID] [--start 2021 --end 2021]
--start/--end are for testing; 5_ only removes records within the years a
pull covered.
"""

import argparse
import gzip
import hashlib
import json
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

from _complete_pull import IncompletePull, YearPull, hints_from_raw_files

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE.parent / "data"
RAW_DIR = DATA_DIR / "raw"
WEEKLY_DIR = DATA_DIR / "weekly"
HINTS_PATH = DATA_DIR / "state" / "pull_hints.json"

FIRST_FISCAL_YEAR = 1985


def next_fiscal_year(today=None):
    """NIH's fiscal year starts Oct 1, so in October 2026 it is already FY2027.
    One year beyond that, as the Utah pipeline does, for early awards."""
    today = today or date.today()
    return today.year + (1 if today.month >= 10 else 0) + 1


def sha256_of(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_hints():
    if HINTS_PATH.exists():
        return json.loads(HINTS_PATH.read_text())
    print("No pull_hints.json yet -- building it from data/raw/fy*.jsonl ...")
    return hints_from_raw_files(RAW_DIR)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ"))
    parser.add_argument("--start", type=int, default=FIRST_FISCAL_YEAR)
    parser.add_argument("--end", type=int, default=next_fiscal_year())
    args = parser.parse_args()

    run_dir = WEEKLY_DIR / args.run_id
    manifest_path = WEEKLY_DIR / f"{args.run_id}.manifest.json"
    if manifest_path.exists():
        raise SystemExit(f"{manifest_path.name} already exists -- this run's pull is complete.")
    run_dir.mkdir(parents=True, exist_ok=True)

    hints = load_hints()
    all_codes = {c for codes in hints["codes_by_year"].values() for c in codes}
    years = list(range(args.start, args.end + 1))
    started = datetime.now(timezone.utc)
    print(f"Weekly pull {args.run_id}: FY{years[0]}-FY{years[-1]}, {len(all_codes)} known activity codes")

    for year in years:
        out_path = run_dir / f"fy{year}.jsonl.gz"
        done_path = run_dir / f"fy{year}.done"
        if done_path.exists():
            print(f"FY{year}: already pulled in this run, skipping.")
            continue

        tmp_path = out_path.with_suffix(".gz.partial")
        log_lines = []

        def log(msg):
            print(msg, flush=True)
            log_lines.append(msg)

        with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
            pull = YearPull(year, lambda r: f.write(json.dumps(r) + "\n"), log)
            try:
                nih_total = pull.run(hints, all_codes)
            except IncompletePull as e:
                raise SystemExit(f"PULL FAILED, nothing to load: {e}")
        tmp_path.replace(out_path)

        done_path.write_text(json.dumps({
            "fiscal_year": year,
            "nih_total": nih_total,
            "records": len(pull.seen),
            "data_file": out_path.name,
            "data_file_bytes": out_path.stat().st_size,
            "data_file_sha256": sha256_of(out_path),
            "activity_codes": sorted(pull.codes),
            "agencies": sorted(pull.agencies),
            "api_meta_total_by_query": pull.totals_by_query,
            "log": log_lines,
        }, indent=2))

    per_year = {}
    for year in years:
        per_year[str(year)] = json.loads((run_dir / f"fy{year}.done").read_text())

    for year, info in per_year.items():
        if info["activity_codes"]:
            hints["codes_by_year"][year] = info["activity_codes"]
        if info["agencies"]:
            hints["agencies_by_year"][year] = info["agencies"]
    HINTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp_hints = HINTS_PATH.with_suffix(".json.tmp")
    tmp_hints.write_text(json.dumps(hints, indent=1, sort_keys=True))
    tmp_hints.replace(HINTS_PATH)

    manifest = {
        "run_id": args.run_id,
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "full_coverage_fiscal_years": [int(y) for y in per_year],
        "records": sum(i["records"] for i in per_year.values()),
        "years": {y: {k: v for k, v in i.items() if k not in ("log", "activity_codes", "agencies")}
                  for y, i in per_year.items()},
    }
    manifest_path.write_text(json.dumps(manifest, indent=1))
    print(f"\nPull complete: {manifest['records']} records across {len(years)} year(s). "
          f"Manifest: {manifest_path.relative_to(DATA_DIR.parent)}")
    free = shutil.disk_usage(DATA_DIR).free / 1e9
    print(f"({free:.0f} GB free on the data volume)")


if __name__ == "__main__":
    main()
