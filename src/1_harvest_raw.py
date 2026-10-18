"""
nih-reporter-harvest, stage 1 nationwide raw pull, every fiscal year NIH
RePORTER has (FY1985-present), no institution scoping. Uses the adaptive
pagination in _nih_api.py (dual-pass, escalating to agency/org_state
splitting only where a bucket actually exceeds what a single query can
page through) to get full coverage despite nationwide per-year volumes
(~50k-94k) far exceeding NIH's 10,000 offset+limit cap.

One JSONL file per fiscal year (data/raw/fy<year>.jsonl), not one giant
file for the whole pull since a run covering 40+ years and millions of
records will take hours; per-year files mean an interruption only costs
the in-progress year, not everything before it. A completed year gets a
matching fy<year>.done marker (written only after that year's JSONL is
fully flushed) reruns skip any year that already has one.

include_fields is never set (full ~45-field default record per row) an
earlier pipeline that explicitly restricted include_fields silently
dropped data whenever a field was needed later but hadn't been requested
up front, which this design avoids entirely.

Usage:
    ../.venv/bin/python3 1_harvest_raw.py                  # all years
    ../.venv/bin/python3 1_harvest_raw.py start 2020 - end 2024   # a range, for testing
"""

import argparse
import json
from datetime import date, datetime
from pathlib import Path

from _nih_api import harvest_year

BASE = Path(__file__).resolve().parent
RAW_DIR = BASE.parent / "data" / "raw"

FIRST_FISCAL_YEAR = 1985


def harvest_one_year(year, log_path):
    out_path = RAW_DIR / f"fy{year}.jsonl"
    done_path = RAW_DIR / f"fy{year}.done"
    manifest_path = RAW_DIR / f"fy{year}.manifest.json"

    if done_path.exists():
        print(f"FY{year}: already done (found {done_path.name}), skipping.")
        return

    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(msg)

    seen_appl_ids = set()
    started = datetime.now()
    count = 0

    with out_path.open("w") as f:
        for record in harvest_year(year, seen_appl_ids, log=log):
            f.write(json.dumps(record) + "\n")
            count += 1

    manifest = {
        "fiscal_year": year,
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "record_count": count,
        "log": log_lines,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))
    done_path.write_text(datetime.now().isoformat(timespec="seconds") + "\n")

    with log_path.open("a") as f:
        f.write(f"\n=== FY{year} ({started.isoformat(timespec='seconds')}) ===\n")
        f.write("\n".join(log_lines) + "\n")

    print(f"FY{year}: wrote {count} records to {out_path.name}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=FIRST_FISCAL_YEAR)
    parser.add_argument("--end", type=int, default=date.today().year)
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    log_path = RAW_DIR.parent.parent / "logs" / f"harvest_{date.today().isoformat()}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    years = list(range(args.start, args.end + 1))
    print(f"Harvesting FY{years[0]}-FY{years[-1]} ({len(years)} years), nationwide, no institution scoping.")
    print(f"Log: {log_path}\n")

    for year in years:
        harvest_one_year(year, log_path)

    print("Done.")


if __name__ == "__main__":
    main()
