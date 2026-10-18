"""
nih-reporter-harvest, stage 1c: close the residual gaps stage 1b logged.

After 1b, most years sit 0-5 records short of NIH's own total, for two
reasons 1b could not reach (see logs/gapfill_*.log):

1. R01 is the one activity code over the 20,000 dual-pass ceiling
   (~25-30k a year), and 1b split it by `org_states`. R01s with no U.S.
   state on file (~160-220 a year, mostly foreign institutions) match no
   state query and were never fetched.
2. Records whose activity code never appears anywhere else in that year's
   file. 1b discovered codes from the year's own records only, so a code
   held by just the missing records could not be found that way. Some
   have no activity code at all (FY2021: two VA records, project_num
   "2018" and "2019"), which no activity-code query can match.

WHAT 1c DOES DIFFERENTLY, for every year (a year whose count matches or
exceeds NIH's total can still have gaps, hidden by records NIH has since
dropped):

- **Localize before fetching.** Count the records on file per activity
  code and compare each count with NIH's meta.total for that code. Only
  short buckets are fetched. The local and API definitions agree (checked:
  FY2021's 11,683 records with a subproject_id equal NIH's
  `sub_project_only` total exactly), so a short bucket really is short.
- **Codes from every year, not just this one.** Codes seen in any year's
  file are probed for this year with one OR-filter query; any that match
  become buckets too.
- **Split oversized buckets by agency, not state.** Measured live: FY2021
  R01 split by agency covers 29,867/29,872, largest agency 3,776, and it
  has no blind spot for foreign records.
- **Last resort: agency x subproject.** If the year is still short, the
  same localize-then-fetch runs on (agency, sub_project_only /
  exclude_subprojects). The two subproject flags partition a year exactly
  (FY2021: 11,683 + 71,285 = 82,968).

STRICTER THAN 1b: every bucket fetched here counts the distinct appl_ids
the API returned for it (already-on-file ones included) and must reach that
bucket's meta.total. A short bucket is logged as a WARNING and listed in the
manifest, never silently accepted. `_nih_api.fetch_paginated` itself stops
quietly on an exhausted retry; this check is what catches that.

APPEND-ONLY, same contract as 1b: new records are appended to
fy<year>.jsonl, nothing on file is rewritten, and earlier manifests are left
as they are. This stage writes fy<year>.gapfill_1c_manifest.json (with NIH's
total for every query it made and the jsonl's sha256 afterwards) and logs
to logs/gapfill_1c_<date>.log. Years that already hold more records than
NIH now reports (records NIH has since dropped) are logged and left alone;
reconciling them is deferred.

Usage:
    ../.venv/bin/python3 1c_close_residual_gaps.py                      # every year
    ../.venv/bin/python3 1c_close_residual_gaps.py --start 2021 --end 2021
"""

import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import date, datetime
from pathlib import Path

from _nih_api import DUAL_PASS_CEILING, FALLBACK_AGENCIES, PAGINATION_CAP, fetch_paginated, get_total

BASE = Path(__file__).resolve().parent
RAW_DIR = BASE.parent / "data" / "raw"

FIRST_FISCAL_YEAR = 1985

CODE_RE = re.compile(r'"activity_code": "([^"]*)"')


def all_known_codes():
    """Every activity code in any year's file. A regex scan, not a JSON
    parse: about 10 seconds over all 42 files instead of several minutes."""
    codes = set()
    for path in sorted(RAW_DIR.glob("fy*.jsonl")):
        with path.open() as f:
            for line in f:
                codes.update(CODE_RE.findall(line))
    codes.discard("")
    return codes


def load_year(jsonl_path):
    """(seen_appl_ids, per-code counts, per-(agency, is_subproject) counts)
    for the records on file. The counts are what each bucket is compared
    against."""
    seen = set()
    by_code = Counter()
    by_agency_sub = Counter()
    with jsonl_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)
            appl_id = r.get("appl_id")
            if appl_id is None or appl_id in seen:
                continue
            seen.add(appl_id)
            by_code[r.get("activity_code")] += 1
            agency = (r.get("agency_ic_admin") or {}).get("abbreviation")
            by_agency_sub[(agency, r.get("subproject_id") is not None)] += 1
    return seen, by_code, by_agency_sub


def sha256_of(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


class YearRun:
    """State for one year: what's on file, what NIH reported for every
    query (for the manifest), and any bucket that came back short."""

    def __init__(self, year, seen, out_file, log):
        self.year = year
        self.seen = seen
        self.out = out_file
        self.log = log
        self.appended = 0
        self.totals_by_query = {}
        self.short_buckets = []

    def total(self, criteria, label):
        n = get_total(criteria)
        self.totals_by_query[label] = n
        return n

    def fetch(self, criteria, total, label):
        """Fetch one bucket (total <= DUAL_PASS_CEILING), append records not
        on file, and check the bucket's distinct appl_ids reach `total`."""
        returned = set()
        passes = ["asc"] if total <= PAGINATION_CAP else ["asc", "desc"]
        for sort_order in passes:
            for r in fetch_paginated(criteria, sort_order=sort_order, label=label):
                appl_id = r.get("appl_id")
                returned.add(appl_id)
                if appl_id not in self.seen:
                    self.seen.add(appl_id)
                    self.out.write(json.dumps(r) + "\n")
                    self.appended += 1
        if len(returned) < total:
            self.log(f"  WARNING: {label} returned {len(returned)}/{total} distinct records.")
            self.short_buckets.append({"bucket": label, "returned": len(returned), "total": total})
        return returned

    def fetch_split_by_agency(self, criteria, total, label, agencies):
        """A bucket over the dual-pass ceiling: one sub-bucket per agency."""
        self.log(f"  {label}: {total} exceeds dual-pass ceiling, splitting by agency")
        covered = 0
        for agency in sorted(agencies):
            sub = {**criteria, "agencies": [agency]}
            sub_label = f"{label}/{agency}"
            n = self.total(sub, sub_label)
            if n == 0:
                continue
            covered += n
            if n > DUAL_PASS_CEILING:
                self.log(f"  WARNING: {sub_label}: {n} still exceeds the dual-pass ceiling; not fetched.")
                self.short_buckets.append({"bucket": sub_label, "returned": 0, "total": n})
                continue
            self.fetch(sub, n, sub_label)
        if covered < total:
            self.log(f"  {label}: agency split covered {covered}/{total}; "
                     f"{total - covered} record(s) have an agency outside the list.")


def close_one_year(year, known_codes, log_path):
    jsonl_path = RAW_DIR / f"fy{year}.jsonl"
    manifest_path = RAW_DIR / f"fy{year}.gapfill_1c_manifest.json"
    if not jsonl_path.exists():
        print(f"FY{year}: no {jsonl_path.name}, skipping.")
        return

    log_lines = []

    def log(msg):
        print(msg, flush=True)
        log_lines.append(msg)

    seen, by_code, by_agency_sub = load_year(jsonl_path)
    before = len(seen)
    started = datetime.now()

    with jsonl_path.open("a") as out:
        run = YearRun(year, seen, out, log)
        year_criteria = {"fiscal_years": [year]}
        year_total = run.total(year_criteria, f"FY{year}")

        # Every year is checked bucket by bucket, even one whose count looks
        # complete: records NIH has since dropped can hide real gaps
        # (FY2024 after 1b: 1 over NIH's total, yet its R01 state split had
        # missed 214).
        log(f"FY{year}: {before} on file, NIH total {year_total} ({year_total - before:+d} vs file)")
        agencies = {a for a, _ in by_agency_sub if a} | set(FALLBACK_AGENCIES)

        # 1. codes from other years that this year's file has never seen
        unseen = sorted(known_codes - set(by_code))
        if unseen and run.total({**year_criteria, "activity_codes": unseen},
                                f"FY{year}/activity:unseen-in-year({len(unseen)})"):
            for code in unseen:
                crit = {**year_criteria, "activity_codes": [code]}
                n = run.total(crit, f"FY{year}/activity:{code}")
                if n:
                    by_code[code] = 0
                    log(f"  FY{year}/activity:{code}: {n} record(s), code not on file for this year")

        # 2. every code bucket whose count on file is short of NIH's.
        # No early exit on len(seen) >= year_total: a year can hold
        # records NIH has since dropped, which would mask a real gap.
        code_total_sum = 0
        for code in sorted(c for c in by_code if c):
            label = f"FY{year}/activity:{code}"
            crit = {**year_criteria, "activity_codes": [code]}
            n = run.totals_by_query.get(label)
            if n is None:
                n = run.total(crit, label)
            code_total_sum += n
            if n <= by_code[code]:
                continue
            log(f"  {label}: {by_code[code]} on file, NIH total {n}")
            if n <= DUAL_PASS_CEILING:
                run.fetch(crit, n, label)
            else:
                run.fetch_split_by_agency(crit, n, label, agencies)

        # 3. last resort: agency x subproject flag, when the code buckets
        # don't add up to the year (a code no file has ever held) or the
        # year is still short
        if code_total_sum < year_total or len(run.seen) < year_total:
            log(f"  FY{year}: activity codes cover {code_total_sum}/{year_total}, "
                f"{len(run.seen)} on file; checking agency x subproject buckets")
            for agency in sorted(agencies):
                for is_sub in (False, True):
                    flag = "sub_project_only" if is_sub else "exclude_subprojects"
                    label = f"FY{year}/{agency}/{flag}"
                    crit = {**year_criteria, "agencies": [agency], flag: True}
                    n = run.total(crit, label)
                    if n <= by_agency_sub[(agency, is_sub)]:
                        continue
                    log(f"  {label}: {by_agency_sub[(agency, is_sub)]} on file, NIH total {n}")
                    if n <= DUAL_PASS_CEILING:
                        run.fetch(crit, n, label)
                    else:
                        log(f"  WARNING: {label}: {n} exceeds the dual-pass ceiling; not fetched.")
                        run.short_buckets.append({"bucket": label, "returned": 0, "total": n})

        residual = year_total - len(run.seen)
        if residual > 0:
            log(f"  WARNING: FY{year} still missing {residual} record(s) after stage 1c.")
        log(f"FY{year}: appended {run.appended} record(s) ({before} -> {len(run.seen)}, NIH total {year_total})")

    manifest = {
        "fiscal_year": year,
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "before_count": before,
        "appended_count": run.appended,
        "after_count": len(run.seen),
        "nih_total": year_total,
        "residual_missing": residual,
        "short_buckets": run.short_buckets,
        "api_meta_total_by_query": run.totals_by_query,
        "data_file": jsonl_path.name,
        "data_file_bytes": jsonl_path.stat().st_size,
        "data_file_sha256": sha256_of(jsonl_path),
        "log": log_lines,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))

    with log_path.open("a") as f:
        f.write(f"\n=== FY{year} stage 1c ({started.isoformat(timespec='seconds')}) ===\n")
        f.write("\n".join(log_lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=int, default=FIRST_FISCAL_YEAR)
    parser.add_argument("--end", type=int, default=date.today().year)
    args = parser.parse_args()

    log_path = BASE.parent / "logs" / f"gapfill_1c_{date.today().isoformat()}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    known_codes = all_known_codes()
    years = list(range(args.start, args.end + 1))
    print(f"Stage 1c: FY{years[0]}-FY{years[-1]}, {len(known_codes)} activity codes known across all years.")
    print(f"Log: {log_path}\n")

    for year in years:
        close_one_year(year, known_codes, log_path)

    print("Done.")


if __name__ == "__main__":
    main()
