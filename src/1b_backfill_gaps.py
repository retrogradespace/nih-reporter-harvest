"""
nih-reporter-harvest, stage 1b second-pass gap backfill for fiscal years where
1_harvest_raw.py's own coverage check logged a shortfall (see
logs/harvest_*.log: "WARNING: FY<year> covered X/Y of N record(s) still
missing").

WHY A SEPARATE SCRIPT: 1_harvest_raw.py / _nih_api.py are intentionally left
untouched. The shortfall isn't a bug in that logic for any year over the
20,000 dual-pass ceiling, it splits by `agencies` (a year-specific discovery
sample unioned with a static fallback list), and the residual gap is
precisely the records that split can never reach: agency_ic_admin is null,
or a code that never showed up in that year's discovery sample or the
fallback list. Re-running the exact same agency-split logic finds the exact
same shortfall every time, since discovery is deterministic it needs a
genuinely different split dimension, not a retry.

SPLIT DIMENSION activity_code, not date. A date-based split (from_date/
to_date on award_notice_date / project_start_date) was tried first and
empirically failed for older years: probing FY1985 directly showed
award_notice_date populated on only 11 of 49,745 records, and
project_start_date on ~34,000 NIH's own admin metadata is sparse before
roughly 2011, and an explicit from/to range excludes null-valued records
entirely (only a wide-open, filter-free query matches nulls), so no amount
of bucket-widening can recover them.

activity_code (e.g. "R01", "T32") doesn't have this problem: it's part of
every award's project_num format (5T32ES007043-08 -> activity_code T32) and
was confirmed 0-null across all 49,745 FY1985 records already on file
structurally present the way agency_ic_admin structurally is NOT. Discovery
here uses every fy<year>.jsonl record already on file as the sample (not a
20k initial dual-pass) since that file already holds the large majority
of the year's records, its own activity_code distribution is a strictly
better source than resampling, and a single get_total() with the full
discovered-codes list as an OR-filter is used as a coverage self-check
(matches nationwide total exactly for FY1985) before spending any fetch
calls. Same escalate-only-as-needed shape as _nih_api.harvest_year: each
code buckets under the dual-pass ceiling gets one dual-pass; a code bucket
that somehow doesn't (not observed, but a future year's mechanism mix could
skew this) escalates to an org_state split under it, exactly mirroring
_nih_api.py's own agency -> org_state escalation.

APPEND-ONLY: new records are appended (not rewritten) to the existing
fy<year>.jsonl, and seen_appl_ids/discovered codes are both seeded from
that file's current contents, so fetch_bucket's own dedup guarantees
nothing already there is touched or duplicated. Stage 1's
fy<year>.manifest.json and fy<year>.done are left exactly as stage 1 wrote
them; this script writes its own fy<year>.gapfill_manifest.json and logs to
a separate logs/gapfill_*.log. 2_load_to_sqlite.py needs no changes either
it's already idempotent and keyed on appl_id, so rerunning it after this
picks up exactly the new rows.

Usage:
    ../.venv/bin/python3 1b_backfill_gaps.py                # all done years, auto-skips clean ones
    ../.venv/bin/python3 1b_backfill_gaps.py start 1985 end 1985   # a single year, for testing
"""

import argparse
import json
from datetime import date, datetime
from pathlib import Path

from _nih_api import DUAL_PASS_CEILING, US_STATES, fetch_bucket, get_total

BASE = Path(__file__).resolve().parent
RAW_DIR = BASE.parent / "data" / "raw"

FIRST_FISCAL_YEAR = 1985


def load_seen_state(jsonl_path):
    """(seen_appl_ids, discovered_activity_codes) from every record
    currently in `jsonl_path` the sample this backfill's discovery is
    based on, per this module's docstring."""
    seen_appl_ids = set()
    discovered_codes = set()
    with jsonl_path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            appl_id = record.get("appl_id")
            if appl_id is not None:
                seen_appl_ids.add(appl_id)
            code = record.get("activity_code")
            if code:
                discovered_codes.add(code)
    return seen_appl_ids, discovered_codes


def sweep_by_activity_code(year, codes, seen_appl_ids, log):
    """One escalation level: split FY`year` by each activity code in
    `codes`, dual-passing (via fetch_bucket) any non-empty bucket, and
    splitting further by org_state (mirroring _nih_api.py's own agency ->
    org_state escalation) for the rare bucket that's still over the
    dual-pass ceiling on its own. Yields records not already in
    seen_appl_ids (added as yielded, same contract as fetch_bucket)."""
    for code in sorted(codes):
        criteria = {"fiscal_years": [year], "activity_codes": [code]}
        total = get_total(criteria)
        if total == 0:
            continue
        label = f"FY{year}/activity:{code}"

        if total <= DUAL_PASS_CEILING:
            yield from fetch_bucket(criteria, total, label, seen_appl_ids, log)
        else:
            log(f"  {label}: {total} exceeds dual-pass ceiling, splitting by org_state")
            state_total_sum = 0
            for state in US_STATES:
                state_criteria = {**criteria, "org_states": [state]}
                state_total = get_total(state_criteria)
                if state_total == 0:
                    continue
                state_total_sum += state_total
                state_label = f"{label}/{state}"
                yield from fetch_bucket(state_criteria, state_total, state_label, seen_appl_ids, log)
            if state_total_sum < total:
                log(f"  WARNING: {label} org_state split covered {state_total_sum}/{total} "
                    f"-- {total - state_total_sum} record(s) have no US org_state and were "
                    f"NOT fetched by this pass.")


def backfill_one_year(year, log_path):
    jsonl_path = RAW_DIR / f"fy{year}.jsonl"
    gapfill_manifest_path = RAW_DIR / f"fy{year}.gapfill_manifest.json"

    if not jsonl_path.exists():
        print(f"FY{year}: no {jsonl_path.name} run 1_harvest_raw.py for this year first, skipping.")
        return

    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(msg)

    seen_appl_ids, discovered_codes = load_seen_state(jsonl_path)
    before_count = len(seen_appl_ids)

    year_total = get_total({"fiscal_years": [year]})
    shortfall = year_total - before_count

    if shortfall <= 0:
        log(f"FY{year}: {before_count} on file, nationwide total {year_total} already complete, skipping.")
        return

    log(f"FY{year}: {before_count} on file, nationwide total {year_total} ({shortfall} record(s) missing)")
    log(f"  discovered {len(discovered_codes)} activity codes from records on file -- {sorted(discovered_codes)}")

    covered_by_discovery = get_total({"fiscal_years": [year], "activity_codes": sorted(discovered_codes)})
    if covered_by_discovery < year_total:
        log(f"  WARNING: discovered activity codes only cover {covered_by_discovery}/{year_total} of the "
            f"year by this criteria alone -- {year_total - covered_by_discovery} record(s) likely use a "
            f"code not yet seen in this file; this pass may not close the full gap.")

    started = datetime.now()
    appended = 0

    with jsonl_path.open("a") as f:
        for record in sweep_by_activity_code(year, discovered_codes, seen_appl_ids, log):
            f.write(json.dumps(record) + "\n")
            appended += 1

    final_residual = year_total - len(seen_appl_ids)
    if final_residual > 0:
        log(f"  WARNING: FY{year} still missing {final_residual} record(s) after the activity_code sweep -- "
            f"likely a record with an activity code not present anywhere else in this year's data. "
            f"Left for a future, more targeted pass; nothing was overwritten.")

    log(f"FY{year}: appended {appended} record(s) to {jsonl_path.name} "
        f"({before_count} -> {len(seen_appl_ids)} total, nationwide total {year_total})")

    manifest = {
        "fiscal_year": year,
        "started_at": started.isoformat(timespec="seconds"),
        "finished_at": datetime.now().isoformat(timespec="seconds"),
        "before_count": before_count,
        "appended_count": appended,
        "after_count": len(seen_appl_ids),
        "nationwide_total_at_backfill_time": year_total,
        "residual_missing": final_residual,
        "log": log_lines,
    }
    gapfill_manifest_path.write_text(json.dumps(manifest, indent=2))

    with log_path.open("a") as f:
        f.write(f"\n=== FY{year} gapfill ({started.isoformat(timespec='seconds')}) ===\n")
        f.write("\n".join(log_lines) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=int, default=FIRST_FISCAL_YEAR)
    parser.add_argument("--end", type=int, default=date.today().year)
    args = parser.parse_args()

    log_path = RAW_DIR.parent.parent / "logs" / f"gapfill_{date.today().isoformat()}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    years = list(range(args.start, args.end + 1))
    print(f"Gap-backfilling FY{years[0]}-FY{years[-1]} ({len(years)} year(s)).")
    print(f"Log: {log_path}\n")

    for year in years:
        backfill_one_year(year, log_path)

    print("Done.")


if __name__ == "__main__":
    main()
