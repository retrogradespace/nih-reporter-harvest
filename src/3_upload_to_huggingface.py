"""
nih-reporter-harvest, stage 3 uploads the current local dataset to Hugging Face
Hub as a private dataset repo (retrogradespace/nih-reporter-harvest) that is later made public.

Rerunnable and incremental by design: this project is being uploaded in
waves as 1_harvest_raw.py completes more fiscal years, not all at once at
the end. Hugging Face's upload is content-addressed (via git/LFS under the
hood) rerunning this after more years finish only transfers what
changed, not the whole dataset again.

Uploads two things:
    data/nih_harvester.db          -> repo root, as nih_harvester.db
    data/raw/fy*.jsonl (+ stage 1, 1b and 1c manifests) -> repo's raw/ folder
(.done marker files are local bookkeeping only, not uploaded.)

Before the db goes up it's checkpointed and switched from WAL to DELETE
journal mode, so the published file is self-contained: no -wal/-shm
sidecars, and it opens read-only in any SQLite build (Apple's
/usr/bin/sqlite3 can't open a WAL db read-only once its -wal file is
gone). _sqlite_landing.connect() switches it back to WAL on the next
local write, so the harvest/load stages are unaffected.

Repo starts PRIVATE flip to public on huggingface.co once you've
reviewed the uploaded content, rather than this script deciding that.

Credentials: reads HF_TOKEN from ~/.config/nih_harvester/hf.env -- kept
outside version control entirely rather than as a gitignored in-repo file,
so there's no risk of it ever ending up in a commit.

Usage:
    ../.venv/bin/python3 3_upload_to_huggingface.py
"""

import sqlite3
from pathlib import Path

from huggingface_hub import HfApi

BASE = Path(__file__).resolve().parent
DATA_DIR = BASE.parent / "data"
HF_ENV_FILE = Path.home() / ".config" / "nih_harvester" / "hf.env"

REPO_ID = "retrogradespace/nih-reporter-harvest"
REPO_TYPE = "dataset"

DATASET_CARD = """\
---
license: other
license_name: us-government-public-domain
license_link: https://reporter.nih.gov/
pretty_name: NIH RePORTER Harvest
task_categories:
  - tabular-classification
  - tabular-regression
tags:
  - nih
  - grants
  - funding
  - research-funding
  - government
  - biomedical
size_categories:
  - 1M<n<10M
---

# NIH RePORTER Harvest

A complete, nationwide pull of NIH RePORTER project records, fiscal years
1985-present, with no institution scoping applied. 2,983,713 current
records across 43 fiscal years (FY1985-FY2027), harvested to nationwide
completeness (see Coverage below) rather than filtered to any subset of
institutions, agencies, or award types.

**As of:** data current through NIH's update loaded by weekly refresh run
`2026-10-26T190005Z` (see `nih_refresh_runs`).

## History

| Load | Date | New | Changed | Removed | Current records |
|------|------|----:|--------:|--------:|----------------:|
| Initial harvest (stages 1, 1b, 1c) | 2026-10-18 | 2,983,341 | | | 2,983,341 |
| Weekly refresh `2026-10-20T010108Z` | 2026-10-20 | 129 | 82,468 | 33 | 2,983,437 |
| Weekly refresh `2026-10-26T190005Z` | 2026-10-26 | 283 | 3,552 | 7 | 2,983,713 |

Changed means a new version of an existing record; the old version is
kept (`is_current = 0`). The first refresh's large change count is the
first comparison against the harvest, which was loaded between
2026-08-18 and 2026-10-18. The five child tables were added on 2026-10-20.

See the harvesting code and full writeup at
https://github.com/retrogradespace/nih-reporter-harvest schema-on-read design,
the adaptive pagination strategy that gets full coverage despite NIH's
10,000-record pagination cap at nationwide volumes, and the derived
views and child tables described below.

## Files

- `nih_harvester.db` a SQLite database (~30 GB). Table `nih_raw_projects`
  holds every version of every record with the full raw API response
  verbatim in `raw_json` (schema-on-read: nothing is dropped, typed, or
  restricted at load time). Two derived views are included:
  - `nih_parsed` flattens `raw_json` into ~50 individual columns
    (organization, dates, funding amounts, PI/investigator info, etc.) via
    SQLite's `json_extract()`.
  - `nih_indicators` layers a worked analytical example on `nih_parsed`:
    multi-year-funding (MYF) classification, budget/project duration in
    days, and a fiscal-year field keyed off `award_notice_date`.

  Five child tables explode the array fields `nih_parsed` keeps as JSON
  text into one row per element: `nih_principal_investigators`,
  `nih_program_officers`, `nih_agency_ic_fundings`,
  `nih_spending_categories` and `nih_covid_response`. Each row carries the
  project's identifying columns (appl_id, project_num, fiscal_year, title,
  contact PI, activity code, administering IC, organization), so one table
  works on its own as a Tableau or pandas source. Identifiers are text;
  the only numbers are `nih_agency_ic_fundings`' per-institute
  `ic_*_cost` dollars. Project-level dollars are left out on purpose (they
  would be double-counted by a SUM): use `nih_parsed` for totals. They are
  rebuilt after every weekly load; `ddl/nih_child_tables.sql` in the
  GitHub repo documents every column and rule.

  The file is a single self-contained database in rollback-journal
  (DELETE) mode, not WAL, so it opens with any SQLite reader, including
  read-only. The pipeline switches a local copy to WAL on its next write.
- `raw/fy<year>.jsonl` (~20 GB total) the original per-fiscal-year raw
  pulls this database was built from, one JSON record per line exactly as
  returned by the API, before any loading or transformation. Each year has
  a matching `fy<year>.manifest.json` recording when it was harvested and
  how many records it produced, plus `fy<year>.gapfill_manifest.json`
  (stage 1b) and `fy<year>.gapfill_1c_manifest.json` (stage 1c) for the
  records the gap-fill passes appended. The 1c manifest lists NIH's own
  total for every query it made and the jsonl file's sha256. These are the
  initial harvest (2026-10-18): weekly refresh changes are in the
  database, not in `raw/`.

## Schema (`nih_raw_projects`)

One row per version of each record. Use `nih_raw_projects_current`
(`is_current = 1`) for one row per record NIH currently reports; the
views read it.

```
version_id         INTEGER PRIMARY KEY   -- one per version
appl_id            INTEGER NOT NULL      -- NIH's unique id per fiscal-year application record
project_num        TEXT
core_project_num   TEXT
fiscal_year        INTEGER
raw_json           TEXT NOT NULL         -- the full API record, verbatim
row_hash           TEXT NOT NULL         -- sha256 of raw_json's canonical form
valid_from         TEXT NOT NULL         -- when this version was first loaded
valid_to           TEXT                  -- when it was superseded or dropped by NIH (NULL = current)
is_current         INTEGER NOT NULL      -- 1 = latest known version
```

`nih_change_log` records every new, changed (per top-level field) and
removed record found by each weekly refresh. History starts at the first
weekly refresh (October 2026).

`nih_refresh_runs` has one row per weekly load, even one with no changes:
`run_id` (UTC, sorts in run order), `loaded_at`, `records_pulled`,
`new_count`, `changed_count`, `removed_count`, and `delta_file` /
`delta_sha256` naming that week's change file (kept locally by the
pipeline, not published here). Its latest `run_id` is the "as of" for
this database.

## Coverage and known limitations

- Harvested to nationwide completeness via dual-pass pagination (asc+desc)
  for buckets under NIH's 20,000-record ceiling, escalating to a
  discovered-agency split (verified against a fallback list of current and
  historical/defunct agency codes) for any bucket over that. Every split
  level's fetched count is checked against that level's own unfiltered
  total from the API, and any shortfall is logged rather than silently
  dropped see each year's `manifest.json` / the GitHub repo's
  `logs/harvest_*.log` convention for those warnings.
- Two gap-fill passes followed the first harvest. Stage 1b re-split short
  years by activity code. Stage 1c (2026-10-18) compared every year's
  records on file with NIH's totals per activity code and per agency x
  subproject flag, and fetched every bucket that was short. After 1c,
  **every fiscal year holds every record NIH reports for it.** Each
  bucket 1c fetched had to return NIH's full count.
- A record NIH stops reporting is closed out (`is_current = 0`, logged
  as `REMOVED`) rather than deleted. Run `2026-10-20T010108Z` closed out
  33 (FY2024: 1, FY2025: 13, FY2026: 19); run `2026-10-26T190005Z`
  closed out 7 (all FY2026).
- FY2026, FY2027 (and possibly the tail of the most recent closed fiscal
  year) are a snapshot as of the latest refresh, not closed-out years the
  federal fiscal year runs Oct 1-Sep 30, so the current year's totals
  will keep growing in NIH's own system. `raw/`'s
  `fy<year>.manifest.json` `finished_at` says when each year was first
  pulled; `nih_refresh_runs` says when it was last refreshed.
- **Worth knowing about the MYF classification specifically:** it's
  narrower than "does this project span multiple years." It requires the
  *budget* period to exactly match the *project* period, which most
  individual fiscal-year records don't, since standard NIH practice is to
  fund one year at a time via annual non-competing continuations. "Multi-
  year Funding" in NIH's own usage means the rarer case where a project's
  entire multi-year budget was obligated in one lump sum up front.

## Data source and license

NIH RePORTER (https://reporter.nih.gov/) is a U.S. government work and in
the public domain. This dataset adds no additional restrictions beyond
that see NIH RePORTER's own terms for the authoritative statement.

## Reproduction

The full harvesting, loading, and view-creation pipeline (plus the
upload script that produced this dataset) is at
https://github.com/retrogradespace/nih-reporter-harvest, along with the design
rationale for schema-on-read and the pagination strategy. 
"""


def load_env(path):
    env = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip()
    return env


def prepare_db_for_upload(db_path):
    """Fold the WAL into the main file and leave it in DELETE journal mode.

    Aborts (rather than uploading a partial or WAL-dependent file) if
    another process still has the db open, or if quick_check fails.
    """
    conn = sqlite3.connect(db_path)
    try:
        busy, _, _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if busy:
            raise SystemExit(f"{db_path.name} is in use by another process (checkpoint blocked) -- close it and rerun.")
        mode = conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
        if mode != "delete":
            raise SystemExit(f"Could not switch {db_path.name} to DELETE journal mode (still '{mode}') -- is it open elsewhere?")
        print(f"Running quick_check on {db_path.name} (a few minutes at full size)...")
        result = conn.execute("PRAGMA quick_check").fetchone()[0]
        if result != "ok":
            raise SystemExit(f"quick_check failed on {db_path.name}: {result}")
    except sqlite3.OperationalError as e:
        raise SystemExit(f"Could not prepare {db_path.name} for upload: {e}")
    finally:
        conn.close()

    for suffix in ("-wal", "-shm"):
        sidecar = db_path.with_name(db_path.name + suffix)
        if sidecar.exists():
            raise SystemExit(f"{sidecar.name} still present after switching to DELETE mode -- is the db open elsewhere?")
    print(f"  {db_path.name} checkpointed, DELETE journal mode, quick_check ok")


def main():
    if not HF_ENV_FILE.exists():
        raise SystemExit(f"No hf.env found at {HF_ENV_FILE}. Create it with HF_TOKEN=... first.")
    token = load_env(HF_ENV_FILE)["HF_TOKEN"]

    # Before anything touches the network, so a failed check uploads nothing.
    db_path = DATA_DIR / "nih_harvester.db"
    if db_path.exists():
        prepare_db_for_upload(db_path)

    api = HfApi(token=token)

    api.create_repo(repo_id=REPO_ID, repo_type=REPO_TYPE, private=True, exist_ok=True)
    print(f"Repo ready: https://huggingface.co/datasets/{REPO_ID} (private)")

    # Dataset card first small, fast, and makes the repo self-explanatory
    # even if the rest of the upload is still running.
    card_path = DATA_DIR / "_dataset_card_README.md"
    card_path.write_text(DATASET_CARD)
    api.upload_file(
        path_or_fileobj=str(card_path),
        path_in_repo="README.md",
        repo_id=REPO_ID,
        repo_type=REPO_TYPE,
    )
    card_path.unlink()
    print("Uploaded dataset card (README.md)")

    if db_path.exists():
        print(f"Uploading {db_path.name} ({db_path.stat().st_size / 1e9:.2f} GB)...")
        api.upload_file(
            path_or_fileobj=str(db_path),
            path_in_repo="nih_harvester.db",
            repo_id=REPO_ID,
            repo_type=REPO_TYPE,
        )
        print("  done")
    else:
        print(f"WARNING: {db_path} not found -- skipping (run 2_load_to_sqlite.py first)")

    raw_dir = DATA_DIR / "raw"
    # fy*manifest.json covers stage 1's fy<year>.manifest.json and the
    # gap-fill manifests from 1b and 1c (fy<year>.gapfill_manifest.json,
    # fy<year>.gapfill_1c_manifest.json).
    raw_files = sorted(raw_dir.glob("fy*.jsonl")) + sorted(raw_dir.glob("fy*manifest.json"))
    if raw_files:
        total_gb = sum(p.stat().st_size for p in raw_files) / 1e9
        print(f"Uploading {len(raw_files)} raw file(s) ({total_gb:.2f} GB total) to raw/...")
        api.upload_folder(
            folder_path=str(raw_dir),
            path_in_repo="raw",
            repo_id=REPO_ID,
            repo_type=REPO_TYPE,
            allow_patterns=["fy*.jsonl", "fy*manifest.json"],
        )
        print("  done")
    else:
        print(f"WARNING: no fy*.jsonl files found in {raw_dir} -- skipping")

    print(f"\nUpload complete: https://huggingface.co/datasets/{REPO_ID}")


if __name__ == "__main__":
    main()
