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
1985-present, with no institution scoping applied. 2,983,343 records
across 42 fiscal years, harvested to nationwide completeness (see
Coverage below) rather than filtered to any subset of institutions,
agencies, or award types.

See the harvesting code and full writeup at
https://github.com/retrogradespace/nih-reporter-harvest schema-on-read design,
the adaptive pagination strategy that gets full coverage despite NIH's
10,000-record pagination cap at nationwide volumes, and the two derived
analytical views described below.

## Files

- `nih_harvester.db` a SQLite database (~22 GB). Table `nih_raw_projects`
  holds one row per record with the full raw API response verbatim in
  `raw_json` (schema-on-read: nothing is dropped, typed, or restricted at
  load time). Two derived views are included:
  - `nih_parsed` flattens `raw_json` into ~50 individual columns
    (organization, dates, funding amounts, PI/investigator info, etc.) via
    SQLite's `json_extract()`.
  - `nih_indicators` layers a worked analytical example on `nih_parsed`:
    multi-year-funding (MYF) classification, budget/project duration in
    days, and a fiscal-year field keyed off `award_notice_date`.

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
  total for every query it made and the jsonl file's sha256.

## Schema (`nih_raw_projects`)

```
appl_id            INTEGER PRIMARY KEY   -- NIH's unique id per fiscal-year application record
project_num        TEXT
core_project_num   TEXT
fiscal_year        INTEGER
raw_json           TEXT NOT NULL         -- the full API record, verbatim
row_hash           TEXT NOT NULL         -- sha256 of raw_json's canonical form
valid_from         TEXT
valid_to           TEXT                  -- unused for now; reserved for future history tracking
is_current         INTEGER NOT NULL DEFAULT 1
```

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
- 30 records on file are no longer reported by NIH (FY2024: 1,
  FY2025: 14, FY2026: 15). They are kept. `is_current` is still 1 for
  them until removal handling is added.
- FY2026 (and possibly the tail of the most recent closed fiscal year) is
  a snapshot as of harvest time, not a closed-out year the federal fiscal
  year runs Oct 1-Sep 30, so the current year's totals will keep growing
  in NIH's own system after this snapshot was taken. Check the relevant
  `fy<year>.manifest.json`'s `finished_at` for exactly when each year was
  pulled.
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
