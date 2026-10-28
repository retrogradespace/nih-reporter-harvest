# NIH Harvester

This repository provides a validated way to pull national level data out of NIH RePORTER. This pulls a full project record (FY1985-present) into a local SQLite database, with a schema-on-read design that stores each record's full raw JSON verbatim rather than flattening it into a fixed set of columns at load time. Then provides two derived views that flatten it back out for querying, and a worked example of building an analytical indicator (multi-year-funding classification) on top of it.

This pipeline was validated on a single institution's own NIH-funded portfolio, then got generalized for use as a tutorial because this API took a while: no institution scoping, and a pagination strategy that actually works at nationwide scale (NIH's own API caps pagination at 10,000 records per query nationwide fiscal-year volumes run 50,000-94,000).

## Why schema-on-read

The most common way to pull an API like this into a database is to flatten each response into one typed column per field as you load it. That's fragile in a specific way: if you also restrict *which* fields you request (a common optimization afterall why pull 45 fields when you only use 10 right?), those two decisions: 1) which fields to request, and 2)which columns to store; end up being made twice, in two different places, and either one can silently drop data if it's wrong. Which is exactly what happened to me. Also, if a field is added later or a value grows past whatever width you picked for its column, the data just vanishes with no error.

This project avoids both failure modes: it never restricts which fields the API returns (`include_fields` is simply never set), and it stores each record's raw JSON response verbatim in one column, with only a handful of fields (`appl_id`, `project_num`, `core_project_num`, `fiscal_year`) promoted out for indexing. Nothing is typed, truncated, or renamed on the way in. The `nih_parsed` view (see below) flattens it back out for convenient querying, but that's a read-time convenience layered on top of an unmodified archive  if NIH adds a field tomorrow, it's already sitting in `raw_json` for every future pull, no pipeline change required.

## The actual hard problem: pagination at scale

NIH RePORTER's `/v2/projects/search` endpoint caps `offset + limit` at 10,000 so you can never page past the first 10,000 matches of a given query, no matter how many more exist. That's a non-issue for a single institution (a few hundred records/year). It's a real problem nationwide: fiscal-year totals run 50,000-94,000, and even the single largest NIH institute (NCI) in a peak year is 12,590 on its own over the cap by itself.

So this pull uses two techniques, escalated only as needed:

1. **Dual-pass.** Paginate the same query once sorted ascending by `appl_id`, once descending. Since each pass can reach up to 10,000 records, and the two windows necessarily overlap once the true total is under 20,000, their union is guaranteed to cover every record this was verified against a real 12,590-record bucket (NCI, FY2010): **exactly 12,590/12,590, 100% coverage**, with no dependency on any secondary filter.
2. **Split by agency, only for buckets that exceed 20,000.** An earlier version of this splitting used NIH's ~27 current institutes/centers as a fixed list that measurably wasn't enough: it covered only 70% of FY1985, missing defunct agency codes (e.g. `NCRR`, dissolved in 2011) and non-NIH Public Health Service agencies (`AHRQ`, `FDA`) that also appear in this data. **No fixed list is reliable across 40 years of agency reorganizations.** The fix: discover the actual agency codes present from a real sample of that year's data (the dual-pass sample is real data, not thrown away after discovery), cross-checked against a broad empirically-derived fallback list. This closed the gap from 70% to 99.4%+ coverage, with any residual gap logged explicitly rather than silently dropped.

See `src/_nih_api.py`'s module docstring for the full design writeup, including why a third split level (originally `org_states`) was abandoned as it introduced its own blind spot and silently missed foreign-funded records with no U.S. state on file.

### NIH API Documentation Stale

Every criteria field tested during this project's development silently accepts its documented camelCase spelling (matching NIH's own published swagger spec) but **does nothing** no error, just quietly ignored, returning the unfiltered result set. The working spelling is snake_case, undocumented anywhere:

| Field             | camelCase (swagger spec)                  | snake_case (what actually works)             |
| ----------------- | ----------------------------------------- | -------------------------------------------- |
| Organization type | `organizationType`                      | `organization_type`                        |
| Date added        | `dateAdded` / `fromDate` / `toDate` | `date_added` / `from_date` / `to_date` |
| Activity codes    | `activityCodes`                         | `activity_codes`                           |
| Org states        | `orgStates`                             | `org_states`                               |

Four for four is enough to call this a general rule for this API, not a per-field quirk: if you add a new criteria field, verify it with a before/after `meta.total` comparison rather than trusting the spec.

## Pipeline

| Stage | Script                      | What it does                                                                                                                                                                                                                      |
| ----- | --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1     | `src/1_harvest_raw.py`    | Nationwide pull, every fiscal year. One JSONL file per year (`data/raw/fy<year>.jsonl`) so an interruption only costs the in-progress year  a `.done` marker means a rerun skips it.                                          |
| 1b    | `src/1b_backfill_gaps.py` | Second pass for years where stage 1 logged a shortfall. Splits by `activity_code` instead of agency, and only appends records that aren't already in `fy<year>.jsonl`. Writes its own `fy<year>.gapfill_manifest.json` and `logs/gapfill_*.log`; stage 1's manifest and `.done` marker are left alone. |
| 1c    | `src/1c_close_residual_gaps.py` | Checks every year bucket by bucket: counts the records on file per activity code (codes seen in any year, not just this one), then per agency x subproject flag, compares each count with NIH's total for that bucket, and fetches only the short ones. Oversized buckets split by agency, not state, so records with no U.S. state are reachable. Every fetched bucket must return NIH's full count. Appends only; writes `fy<year>.gapfill_1c_manifest.json` (NIH's total for every query, plus the file's sha256) and `logs/gapfill_1c_*.log`. |
| 2     | `src/2_load_to_sqlite.py` | Initial build: loads every `fy*.jsonl` into `data/nih_harvester.db` (table `nih_raw_projects`). Insert-only: an `appl_id` already in the table is never touched, so it can't overwrite newer versions from the weekly refresh with the older harvest files. |
| 4     | `src/4_weekly_refresh_pull.py` | Weekly re-pull of every fiscal year (FY1985 through next FY), about three hours. Each year must come back with exactly NIH's total, bucket by bucket, or the pull fails and nothing is loaded. Writes a temporary snapshot to `data/weekly/<run_id>/` and, only when every year is complete, `data/weekly/<run_id>.manifest.json` (NIH's total for every query, each year's count and sha256). A failed pull resumes with the same `--run-id`. |
| 5     | `src/5_load_weekly_refresh.py` | Loads that pull. New records are inserted; a changed record's current version is closed out and the new one inserted; a record NIH no longer returns is closed out (never deleted). Every difference goes to `nih_change_log`. Stops without committing if more than `--max-removals` (default 100) would be removed. Then writes the week's changes to `data/weekly/delta_<run_id>.jsonl.gz` and deletes the full snapshot. |
| 6     | `src/6_run_weekly_refresh.py` | The scheduled job: 4 then 5 with one run id, then 7, logged to `logs/weekly_refresh_<date>.log`. `src/schedule_weekly.py` installs it (macOS launchd, Mondays 15:00; prints a crontab line elsewhere). |
| 7     | `src/7_build_child_tables.py` | Rebuilds the five child tables (`ddl/nih_child_tables.sql`) from the current records in one transaction, then runs `ddl/nih_spending_categories_check.sql` and logs any rows it returns as a WARNING. About 25 minutes at ~3M records; about 5 GB of tables. |

`src/migrate_to_versions.py` converted a pre-versioning database (one row per `appl_id`) to this layout on 2026-10-18. It refuses to run on a database that's already versioned.

```bash
./setup.sh                              # builds .venv, installs requests
cd src
../.venv/bin/python3 1_harvest_raw.py   # all years  takes hours; add start/end to test on a range
../.venv/bin/python3 1b_backfill_gaps.py # fills shortfalls stage 1 logged; skips years already complete
../.venv/bin/python3 1c_close_residual_gaps.py # closes what 1b couldn't reach; checks every year
../.venv/bin/python3 2_load_to_sqlite.py
../.venv/bin/python3 7_build_child_tables.py   # child tables; 6_ rebuilds them after every weekly load
../.venv/bin/python3 6_run_weekly_refresh.py   # then weekly: brings every record up to date, keeps every version
../.venv/bin/python3 schedule_weekly.py        # optional: run 6_ every Monday at 15:00
```

Then create the two derived views (either via the `sqlite3` CLI or Python's `sqlite3` module):

```bash
sqlite3 data/nih_harvester.db < ddl/nih_parsed_view.sql
sqlite3 data/nih_harvester.db < ddl/nih_indicators_view.sql
```

### Waking the Mac for the run

macOS keeps only one *repeating* wake per machine (`pmset repeat`), and on the machine this was built on it is already used by another weekly job. So the Monday 15:00 run is covered by one-off wakes instead, a year at a time. Run this once in Terminal (it asks for your password once):

```bash
for i in $(seq 0 51); do sudo pmset schedule wake "$(date -v+mon -v+${i}w '+%m/%d/%y') 14:55:00"; done
pmset -g sched    # lists them
```

The job holds a `caffeinate` assertion while it runs, so the Mac stays awake for the whole three hours. Each run's log warns when fewer than 4 of these wakes are left. A Mac that is shut down, or restarted and not logged in, skips the run: the job is a LaunchAgent and runs only in a logged-in session.

## Schema

`nih_raw_projects` -- the landing table, one row per **version** of each record:

```
version_id         INTEGER PRIMARY KEY    one per version
appl_id            INTEGER NOT NULL       NIH's unique id per fiscal-year application record
project_num        TEXT
core_project_num   TEXT
fiscal_year        INTEGER
raw_json           TEXT NOT NULL          the full API record, verbatim
row_hash           TEXT NOT NULL          sha256 of raw_json's canonical form
valid_from         TEXT NOT NULL          when this version was first loaded
valid_to           TEXT                   when it was superseded or NIH dropped it (NULL = current)
is_current         INTEGER NOT NULL       1 = latest known version; at most one per appl_id
```

`nih_raw_projects_current` is a view over `is_current = 1`: one row per record NIH currently reports. The views below read it.

`nih_change_log` has one row per changed top-level field per record per weekly run, plus one row per new or removed record: `run_id`, `detected_at` (load time, not when NIH made the change), `appl_id`, `project_num`, `fiscal_year`, `change_type` (`NEW` / `CHANGED` / `REMOVED`), `field_name`, `old_value` and `new_value` (the field's JSON; for `REMOVED`, `old_value` is the whole last-known record), `found_by`. It starts at the first weekly refresh; before that the table was overwritten in place and kept no history.

`nih_refresh_runs` has one row per weekly load, including weeks with no changes: counts of records pulled, new, changed and removed, and the delta file's name and sha256.

```sql
-- every dollar change, newest first
SELECT detected_at, appl_id, project_num, field_name, old_value, new_value
  FROM nih_change_log
 WHERE field_name IN ('award_amount', 'direct_cost_amt', 'indirect_cost_amt')
 ORDER BY detected_at DESC;
```

`nih_parsed` (`ddl/nih_parsed_view.sql`) then flattens `raw_json` into ~50 individual columns via SQLite's `json_extract()`: scalars, dates (as ISO8601 text), flattened nested objects (`organization_*`, `organization_type_*`, `geo_lat`/`geo_lon`, etc.), and short array fields as JSON text (multi-PI grants show as a JSON array string in one cell, not exploded into separate rows). Not exhaustive by design `raw_json` still has every field NIH returns; this covers what's likely to matter for querying.

`nih_indicators` (`ddl/nih_indicators_view.sql`) is layered on `nih_parsed`, adds a worked analytical example: multi-year-funding (MYF) classification, `BUDGET_DURATION_DAYS`/`PROJECT_DURATION_DAYS`, and a custom Oct-1 fiscal year keyed off `award_notice_date`. See the SQL file's header comment for why `FUNDING_CLASSIFICATION` has five values (`Multi-year Funding` / `Single Year Funding` / `Incremental Funding` / `Budget Exceeds Project` / `Missing Dates`), not the three a naive version started with using only a single `'Undefined Funding'` catch-all which was quietly conflating three very different situations (normal incremental annual funding, genuinely missing dates, and a small anomaly bucket), found by querying the actual data rather than assuming.

**Worth knowing about the MYF classification specifically:** it's *narrower* than "does this project span multiple years." It requires the *budget* period to exactly match the *project* period  which most individual fiscal-year records don't, because standard NIH practice is to fund one year at a time via annual non-competing continuations, even for awards that span many years. "Multi-year Funding" in NIH's own usage means the rare case where a project's entire multi-year budget was obligated in one lump sum up front, not simply "a project that runs more than a year."

Five child tables (`ddl/nih_child_tables.sql`, rebuilt by `src/7_build_child_tables.py`) explode the array fields `nih_parsed` keeps as JSON text into one row per element: `nih_principal_investigators`, `nih_program_officers`, `nih_agency_ic_fundings`, `nih_spending_categories` and `nih_covid_response`. Each row carries the project's identifying columns (`appl_id`, `project_num`, `fiscal_year`, title, contact PI, activity code, administering IC, organization name / IPF code / state, start and end dates), so one table works on its own as a Tableau or pandas source. They're tables rather than views because unpacking ~3M records' arrays at query time is slow. Identifiers and labels (`appl_id`, `fiscal_year`, `organization_org_ipf_code`, every `*_seq`, `pi_profile_id`, `funding_fy`, `spending_category_id`) are text, booleans are `'true'`/`'false'`, and the only numbers are the per-institute `ic_*_cost` dollars. Project-level dollars are left out on purpose: they would repeat on every element row and be double-counted by a SUM. The SQL file's header covers the details, including how spending category names are paired with their ids.

## Data model

[`docs/data_model.md`](docs/data_model.md) has every table and view above as an ER diagram of the key and join columns, and a data dictionary of every column. It's generated from the database's own catalog, so don't edit it by hand. The database declares no foreign keys: the joins are listed in `tools/relationships.yml`, and each run checks every one against the data (orphans, and duplicates on the side that should be unique) and documents any that doesn't hold cleanly, with its counts.

Regenerate after any change to a table, a view or `relationships.yml`, while the weekly job isn't running (about 3 minutes; it opens the database read-only):

```bash
.venv/bin/python3 tools/make_data_model.py
```

It exits 2 without writing anything if `relationships.yml` names an object or column the database doesn't have.

## Data

Harvesting from scratch takes hours and pulls several GB of JSON. A pre-built copy of this dataset is published on Hugging Face: *([retrogradespace/nih-reporter-harvest](https://huggingface.co/datasets/retrogradespace/nih-reporter-harvest))*.

The pipeline writes `nih_harvester.db` in WAL journal mode, but `src/3_upload_to_huggingface.py` checkpoints it and switches it to rollback-journal (DELETE) mode before uploading, so the published file is a single self-contained database that opens read-only in any SQLite build. If you open a local WAL copy with `sqlite3 -readonly` and get `unable to open database file (14)`, that's Apple's `/usr/bin/sqlite3` refusing to open a WAL database read-only once its `-wal` file is gone. Open it without `-readonly`, use Python's `sqlite3` module or a newer `sqlite3` (e.g. Homebrew's), or run `sqlite3 data/nih_harvester.db "PRAGMA journal_mode=DELETE"` once.

NIH RePORTER data is a U.S. government work and in the public domain. See [NIH RePORTER&#39;s terms](https://reporter.nih.gov/) for the authoritative statement; this project adds no additional restrictions on top of that for the harvested data itself. The code in this repository is MIT-licensed (see `LICENSE`).

## Requirements

Python 3.9+, `requests`, and `PyYAML` for `tools/make_data_model.py` (see `requirements.txt`). SQLite support is via Python's standard library  no separate database install needed.

## Future Releases

Stage 1c ran across every fiscal year on 2026-10-18 and added 741 records. Every fiscal year now holds every record NIH reports for it (2,983,343 records on file). Most of what 1b missed had activity codes that never appear anywhere else in that year's file (43 U61 records in FY2010, for example), plus a few VA records with no activity code at all. The R01s with no U.S. state that 1b's log flagged had already been fetched by stage 1's agency split.

30 records in the harvest files are no longer reported by NIH (FY2024: 1, FY2025: 14, FY2026: 15). Two of them are records NIH moved to a later fiscal year (type 7 awards, appl_ids 11337819 and 11398315); the database already holds their later-year version. The first weekly refresh closes out the rest as `REMOVED`.

Versioning and the weekly refresh were added on 2026-10-18; the first weekly refresh (run `2026-10-20T010108Z`) closed out 33 records as `REMOVED` (FY2024: 1, FY2025: 13, FY2026: 19), added 129 and updated 82,468, leaving 2,983,437 current records. The five child tables were added on 2026-10-20. The second (run `2026-10-26T190005Z`) added 283, updated 3,552 and closed out 7 (all FY2026), leaving 2,983,713.

Hugging Face now has the database as of the second run, with the child tables, `nih_change_log` and `nih_refresh_runs`, plus the original `raw/` harvest files (which don't include weekly changes). The published copy is updated by hand with `src/3_upload_to_huggingface.py`, not by the weekly job, so it lags the local database by however many runs have happened since; its dataset card names the run it's current through. Publishing each week's changes automatically is next.

# Happy Hacking Folk
