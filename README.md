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
| 2     | `src/2_load_to_sqlite.py` | Loads every`fy*.jsonl` into `data/nih_harvester.db` (table `nih_raw_projects`). Idempotent: hashes existing rows first, only inserts new / updates changed / skips unchanged  safe to rerun after 1_ adds more years later. |

```bash
./setup.sh                              # builds .venv, installs requests
cd src
../.venv/bin/python3 1_harvest_raw.py   # all years  takes hours; add start/end to test on a range
../.venv/bin/python3 1b_backfill_gaps.py # fills shortfalls stage 1 logged; skips years already complete
../.venv/bin/python3 2_load_to_sqlite.py
```

Then create the two derived views (either via the `sqlite3` CLI or Python's `sqlite3` module):

```bash
sqlite3 data/nih_harvester.db < ddl/nih_parsed_view.sql
sqlite3 data/nih_harvester.db < ddl/nih_indicators_view.sql
```

## Schema

`nih_raw_projects`  the landing table:

```
appl_id            INTEGER PRIMARY KEY    NIH's unique id per fiscal-year application record
project_num        TEXT
core_project_num   TEXT
fiscal_year        INTEGER
raw_json           TEXT NOT NULL          the full API record, verbatim
row_hash           TEXT NOT NULL          sha256 of raw_json's canonical form, for idempotent reloads
valid_from         TEXT
valid_to           TEXT                   unused for now (no removal handling yet); present for future use
is_current         INTEGER NOT NULL DEFAULT 1
```

`nih_raw_projects_current` is a view over `is_current = 1` if you are only interested in what actively being funded.

`nih_parsed` (`ddl/nih_parsed_view.sql`) then flattens `raw_json` into ~50 individual columns via SQLite's `json_extract()`: scalars, dates (as ISO8601 text), flattened nested objects (`organization_*`, `organization_type_*`, `geo_lat`/`geo_lon`, etc.), and short array fields as JSON text (multi-PI grants show as a JSON array string in one cell, not exploded into separate rows). Not exhaustive by design `raw_json` still has every field NIH returns; this covers what's likely to matter for querying.

`nih_indicators` (`ddl/nih_indicators_view.sql`) is layered on `nih_parsed`, adds a worked analytical example: multi-year-funding (MYF) classification, `BUDGET_DURATION_DAYS`/`PROJECT_DURATION_DAYS`, and a custom Oct-1 fiscal year keyed off `award_notice_date`. See the SQL file's header comment for why `FUNDING_CLASSIFICATION` has five values (`Multi-year Funding` / `Single Year Funding` / `Incremental Funding` / `Budget Exceeds Project` / `Missing Dates`), not the three a naive version started with using only a single `'Undefined Funding'` catch-all which was quietly conflating three very different situations (normal incremental annual funding, genuinely missing dates, and a small anomaly bucket), found by querying the actual data rather than assuming.

**Worth knowing about the MYF classification specifically:** it's *narrower* than "does this project span multiple years." It requires the *budget* period to exactly match the *project* period  which most individual fiscal-year records don't, because standard NIH practice is to fund one year at a time via annual non-competing continuations, even for awards that span many years. "Multi-year Funding" in NIH's own usage means the rare case where a project's entire multi-year budget was obligated in one lump sum up front, not simply "a project that runs more than a year."

## Data

Harvesting from scratch takes hours and pulls several GB of JSON. A pre-built copy of this dataset is published on Hugging Face: *([retrogradespace/nih-reporter-harvest](https://huggingface.co/datasets/retrogradespace/nih-reporter-harvest))*.

The pipeline writes `nih_harvester.db` in WAL journal mode, but `src/3_upload_to_huggingface.py` checkpoints it and switches it to rollback-journal (DELETE) mode before uploading, so the published file is a single self-contained database that opens read-only in any SQLite build. If you open a local WAL copy with `sqlite3 -readonly` and get `unable to open database file (14)`, that's Apple's `/usr/bin/sqlite3` refusing to open a WAL database read-only once its `-wal` file is gone. Open it without `-readonly`, use Python's `sqlite3` module or a newer `sqlite3` (e.g. Homebrew's), or run `sqlite3 data/nih_harvester.db "PRAGMA journal_mode=DELETE"` once.

NIH RePORTER data is a U.S. government work and in the public domain. See [NIH RePORTER&#39;s terms](https://reporter.nih.gov/) for the authoritative statement; this project adds no additional restrictions on top of that for the harvested data itself. The code in this repository is MIT-licensed (see `LICENSE`).

## Requirements

Python 3.9+, `requests` (see `requirements.txt`). SQLite support is via Python's standard library  no separate database install needed.

## Future Releases

Stage 1b has now run across every fiscal year. Most years land within 1-3 records of NIH's own total. The rest are records that neither the agency split nor the activity-code split can reach, mainly R01s with no U.S. `org_state` on file and activity codes that never appear anywhere else in that year's data. A targeted stage 1c pass for those is next. Running 1b writes the per-year counts to `logs/gapfill_*.log`.

# Happy Hacking Folk
