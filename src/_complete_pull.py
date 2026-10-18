"""
Complete, verified pull of one fiscal year, nationwide -- the weekly
refresh's fetch (4_weekly_refresh_pull.py). Same splitting ideas as stages
1/1b/1c, but strict: a year either comes back with exactly NIH's total or
the pull raises IncompletePull and nothing gets loaded. A short pull must
never reach the loader, where its missing records would look like removals.

PER YEAR:

1. Year at or under the 20,000 dual-pass ceiling: one dual-pass.
2. Otherwise split by activity code. The codes to try are this year's codes
   from the last successful pull (or the raw files, the first time) plus,
   if one OR-filter probe says any match, codes seen in any other year.
   Each code bucket is one dual-pass; the one code over the ceiling (R01,
   ~25-30k a year) is split by agency, then by state if that falls short,
   then by two more sort orders (award_amount asc/desc) as a last window.
3. If the year is still short (records with no activity code at all exist,
   e.g. two VA records in FY2021), compare what was fetched per (agency,
   subproject flag) with NIH's total for that bucket and fetch the short
   ones whole. The two subproject flags partition a year exactly.

EVERY BUCKET IS VERIFIED: the distinct appl_ids returned for a bucket must
reach its meta.total. _nih_api.fetch_paginated stops quietly on an
exhausted retry; a short bucket is retried once, then raises.

Every meta.total asked for is kept (api_meta_total_by_query) for the run's
manifest.
"""

import json
import re
from collections import Counter

from _nih_api import (DUAL_PASS_CEILING, FALLBACK_AGENCIES, PAGINATION_CAP, US_STATES,
                      fetch_paginated, get_total)

CODE_RE = re.compile(r'"activity_code": "([^"]*)"')
AGENCY_RE = re.compile(r'"agency_ic_admin": \{"code": "[^"]*", "abbreviation": "([^"]*)"')


class IncompletePull(Exception):
    pass


def hints_from_raw_files(raw_dir):
    """{"codes_by_year": {year: [...]}, "agencies_by_year": {year: [...]}}
    from data/raw/fy*.jsonl -- a regex scan, about 10 seconds. Used only
    when no earlier weekly pull has saved its own."""
    codes, agencies = {}, {}
    for path in sorted(raw_dir.glob("fy*.jsonl")):
        year = path.stem.removeprefix("fy")
        c, a = set(), set()
        with path.open() as f:
            for line in f:
                c.update(CODE_RE.findall(line))
                a.update(AGENCY_RE.findall(line))
        codes[year] = sorted(c - {""})
        agencies[year] = sorted(a - {""})
    return {"codes_by_year": codes, "agencies_by_year": agencies}


class YearPull:
    def __init__(self, year, write, log):
        self.year = year
        self.write = write          # called with each new record
        self.log = log
        self.seen = set()
        self.by_agency_sub = Counter()
        self.codes = set()
        self.agencies = set()
        self.totals_by_query = {}

    def total(self, criteria, label):
        n = get_total(criteria)
        self.totals_by_query[label] = n
        return n

    def _take(self, r):
        appl_id = r.get("appl_id")
        if appl_id in self.seen:
            return
        self.seen.add(appl_id)
        agency = (r.get("agency_ic_admin") or {}).get("abbreviation")
        self.by_agency_sub[(agency, r.get("subproject_id") is not None)] += 1
        if r.get("activity_code"):
            self.codes.add(r["activity_code"])
        if agency:
            self.agencies.add(agency)
        self.write(r)

    def _passes(self, criteria, passes, label):
        returned = set()
        for sort_field, sort_order in passes:
            for r in fetch_paginated(criteria, sort_order=sort_order, label=label, sort_field=sort_field):
                returned.add(r.get("appl_id"))
                self._take(r)
        return returned

    def bucket(self, criteria, n, label):
        """One bucket at or under the dual-pass ceiling; retried once if short."""
        passes = [("appl_id", "asc")] if n <= PAGINATION_CAP else [("appl_id", "asc"), ("appl_id", "desc")]
        for attempt in (1, 2):
            returned = self._passes(criteria, passes, label)
            if len(returned) >= n:
                return returned
            self.log(f"  {label}: returned {len(returned)}/{n} on attempt {attempt}")
        raise IncompletePull(f"{label}: returned {len(returned)}/{n} after a retry")

    def oversized(self, criteria, n, label, agencies):
        """A bucket over the dual-pass ceiling: union of an agency split, a
        state split if needed, and two extra sort orders if still needed."""
        self.log(f"  {label}: {n} exceeds dual-pass ceiling, splitting by agency")
        returned = set()
        for dim, values in (("agencies", sorted(agencies)), ("org_states", US_STATES)):
            for v in values:
                sub = {**criteria, dim: [v]}
                sub_label = f"{label}/{v}"
                m = self.total(sub, sub_label)
                if m == 0:
                    continue
                if m > DUAL_PASS_CEILING:
                    raise IncompletePull(f"{sub_label}: {m} still exceeds the dual-pass ceiling")
                returned |= self.bucket(sub, m, sub_label)
            if len(returned) >= n:
                return
            self.log(f"  {label}: {len(returned)}/{n} after {dim} split" +
                     (", adding a state split" if dim == "agencies" else ""))
        self.log(f"  {label}: adding award_amount asc/desc windows")
        returned |= self._passes(criteria, [("award_amount", "asc"), ("award_amount", "desc")], label)
        if len(returned) < n:
            raise IncompletePull(f"{label}: {len(returned)}/{n} after agency, state and extra sort splits")

    def run(self, hints, all_codes):
        year = self.year
        year_crit = {"fiscal_years": [year]}
        year_total = self.total(year_crit, f"FY{year}")
        self.log(f"FY{year}: NIH total {year_total}")

        if year_total <= DUAL_PASS_CEILING:
            self.bucket(year_crit, year_total, f"FY{year}")
        else:
            codes = set(hints["codes_by_year"].get(str(year), []))
            agencies = set(hints["agencies_by_year"].get(str(year), [])) | set(FALLBACK_AGENCIES)
            others = sorted(all_codes - codes)
            if others and self.total({**year_crit, "activity_codes": others},
                                     f"FY{year}/activity:other-years({len(others)})"):
                codes |= set(others)
            for code in sorted(codes):
                crit = {**year_crit, "activity_codes": [code]}
                label = f"FY{year}/activity:{code}"
                n = self.total(crit, label)
                if n == 0:
                    continue
                if n <= DUAL_PASS_CEILING:
                    self.bucket(crit, n, label)
                else:
                    self.oversized(crit, n, label, agencies | self.agencies)

            if len(self.seen) < year_total:
                self.log(f"  FY{year}: {len(self.seen)}/{year_total} after the activity-code split; "
                         f"checking agency x subproject buckets")
                for agency in sorted(agencies | self.agencies):
                    for is_sub in (False, True):
                        flag = "sub_project_only" if is_sub else "exclude_subprojects"
                        crit = {**year_crit, "agencies": [agency], flag: True}
                        label = f"FY{year}/{agency}/{flag}"
                        n = self.total(crit, label)
                        if n <= self.by_agency_sub[(agency, is_sub)]:
                            continue
                        if n > DUAL_PASS_CEILING:
                            raise IncompletePull(f"{label}: {n} exceeds the dual-pass ceiling")
                        self.bucket(crit, n, label)

        if len(self.seen) < year_total:
            raise IncompletePull(f"FY{year}: {len(self.seen)}/{year_total} after every split")
        self.log(f"FY{year}: {len(self.seen)} records (NIH total {year_total})")
        return year_total
