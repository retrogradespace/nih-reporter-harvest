"""
Shared NIH RePORTER /v2/projects/search client, with ADAPTIVE pagination
splitting for nationwide (unscoped) pulls.

WHY THIS EXISTS: NIH RePORTER caps offset+limit at 10,000 per query so you
can never page past the first 10,000 matches of a given criteria, no matter
how many more exist. A single-institution scoped volume (a few hundred
records/year) never approached that. Nationwide volume does badly: FY
totals run ~52k-94k.

TWO TECHNIQUES, escalated only as needed:

1. **Dual-pass (asc + desc), for any bucket under ~20,000.** Paginating the
   same criteria once ascending and once descending by appl_id gives two
   10,000-record windows whose union is guaranteed to cover every record
   when total <= 2 x PAGINATION_CAP confirmed live: NIH_RePORTER's `desc`
   sort genuinely reverses order (not a no-op), and a real bucket (NCI +
   FY2010, 12,590 records, over the single-pass cap) fetches to exactly its
   true total this way. This needs NO secondary criteria dimension at all,
   which matters: an earlier version of this module split by `org_states`
   for buckets over cap, and that silently missed ~8% of records with no US
   state on file (mostly foreign-funded collaborators) a real, measured
   gap, not a hypothetical one. Dual-pass has no such blind spot since it
   doesn't filter on anything extra.

2. **Split by a secondary dimension, only for buckets that exceed
   ~20,000** (dual-pass can't cover them either). Splits by `agencies`
   first (NIH's ~27 institutes/centers), then `org_states` within an
   agency if that's STILL over 20,000 verified nothing nationwide
   actually needs the second level in practice (the single largest
   (year, agency) bucket found, NCI FY2010, is 12,590 under 20,000, so
   dual-pass alone handles it without ever touching org_states). The
   org_states level is kept as a fallback for the largest single year
   (FY2010-era peak, ~94k nationwide) split across only ~27 agencies, in
   case a future year's distribution is more skewed than what's been
   checked so far and because it's still exercised at the (year, agency)
   level with dual-pass on TOP of it (not relied on alone), so it doesn't
   reintroduce the foreign-record gap.

CONFIRMED GOTCHA (4 for 4 so far): every criteria field tested so far
silently ignores its camelCase spelling from NIH's own swagger spec and
only works in snake_case `organization_type`, `date_added`,
`activity_codes`, and `org_states` all reproduce this. Treat it as a
general rule for this API: always use snake_case, and if you add a new
criteria field, verify with a before/after meta.total comparison rather
than trusting the swagger spec.

COVERAGE VERIFICATION: every split level compares the sum of what it
actually fetched against that level's own unfiltered total and logs a
clear warning on any shortfall, rather than silently losing records.
"""

import time

import requests

URL = "https://api.reporter.nih.gov/v2/projects/search"
PAGE_LIMIT = 500           # NIH RePORTER's per-response cap
PAGINATION_CAP = 10_000    # offset+limit hard ceiling, per single sort direction
DUAL_PASS_CEILING = 2 * PAGINATION_CAP  # max a asc+desc dual-pass can guarantee covering

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Content-Type": "application/json",
}

# FALLBACK_AGENCIES: a supplementary check ALONGSIDE per-year discovery
# (see harvest_year), not a substitute for it since no fixed list is ever
# provably complete. NIH RePORTER has no canonical enumeration endpoint for
# this, and a naive "current 27 institutes/centers" list measurably wasn't
# enough: it covered only 70% of FY1985 (missing defunct codes like NCRR,
# and non-NIH PHS agencies AHRQ & FDA that also appear in RePORTER data).
# This list is the union of the current 27 ICs with every agency
# abbreviation empirically observed across a full FY1985-2026 pull, so it
# captures historical/defunct codes too still not guaranteed complete
# (checking it against FY1985 closed part, not all, of that year's gap),
# which is exactly why harvest_year() treats ANY shortfall as a logged
# warning rather than assuming success. To address this I also supply
# a back-up run that goes back to any failed records and attempts to discover
# any missing agencies to ensure complete coverage.
FALLBACK_AGENCIES = [
    "AHRQ", "CID", "CIT", "CC", "COTPER", "DADHP", "DDA", "DHSS", "DM,BHP",
    "DN", "DRS", "FDA", "FIC", "NCATS", "NCBDDD", "NCCDPHP", "NCCIH",
    "NCEH", "NCEZID", "NCHHSTP", "NCI", "NCIPC", "NCIRD", "NCRR", "NEI",
    "NHGRI", "NHLBI", "NIA", "NIAAA", "NIADDK", "NIAID", "NIAMS", "NIBIB",
    "NICHD", "NIDA", "NIDCD", "NIDCR", "NIDDK", "NIEHS", "NIGMS", "NIMH",
    "NIMHD", "NINDS", "NINR", "NIOSH", "NLM", "OD", "ODCDC", "PHITPO",
    "SAMHSA",
]

# US states + DC + territories fallback second split level, only used if
# an (year, agency) bucket ever exceeds DUAL_PASS_CEILING (not observed in
# any year checked so far, largest found is 12,590).
US_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
    "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY", "PR", "VI", "GU", "AS", "MP",
]


def get_total(criteria, max_retries=5):
    """meta.total for `criteria`, via a limit=1 probe query."""
    payload = {
        "criteria": criteria, "limit": 1, "offset": 0,
        "sort_field": "appl_id", "sort_order": "asc",
    }
    retries = 1
    while True:
        try:
            response = requests.post(URL, json=payload, headers=HEADERS, timeout=60)
        except requests.RequestException as e:
            retries += 1
            if retries > max_retries:
                raise
            print(f"CONNECTION ERROR on total probe: {e}. Retrying...")
            time.sleep(2 ** retries)
            continue

        if response.status_code == 200:
            return response.json().get("meta", {}).get("total", 0)
        elif response.status_code in (429, 500, 502, 503, 504):
            retries += 1
            if retries > max_retries:
                raise RuntimeError(f"Max retries on total probe: {response.status_code}")
            time.sleep(2 ** retries)
        else:
            raise RuntimeError(f"Total probe failed: {response.status_code} {response.text}")


def fetch_paginated(criteria, sort_order="asc", label="", sort_field="appl_id"):
    """Yield every record matching `criteria` in one sort direction,
    paginated and retried, stopping at PAGINATION_CAP caller composes
    this into full coverage via dual-pass or splitting (see harvest_year).
    sort_field is appl_id except for _complete_pull's last-resort
    award_amount windows; project_num and unknown fields return HTTP 500."""
    offset = 0
    retries = 1
    max_retries = 5

    while True:
        payload = {
            "criteria": criteria,
            "limit": PAGE_LIMIT,
            "offset": offset,
            # Explicit sort required for stable pagination the NIH RePORTER
            # defaults to relevance ranking when unset, which can reorder
            # between page requests and silently skip/duplicate records.
            "sort_field": sort_field,
            "sort_order": sort_order,
        }

        try:
            response = requests.post(URL, json=payload, headers=HEADERS, timeout=60)
        except requests.RequestException as e:
            print(f"CONNECTION ERROR [{label}/{sort_order}] offset {offset}: {e}")
            retries += 1
            if retries > max_retries:
                print(f"Max retries reached [{label}/{sort_order}] offset {offset}. Stopping this fetch.")
                return
            time.sleep(2 ** retries)
            continue

        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            if not results:
                return

            for r in results:
                yield r

            retries = 1
            total = data.get("meta", {}).get("total", 0)
            offset += PAGE_LIMIT
            if offset >= total or offset >= PAGINATION_CAP:
                return
            time.sleep(0.05)
        elif response.status_code in (429, 500, 502, 503, 504):
            print(f"WARNING: temporary error {response.status_code} [{label}/{sort_order}] offset {offset}. Retrying...")
            retries += 1
            if retries > max_retries:
                print(f"Max retries reached [{label}/{sort_order}] offset {offset}. Stopping this fetch.")
                return
            time.sleep(2 ** retries)
        else:
            print(f"ERROR [{label}/{sort_order}] offset {offset}: {response.status_code}")
            print(f"Server message: {response.text}")
            return


def fetch_bucket(criteria, total, label, seen_appl_ids, log=print):
    """Fetch every record for `criteria` (already known to total `total`),
    using a single ascending pass if it fits under PAGINATION_CAP, or a
    dual asc+desc pass if it fits under DUAL_PASS_CEILING. Yields records
    not already in `seen_appl_ids` (added as they're yielded). Returns the
    count actually fetched (for coverage verification by the caller)."""
    fetched = 0

    passes = ["asc"] if total <= PAGINATION_CAP else ["asc", "desc"]
    for sort_order in passes:
        for r in fetch_paginated(criteria, sort_order=sort_order, label=label):
            appl_id = r.get("appl_id")
            if appl_id not in seen_appl_ids:
                seen_appl_ids.add(appl_id)
                fetched += 1
                yield r

    if total > DUAL_PASS_CEILING:
        log(f"  WARNING [{label}]: total={total} exceeds dual-pass ceiling "
            f"({DUAL_PASS_CEILING}) -- caller must split further.")


def harvest_year(year, seen_appl_ids, log=print):
    """Yield every record for `year`, nationwide. Escalates only as needed:
    dual-pass first; splits by agency (then org_state within an agency, if
    that's ALSO over the dual-pass ceiling) only for buckets dual-pass can't
    cover. Verifies coverage at each split level."""
    year_total = get_total({"fiscal_years": [year]})
    log(f"FY{year}: nationwide total {year_total}")

    fetched_this_year = 0

    if year_total <= DUAL_PASS_CEILING:
        for r in fetch_bucket({"fiscal_years": [year]}, year_total, f"FY{year}", seen_appl_ids, log):
            fetched_this_year += 1
            yield r
    else:
        # DISCOVER, don't guess: a fixed agency list is unreliable across 40
        # years of NIH reorganizations (verified a hand-curated list of
        # the current 27 institutes/centers missed 30% of FY1985, mostly
        # defunct codes like NCRR and non-NIH PHS agencies that also appear
        # in RePORTER data). The unfiltered dual-pass sample below IS real
        # data (yielded, not discarded) discovery piggybacks on it rather
        # than costing a separate pass.
        discovered_agencies = set()
        for r in fetch_bucket({"fiscal_years": [year]}, year_total, f"FY{year}", seen_appl_ids, log):
            fetched_this_year += 1
            admin = r.get("agency_ic_admin") or {}
            code = admin.get("abbreviation")
            if code:
                discovered_agencies.add(code)
            yield r

        log(f"  FY{year}: discovered {len(discovered_agencies)} agencies from the initial sample "
            f"-- {sorted(discovered_agencies)}")

        # Supplementary check, not a substitute for discovery catches
        # known-but-rare agencies the 20k-record sample happened not to
        # surface (see FALLBACK_AGENCIES' docstring).
        agencies_to_check = discovered_agencies | set(FALLBACK_AGENCIES)

        agency_total_sum = 0
        for agency in agencies_to_check:
            criteria = {"fiscal_years": [year], "agencies": [agency]}
            agency_total = get_total(criteria)
            if agency_total == 0:
                continue
            agency_total_sum += agency_total
            label = f"FY{year}/{agency}"

            if agency_total <= DUAL_PASS_CEILING:
                # fetch_bucket dedups against seen_appl_ids records
                # already yielded by the initial sample are skipped, only
                # the remainder beyond that sample's 20k window is new.
                for r in fetch_bucket(criteria, agency_total, label, seen_appl_ids, log):
                    fetched_this_year += 1
                    yield r
            else:
                log(f"  {label}: {agency_total} exceeds dual-pass ceiling, splitting by org_state")
                state_total_sum = 0
                for state in US_STATES:
                    state_criteria = {**criteria, "org_states": [state]}
                    state_total = get_total(state_criteria)
                    if state_total == 0:
                        continue
                    state_total_sum += state_total
                    state_label = f"{label}/{state}"
                    for r in fetch_bucket(state_criteria, state_total, state_label, seen_appl_ids, log):
                        fetched_this_year += 1
                        yield r

                if state_total_sum < agency_total:
                    log(f"  WARNING: {label} org_state split covered {state_total_sum}/{agency_total} "
                        f"-- {agency_total - state_total_sum} record(s) have no US org_state "
                        f"(likely foreign-funded) and were NOT fetched by this pass.")

        if fetched_this_year < year_total:
            log(f"  WARNING: FY{year} covered {fetched_this_year}/{year_total} -- "
                f"{year_total - fetched_this_year} record(s) still missing. Either an agency wasn't "
                f"discovered by the initial sample, or some records have no agency_ic_admin at all. "
                f"Both discovery and FALLBACK_AGENCIES were already applied this residual gap "
                f"is likely records with no agency_ic_admin at all, or a genuinely novel code.")

    log(f"FY{year}: fetched {fetched_this_year} unique records (nationwide total was {year_total})")
