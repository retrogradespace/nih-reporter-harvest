"""
nih-reporter-harvest, stage 6 -- the scheduled weekly job. Runs the re-pull
(4_) and then the load (5_) with the same run id, so 5_ loads exactly the
pull 4_ just made, then rebuilds the child tables (7_). Each step runs only
if the one before it succeeded.

Which years: most weeks 4_ re-pulls FY2020 on (RECENT_FROM) and 5_ only
looks for removals there. A full sweep from FY1985 runs when no complete
full sweep has run in the FULL_SWEEP_DAYS (28) before this run, so about
every fourth week, or with --full. Why: in the four weeks to 2026-11-16,
nothing before FY2020 changed except organization and PI details (UEIs, zip
codes, PI profiles); money changed back to FY2023, dates to FY2022, new and
removed records to FY2024. FY1985-FY2019 are 81% of the records and most of
the pull time, so those details now arrive up to four weeks late.

Output from all three goes to logs/weekly_refresh_<date>.log, ending in SUCCESS
or FAILED. On SUCCESS it also writes logs/last_success.json (run id and finish
time), which another job can watch to start after this one. On macOS a `caffeinate -i` assertion is held for the whole run
(a full sweep took 5 h 50 min to 6 h 15 min in Oct-Nov 2026) so the Mac can't idle-sleep partway through.

If 4_ fails partway, rerun the same pull with
    ../.venv/bin/python3 4_weekly_refresh_pull.py --run-id <run_id>
(it resumes from the last finished year), then 5_ with that --run-id, then 7_.

Scheduled by schedule_weekly.py (Mondays 15:00; NIH updates RePORTER on
Saturdays), optionally also started by another job's file (--after). With
--once-a-week it does nothing if a run has already finished (SUCCESS or
FAILED) since the last Saturday, so the trigger and the 15:00 fallback don't
both run it, and a failed run isn't restarted from scratch.

An interrupted run is different: its log has no SUCCESS/FAILED line and no
process holds logs/weekly_refresh.lock (a restart or shutdown killed it).
--once-a-week resumes that run in the same log with the same run id, from
the step it was in; 4_ skips the years it already finished. Every run holds
the lock, so a second start while one is running does nothing. A resumed
run keeps the years it started with (read back from its "started" line).

Usage:
    ../.venv/bin/python3 6_run_weekly_refresh.py [--max-removals N] [--once-a-week] [--full]
"""

import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
LOG_DIR = BASE.parent / "logs"
SUCCESS_FILE = LOG_DIR / "last_success.json"
LOCK_FILE = LOG_DIR / "weekly_refresh.lock"
WEEKLY_DIR = BASE.parent / "data" / "weekly"

FIRST_FISCAL_YEAR = 1985  # as in 4_
RECENT_FROM = 2020
FULL_SWEEP_DAYS = 28

# "=== weekly refresh <run_id> started|resumed|SUCCESS|FAILED <time> ..." and
# "--- <script> (<time>) ---", as written by main(). A "started" line names
# its first year: "(FY2020 on)".
RUN_LINE = re.compile(r"^=== weekly refresh (\S+) (started|resumed|SUCCESS|FAILED) ")
START_YEAR = re.compile(r"\(FY(\d{4}) on")
STEP_LINE = re.compile(r"^--- (\S+\.py) \(")


def keep_awake():
    """macOS: stop idle sleep until this process exits. Best effort."""
    if sys.platform != "darwin":
        return
    try:
        subprocess.Popen(["/usr/bin/caffeinate", "-i", "-w", str(os.getpid())])
    except OSError as e:
        print(f"(couldn't start caffeinate, the Mac may sleep during the run: {e})")


def remaining_wakes():
    """macOS: how many one-off 'Monday 14:55' wakes are still scheduled
    (README: "Waking the Mac for the run"). None elsewhere or on error."""
    if sys.platform != "darwin":
        return None
    try:
        out = subprocess.run(["/usr/bin/pmset", "-g", "sched"], capture_output=True, text=True).stdout
    except OSError:
        return None
    return sum(1 for line in out.splitlines() if "wake at" in line and "14:55:00" in line)


def hold_lock():
    """An exclusive lock on logs/weekly_refresh.lock for the life of this
    process (the OS drops it when the process dies, however it dies), or
    None if another run holds it."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    f = LOCK_FILE.open("w")
    try:
        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        f.close()
        return None
    return f


def started_this_week():
    """The newest weekly log of a run started on or after the last Saturday
    (NIH's update day), or None. Logs are named by local start date."""
    today = date.today()
    saturday = today - timedelta(days=(today.weekday() - 5) % 7)
    newest = None
    for path in sorted(LOG_DIR.glob("weekly_refresh_*.log")):
        try:
            started = date.fromisoformat(path.stem.removeprefix("weekly_refresh_"))
        except ValueError:
            continue
        if started >= saturday:
            newest = path
    return newest


def interrupted(log_path):
    """(run_id, step script, first fiscal year) if the log's last run never
    wrote SUCCESS or FAILED, else None. The step is the one it was in, or
    None if it died before starting one; the year is None for a run logged
    before the year was. Only meaningful while holding the lock."""
    run_id = state = step = start_year = None
    for line in log_path.read_text(errors="replace").splitlines():
        if m := RUN_LINE.match(line):
            run_id, state = m.groups()
            if state == "started":
                step = None
                y = START_YEAR.search(line)
                start_year = int(y.group(1)) if y else None
        elif m := STEP_LINE.match(line):
            step = m.group(1)
    if state in ("started", "resumed"):
        return run_id, step, start_year
    return None


def full_sweep_due(run_id):
    """True unless a complete pull from FY1985 ran in the FULL_SWEEP_DAYS
    before this run (by run id date, from the pull manifests)."""
    run_day = date.fromisoformat(run_id[:10])
    for path in WEEKLY_DIR.glob("*.manifest.json"):
        manifest = json.loads(path.read_text())
        if manifest["run_id"] >= run_id:
            continue
        if (min(manifest["full_coverage_fiscal_years"]) == FIRST_FISCAL_YEAR
                and (run_day - date.fromisoformat(manifest["run_id"][:10])).days < FULL_SWEEP_DAYS):
            return False
    return True


def write_success(run_id):
    """logs/last_success.json, replaced in one step so a job watching it
    never reads half a file."""
    tmp = SUCCESS_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"run_id": run_id,
                               "finished_at": datetime.now().astimezone().isoformat(timespec="seconds")},
                              indent=1) + "\n")
    os.replace(tmp, SUCCESS_FILE)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-removals", type=int, help="passed through to 5_ (default there: 100)")
    parser.add_argument("--once-a-week", action="store_true",
                        help="do nothing if a run has finished since the last Saturday; "
                             "resume one that was interrupted")
    parser.add_argument("--full", action="store_true",
                        help=f"re-pull every year from FY{FIRST_FISCAL_YEAR}, even if a full sweep isn't due")
    args = parser.parse_args()

    # Before anything is written: a skipped start must not leave a weekly log
    # without a SUCCESS/FAILED line, which readers take to mean "running".
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    lock = hold_lock()  # held until exit
    if lock is None:
        print(f"{now}: a weekly refresh is already running; nothing to do.")
        return

    resume = None
    if args.once_a_week and (earlier := started_this_week()):
        resume = interrupted(earlier)
        if resume is None:
            print(f"{now}: already run this week ({earlier.name}); nothing to do.")
            return

    keep_awake()
    started = datetime.now().astimezone()
    if resume:
        log_path = earlier
        run_id, from_step, start_year = resume
        if start_year is None:  # logged before runs named their years: they were all full
            start_year = FIRST_FISCAL_YEAR
    else:
        run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
        log_path = LOG_DIR / f"weekly_refresh_{started.date().isoformat()}.log"
        from_step = None
        start_year = FIRST_FISCAL_YEAR if args.full or full_sweep_due(run_id) else RECENT_FROM

    load_args = ["--run-id", run_id]
    if args.max_removals is not None:
        load_args += ["--max-removals", str(args.max_removals)]
    steps = [("4_weekly_refresh_pull.py", ["--run-id", run_id, "--start", str(start_year)]),
             ("5_load_weekly_refresh.py", load_args),
             ("7_build_child_tables.py", [])]
    # Steps before the interrupted one finished (each runs only if the one
    # before it succeeded).
    if from_step:
        steps = steps[[s for s, _ in steps].index(from_step):]

    ok = True
    with log_path.open("a") as log:
        def say(msg):
            print(msg, flush=True)
            log.write(msg + "\n")
            log.flush()

        if resume:
            say(f"\n=== weekly refresh {run_id} resumed {started.isoformat(timespec='seconds')} "
                f"(interrupted; restarting at {steps[0][0]}) ===")
        else:
            scope = "full sweep" if start_year == FIRST_FISCAL_YEAR else "recent years"
            say(f"=== weekly refresh {run_id} started {started.isoformat(timespec='seconds')} "
                f"(FY{start_year} on, {scope}) ===")
        for script, extra in steps:
            say(f"\n--- {script} ({datetime.now().astimezone().isoformat(timespec='seconds')}) ---")
            proc = subprocess.Popen(
                [sys.executable, "-u", str(BASE / script), *extra], cwd=BASE,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            )
            for line in proc.stdout:
                print(line, end="", flush=True)
                log.write(line)
                log.flush()
            if proc.wait() != 0:
                say(f"!! {script} failed (exit {proc.returncode}) -- stopping.")
                ok = False
                break

        wakes = remaining_wakes()
        if wakes is not None and wakes < 4:
            say(f"!! WARNING: only {wakes} scheduled Monday 14:55 wake(s) left -- "
                f"schedule more (README: \"Waking the Mac for the run\"). Run status unchanged.")

        status = "SUCCESS" if ok else "FAILED"
        say(f"\n=== weekly refresh {run_id} {status} "
            f"{datetime.now().astimezone().isoformat(timespec='seconds')} ===")

    # After the SUCCESS line, so a job started by this file finds the log finished.
    if ok:
        write_success(run_id)

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
