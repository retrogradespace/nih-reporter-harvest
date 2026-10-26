"""
nih-reporter-harvest, stage 6 -- the scheduled weekly job. Runs the full
re-pull (4_) and then the load (5_) with the same run id, so 5_ loads
exactly the pull 4_ just made, then rebuilds the child tables (7_). Each
step runs only if the one before it succeeded.

Output from all three goes to logs/weekly_refresh_<date>.log, ending in SUCCESS
or FAILED. On macOS a `caffeinate -i` assertion is held for the whole run
(about three hours) so the Mac can't idle-sleep partway through.

If 4_ fails partway, rerun the same pull with
    ../.venv/bin/python3 4_weekly_refresh_pull.py --run-id <run_id>
(it resumes from the last finished year), then 5_ with that --run-id, then 7_.

Scheduled by schedule_weekly.py (Mondays 15:00; NIH updates RePORTER on
Saturdays).

Usage:
    ../.venv/bin/python3 6_run_weekly_refresh.py [--max-removals N]
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

BASE = Path(__file__).resolve().parent
LOG_DIR = BASE.parent / "logs"


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-removals", type=int, help="passed through to 5_ (default there: 100)")
    args = parser.parse_args()

    keep_awake()
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    started = datetime.now().astimezone()
    run_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    log_path = LOG_DIR / f"weekly_refresh_{started.date().isoformat()}.log"

    load_args = ["--run-id", run_id]
    if args.max_removals is not None:
        load_args += ["--max-removals", str(args.max_removals)]
    steps = [("4_weekly_refresh_pull.py", ["--run-id", run_id]),
             ("5_load_weekly_refresh.py", load_args),
             ("7_build_child_tables.py", [])]

    ok = True
    with log_path.open("a") as log:
        def say(msg):
            print(msg, flush=True)
            log.write(msg + "\n")
            log.flush()

        say(f"=== weekly refresh {run_id} started {started.isoformat(timespec='seconds')} ===")
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

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
