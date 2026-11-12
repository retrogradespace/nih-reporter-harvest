"""
Installs the weekly refresh (6_run_weekly_refresh.py, Mondays 15:00 local
time) for the user running it.

    --after FILE   (macOS) also start the job whenever FILE changes, e.g. a
                   file another job writes when it succeeds. Mondays 15:00
                   stays as the fallback, and the job runs with --once-a-week,
                   so it runs once per RePORTER week whichever comes first.
                   Pass it again on every reinstall; without it the job is
                   15:00 only.

    macOS   a launchd LaunchAgent from launchd/weekly-refresh.plist.template.
            No pmset wake: macOS keeps one repeating wake per machine, and
            this one may already be used by another job. A sleeping Mac runs
            the job when it next wakes.
    Linux   prints the crontab line to add; it doesn't edit your crontab.

    ../.venv/bin/python3 schedule_weekly.py            install / reinstall
    ../.venv/bin/python3 schedule_weekly.py --after ~/other/logs/last_success.json
    ../.venv/bin/python3 schedule_weekly.py --print    show what it would install
    ../.venv/bin/python3 schedule_weekly.py --remove   remove the job

Install it on one machine only: the job writes this clone's database.
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from xml.sax.saxutils import escape

REPO = Path(__file__).resolve().parent.parent
LABEL = "com.retrogradespace.nih-reporter-harvest-weekly"
TEMPLATE = REPO / "launchd" / "weekly-refresh.plist.template"
RUNNER = REPO / "src" / "6_run_weekly_refresh.py"

ONCE_A_WEEK = "        <string>--once-a-week</string>\n"
WATCH_PATHS = """
    <!-- Also start when this file changes (another job's success). The
         15:00 run above is the fallback; --once-a-week keeps it to one run
         per week. -->
    <key>WatchPaths</key>
    <array>
        <string>{}</string>
    </array>
"""


def run(*cmd, check=False):
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def mac(args):
    home = Path.home()
    target = home / "Library" / "LaunchAgents" / f"{LABEL}.plist"
    domain = f"gui/{os.getuid()}"
    after = args.after.expanduser().resolve() if args.after else None
    if after and not after.parent.is_dir():
        raise SystemExit(f"--after: no such directory {after.parent}")
    plist = (TEMPLATE.read_text()
             .replace("__LABEL__", LABEL).replace("__REPO__", str(REPO)).replace("__HOME__", str(home))
             .replace("__ONCE_A_WEEK__", ONCE_A_WEEK if after else "")
             .replace("__WATCH_PATHS__", WATCH_PATHS.format(escape(str(after))) if after else ""))
    if args.print:
        print(plist, end="")
        return
    run("launchctl", "bootout", f"{domain}/{LABEL}")
    if args.remove:
        target.unlink(missing_ok=True)
        print(f"Removed {LABEL}.")
        return
    (home / "Library" / "Logs" / "nih-reporter-harvest").mkdir(parents=True, exist_ok=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(plist)
    run("plutil", "-lint", str(target), check=True)
    result = run("launchctl", "bootstrap", domain, str(target))
    if result.returncode != 0:
        raise SystemExit(f"launchctl bootstrap failed: {result.stderr.strip()}")
    print(f"Installed {LABEL}: Mondays 15:00, running {RUNNER}")
    if after:
        print(f"  and whenever {after} changes (once a week; 15:00 is the fallback)")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--print", action="store_true")
    group.add_argument("--remove", action="store_true")
    parser.add_argument("--after", type=Path, metavar="FILE",
                        help="macOS: also start when FILE changes; 15:00 stays as the fallback")
    args = parser.parse_args()

    if sys.platform == "darwin":
        mac(args)
    else:
        print("Add this line with `crontab -e`:")
        print(f"0 15 * * 1 {REPO}/.venv/bin/python3 {RUNNER}")


if __name__ == "__main__":
    main()
