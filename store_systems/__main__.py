"""Lotus Retails' store systems, from the command line.

    uv run python -m store_systems deliver --day 2026-08-24       the files of one night
    uv run python -m store_systems deliver --through 2026-08-23   every night up to that day
    uv run python -m store_systems fault file-twice --store S02 --day 2026-08-20
    uv run python -m store_systems fix --case 1                    the store sends it right
    uv run python -m store_systems reset                           an empty landing folder
    uv run python -m store_systems status                          what has arrived

The files go to the landing folder: data/landing in the project, or LOTUS_LANDING.
"""

import argparse
import os
import shutil
import sys
from datetime import date
from pathlib import Path

from store_systems import files, planted
from store_systems.catalog import FIRST_DAY, LAST_DAY, STORES

PROJECT_DIR = Path(__file__).resolve().parent.parent


def landing_folder() -> Path:
    return Path(os.environ.get("LOTUS_LANDING", PROJECT_DIR / "data" / "landing"))


def _day(text: str) -> date:
    day = date.fromisoformat(text)
    if not FIRST_DAY <= day <= LAST_DAY:
        raise argparse.ArgumentTypeError(f"The stores only have days {FIRST_DAY} to {LAST_DAY}.")
    return day


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="store_systems", description="The stores send files.")
    commands = parser.add_subparsers(dest="command", required=True)

    deliver = commands.add_parser("deliver", help="write the files of one night, or of many")
    which = deliver.add_mutually_exclusive_group(required=True)
    which.add_argument("--day", type=_day, help="one business day, like 2026-08-24")
    which.add_argument("--through", type=_day, help="every day from the first up to this one")

    fault = commands.add_parser("fault", help="make a store misbehave on one day")
    fault.add_argument("kind", choices=files.FAULT_KINDS)
    fault.add_argument("--store", required=True, choices=list(STORES))
    fault.add_argument("--day", required=True, type=_day)

    fix = commands.add_parser("fix", help="the source sends it right the next time")
    fix.add_argument("--case", type=int, choices=range(1, 14), help="a planted case, 1 to 13")
    fix.add_argument("--all-faults", action="store_true", help="switch off every fault you set")

    commands.add_parser("reset", help="empty the landing folder and forget every fault")
    commands.add_parser("status", help="show what has arrived")

    args = parser.parse_args(argv)
    landing = landing_folder()

    if args.command == "deliver":
        if args.day:
            written = len(files.deliver(args.day, landing))
            print(f"The stores sent {written} files for {args.day:%a %Y-%m-%d} to {landing}")
        else:
            written = files.deliver_through(args.through, landing)
            print(f"The stores sent {written} files, {FIRST_DAY} to {args.through}, to {landing}")
    elif args.command == "fault":
        state = files.read_state(landing)
        entry = {"kind": args.kind, "store": args.store, "day": args.day.isoformat()}
        if entry not in state["faults"]:
            state["faults"].append(entry)
        files.write_state(landing, state)
        print(f"Next time {args.store} sends {args.day}: {args.kind}. Deliver that day to see it.")
    elif args.command == "fix":
        state = files.read_state(landing)
        if args.all_faults:
            state["faults"] = []
        if args.case and args.case not in state["fixed"]:
            state["fixed"].append(args.case)
            print(f"Fixed at the source: case {args.case}, {planted.case(args.case)['name']}.")
        files.write_state(landing, state)
        print("Deliver the day again to get the corrected files.")
    elif args.command == "reset":
        if landing.exists():
            shutil.rmtree(landing)
        print(f"The landing folder is empty: {landing}")
    elif args.command == "status":
        state = files.read_state(landing)
        arrivals = landing / files.ARRIVALS_FILE
        count = max(len(arrivals.read_text().splitlines()) - 1, 0) if arrivals.exists() else 0
        print(f"Landing folder: {landing}")
        print(f"Files arrived:  {count}")
        print(f"Faults you set: {state['faults'] or 'none'}")
        print(f"Cases fixed:    {state['fixed'] or 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
