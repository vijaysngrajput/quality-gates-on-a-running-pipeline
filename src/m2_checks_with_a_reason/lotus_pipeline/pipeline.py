"""One night, start to end: load, clean, check, publish, report.

Every step is also a command of its own, so a scheduler can start them one by one:

    uv run python -m lotus_pipeline run     --day 2026-08-24    all five steps
    uv run python -m lotus_pipeline load    --day 2026-08-24
    uv run python -m lotus_pipeline clean   --day 2026-08-24
    uv run python -m lotus_pipeline check   --day 2026-08-24
    uv run python -m lotus_pipeline publish --day 2026-08-24
    uv run python -m lotus_pipeline report  --day 2026-08-24

`--day` is the BUSINESS DAY: the day the stores were open. The run happens the night after.
A step that fails ends with exit code 1, and the steps after it must not run.

Three more commands do not change anything. They help you look:

    uv run python -m lotus_pipeline by-hand --store S03 --day 2026-08-24   add up one store's file
    uv run python -m lotus_pipeline impact  --table sales_lines            who is hurt if it is wrong
    uv run python -m lotus_pipeline profile --table clean.sales_lines      read a table before checking it

New in milestone 2, the checks with a reason. They also change nothing:

    uv run python -m lotus_pipeline inspect                         every check, and what it found
    uv run python -m lotus_pipeline inspect --check till_total      the bad rows of one check
"""

import argparse
from datetime import date

from lotus_pipeline import (
    by_hand,
    checks,
    clean,
    consumers,
    inspect,
    load,
    profile,
    publish,
    report,
)
from lotus_pipeline.report import money
from lotus_pipeline.warehouse import WarehouseDown, connect

STEPS = ("load", "clean", "check", "publish", "report")


class ChecksFailed(Exception):
    pass


def step_load(wh, day: date) -> str:
    done = load.load(wh, day)
    return f"load:    {done['files']} new files, {done['rows']:,} rows copied to the raw tables."


def step_clean(wh, day: date) -> str:
    counts = clean.clean(wh)
    return f"clean:   {len(counts)} clean tables rebuilt. sales_lines has {counts['sales_lines']:,} rows."


def step_check(wh, day: date) -> str:
    results = checks.run_checks(wh, day)
    text = "check:   the five standard checks\n" + checks.format_results(results)
    if not all(r.passed for r in results):
        wh.commit()  # keep the results, so a person can read what failed
        raise ChecksFailed(text)
    return text


def step_publish(wh, day: date) -> str:
    counts = publish.publish(wh, day)
    return (
        f"publish: daily_store_sales has {counts['daily_store_sales']} rows. "
        f"replenishment_list has {counts['replenishment_list']} rows for tomorrow."
    )


def step_report(wh, day: date) -> str:
    return "\n" + report.format_report(report.bonus_report(wh, day))


RUN_STEP = {
    "load": step_load,
    "clean": step_clean,
    "check": step_check,
    "publish": step_publish,
    "report": step_report,
}


def run(day: date, steps: tuple[str, ...] = STEPS, say=print) -> None:
    """Run the steps for one business day. Each step is one transaction."""
    for name in steps:
        with connect() as wh:
            say(RUN_STEP[name](wh, day))


def look(args) -> str:
    """The three commands that only look."""
    if args.step == "impact":
        if args.table:
            return consumers.format_impact(consumers.impact(args.table))
        lines = ["The seven tables, the most harmful first:"]
        for row in consumers.rank_tables():
            mark = "CRITICAL" if row["critical"] else ""
            people = ", ".join(row["people"]) or "nobody yet"
            lines.append(f"  {row['rank']}. {row['table']:<16}{mark:<10}reaches: {people}")
        return "\n".join(lines)
    with connect() as wh:
        if args.step == "inspect":
            profile.column_names(wh, "clean.sales_lines")  # a clear message when it is not there
            if args.check:
                return inspect.format_rows(inspect.run_one(wh, args.check))
            return inspect.format_results(inspect.run_all(wh))
        if args.step == "profile":
            table = args.table or "clean.sales_lines"
            return profile.format_profile(table, profile.profile_columns(wh, table))
        found = by_hand.compare(wh, args.store, args.day)
    lines = [
        f"{found['store_id']} on {found['day']}",
        f"  the pipeline says   {money(found['pipeline_cents'] or 0):>12}",
        f"  by hand, from file  {money(found['by_hand_cents']):>12}",
        f"  the tills' own total{money(found['till_total_cents']):>12}",
        f"  difference          {money(found['difference_cents'] or 0):>12}",
    ]
    for line in found["suspects"]:
        lines.append(
            f"  look at line {line['line_id']}: a {line['line_type']} of "
            f"{money(int(line['amount_cents']))}, and the amount is not negative"
        )
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lotus_pipeline", description=__doc__.split("\n")[0])
    parser.add_argument(
        "step", choices=("run",) + STEPS + ("by-hand", "impact", "profile", "inspect")
    )
    parser.add_argument("--check", help="one check, like till_total")
    parser.add_argument("--day", type=date.fromisoformat, help="the business day, like 2026-08-24")
    parser.add_argument("--store", help="a store, like S03")
    parser.add_argument("--table", help="a table, like sales_lines or clean.sales_lines")
    args = parser.parse_args(argv)
    needs_day = args.step in ("run", "by-hand") + STEPS
    if needs_day and args.day is None:
        parser.error(f"{args.step} needs --day, like --day 2026-08-24")
    if args.step == "by-hand" and args.store is None:
        parser.error("by-hand needs --store, like --store S03")
    try:
        if args.step in ("by-hand", "impact", "profile", "inspect"):
            print(look(args))
            return 0
        run(args.day, STEPS if args.step == "run" else (args.step,))
    except ChecksFailed as failed:
        print(failed)
        print("A check failed. Nothing is published.")
        return 1
    except (WarehouseDown, KeyError, ValueError) as problem:
        print(problem.args[0] if isinstance(problem, KeyError) else problem)
        return 1
    return 0
