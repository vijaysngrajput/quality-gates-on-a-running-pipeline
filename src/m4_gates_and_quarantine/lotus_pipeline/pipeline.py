"""One night, start to end.

Every step is also a command of its own, so a scheduler can start them one by one:

    uv run python -m lotus_pipeline run          --day 2026-08-24    all eight steps
    uv run python -m lotus_pipeline load         --day 2026-08-24
    uv run python -m lotus_pipeline gate-landing --day 2026-08-24    new: a gate
    uv run python -m lotus_pipeline clean        --day 2026-08-24
    uv run python -m lotus_pipeline gate-clean   --day 2026-08-24    new: a gate
    uv run python -m lotus_pipeline build        --day 2026-08-24    new: write the outputs, in staging
    uv run python -m lotus_pipeline gate-publish --day 2026-08-24    new: a gate
    uv run python -m lotus_pipeline publish      --day 2026-08-24
    uv run python -m lotus_pipeline report       --day 2026-08-24

`--day` is the BUSINESS DAY: the day the stores were open. The run happens the night after.
A step that fails ends with exit code 1, and the steps after it must not run.

These commands only look, and change nothing:

    uv run python -m lotus_pipeline by-hand --store S03 --day 2026-08-24   add up one store's file
    uv run python -m lotus_pipeline impact  --table sales_lines            who is hurt if it is wrong
    uv run python -m lotus_pipeline profile --table clean.sales_lines      read a table before checking it
    uv run python -m lotus_pipeline contract                               the suppliers' files against the contract
    uv run python -m lotus_pipeline dbt test --select test_type:unit       any dbt command, on this project
"""

import argparse
import sys
from datetime import date
from pathlib import Path

from lotus_pipeline import (
    by_hand,
    clean,
    consumers,
    contract,
    dbt_runner,
    gates,
    load,
    profile,
    publish,
    report,
)
from lotus_pipeline.gates import GateBlocked
from lotus_pipeline.report import money
from lotus_pipeline.warehouse import WarehouseDown, connect

STEPS = (
    "load", "gate-landing", "clean", "gate-clean", "build", "gate-publish", "publish", "report",
)  # fmt: skip


def step_load(day: date) -> str:
    with connect() as wh:
        done = load.load(wh, day)
    return (
        f"load:         {done['files']} new files, {done['rows']:,} rows copied to the raw tables."
    )


def step_clean(day: date) -> str:
    with connect() as wh:
        counts = clean.clean(wh)
    return f"clean:        {len(counts)} clean tables rebuilt. sales_lines has {counts['sales_lines']:,} rows."


def step_gate(gate: str, day: date) -> str:
    result = gates.run_gate(gate, day)
    text = gates.format_gate(result)
    if result.blocked_by:
        raise GateBlocked(text)
    return text


def step_build(day: date) -> str:
    with connect() as wh:
        counts = publish.build(wh, day)
    return (
        f"build:        staging.daily_store_sales has {counts['daily_store_sales']} rows. "
        "Nobody reads staging."
    )


def step_publish(day: date) -> str:
    with connect() as wh:
        counts = publish.publish(wh, day)
    return (
        f"publish:      daily_store_sales has {counts['daily_store_sales']} rows. "
        f"replenishment_list has {counts['replenishment_list']} rows for tomorrow. "
        f"{counts['not_ready']} store days are not ready to pay."
    )


def step_report(day: date) -> str:
    with connect() as wh:
        return "\n" + report.format_report(report.bonus_report(wh, day))


RUN_STEP = {
    "load": step_load,
    "gate-landing": lambda day: step_gate("landing", day),
    "clean": step_clean,
    "gate-clean": lambda day: step_gate("clean", day),
    "build": step_build,
    "gate-publish": lambda day: step_gate("publish", day),
    "publish": step_publish,
    "report": step_report,
}


def run(day: date, steps: tuple[str, ...] = STEPS, say=print) -> None:
    """Run the steps for one business day. A gate that blocks stops the run."""
    for name in steps:
        say(RUN_STEP[name](day))


def look(args) -> str:
    """The three commands that only look."""
    if args.step == "contract":
        if args.file:
            path = Path(args.file)
            problems = contract.check_file(path)
            results = [
                {"file": path.name, "verdict": contract.verdict(problems), "problems": problems}
            ]
        else:
            results = contract.check_landing()
        return contract.format_results(results)
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
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["dbt"]:  # hand everything after `dbt` to dbt itself
        done = dbt_runner.invoke(argv[1:], quiet=False)
        return 0 if done.success else 1
    parser = argparse.ArgumentParser(prog="lotus_pipeline", description=__doc__.split("\n")[0])
    looking = ("by-hand", "impact", "profile", "contract")
    parser.add_argument("step", choices=("run",) + STEPS + looking)
    parser.add_argument("--file", help="one supplier file to hold against the contract")
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
        if args.step in looking:
            print(look(args))
            return 0
        run(args.day, STEPS if args.step == "run" else (args.step,))
    except GateBlocked as blocked:
        print(blocked)
        print("A gate blocked. The steps after it did not run, and nothing new is published.")
        return 1
    except (WarehouseDown, KeyError, ValueError, dbt_runner.DbtError) as problem:
        print(problem.args[0] if isinstance(problem, KeyError) else problem)
        return 1
    return 0
