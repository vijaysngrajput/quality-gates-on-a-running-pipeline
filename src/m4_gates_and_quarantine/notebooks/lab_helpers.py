"""Small helpers for the milestone 4 lab.

Nothing here is hidden magic. Open this file any time you want to see how
something works.
"""

import json
import re
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

LAB_DIR = Path(__file__).resolve().parent
MILESTONE_DIR = LAB_DIR.parent
PROJECT_DIR = MILESTONE_DIR.parents[1]
RESULTS_DIR = LAB_DIR / "results"
DBT_PROJECT = MILESTONE_DIR / "dbt_project"

# Lets a notebook write `from lotus_pipeline import ...` (the production code)
# and `import airflow_tools`.
if str(MILESTONE_DIR) not in sys.path:
    sys.path.insert(0, str(MILESTONE_DIR))

from lotus_pipeline import config, pipeline, report  # noqa: E402
from lotus_pipeline.warehouse import WarehouseDown, connect, plain  # noqa: E402
from store_systems import files  # noqa: E402

KB = "https://www.d8loop.com/modules/knowledge-corner/"
LANDING = config.LANDING
FIRST_NIGHT = date(2026, 8, 24)  # Monday of the bonus week
LAST_NIGHT = date(2026, 8, 30)  # Sunday of the bonus week
WEEK = [FIRST_NIGHT + timedelta(days=n) for n in range(7)]
SCHEMAS = ("raw", "clean", "output", "ops", "staging", "quarantine", "audit", "prepared", "lab")

pd.set_option("display.max_rows", 60)
pd.set_option("display.width", 180)
pd.set_option("display.max_colwidth", 90)
pd.set_option("display.max_columns", None)


def quiet(text: str) -> None:
    """Say nothing. Passed to the pipeline when its own lines are not wanted."""


# ---- The week ---------------------------------------------------------------


def _nights_loaded() -> int:
    with connect() as wh:
        there = wh.query(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'output' AND table_name = 'store_day_status'"
        )
        if not there:
            return 0
        return wh.value(
            "SELECT count(DISTINCT run_day) FROM ops.gate_results WHERE gate = 'publish'"
        )


def prepare_week() -> None:
    """Make sure the whole bonus week went through the gates: seven nights, 24 to 30 August.

    The first time it takes about half a minute: the stores send their files, and the
    pipeline of this milestone runs night by night. After that it only says it is ready.
    """
    try:
        loaded = _nights_loaded()
    except WarehouseDown as down:
        print(down)
        return
    if loaded != 7:
        files.deliver_through(LAST_NIGHT, LANDING)
        for night in WEEK:
            pipeline.run(night, say=quiet)
    with connect() as wh:
        tests = wh.value("SELECT count(*) FROM ops.gate_results WHERE run_day = ?", (LAST_NIGHT,))
        ready = report.bonus_report(wh, LAST_NIGHT)
    paid = sum(store["pay"] for store in ready["stores"])
    print("The bonus week is loaded: seven nights, Mon 24 to Sun 30 August, through three gates.")
    print(f"Sunday night ran {tests} tests. {paid} of 6 stores can be paid.")


def reset_warehouse() -> None:
    """Empty the lab warehouse of this milestone. Only notebook 8 does this, on purpose."""
    with connect() as wh:
        for schema in SCHEMAS:
            wh.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    print("The warehouse of this milestone is empty.")


# ---- The warehouse ----------------------------------------------------------


def sql(query: str, params: tuple = ()) -> pd.DataFrame:
    """Run one SQL query on the warehouse, and show the result as a table."""
    with connect() as wh:
        cursor = (
            wh.con.execute(query.replace("?", "%s"), params) if params else wh.con.execute(query)
        )
        names = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame([[plain(v) for v in row] for row in rows], columns=names)


def change(statement: str, params: tuple = ()) -> None:
    """Run one statement that changes the warehouse. Used to plant a fault, and to undo it."""
    with connect() as wh:
        wh.execute(statement, params)


# ---- Planting a fault in the raw rows, and taking it out again ---------------


def _backup_name(table: str) -> str:
    return "lab.backup_" + table.replace(".", "_")


def backup(*tables: str) -> None:
    """Keep a copy of one or more tables, so a planted fault can be taken out again."""
    with connect() as wh:
        wh.execute("CREATE SCHEMA IF NOT EXISTS lab")
        for table in tables:
            wh.execute(f"DROP TABLE IF EXISTS {_backup_name(table)}")
            wh.execute(f"CREATE TABLE {_backup_name(table)} AS SELECT * FROM {table}")
    print(f"Kept a copy of {', '.join(tables)}.")


def restore(*tables: str) -> None:
    """Put the kept copies back, and run Sunday's night again from the clean step."""
    with connect() as wh:
        for table in tables:
            wh.execute(f"DELETE FROM {table}")
            wh.execute(f"INSERT INTO {table} SELECT * FROM {_backup_name(table)}")
            wh.execute(f"DROP TABLE {_backup_name(table)}")
    pipeline.run(LAST_NIGHT, steps_from("gate-landing"), say=quiet)
    print(f"Put back: {', '.join(tables)}. Sunday's night ran again, and the fault is gone.")


def steps_from(step: str) -> tuple[str, ...]:
    """This step and every step after it, without the printed report."""
    steps = pipeline.STEPS[pipeline.STEPS.index(step) :]
    return tuple(s for s in steps if s != "report")


def run_from(step: str, day: date = LAST_NIGHT) -> None:
    """Run one night from a step on, as the plain program, and print what each step says.

    Each step is the command you can type yourself in this milestone's folder:
        uv run python -m lotus_pipeline gate-clean --day 2026-08-30
    A step that fails stops the ones after it, as in the real run.
    """
    for name in steps_from(step):
        done = subprocess.run(
            [sys.executable, "-m", "lotus_pipeline", name, "--day", day.isoformat()],
            cwd=MILESTONE_DIR, capture_output=True, text=True,
        )  # fmt: skip
        # dbt adds its own lines when a test is an error. The pipeline's lines say the same.
        said = [
            line
            for line in done.stdout.splitlines()
            if not re.match(r"^(\x1b\[0m)?\d\d:\d\d:\d\d", line)
        ]
        print("\n".join(said))
        if done.returncode != 0:
            return


def before_the_clean_gate() -> None:
    """Rebuild the clean tables from the raw rows. Every bad row is back, as the gate finds them."""
    pipeline.run(LAST_NIGHT, ("clean",), say=quiet)
    print("The clean tables are rebuilt from the raw rows. No gate has looked at them yet.")


def after_the_gates() -> None:
    """Run Sunday's night again from the clean step, so the warehouse is as the night left it."""
    pipeline.run(LAST_NIGHT, steps_from("clean"), say=quiet)
    print("Sunday's night ran again. The warehouse is as the nightly run left it.")


# ---- dbt --------------------------------------------------------------------


def dbt(*args: str, only_failed: bool = False) -> None:
    """Run one dbt command on this milestone's dbt project, and print what dbt says.

    The same as typing, in this milestone's folder:
        uv run python -m lotus_pipeline dbt <command>

    dbt says a lot. This prints its count of what it found, one line per test, and its
    last line. `only_failed=True` leaves out the tests that passed.
    """
    done = subprocess.run(
        [sys.executable, "-m", "lotus_pipeline", "dbt", "--no-use-colors", *args],
        cwd=MILESTONE_DIR, capture_output=True, text=True,
    )  # fmt: skip
    said = [re.sub(r"^\d\d:\d\d:\d\d\s+", "", line) for line in done.stdout.splitlines()]
    passed = 0
    for line in said:
        result = re.match(r"\d+ of \d+ (PASS|WARN|FAIL|ERROR|OK)", line)
        if result and "START" not in line:
            if only_failed and result.group(1) in ("PASS", "OK"):
                passed += 1
                continue
            print(re.sub(r"\.{6,}", " ... ", line))
        elif line.startswith(("Found ", "Done.")):
            if line.startswith("Done.") and passed:
                print(f"({passed} tests passed, and are not listed)")
            print(line)
    if done.returncode != 0 and not any(line.startswith("Done.") for line in said):
        print(done.stdout[-1500:], done.stderr[-1500:])


def show_file(path: str, start: str | None = None, lines: int = 40) -> None:
    """Print part of a file of this milestone: from the first line that holds `start`."""
    text = (MILESTONE_DIR / path).read_text().splitlines()
    first = next((n for n, line in enumerate(text) if start and start in line), 0)
    print(f"# {path}" + (f", from line {first + 1}" if first else ""))
    print("\n".join(text[first : first + lines]))


def all_tests() -> pd.DataFrame:
    """Every singular test of the dbt project: its gate, what a failure does, and its rule."""
    rows = []
    for path in sorted(DBT_PROJECT.glob("tests/*/*.sql")):
        text = path.read_text()
        rows.append(
            {
                "gate": path.parent.name,
                "test": path.stem,
                "action": re.search(r"'action': '(\w+)'", text).group(1),
                "severity": re.search(r"severity='(\w+)'", text).group(1),
                "lens": re.search(r"'lens': '(\w+)'", text).group(1),
                "rule": text.splitlines()[0].lstrip("- "),
            }
        )
    order = {"landing": 0, "clean": 1, "publish": 2}
    table = pd.DataFrame(rows)
    return table.sort_values("gate", key=lambda g: g.map(order), kind="stable").reset_index(
        drop=True
    )


def gate_results(day: date = LAST_NIGHT, only_failed: bool = True) -> pd.DataFrame:
    """What the three gates found on one night, from the table the pipeline keeps."""
    found = sql(
        "SELECT gate, test_name, action, lens, status, bad_rows, title FROM ops.gate_results "
        "WHERE run_day = ? ORDER BY CASE gate WHEN 'landing' THEN 1 WHEN 'clean' THEN 2 ELSE 3 END, "
        "number, test_name",
        (day,),
    )
    return found[found["status"] != "pass"].reset_index(drop=True) if only_failed else found


# ---- The bonus report -------------------------------------------------------


def bonus_report(day: date = LAST_NIGHT) -> dict:
    with connect() as wh:
        return report.bonus_report(wh, day)


def show_report(day: date = LAST_NIGHT) -> None:
    print(report.format_report(bonus_report(day)))


def can_be_paid(day: date) -> int:
    return sum(store["pay"] for store in bonus_report(day)["stores"])


# ---- Saving what the lab found ----------------------------------------------


def save_result(name: str, value) -> None:
    """Keep a result, so a test can hold the production code against it."""
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{name}.json"
    path.write_text(json.dumps(value, indent=2) + "\n")
    print(f"Saved {path.relative_to(MILESTONE_DIR)}")


def load_result(name: str):
    path = RESULTS_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Run notebook 8 first: {path.name} is not there yet.")
    return json.loads(path.read_text())


# ---- Small things -----------------------------------------------------------


def money(cents) -> str:
    """Whole cents as dollars: 79618 becomes $796.18."""
    cents = int(cents)
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) // 100:,}.{abs(cents) % 100:02d}"
