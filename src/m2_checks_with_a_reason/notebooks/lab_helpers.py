"""Small helpers for the milestone 2 lab.

Nothing here is hidden magic. Open this file any time you want to see how
something works.
"""

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

LAB_DIR = Path(__file__).resolve().parent
MILESTONE_DIR = LAB_DIR.parent
PROJECT_DIR = MILESTONE_DIR.parents[1]
RESULTS_DIR = LAB_DIR / "results"

# Lets a notebook write `from lotus_pipeline import ...` (the production code).
if str(MILESTONE_DIR) not in sys.path:
    sys.path.insert(0, str(MILESTONE_DIR))

from lotus_pipeline import config, inspect, pipeline  # noqa: E402
from lotus_pipeline.checks import STANDARD_CHECKS  # noqa: E402
from lotus_pipeline.report import bonus_cents  # noqa: E402
from lotus_pipeline.warehouse import WarehouseDown, connect, plain  # noqa: E402
from store_systems import files  # noqa: E402

KB = "https://www.d8loop.com/modules/knowledge-corner/"
LANDING = config.LANDING
FIRST_NIGHT = date(2026, 8, 24)  # Monday of the bonus week
LAST_NIGHT = date(2026, 8, 30)  # Sunday of the bonus week

pd.set_option("display.max_rows", 60)
pd.set_option("display.width", 180)
pd.set_option("display.max_colwidth", 70)


# ---- The week ---------------------------------------------------------------


def prepare_week() -> None:
    """Make sure the whole bonus week is in the warehouse: seven nights, 24 to 30 August.

    The first time it takes a few seconds: the stores send their files, and the
    given pipeline runs night by night. After that it only says it is ready.
    """
    try:
        with connect() as wh:
            loaded = wh.query(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = 'ops' AND table_name = 'check_results'"
            ) and wh.value("SELECT count(DISTINCT run_day) FROM ops.check_results")
    except WarehouseDown as down:
        print(down)
        return
    if loaded != 7:
        files.deliver_through(LAST_NIGHT, LANDING)
        for n in range(7):
            pipeline.run(FIRST_NIGHT + timedelta(days=n), say=lambda text: None)
    with connect() as wh:
        lines = wh.value("SELECT count(*) FROM clean.sales_lines")
        green = wh.value("SELECT count(*) FROM ops.check_results WHERE passed")
    print(
        f"The bonus week is loaded: seven nights, Mon 24 to Sun 30 August. {lines:,} sales lines."
    )
    print(f"The five standard checks ran on every night: {green} of 35 passed.")


# ---- The warehouse ----------------------------------------------------------


def sql(query: str, params: tuple = ()) -> pd.DataFrame:
    """Run one SQL query on the warehouse, and show the result as a table."""
    with connect() as wh:
        rows = wh.query(query, params)
        names = (
            [d[0] for d in wh.con.execute(_limit_zero(query), params).description]
            if not rows
            else []
        )
    if not rows:
        return pd.DataFrame(columns=names)
    return pd.DataFrame([{k: plain(v) for k, v in row.items()} for row in rows])


def _limit_zero(query: str) -> str:
    """The same query with no rows, to learn its column names."""
    return f"SELECT * FROM ({query.strip().rstrip(';').replace('?', '%s')}) empty_result LIMIT 0"


# ---- The lab: a private copy you may break ----------------------------------


def lab_copy() -> None:
    """Make a fresh private copy of the sales lines: lab.sales_lines.

    You plant faults in the copy. The real table, clean.sales_lines, is never touched.
    """
    with connect() as wh:
        wh.execute("CREATE SCHEMA IF NOT EXISTS lab")
        wh.execute("DROP TABLE IF EXISTS lab.sales_lines")
        wh.execute("CREATE TABLE lab.sales_lines AS SELECT * FROM clean.sales_lines")
        rows = wh.value("SELECT count(*) FROM lab.sales_lines")
    print(f"lab.sales_lines is a fresh copy of clean.sales_lines: {rows:,} rows.")


def plant(statement: str, params: tuple = ()) -> None:
    """Change the private copy. It refuses to touch anything outside the lab."""
    words = statement.replace("\n", " ").lower()
    if "lab.sales_lines" not in words or "clean." in words or "raw." in words:
        raise ValueError("plant() only changes lab.sales_lines, the private copy.")
    with connect() as wh:
        wh.execute(statement, params)
    print("Planted in lab.sales_lines.")


def on_lab(query: str) -> str:
    """The same query, pointed at the private copy."""
    return query.replace("clean.sales_lines", "lab.sales_lines")


def standard_checks(on_the_lab: bool = False) -> None:
    """Run the five standard checks, on the real table or on the private copy."""
    for check in STANDARD_CHECKS:
        query = on_lab(check.sql) if on_the_lab else check.sql
        bad = sql(query, (LAST_NIGHT,) if check.for_the_day else ())
        print(
            f"{check.number}. {check.name:<32} {len(bad)} bad rows  {'PASS' if bad.empty else 'FAIL'}"
        )


# ---- The production checks --------------------------------------------------


def production(key: str, on_the_lab: bool = False) -> pd.DataFrame:
    """The bad rows the production check finds: the one in lotus_pipeline/."""
    check = inspect.BY_KEY[key]
    return sql(on_lab(check.sql) if on_the_lab else check.sql)


def show_check(key: str) -> None:
    """Print a production check: why it exists, and its query."""
    check = inspect.BY_KEY[key]
    print(f"{check.number}. {check.name}")
    print(f"   lens:      {check.lens}")
    print(f"   answers:   {check.question}")
    print(f"   protects:  {check.protects}")
    print(f"   prepare:   {check.prepare}")
    print(f"   limit:     {check.limit}")
    print(check.sql.rstrip())


def store_days(bad_rows: pd.DataFrame) -> list[list[str]]:
    """The stores and days the bad rows belong to, each once."""
    if bad_rows.empty:
        return []
    pairs = {(row["store_id"], str(row["sales_date"])[:10]) for _, row in bad_rows.iterrows()}
    return [list(pair) for pair in sorted(pairs, key=lambda p: (p[1], p[0]))]


def keep(key: str, yours: pd.DataFrame) -> None:
    """Hold your hand-written check against the production one, and save what yours found."""
    theirs = production(key)
    same = len(yours) == len(theirs) and store_days(yours) == store_days(theirs)
    print(f"Your query:           {len(yours)} bad rows")
    print(f"The production check: {len(theirs)} bad rows  ({inspect.BY_KEY[key].name})")
    print("The same rows." if same else "NOT the same. Look again.")
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / "answer_key_checks.json"
    saved = json.loads(path.read_text()) if path.exists() else {}
    saved[key] = {"bad_rows": len(yours), "store_days": store_days(yours)}
    path.write_text(json.dumps(dict(sorted(saved.items())), indent=2) + "\n")


def load_result(name: str):
    path = RESULTS_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Run the earlier notebooks first: {path.name} is not there yet.")
    return json.loads(path.read_text())


# ---- Small things -----------------------------------------------------------


def money(cents) -> str:
    """Whole cents as dollars: 79618 becomes $796.18."""
    cents = int(cents)
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) // 100:,}.{abs(cents) % 100:02d}"


def bonus(cents) -> int:
    """A manager's bonus on some net sales: 1%, rounded to a cent."""
    return bonus_cents(int(cents))


def true_net_sales(store_id: str, day: str) -> int:
    """The answer key: what the store really sold that day, from the tills' own journal."""
    key = json.loads((PROJECT_DIR / "answer_key" / "answer_key.json").read_text())
    for row in key["daily_store_sales"]:
        if (row["store_id"], row["sales_date"]) == (store_id, day):
            return row["net_sales_cents"]
    raise KeyError(f"The answer key has no row for {store_id} on {day}")
