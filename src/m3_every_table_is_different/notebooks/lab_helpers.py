"""Small helpers for the milestone 3 lab.

Nothing here is hidden magic. Open this file any time you want to see how
something works.
"""

import json
import re
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from IPython.display import Markdown, display

LAB_DIR = Path(__file__).resolve().parent
MILESTONE_DIR = LAB_DIR.parent
PROJECT_DIR = MILESTONE_DIR.parents[1]
RESULTS_DIR = LAB_DIR / "results"

# Lets a notebook write `from lotus_pipeline import ...` (the production code).
if str(MILESTONE_DIR) not in sys.path:
    sys.path.insert(0, str(MILESTONE_DIR))

from lotus_pipeline import config, inspect, pipeline  # noqa: E402
from lotus_pipeline.checks import STANDARD_CHECKS  # noqa: E402
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
        tables = wh.value(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'clean'"
        )
        green = wh.value("SELECT count(*) FROM ops.check_results WHERE passed")
    print(
        f"The bonus week is loaded: seven nights, Mon 24 to Sun 30 August. {tables} clean tables."
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


# ---- The lab: private copies you may break ----------------------------------

_COPIED: dict[str, str] = {}  # the tables copied in this notebook: real name -> lab name


def _lab_name(table: str) -> str:
    """clean.products becomes lab.products. raw.stores becomes lab.raw_stores."""
    schema, name = table.split(".")
    return f"lab.{name}" if schema == "clean" else f"lab.{schema}_{name}"


def lab_copy(*tables: str) -> None:
    """Make a fresh private copy of one or more tables, in the schema `lab`.

    You plant faults in the copies. The real tables are never touched.
    """
    with connect() as wh:
        wh.execute("CREATE SCHEMA IF NOT EXISTS lab")
        for table in tables:
            copy = _lab_name(table)
            wh.execute(f"DROP TABLE IF EXISTS {copy}")
            wh.execute(f"CREATE TABLE {copy} AS SELECT * FROM {table}")
            _COPIED[table] = copy
            rows = wh.value(f"SELECT count(*) FROM {copy}")
            print(f"{copy} is a fresh copy of {table}: {rows:,} rows.")


def plant(statement: str, params: tuple = ()) -> None:
    """Change a private copy. It refuses to touch anything outside the lab."""
    words = statement.replace("\n", " ").lower()
    if "lab." not in words or "clean." in words or "raw." in words or "output." in words:
        raise ValueError("plant() only changes the private copies in the schema lab.")
    with connect() as wh:
        wh.execute(statement, params)
    print("Planted in the lab copy.")


def on_lab(query: str) -> str:
    """The same query, pointed at the private copies you made in this notebook."""
    for table, copy in _COPIED.items():
        query = re.sub(rf"\b{re.escape(table)}\b", copy, query)
    return query


def rebuild_sales_lines() -> None:
    """Run the pipeline's own cleaning join again, into the private copy lab.sales_lines.

    It is the exact SQL of the clean step. Only the table names differ: it reads your
    lab copies of products and stores where you made them, and it writes to the lab.
    """
    from lotus_pipeline import clean

    statement = on_lab(clean.SALES_LINES).replace(
        "CREATE TABLE clean.sales_lines", "CREATE TABLE lab.sales_lines"
    )
    with connect() as wh:
        wh.execute("DROP TABLE IF EXISTS lab.sales_lines")
        wh.execute(statement)
        rebuilt = wh.value("SELECT count(*) FROM lab.sales_lines")
        real = wh.value("SELECT count(*) FROM clean.sales_lines")
    _COPIED["clean.sales_lines"] = "lab.sales_lines"
    print(f"The real clean.sales_lines has {real:,} rows.")
    print(f"Rebuilt with your lab copy: {rebuilt:,} rows. That is {rebuilt - real:+,}.")


def rebuilt_against_real() -> pd.DataFrame:
    """Each store's five weeks of net sales: the real table beside the rebuilt lab copy."""
    table = sql("""
        SELECT real.store_id,
               real.cents AS real_cents,
               coalesce(rebuilt.cents, 0) AS rebuilt_cents,
               coalesce(rebuilt.cents, 0) - real.cents AS difference_cents
        FROM (SELECT store_id, sum(amount_cents) AS cents
              FROM clean.sales_lines GROUP BY store_id) real
        LEFT JOIN (SELECT store_id, sum(amount_cents) AS cents
                   FROM lab.sales_lines GROUP BY store_id) rebuilt
          ON rebuilt.store_id = real.store_id
        ORDER BY real.store_id
    """)
    for column in ("real_cents", "rebuilt_cents", "difference_cents"):
        table[column.replace("_cents", "")] = table.pop(column).map(money)
    return table


def standard_checks(on_the_lab: bool = False) -> None:
    """Run the five standard checks of the nightly pipeline, on the real tables or the lab."""
    for check in STANDARD_CHECKS:
        query = on_lab(check.sql) if on_the_lab else check.sql
        bad = sql(query, (LAST_NIGHT,) if check.for_the_day else ())
        print(
            f"{check.number}. {check.name:<32} {len(bad)} bad rows  {'PASS' if bad.empty else 'FAIL'}"
        )


# ---- The check cards --------------------------------------------------------


def show_card(table: str, question: int) -> None:
    """Show one of the eight questions of a table's check card."""
    text = (MILESTONE_DIR / "checks" / f"{table}.md").read_text()
    match = re.search(rf"^## ({question}\. .*?)\n(.*?)(?=^## |\Z)", text, re.M | re.S)
    if not match:
        raise ValueError(f"The card of {table} has no question {question}")
    display(
        Markdown(
            f"**From the check card `checks/{table}.md`, question {match.group(1)}**\n\n"
            f"{match.group(2).strip()}"
        )
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


def where(bad_rows: pd.DataFrame) -> list[list[str]]:
    """Where the bad rows are: a store and a day, or a thing and a day. Each once."""
    found = set()
    for _, row in bad_rows.iterrows():
        if "store_id" in row and "sales_date" in row:
            found.add((str(row["store_id"]), str(row["sales_date"])[:10]))
        else:
            day = row.get("day")
            found.add((str(row["thing"]), "" if day is None or pd.isna(day) else str(day)[:10]))
    return [list(pair) for pair in sorted(found, key=lambda p: (p[1], p[0]))]


def keep(key: str, yours: pd.DataFrame) -> None:
    """Hold your hand-written check against the production one, and save what yours found."""
    theirs = production(key)
    same = len(yours) == len(theirs) and where(yours) == where(theirs)
    print(f"Your query:           {len(yours)} bad rows")
    print(f"The production check: {len(theirs)} bad rows  ({inspect.BY_KEY[key].name})")
    print("The same rows." if same else "NOT the same. Look again.")
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / "answer_key_checks.json"
    saved = json.loads(path.read_text()) if path.exists() else {}
    saved[key] = {"bad_rows": len(yours), "where": where(yours)}
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


def mask(email: str) -> str:
    """Hide most of an email, so it can be shown: priya.nair@example.com becomes p***@example.com."""
    name, _, domain = str(email).strip().partition("@")
    return f"{name[:1].lower()}***@{domain.lower()}"


def planted_case(number: int) -> dict:
    """One of the thirteen planted cases, from the answer key."""
    cases = json.loads((PROJECT_DIR / "answer_key" / "planted_cases.json").read_text())["cases"]
    return next(case for case in cases if case["case"] == number)
