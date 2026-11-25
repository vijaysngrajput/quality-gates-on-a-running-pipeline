"""Build the numbers shown on milestone_3_flow.html and milestone_3_recap.html.

The pages must never show a number typed by hand. This script runs the real
pipeline for the seven nights of the bonus week, on a private warehouse
(one DuckDB file in a temporary folder), and then runs this milestone's checks
and the supplier's contract, the way `inspect` and `contract` do.

Two tables, products and stores, are right on every day. Their faults are planted
in the lab: on a private copy of four clean weeks, exactly as the tests plant them.
The pipeline is then run again on that copy, and the page shows what really happened.

    uv run python -m lotus_pipeline.flow_data          write the pages' data
    uv run python -m lotus_pipeline.flow_data --check  fail if a page is out of date

It never touches your own warehouse, Docker or Airflow.
"""

import csv
import json
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

from lotus_pipeline import checks, config, contract, inspect, load, pipeline
from lotus_pipeline.inspect import ALL_CHECKS, CHECKS_BY_TABLE
from lotus_pipeline.other_checks import OTHER_CHECKS
from lotus_pipeline.report import money
from lotus_pipeline.warehouse import connect, plain
from store_systems import files, planted, truth
from store_systems.catalog import BONUS_WEEK_START, CLOSED, HOLIDAY, LAST_DAY, PRODUCTS
from store_systems.generate import build as build_world

MILESTONE_DIR = config.MILESTONE_DIR
PAGE = MILESTONE_DIR / "milestone_3_flow.html"
RECAP_PAGE = MILESTONE_DIR / "milestone_3_recap.html"
DAG_FILE = MILESTONE_DIR / "dags" / "lotus_nightly_dag.py"
MARKER = re.compile(
    r'(<script id="page-data" type="application/json">)(.*?)(</script>)', flags=re.DOTALL
)
WEEK = [BONUS_WEEK_START + timedelta(days=n) for n in range(7)]
LAB_DAY = BONUS_WEEK_START - timedelta(days=1)  # the lab holds the four clean weeks
NEW_TABLES = ("products", "stores", "stock_counts", "supplier_prices", "members")
BY_KEY = {c.key: c for c in ALL_CHECKS}

QUESTION = (
    "Can I pay the bonus from last week's net sales, store by store? "
    "And if I cannot trust a number, which one, and why?"
)
OWN_QUESTION = (
    "What does each of the other five tables need, and why is it not what sales_lines needed?"
)
STILL_WRONG = (
    "The checks exist in notebooks. The pipeline does not run them, and a failure changes nothing."
)
NOT_IN_THE_RUN = (
    "All seven tables now have a check card and checks. You still start them by hand, "
    "the morning after."
)

# The words this milestone teaches. The meanings are the same as in GLOSSARY.md.
WORDS = {
    "Validity dates": "The first and the last day a row is true, such as the days a price applies.",
    "Snapshot table": "A table that holds the state of something on each day, such as a stock "
    "count. A missing day is a gap, not a zero.",
    "Duplicate": "One real thing stored more than once under different keys, such as one person "
    "with two member ids.",
    "Personal data": "Data about a person: a name, an email, a phone number. It must not travel "
    "into logs and check results.",
    "Schema": "The shape of a table or a file: the column names, the type of each column, "
    "and the rules for the values.",
    "Schema drift": "A schema that changes without anyone agreeing: a column renamed, "
    "added or removed.",
    "Breaking change": "A change that removes or changes something a reader uses, so old readers "
    "fail. Its opposite is an additive change, which only adds.",
    "Change of meaning": "The shape stays the same and the meaning changes, such as dollars sent "
    "as cents. No schema check can see it.",
    "Data contract": "A written agreement between the producer of data and its consumers: "
    "an owner, the columns, the rules for the values, and a change policy.",
}

# One word from milestone 2 is needed again, for the stores table.
RECALLED = {
    "Reference data": "A small list that people keep by hand, and that other data is judged "
    "against: prices, opening hours, holidays.",
}
ALL_WORDS = WORDS | RECALLED

# The word a check teaches when it catches a row. Other checks teach the word of their case.
CHECK_WORD = {
    "files_have_their_columns": "Schema drift",
    "cost_is_filled": "Schema",
    "cost_near_the_last_file": "Change of meaning",
    "cost_below_the_shelf_price": "Change of meaning",
    "a_count_for_every_day": "Snapshot table",
    "one_person_once": "Duplicate",
    "one_price_at_a_time": "Validity dates",
    "no_gap_between_prices": "Validity dates",
    "six_stores_once": "Duplicate",
}

# Why a family of check is on one card only. The counts come from inspect.side_by_side().
ONLY_ONE_BECAUSE = {
    "a row agrees with its neighbours": "Only a price has a start and an end, so only products "
    "is checked row against the next row.",
    "it has the columns we expect": "Only the supplier's file is written by another company, "
    "so it is the file most likely to change its columns.",
    "it looks like a normal day": "Only sales arrive every day in an amount that has a rhythm. "
    "A list of people has no normal day.",
    "two counts of one thing agree": "Only the money is counted twice: once by the receipts, "
    "once by the till. No other table has a second count.",
}

LENSES = [
    {"id": "data", "name": "Data", "meaning": "A data check tests the values themselves."},
    {
        "id": "business",
        "name": "Business",
        "meaning": "A business check tests a rule of how the company works.",
    },
    {
        "id": "operational",
        "name": "Operational",
        "meaning": "An operational check tests the pipeline: did it run, on time, "
        "with a normal amount of data.",
    },
]

STAGES = [
    {"id": "files", "role": "Source", "name": "Store files", "task": "stores_send_files"},
    {"id": "raw", "role": "As received", "name": "Raw tables", "task": "load"},
    {"id": "clean", "role": "Typed, joined", "name": "Clean tables", "task": "clean"},
    {"id": "checks", "role": "Five standard checks", "name": "Checks", "task": "check"},
    {"id": "outputs", "role": "What people read", "name": "Outputs", "task": "publish"},
    {"id": "people", "role": "Consumers", "name": "The people", "task": None},
]

# The three places a gate will stand. None exists in this milestone either.
GATES = [
    {"id": "landing", "before": "raw", "when": "when the files land", "state": "not_yet"},
    {"id": "cleaned", "before": "checks", "when": "after cleaning", "state": "not_yet"},
    {"id": "publish", "before": "outputs", "when": "before the outputs are published",
     "state": "not_yet"},
]  # fmt: skip

READERS = [
    {"who": "Store managers", "reads": "daily_store_sales", "hurt": "wrong pay"},
    {"who": "Head of retail operations", "reads": "the bonus report",
     "hurt": "she signs a wrong number"},
    {"who": "The replenishment team", "reads": "replenishment_list",
     "hurt": "empty shelves, or wasted food"},
    {"who": "Finance", "reads": "the costs in replenishment_list",
     "hurt": "wrong money leaves the company"},
    {"who": "Marketing", "reads": "the clean members table",
     "hurt": "two emails to one person"},
]  # fmt: skip

ORDER = ["files", "raw", "clean", "checks", "outputs", "people"]
EMPTY = {"count": "", "cards": []}


def _day(day: date) -> str:
    """2026-08-24 -> Mon 08-24"""
    return day.strftime("%a %m-%d")


def _qty(value) -> str:
    """'2.000' -> 2, '1.180' -> 1.18"""
    return f"{float(value):g}"


def _count(n: int, thing: str) -> str:
    return f"{n} {thing}" if n == 1 else f"{n} {thing}s"


def _and(things: list[str]) -> str:
    named = [f'"{t}"' for t in things]
    return named[0] if len(named) == 1 else ", ".join(named[:-1]) + " and " + named[-1]


def _mask(email: str) -> str:
    """An email with its name hidden: the first letter, three dots, the domain."""
    local, _, domain = email.partition("@")
    return f"{local[:1]}•••@{domain}"


def _card(title: str, amount: str, sub: str, tone: str = "") -> dict:
    return {"id": title, "amount": amount, "sub": sub, "tone": tone}


def _sorted(rows: list[dict]) -> list[dict]:
    """A query gives its rows in no promised order. Sort them, so the page never changes."""
    return sorted(
        ({k: plain(v) for k, v in row.items()} for row in rows),
        key=lambda row: json.dumps(row, default=str, sort_keys=True),
    )


def _findings(wh) -> dict[str, list[dict]]:
    return {f.check.key: _sorted(f.bad_rows) for f in inspect.run_all(wh)}


def _table(rows: list[dict], limit: int = 4) -> dict:
    """The bad rows a check returned, ready to show: money as dollars, at most four rows."""
    repeated = {"store_id"} | (
        {"thing"} if {"supplier_id", "first_member_id"} & set(rows[0]) else set()
    )
    repeated |= {"table_name"} if "kind_of_change" in rows[0] else set()
    columns = [c for c in rows[0] if c not in repeated][:7]

    def show(column: str, value) -> str:
        if value is None:
            return "none"
        if column.endswith("_cents"):
            return money(int(value))
        return str(value)

    return {
        "columns": [c.removesuffix("_cents") for c in columns],
        "rows": [[show(c, row[c]) for c in columns] for row in rows[:limit]],
        "more": max(len(rows) - limit, 0),
    }


def _read_csv(folder: Path, table: str, name: str) -> tuple[list[str], list[dict]]:
    with (folder / table / name).open(newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


# ---------------------------------------------------------------- the steps of one case


def _steps(
    *,
    word: str,
    words: dict,
    clocks: dict,
    text: dict,
    full: dict,
    catches: list[tuple],
    silent: tuple,
    door: dict | None = None,
    all_caught: list[str],
    failed_check: int | None = None,
    outcome: str,
) -> list[dict]:
    """The same path for every case: the nightly run, then this milestone's checks by hand.

    `full` holds what each stage shows once the rows have reached it. `catches` is a list of
    (check, its bad rows, one sentence). `silent` is (check, why it says nothing).
    """

    def the_word(step_id: str) -> dict:
        term = words.get(step_id, word)
        return {"term": term, "meaning": ALL_WORDS[term]}

    done = 5 if failed_check is None else failed_check - 1
    check_state = {"done": done, "active": 0, "failed": [failed_check] if failed_check else []}

    def stages(upto: str, with_checks: bool = True) -> dict:
        reach = ORDER.index(upto)
        shown = {}
        for name in ORDER:
            if name == "checks":
                shown[name] = check_state if with_checks else {"done": 0, "active": 0, "failed": []}
            elif ORDER.index(name) <= reach:
                shown[name] = full[name]
            else:
                shown[name] = EMPTY
        return shown

    def reasoned(caught=(), quiet=(), now=None, all_done=False) -> dict:
        caught, quiet = list(caught), list(quiet)
        if all_done:
            caught = list(all_caught)
            quiet = [c.key for c in ALL_CHECKS if c.key not in caught]
        return {"run": bool(caught or quiet or now), "caught": caught, "silent": quiet, "now": now}

    not_run = reasoned()
    night, desk = clocks["night"], clocks["desk"]
    reach = "clean" if failed_check else "people"
    standard_title = (
        "Task check: the five standard checks, all green"
        if failed_check is None
        else f"Task check: standard check {failed_check} fails"
    )
    steps = [
        {"id": "happened", "active": "files", "task": None, "clock": clocks["happened"],
         "title": "What really happened", "text": text["happened"],
         "stages": stages("files", False) | {"files": EMPTY}, "moves": []},
        {"id": "files", "active": "files", "task": "stores_send_files", "clock": clocks["files"],
         "title": text["files_title"], "text": text["files"],
         "stages": stages("files", False), "moves": [], "plant": True, "problem": True},
        {"id": "load", "active": "raw", "task": "load", "clock": night,
         "title": "Task load: copy the files, as received", "text": text["load"],
         "stages": stages("raw", False), "moves": [{"from": "files", "to": "raw"}]},
        {"id": "clean", "active": "clean", "task": "clean", "clock": night,
         "title": "Task clean: give types, and join", "text": text["clean"],
         "stages": stages("clean", False), "moves": [{"from": "raw", "to": "clean"}]},
        {"id": "standard", "active": "checks", "task": "check", "clock": night,
         "title": standard_title, "text": text["standard"], "stages": stages("clean"),
         "moves": [{"from": "clean", "to": "checks"}], "problem": bool(failed_check)},
    ]  # fmt: skip
    if failed_check:
        steps[-1]["sql"] = textwrap.dedent(checks.STANDARD_CHECKS[failed_check - 1].sql).strip()
        steps.append(
            {"id": "publish", "active": "outputs", "task": None, "clock": night,
             "title": "Task publish: it does not run", "text": text["publish"],
             "stages": stages("clean") | {"outputs": full["outputs"]}, "moves": [], "problem": True}
        )  # fmt: skip
    else:
        steps.append(
            {"id": "publish", "active": "outputs", "task": "publish", "clock": night,
             "title": "Task publish: the check passed, so it runs", "text": text["publish"],
             "stages": stages("outputs"), "moves": [{"from": "checks", "to": "outputs"}],
             "problem": True}
        )  # fmt: skip
    steps.append(
        {"id": "people", "active": "people", "task": None, "clock": clocks["morning"],
         "title": text["people_title"], "text": text["people"],
         "stages": stages(reach) | {"outputs": full["outputs"], "people": full["people"]},
         "moves": [] if failed_check else [{"from": "outputs", "to": "people"}], "problem": True}
    )  # fmt: skip
    for step in steps:
        step["reasoned"] = not_run
    end = stages(reach) | {"outputs": full["outputs"], "people": full["people"]}

    steps.append(
        {"id": "prepare", "active": "reasoned", "task": None, "clock": desk,
         "title": "Prepare first: question 5 of the check card",
         "text": "Now run the same night again, with this milestone's checks. You start them "
                 "yourself: python -m lotus_pipeline inspect. Before any rule can be tested, "
                 f"the data must be prepared. {text['prepare']}",
         "facts": [[f"Check {c.number}", c.prepare] for c, _rows, _why in catches],
         "stages": end, "moves": [], "reasoned": reasoned(now=catches[0][0].key)}
    )  # fmt: skip
    so_far: list[str] = []
    for check, rows, why in catches:
        so_far = so_far + [check.key]
        steps.append(
            {"id": f"catch_{check.key}", "active": "reasoned", "task": None, "clock": desk,
             "title": f"Check {check.number} catches it: {check.name.lower()}",
             "text": f"{why} It returns {_count(len(rows), 'bad row')}.",
             "facts": [["Lens", check.lens], ["Protects", check.protects],
                       ["The limit", check.limit]],
             "sql": textwrap.dedent(check.sql).strip(), "rows": _table(rows),
             "stages": end, "moves": [], "reasoned": reasoned(so_far, now=check.key),
             "caught": True}
        )  # fmt: skip
        words.setdefault(f"catch_{check.key}", CHECK_WORD.get(check.key, word))
    quiet_check, quiet_why = silent
    steps.append(
        {"id": "silent", "active": "reasoned", "task": None, "clock": desk,
         "title": f"Check {quiet_check.number} stays silent: {quiet_check.name.lower()}",
         "text": f"{quiet_why} A check answers only the question it asks. That is why each "
                 "table needs its own card, and each check its own reason.",
         "facts": [["Lens", quiet_check.lens], ["The limit", quiet_check.limit]],
         "stages": end, "moves": [],
         "reasoned": reasoned(so_far, [quiet_check.key], now=quiet_check.key)}
    )  # fmt: skip
    if door:
        steps.append(
            {"id": "door", "active": "files", "task": None, "clock": desk,
             "title": f"At the door: the contract would {door['verdict']} this file",
             "text": door["text"], "facts": door["facts"], "rows": door["rows"],
             "stages": end, "moves": [], "reasoned": reasoned(so_far, [quiet_check.key]),
             "caught": True, "door": True}
        )  # fmt: skip
    steps.append(
        {"id": "still_wrong", "active": "outputs", "task": None, "clock": desk,
         "title": "Still wrong: the checks found it, and nothing changed",
         "text": f"{_count(len(all_caught), 'check')} of {len(ALL_CHECKS)} found bad rows for "
                 f"this fault. But you ran them by hand, the morning after. The nightly run "
                 f"does not know them. {outcome} There is no gate to stop a bad night, and no "
                 "quarantine to set a bad row aside. The dashed lines show where they will stand.",
         "stages": end, "moves": [], "reasoned": reasoned(all_done=True), "problem": True}
    )  # fmt: skip

    finished = {"happened": 0, "files": 0, "load": 1, "clean": 2, "standard": 3, "publish": 4}
    for step in steps:
        step["word"] = the_word(step["id"])
        if failed_check:
            step["tasks_done"] = min(finished.get(step["id"], 3), 3)
            step["failed_task"] = (
                "check"
                if step["id"] not in finished or step["id"] in ("standard", "publish")
                else None
            )
        else:
            step["tasks_done"] = finished.get(step["id"], 5)
            step["failed_task"] = None
    return steps


def _caught_by(found: dict) -> list[dict]:
    return [
        {"number": c.number, "name": c.name, "lens": c.lens, "table": inspect.table_of(c)}
        for c in ALL_CHECKS
        if found.get(c.key)
    ]


def _clocks(day: date) -> dict:
    return {
        "happened": f"{_day(day)}, during the day",
        "files": f"{_day(day)}, 23:00",
        "night": f"Night after {_day(day)}, 02:00",
        "morning": f"{_day(day + timedelta(days=1))}, morning",
        "desk": f"{_day(day + timedelta(days=1))}, 09:00, at your desk",
    }


def _green(wh, day: date) -> str:
    """Prove the five standard checks passed that night, and say so."""
    results = wh.query("SELECT passed FROM ops.check_results WHERE run_day = ?", (day,))
    assert len(results) == 5 and all(r["passed"] for r in results), day
    return (
        "The task check asks five questions: is the table empty, is a key empty, is a key "
        "there twice, is a status not allowed, is a total not above zero. All five look at "
        "sales_lines. Five queries, and each returns no rows."
    )


# ---------------------------------------------------------------- the four planted cases


def _case_10(wh, world, landing: Path, found: dict) -> dict:
    """The supplier renames a column."""
    case = planted.case(10)
    day, supplier = case["day"], case["supplier"]
    name = f"supplier_prices_{supplier}_{day}.csv"
    header, in_file = _read_csv(landing, "supplier_prices", name)
    old, new = case["old_name"], case["new_name"]
    assert new in header and old not in header
    raw = wh.query(
        "SELECT * FROM raw.supplier_prices WHERE _file_name = ? ORDER BY product_id", (name,)
    )
    clean = wh.query(
        "SELECT * FROM clean.supplier_prices WHERE supplier_id = ? AND price_date = ? ORDER BY product_id",
        (supplier, day),
    )
    assert all(r["unit_cost"] is None for r in raw) and all(
        r["unit_cost_cents"] is None for r in clean
    )
    orders = wh.query(
        "SELECT * FROM output.replenishment_list WHERE for_date = CAST(? AS DATE) + 1 "
        "AND unit_cost_cents IS NULL ORDER BY order_qty DESC, store_id, product_id",
        (day,),
    )
    to_order = [o for o in orders if o["order_qty"] > 0]
    products = sorted({o["product_id"] for o in orders})
    assert products == sorted(r["product_id"] for r in in_file)
    true_orders = truth.replenishment_list(world, day)
    true_cost = sum(
        int(round(float(o["order_qty"]) * o["unit_cost_cents"]))
        for (_store, product), o in true_orders.items()
        if product in products
    )
    names = {p: PRODUCTS[p][0] for p in products}

    mine = {
        "files_have_their_columns": [
            r for r in found["files_have_their_columns"] if r["thing"] == name
        ],
        "cost_is_filled": [r for r in found["cost_is_filled"] if r["supplier_id"] == supplier],
    }
    assert len(mine["files_have_their_columns"]) == 2 and len(mine["cost_is_filled"]) == len(
        products
    )
    quiet = BY_KEY["cost_near_the_last_file"]
    assert not [r for r in found[quiet.key] if r["supplier_id"] == supplier]
    problems = contract.check_file(landing / "supplier_prices" / name)
    verdict = contract.verdict(problems)
    assert verdict == "refuse"

    full = {
        "files": {"count": f"{len(in_file)} rows from {supplier}", "cards": [
            _card(names[r["product_id"]], f"${r[new]}", f"{new} · per {r['unit']}", "bad")
            for r in in_file[:4]]},
        "raw": {"count": f"{len(raw)} rows, unit_cost empty", "cards": [
            _card(names[r["product_id"]], "empty", f"unit_cost · per {r['unit']}", "bad")
            for r in raw[:4]]},
        "clean": {"count": f"{len(clean)} rows, no cost", "cards": [
            _card(names[r["product_id"]], "empty", f"unit_cost_cents · {r['price_date']:%m-%d}", "bad")
            for r in clean[:4]]},
        "outputs": {"count": "replenishment_list", "cards": [
            _card(o["product_name"], "no cost", f"{o['store_id']} · order {_qty(o['order_qty'])}", "bad")
            for o in to_order[:3]] + [
            _card("Answer key", money(true_cost), f"the true cost of these {len(to_order)} order lines", "good")]},
        "people": {"count": "", "cards": [
            _card("Finance", "no cost", f"{len(to_order)} order lines they cannot approve", "bad")]},
    }  # fmt: skip
    text = {
        "happened": f"{supplier} is a supplier of fruit, vegetables, meat and fish. Every Tuesday "
        f"its system writes a price file for Lotus: {len(in_file)} products, each with its cost. "
        f"This week somebody at {supplier} tidied up the program, and the column {old} got a "
        f"new name: {new}. Nobody at Lotus was told. Lotus does not control this file.",
        "files_title": "The supplier sends its file, with a renamed column",
        "files": f"The file arrives on time. It has {len(header)} columns, as always, and "
        f"{len(in_file)} rows. Every cost is in it, and every cost is right. Only the first "
        f"line of the file is different: {', '.join(header)}.",
        "load": f"The task load reads each file by column name. It asks the file for {old}. "
        f"The file has no column with that name, so the loader writes an empty cell, "
        f"{len(raw)} times. Nothing fails. The column {new} is not expected, so it is thrown away.",
        "clean": f"The task clean turns the text into numbers. An empty cell becomes an empty "
        f"number. clean.supplier_prices now has {len(clean)} rows for {supplier} with no cost.",
        "standard": _green(wh, day) + " None of them looks at supplier_prices at all.",
        "publish": f"Airflow starts the task publish. The order list for tomorrow takes each "
        f"product's newest cost. For {len(products)} products the newest cost is empty. "
        f"{len(orders)} rows of replenishment_list have no cost, and {len(to_order)} of them "
        "are products a store must order.",
        "people_title": "Finance cannot approve tomorrow's order",
        "people": f"Finance approves each order from its cost. For {len(to_order)} order lines "
        f"the cost is empty. The true cost of those lines is {money(true_cost)}. Nobody knows "
        "why the number is missing, and the report still says: 5 of 5 checks passed.",
        "prepare": "The loaded table cannot say which columns a file had: a column the loader "
        "did not expect is not in it. So the loader now keeps the first line of every file, "
        "one row per column, in raw.file_columns.",
    }
    catches = [
        (BY_KEY["files_have_their_columns"], mine["files_have_their_columns"],
         "This check puts the columns Lotus expects beside the columns the file really had. "
         f"{old} is missing: old readers fail, so that is a breaking change. {new} is new: "
         "that is an additive change."),
        (BY_KEY["cost_is_filled"], mine["cost_is_filled"],
         "A plain not-null check also sees it, one step later: after the load, the cost "
         "is empty. It can say that the cost is missing. It cannot say why."),
    ]  # fmt: skip
    kinds = sorted({p["kind"] for p in problems})
    door = {
        "verdict": verdict,
        "text": f"Lotus and its suppliers have a written agreement for this file: the data "
        f"contract in contracts/supplier_prices.yml. Held against it before the load, this file "
        f"has {_count(len(problems), 'problem')}: {' and '.join(kinds)}. The verdict is "
        f"\"{verdict}\". A check speaks after the load. A contract speaks at the door.",
        "facts": [["Command", "python -m lotus_pipeline contract"], ["Verdict", verdict]],
        "rows": {"columns": ["kind", "column", "problem"],
                 "rows": [[p["kind"], p["column"], p["problem"]] for p in problems], "more": 0},
    }  # fmt: skip
    mine_all = {k: v for k, v in mine.items()}
    steps = _steps(
        word="Schema drift",
        words={"happened": "Schema", "load": "Schema", "door": "Data contract",
               "silent": "Breaking change", "still_wrong": "Schema drift"},
        clocks=_clocks(day), text=text, full=full, catches=catches,
        silent=(quiet, f"This check compares each cost with the cost in {supplier}'s last file. "
                "An empty cost cannot be compared with anything, so no row comes back."),
        door=door, all_caught=list(mine_all),
        outcome=f"replenishment_list still has no cost for {len(products)} products.",
    )  # fmt: skip
    return {
        "id": "10", "label": "Case 10", "hint": "the supplier renames a column",
        "name": case["name"], "day": _day(day), "store": supplier,
        "where": f"{_day(day)} · supplier {supplier} · table supplier_prices",
        "table": "supplier_prices", "output": "replenishment_list", "lab": False,
        "says_label": "The order list says", "key_label": "The answer key says",
        "pipeline_says": "no cost", "answer_key_says": money(true_cost),
        "table_is_right": False,
        "note": f"{len(to_order)} order lines for {len(products)} products have no cost.",
        "caught_by": _caught_by(mine_all), "at_the_door": verdict, "steps": steps,
    }  # fmt: skip


def _case_11(wh, world, landing: Path, found: dict) -> dict:
    """The supplier sends cost in cents."""
    case = planted.case(11)
    day, supplier = case["day"], case["supplier"]
    name = f"supplier_prices_{supplier}_{day}.csv"
    header, in_file = _read_csv(landing, "supplier_prices", name)
    assert header == load.TABLES["supplier_prices"]  # the shape is perfect
    clean = wh.query(
        "SELECT * FROM clean.supplier_prices WHERE supplier_id = ? AND price_date = ? ORDER BY product_id",
        (supplier, day),
    )
    products = [r["product_id"] for r in clean]
    names = {p: PRODUCTS[p][0] for p in products}
    true_costs = truth.true_costs(world, day)
    orders = wh.query(
        "SELECT * FROM output.replenishment_list WHERE for_date = CAST(? AS DATE) + 1 "
        "ORDER BY order_cost_cents DESC, store_id, product_id",
        (day,),
    )
    mine_orders = [o for o in orders if o["product_id"] in products and o["order_qty"] > 0]
    says = sum(int(o["order_cost_cents"]) for o in mine_orders)
    true_orders = truth.replenishment_list(world, day)
    right = sum(
        int(round(float(o["order_qty"]) * o["unit_cost_cents"]))
        for (_store, product), o in true_orders.items()
        if product in products
    )
    first = clean[0]
    true_first = int(true_costs[first["product_id"]] * 100)
    assert first["unit_cost_cents"] == 100 * true_first

    mine = {
        key: [
            r
            for r in found[key]
            if r["supplier_id"] == supplier and str(r["day"]) == day.isoformat()
        ]
        for key in ("cost_near_the_last_file", "cost_below_the_shelf_price")
    }
    assert all(len(rows) == len(products) for rows in mine.values())
    quiet = BY_KEY["files_have_their_columns"]
    assert not [r for r in found[quiet.key] if r["thing"] == name]
    assert not [r for r in found["cost_is_filled"] if r["supplier_id"] == supplier]
    problems = contract.check_file(landing / "supplier_prices" / name)
    verdict = contract.verdict(problems)
    assert verdict == "refuse" and len(problems) == len(products)

    def cost_cards(rows, column, as_cents):
        return [
            _card(names[r["product_id"]],
                  money(int(r[column])) if as_cents else str(r[column]),
                  f"true cost {money(int(true_costs[r['product_id']] * 100))}", "bad")
            for r in rows[:4]
        ]  # fmt: skip

    full = {
        "files": {"count": f"{len(in_file)} rows from {supplier}", "cards": cost_cards(in_file, "unit_cost", False)},
        "raw": {"count": f"{len(in_file)} rows, every cell filled", "cards": cost_cards(in_file, "unit_cost", False)},
        "clean": {"count": f"{len(clean)} rows, read as dollars", "cards": cost_cards(clean, "unit_cost_cents", True)},
        "outputs": {"count": "replenishment_list", "cards": [
            _card(o["product_name"], money(int(o["order_cost_cents"])),
                  f"{o['store_id']} · order {_qty(o['order_qty'])}", "bad")
            for o in mine_orders[:3]] + [
            _card("Answer key", money(right), f"all {len(mine_orders)} order lines of {supplier}", "good")]},
        "people": {"count": "", "cards": [
            _card("Finance", money(says), f"tomorrow's order from {supplier}", "bad"),
            _card("Answer key", money(right), "the same order, at the true cost", "good")]},
    }  # fmt: skip
    text = {
        "happened": f"{supplier} supplies the pantry, the drinks and the household goods: "
        f"{len(products)} products. Lotus and {supplier} agreed that the cost is written in "
        f"dollars, like {true_costs[first['product_id']]:.2f}. This week {supplier} changed its "
        f"system, and the file now holds whole cents: {in_file[0]['unit_cost']}. "
        "It is the same money, written another way. Nobody at Lotus was told.",
        "files_title": "The supplier sends its file, in cents",
        "files": f"The file arrives on time. Its columns are the agreed ones: "
        f"{', '.join(header)}. It has {len(in_file)} rows and no empty cell. Every unit_cost "
        "is a valid number. Every one is 100 times too high.",
        "load": f"The task load finds every column it asks for, and copies {len(in_file)} rows. "
        "The shape of the file is perfect. A load can test a shape. It cannot test a meaning.",
        "clean": f"The task clean reads unit_cost as dollars and turns it into cents. "
        f"{in_file[0]['unit_cost']} becomes {money(int(first['unit_cost_cents']))} for one "
        f"{names[first['product_id']]}. The true cost is {money(true_first)}.",
        "standard": _green(wh, day) + " And every cost is filled, positive and a number.",
        "publish": f"Airflow starts the task publish. The order list multiplies each quantity "
        f"by the newest cost. Tomorrow's order from {supplier} comes to {money(says)}. "
        f"At the true cost it is {money(right)}.",
        "people_title": "Finance sees an order 100 times too dear",
        "people": f"Finance opens tomorrow's order: {money(says)} for {supplier}, where "
        f"{money(right)} is right. If they notice, they stop a good order. If they do not, "
        "wrong money leaves the company. The report says: 5 of 5 checks passed.",
        "prepare": "A wrong meaning cannot be seen on the row itself: the value is a valid "
        "number. It shows only beside something else: the same product in the supplier's "
        "last file, or the price the product sells for.",
    }
    catches = [
        (BY_KEY["cost_near_the_last_file"], mine["cost_near_the_last_file"],
         f"This check joins each cost to the same product in {supplier}'s file of the week "
         "before. A cost may change. It does not become 100 times itself in a week."),
        (BY_KEY["cost_below_the_shelf_price"], mine["cost_below_the_shelf_price"],
         "A second table agrees: joined to the price list, every one of these products "
         "would cost Lotus more than it sells for."),
    ]  # fmt: skip
    door = {
        "verdict": verdict,
        "text": f"The contract says what a value must look like: unit_cost is dollars with "
        f"two decimals, between 0.01 and 50.00. Held against it before the load, "
        f"{len(problems)} values break that rule. The verdict is \"{verdict}\". The meaning "
        "was written down, so the door can test it.",
        "facts": [["Command", "python -m lotus_pipeline contract"], ["Verdict", verdict]],
        "rows": {"columns": ["kind", "column", "line", "value", "problem"],
                 "rows": [[p["kind"], p["column"], str(p["row"]), p["value"], p["problem"]]
                          for p in problems[:3]],
                 "more": len(problems) - 3},
    }  # fmt: skip
    steps = _steps(
        word="Change of meaning",
        words={"load": "Schema", "silent": "Schema", "door": "Data contract"},
        clocks=_clocks(day), text=text, full=full, catches=catches,
        silent=(quiet, "This check compares the columns of the file with the columns Lotus "
                "expects. Every column is there, with its agreed name. The not-null check "
                "is silent too: every cost is filled. The shape is perfect."),
        door=door, all_caught=list(mine),
        outcome=f"replenishment_list still says {money(says)} for tomorrow's {supplier} order.",
    )  # fmt: skip
    return {
        "id": "11", "label": "Case 11", "hint": "the supplier sends cost in cents",
        "name": case["name"], "day": _day(day), "store": supplier,
        "where": f"{_day(day)} · supplier {supplier} · table supplier_prices",
        "table": "supplier_prices", "output": "replenishment_list", "lab": False,
        "says_label": "The order list says", "key_label": "The answer key says",
        "pipeline_says": money(says), "answer_key_says": money(right), "table_is_right": False,
        "note": f"{len(products)} costs are 100 times too high. Every one is a valid number.",
        "caught_by": _caught_by(mine), "at_the_door": verdict, "steps": steps,
    }  # fmt: skip


def _case_12(wh, world, landing: Path, found: dict) -> dict:
    """A stock count is missing for one day."""
    case = planted.case(12)
    day, store, product = case["day"], case["store"], case["product"]
    product_name = PRODUCTS[product][0]
    _header, in_file = _read_csv(landing, "stock_counts", f"stock_counts_{store}_{day}.csv")
    assert product not in {r["product_id"] for r in in_file}
    counted = wh.value(
        "SELECT count(*) FROM clean.stock_counts WHERE store_id = ? AND count_date = ?",
        (store, day),
    )
    order = wh.query(
        "SELECT * FROM output.replenishment_list WHERE for_date = CAST(? AS DATE) + 1 "
        "AND store_id = ? AND product_id = ?",
        (day, store, product),
    )[0]
    right = truth.replenishment_list(world, day)[(store, product)]
    yesterday = wh.value(
        "SELECT counted_qty FROM clean.stock_counts WHERE store_id = ? AND product_id = ? "
        "AND count_date = CAST(? AS DATE) - 1",
        (store, product, day),
    )
    near = [r for r in in_file if r["product_id"] in ("P-002", "P-003", "P-004")]
    mine = {
        "a_count_for_every_day": [
            r for r in found["a_count_for_every_day"]
            if r["store_id"] == store and str(r["sales_date"]) == day.isoformat()
        ]
    }  # fmt: skip
    assert [r["product_id"] for r in mine["a_count_for_every_day"]] == [product]
    quiet = BY_KEY["count_values_allowed"]
    assert not found[quiet.key]
    over = _qty(order["order_qty"] - right["order_qty"])

    def count_cards(rows):
        return [
            _card(
                PRODUCTS[r["product_id"]][0],
                _qty(r["counted_qty"]),
                f"counted by {r['counted_by']}",
            )
            for r in rows
        ] + [_card(product_name, "no row", "nobody counted it", "bad")]

    full = {
        "files": {"count": f"{len(in_file)} counts from {store}", "cards": count_cards(near)},
        "raw": {"count": f"{len(in_file)} rows", "cards": count_cards(near)},
        "clean": {"count": f"{counted} counts, one missing", "cards": count_cards(near)},
        "outputs": {"count": "replenishment_list", "cards": [
            _card(product_name, f"order {_qty(order['order_qty'])}",
                  f"sold {_qty(order['sold_yesterday'])} · stock read as {_qty(order['stock_counted'])}", "bad"),
            _card("Answer key", f"order {_qty(right['order_qty'])}",
                  f"sold {_qty(right['sold_yesterday'])} · {_qty(right['stock_counted'])} on the shelf", "good")]},
        "people": {"count": "", "cards": [
            _card("The replenishment team", f"{_qty(order['order_qty'])} bottles", f"ordered for {store}", "bad"),
            _card("Answer key", f"{_qty(right['order_qty'])} bottles", f"{over} too many", "good")]},
    }  # fmt: skip
    text = {
        "happened": f"Before closing, somebody walks through store {store} and counts what is "
        f"on every shelf. Friday is busy for milk: the store sells "
        f"{_qty(right['sold_yesterday'])} bottles of {product_name}, and "
        f"{_qty(right['stock_counted'])} are left. Tonight the milk shelf is skipped. "
        f"Yesterday it was counted: {_qty(yesterday)} bottles.",
        "files_title": "The store sends its counts, with one shelf missing",
        "files": f"The file has {len(in_file)} rows. Every row in it is right. One row is not "
        f"in it: {product_name}. A stock count is a snapshot: the state of every shelf on one "
        "day. Here the snapshot has a hole.",
        "load": f"The task load copies {len(in_file)} rows. It cannot copy a row that is not "
        "there, and it does not know how many rows to expect.",
        "clean": f"The task clean gives each count its type. clean.stock_counts has {counted} "
        f"counts for {store} on {_day(day)}. No value is wrong.",
        "standard": _green(wh, day) + " None of them looks at stock_counts at all.",
        "publish": f"Airflow starts the task publish. The order rule is: twice what sold "
        f"yesterday, less what is on the shelf. For {product_name} there is no count, and the "
        f"pipeline reads the gap as zero. 2 x {_qty(order['sold_yesterday'])} - 0 is "
        f"{_qty(order['order_qty'])}. The right order is 2 x {_qty(right['sold_yesterday'])} - "
        f"{_qty(right['stock_counted'])}, which is {_qty(right['order_qty'])}.",
        "people_title": "The replenishment team orders too much milk",
        "people": f"The order list says {_qty(order['order_qty'])} bottles of {product_name} "
        f"for {store}. {_qty(right['order_qty'])} is right. {over} bottles too many will arrive, "
        "and milk does not keep. A missing day is a gap. It is not a zero.",
        "prepare": "A check can only return rows that exist. To find a row that is missing, "
        "you must first build the rows you expected: every open store, every product on sale, "
        "every day.",
    }
    catches = [
        (BY_KEY["a_count_for_every_day"], mine["a_count_for_every_day"],
         "This check builds the list of counts the day should have, and looks for the ones "
         "that are not there."),
    ]  # fmt: skip
    steps = _steps(
        word="Snapshot table", words={}, clocks=_clocks(day), text=text, full=full,
        catches=catches,
        silent=(quiet, "This check tests each count: not below zero, a whole number of bottles, "
                "counted on its own day. A row that is not there cannot have a wrong value."),
        all_caught=list(mine),
        outcome=f"replenishment_list still says: order {_qty(order['order_qty'])}.",
    )  # fmt: skip
    return {
        "id": "12", "label": "Case 12", "hint": "a stock count is missing for one day",
        "name": case["name"], "day": _day(day), "store": store,
        "where": f"{_day(day)} · store {store} · table stock_counts",
        "table": "stock_counts", "output": "replenishment_list", "lab": False,
        "says_label": "The order list says", "key_label": "The answer key says",
        "pipeline_says": f"order {_qty(order['order_qty'])}",
        "answer_key_says": f"order {_qty(right['order_qty'])}", "table_is_right": False,
        "note": f"{product_name}: the gap is read as zero stock.",
        "caught_by": _caught_by(mine), "steps": steps,
    }  # fmt: skip


def _case_13(wh, world, landing: Path, found: dict) -> dict:
    """The same member twice. No name, full email or phone number may reach the page."""
    case = planted.case(13)
    day = case["day"]
    first_id, second_id = case["first"]["member_id"], case["second"]["member_id"]
    _header, in_file = _read_csv(landing, "members", f"members_{day}.csv")
    by_id = {r["member_id"]: r for r in in_file}
    shown = [by_id[first_id], by_id[f"M-{int(first_id[2:]) + 1:04d}"],
             by_id[f"M-{int(second_id[2:]) - 1:04d}"], by_id[second_id]]  # fmt: skip
    members = wh.value("SELECT count(*) FROM clean.members")
    ids_twice = wh.value("SELECT count(*) - count(DISTINCT member_id) FROM clean.members")
    emails_twice = wh.value("SELECT count(*) - count(DISTINCT email) FROM clean.members")
    people = wh.value("SELECT count(DISTINCT lower(trim(email))) FROM clean.members")
    assert (ids_twice, emails_twice, members - people) == (0, 0, 1)
    mine = {"one_person_once": found["one_person_once"]}
    assert [(r["first_member_id"], r["second_member_id"]) for r in mine["one_person_once"]] == [
        (first_id, second_id)
    ]
    quiet = BY_KEY["email_has_the_right_shape"]
    assert not found[quiet.key]

    def member_cards(rows):
        return [
            _card(r["member_id"], _mask(r["email"]),
                  f"joined {r['joined_on']} · home store {r['home_store_id']}",
                  "bad" if r["member_id"] in (first_id, second_id) else "")
            for r in rows
        ]  # fmt: skip

    cards = member_cards(shown)
    full = {
        "files": {"count": f"{len(in_file)} members", "cards": cards},
        "raw": {"count": f"{len(in_file)} rows", "cards": cards},
        "clean": {"count": f"{members} members, at the end of the week", "cards": cards},
        "outputs": {"count": "no output table", "cards": [
            _card("clean.members", "read as it is", "marketing reads the clean table itself")]},
        "people": {"count": "", "cards": [
            _card("Marketing", "2 emails", f"to one person: {first_id} and {second_id}", "bad"),
            _card("Answer key", "1 person", f"{members} member ids, {people} people", "good")]},
    }  # fmt: skip
    text = {
        "happened": f"A customer of Lotus joined the loyalty app long ago. She is member "
        f"{first_id}. On {_day(day)} she gets a new phone and signs up again, with the same "
        f"email. This time the phone writes it with capital letters. The app gives her a second "
        f"id: {second_id}. This page never shows her name or her full email. They are personal "
        "data, and personal data must not travel into logs and check results.",
        "files_title": "The app sends the members, with one person twice",
        "files": f"The file has {len(in_file)} members. Two rows are one person: {first_id} with "
        f"{_mask(by_id[first_id]['email'])}, and {second_id} with "
        f"{_mask(by_id[second_id]['email'])}. As texts the two emails differ: one has capital "
        "letters. For a mail server they are the same address.",
        "load": f"The task load copies {len(in_file)} rows. Every member_id is new, so nothing "
        "looks twice.",
        "clean": f"The task clean gives each column its type. It does not touch the email. "
        f"At the end of the week clean.members has {members} rows. A unique check on member_id "
        f"finds {ids_twice} doubles. A unique check on the email as typed finds {emails_twice}.",
        "standard": _green(wh, day) + " None of them looks at members at all.",
        "publish": "Airflow starts the task publish. No output table is built from members. "
        "Marketing reads the clean table itself, so the two rows are already where they are read.",
        "people_title": "One person gets two emails, and two sets of points",
        "people": f"Marketing sends an offer to every member. One person gets it twice, and "
        f"her points are split over {first_id} and {second_id}. The table has {members} "
        f"member ids and {people} people.",
        "prepare": "The value must be cleaned before it can be compared: remove the spaces "
        "around the email and make it lower case. Only then group by it.",
    }
    catches = [
        (BY_KEY["one_person_once"], mine["one_person_once"],
         "This check cleans the email first, then groups by it. It names the two rows by "
         "their member ids. It returns no name and no email."),
    ]  # fmt: skip
    steps = _steps(
        word="Duplicate",
        words={"happened": "Personal data", "files": "Personal data", "silent": "Personal data"},
        clocks=_clocks(day), text=text, full=full, catches=catches,
        silent=(quiet, "This check takes each email apart at the @ sign: one @, text before "
                "it, a dot after it. Both emails have a perfect shape. Two well-formed rows "
                "can still be one person."),
        all_caught=list(mine),
        outcome=f"clean.members still has two rows for one person: {first_id} and {second_id}.",
    )  # fmt: skip
    return {
        "id": "13", "label": "Case 13", "hint": "the same member twice",
        "name": case["name"], "day": _day(day), "store": "members",
        "where": f"{_day(day)} · the loyalty app · table members",
        "table": "members", "output": None, "lab": False,
        "says_label": "The table says", "key_label": "The answer key says",
        "pipeline_says": f"{members} members", "answer_key_says": f"{people} people",
        "table_is_right": False,
        "note": "Members are shown by id, with the email masked. Personal data stays out of this page.",
        "caught_by": _caught_by(mine), "steps": steps,
    }  # fmt: skip


def _case_9(wh, world, found: dict) -> dict:
    """A table of money, from milestone 2, to compare the other tables with."""
    case = planted.case(9)
    day, store = case["day"], case["store"]
    names = {
        r["product_id"]: r["product_name"]
        for r in wh.query("SELECT DISTINCT product_id, product_name FROM clean.products")
    }
    raw = wh.query(
        "SELECT * FROM raw.sales_lines WHERE store_id = ? AND _file_name LIKE ? ORDER BY line_id",
        (store, f"sales_lines_{store}_{day}%"),
    )
    first = [r for r in raw if not r["_file_name"].endswith("_2.csv")]
    second = [r for r in raw if r["_file_name"].endswith("_2.csv")]
    assert len(first) == len(second) > 0
    says = int(
        wh.value(
            "SELECT net_sales_cents FROM output.daily_store_sales WHERE store_id = ? AND sales_date = ?",
            (store, day),
        )
    )
    right = truth.daily_store_sales(world)[(day, store)]["net_sales_cents"]
    manager = wh.value("SELECT manager_name FROM clean.stores WHERE store_id = ?", (store,))
    bonus_extra = truth.bonus_cents(says) - truth.bonus_cents(right)
    mine = {
        key: [
            r
            for r in rows
            if r.get("store_id") == store and str(r.get("sales_date")) == day.isoformat()
        ]
        for key, rows in found.items()
    }
    mine = {k: v for k, v in mine.items() if v}
    assert set(mine) == {"true_key_unique", "till_total", "normal_volume"}
    quiet = BY_KEY["every_store_reported"]

    def line(r, tone=""):
        return _card(names[r["product_id"]], money(int(r["amount_cents"])),
                     f"{r['line_id']} · qty {_qty(r['quantity'])}", tone)  # fmt: skip

    cards = [line(first[0]), line(first[1]), line(second[0], "bad"), line(second[1], "bad")]
    full = {
        "files": {"count": f"2 files, {len(first)} lines each", "cards": cards},
        "raw": {"count": f"{len(raw)} lines from {store}", "cards": cards},
        "clean": {"count": f"{len(raw)} lines from {store}", "cards": cards},
        "outputs": {"count": "daily_store_sales", "cards": [
            _card(f"{store} · {_day(day)}", money(says), f"{len(raw)} lines", "bad"),
            _card("Answer key", money(right), "from the till journal", "good")]},
        "people": {"count": "", "cards": [
            _card(f"{manager}, {store}", money(truth.bonus_cents(says)), f"bonus on {money(says)}", "bad"),
            _card("Answer key", money(truth.bonus_cents(right)), f"bonus on {money(right)}", "good")]},
    }  # fmt: skip
    text = {
        "happened": f"You met this fault in milestone 2. It is here to compare a table of money "
        f"with the five other tables. On {_day(day)} store {store} sells {money(right)}. At "
        "night the export program in the back office runs twice.",
        "files_title": "The store sends its sales file twice",
        "files": f"Two files arrive, with the same {len(first)} lines. The export program gives "
        "every line a new line_id on every export, so no id is there twice.",
        "load": f"The task load copies both files: {len(raw)} lines for one day.",
        "clean": "The task clean joins each line to its product and its store. Every line "
        "finds its partners, twice.",
        "standard": _green(wh, day) + " The key they trust, line_id, is unique.",
        "publish": f"Airflow starts the task publish. daily_store_sales says {money(says)} "
        f"for {store}. The receipts say {money(right)}.",
        "people_title": f"{manager} is paid on a doubled day",
        "people": f"{manager} manages {store}. The bonus is 1% of net sales: "
        f"{money(bonus_extra)} too much for this one day.",
        "prepare": "A table of money has three things the other tables do not have: a true "
        "key on the receipt, a second count in the till, and a normal day to compare with.",
    }
    catches = [
        (BY_KEY[key], mine[key], why)
        for key, why in (
            ("true_key_unique", "Grouped by the true key, receipt_id + line_no, every line of the day is there twice."),
            ("till_total", "The tills counted the day themselves, once. The lines add up to twice that."),
            ("normal_volume", "And the day has far more lines than a normal Friday for this store."),
        )
    ]  # fmt: skip
    steps = _steps(
        word="Duplicate", words={}, clocks=_clocks(day), text=text, full=full, catches=catches,
        silent=(quiet, f"{store} did report. It reported twice. This check asks only whether "
                "an open store has at least one line."),
        all_caught=list(mine),
        outcome=f"daily_store_sales still says {money(says)} for {store} on {_day(day)}.",
    )  # fmt: skip
    return {
        "id": "9", "label": "Case 9", "hint": "from milestone 2: a sales file sent twice",
        "name": case["name"], "day": _day(day), "store": store,
        "where": f"{_day(day)} · store {store} · table sales_lines",
        "table": "sales_lines", "output": "daily_store_sales", "lab": False,
        "says_label": "The pipeline says", "key_label": "The answer key says",
        "pipeline_says": money(says), "answer_key_says": money(right), "table_is_right": False,
        "note": "A table of money, to compare with the others.",
        "caught_by": _caught_by(mine), "steps": steps,
    }  # fmt: skip


# ---------------------------------------------------------------- faults planted in the lab

NEWEST = "(SELECT max(_file_name) FROM raw.{table})"


def _lab(folder: Path, base: Path, name: str, plants: list[str], probe) -> dict:
    """Copy the four clean weeks, plant one fault in the raw table, run the pipeline again."""
    path = folder / f"lab_{name}.duckdb"
    shutil.copy(base, path)
    config.DUCKDB_PATH = path
    with connect() as wh:
        before = probe(wh)
        for sql in plants:
            wh.execute(sql)
    stopped = False
    for step in ("clean", "check", "publish"):  # a step that fails stops the steps after it
        wh = connect()
        try:
            pipeline.RUN_STEP[step](wh, LAB_DAY)
            wh.commit()
        except pipeline.ChecksFailed:  # the check step keeps its results before it stops
            stopped = True
            break
        finally:
            wh.close()
    with connect() as wh:
        standard = wh.query(
            "SELECT check_number, passed, bad_rows FROM ops.check_results WHERE run_day = ? "
            "ORDER BY check_number",
            (LAB_DAY,),
        )
        found = {k: v for k, v in _findings(wh).items() if v}
        after = probe(wh)
    failed = [r["check_number"] for r in standard if not r["passed"]]
    assert stopped == bool(failed)
    return {"before": before, "after": after, "found": found, "standard": standard,
            "failed": failed[0] if failed else None}  # fmt: skip


def _lab_clocks() -> dict:
    return {
        "happened": "In the lab, on a private copy",
        "files": "In the lab: the fault is planted",
        "night": "In the lab: the nightly run, again",
        "morning": "In the lab, the morning after",
        "desk": "In the lab, at your desk",
    }


def _price_cards(rows: list[dict], bad_from: str | None = None) -> list[dict]:
    return [
        _card(r["product_name"], money(int(r["unit_price_cents"])),
              f"from {r['valid_from']} to {r['valid_to'] or 'open'}",
              "bad" if bad_from and str(r["valid_from"]) == bad_from else "")
        for r in rows
    ]  # fmt: skip


def _lab_two_prices(folder: Path, base: Path) -> dict:
    product, start, price = "P-001", "2026-08-01", 319
    name = PRODUCTS[product][0]

    def probe(wh):
        lines = "FROM clean.sales_lines WHERE product_id = ? AND sales_date >= CAST(? AS DATE)"
        return {
            "rows": wh.value(f"SELECT count(*) {lines}", (product, start)),
            "ids": wh.value(f"SELECT count(DISTINCT line_id) {lines}", (product, start)),
            "prices": wh.query(
                "SELECT * FROM clean.products WHERE product_id = ? ORDER BY valid_from", (product,)
            ),
            "published": wh.value("SELECT count(*) FROM output.daily_store_sales"),
        }

    lab = _lab(folder, base, "two_prices", [
        f"INSERT INTO raw.products SELECT product_id, product_name, category, unit, '{price}', "
        f"'{start}', NULL, status, _file_name, _received_at, _loaded_at FROM raw.products "
        f"WHERE product_id = '{product}' AND _file_name = {NEWEST.format(table='products')}"
    ], probe)  # fmt: skip
    before, after, found = lab["before"], lab["after"], lab["found"]
    assert after["rows"] == 2 * before["rows"] and after["ids"] == before["ids"]
    assert lab["failed"] == 3 and "one_price_at_a_time" in found
    twice = next(r["bad_rows"] for r in lab["standard"] if r["check_number"] == 3)
    quiet = BY_KEY["no_gap_between_prices"]
    assert quiet.key not in found
    prices = _price_cards(after["prices"], start)
    full = {
        "files": {"count": "the price list, as typed", "cards": prices},
        "raw": {"count": f"{len(after['prices'])} price rows for {name}", "cards": prices},
        "clean": {"count": f"{name} since 08-01", "cards": [
            _card("Lines sold", str(before["ids"]), "what the tills really sold", "good"),
            _card("Rows after the join", str(after["rows"]), "every line, once per price", "bad")]},
        "outputs": {"count": "daily_store_sales", "cards": [
            _card("Not published", "old numbers", "the check failed, so publish did not run", "changed")]},
        "people": {"count": "", "cards": [
            _card("Head of retail operations", "no report", "tonight's numbers are missing", "bad")]},
    }  # fmt: skip
    text = {
        "happened": f"A price has validity dates: the first and the last day it applies. To "
        f"change a price, the buying team closes the old row and opens a new one. Here somebody "
        f"types a new price for {name}, {money(price)} from {start}, and forgets to close the "
        "old row. This fault is planted in the lab: the real price list is right on every day.",
        "files_title": "The price list now has two open prices for one product",
        "files": f"{name} has {len(after['prices'])} price rows, and both are open. On every day "
        f"since {start} the product has two prices. Each row, alone, is a perfectly good row.",
        "load": "The task load copies the price list as it is. A table of prices is read whole "
        "every night.",
        "clean": f"The task clean joins every sales line to the price valid on its day. For "
        f"{name} two prices are valid, so each line finds two partners. {before['ids']} lines "
        f"sold since {start} become {after['rows']} rows.",
        "standard": f"This time a standard check does fail. Check 3 finds {twice} line ids that "
        "are there twice. It is loud, and it is lucky: it was written for another fault. It "
        "points at sales_lines, where nothing was typed wrong. It cannot say which table "
        "caused it, or which row.",
        "publish": "The check task failed, so Airflow does not start publish. The outputs keep "
        "the numbers of the night before. Nobody is paid on doubled milk, and nobody gets "
        "tonight's numbers either.",
        "people_title": "No report this morning, and no reason",
        "people": f"The head of retail operations has no new numbers. The only message is: "
        f"{twice} keys are not unique in sales_lines. Somebody must now search every table "
        "that sales_lines is joined to.",
        "prepare": "A price list is checked against itself: each price row against the later "
        "rows of the same product. Nothing like that was needed for sales_lines.",
    }
    catches = [
        (BY_KEY["one_price_at_a_time"], found["one_price_at_a_time"],
         "This check joins the price list to itself and looks for two rows of one product "
         "that cover the same day. It points at the right table, the product and the day."),
    ]  # fmt: skip
    steps = _steps(
        word="Validity dates", words={}, clocks=_lab_clocks(), text=text, full=full,
        catches=catches,
        silent=(quiet, "This check looks for a day with no price. Here no day is without a "
                "price. One product has a day with two. A gap and an overlap are opposite "
                "faults, and each needs its own check."),
        all_caught=list(found), failed_check=3,
        outcome=f"The price list still has two open prices for {name}.",
    )  # fmt: skip
    return {
        "id": "L1", "label": "Lab", "hint": "planted in the lab: two prices on one day",
        "name": "Two prices on one day", "day": "planted in the lab", "store": "products",
        "where": "Planted in the lab · four clean weeks · table products",
        "table": "products", "output": "daily_store_sales", "lab": True,
        "says_label": "After the join", "key_label": "The tills sold",
        "pipeline_says": f"{after['rows']} rows", "answer_key_says": f"{before['ids']} lines",
        "table_is_right": False,
        "note": "Standard check 3 fails here, by luck. It points at the wrong table.",
        "caught_by": _caught_by(found), "steps": steps,
    }  # fmt: skip


def _lab_no_price(folder: Path, base: Path) -> dict:
    product, late = "P-006", "2026-08-12"
    name = PRODUCTS[product][0]
    gap = ("2026-08-10", "2026-08-11")

    def probe(wh):
        where = "product_id = ? AND sales_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)"
        return {
            "lines": wh.value(f"SELECT count(*) FROM clean.sales_lines WHERE {where}", (product, *gap)),
            "cents": int(wh.value(f"SELECT coalesce(sum(amount_cents), 0) FROM clean.sales_lines WHERE {where}", (product, *gap))),
            "published": int(wh.value(
                "SELECT sum(net_sales_cents) FROM output.daily_store_sales "
                "WHERE sales_date BETWEEN CAST(? AS DATE) AND CAST(? AS DATE)", gap)),
            "prices": wh.query("SELECT * FROM clean.products WHERE product_id = ? ORDER BY valid_from", (product,)),
        }  # fmt: skip

    lab = _lab(folder, base, "no_price", [
        f"UPDATE raw.products SET valid_from = '{late}' WHERE product_id = '{product}' "
        f"AND valid_to IS NULL AND _file_name = {NEWEST.format(table='products')}"
    ], probe)  # fmt: skip
    before, after, found = lab["before"], lab["after"], lab["found"]
    assert lab["failed"] is None and after["lines"] == 0 and before["lines"] > 0
    assert before["published"] - after["published"] == before["cents"]
    assert {"no_gap_between_prices", "every_sale_has_a_product", "till_total"} <= set(found)
    quiet = BY_KEY["one_price_at_a_time"]
    assert quiet.key not in found
    prices = _price_cards(after["prices"], late)
    full = {
        "files": {"count": "the price list, as typed", "cards": prices},
        "raw": {"count": f"{len(after['prices'])} price rows for {name}", "cards": prices},
        "clean": {"count": f"{name} on 08-10 and 08-11", "cards": [
            _card("Lines sold", str(before["lines"]), money(before["cents"]), "good"),
            _card("Rows after the join", str(after["lines"]), "no price on those days", "bad")]},
        "outputs": {"count": "daily_store_sales", "cards": [
            _card("All stores, 08-10 and 08-11", money(after["published"]), "published", "bad"),
            _card("Answer key", money(before["published"]), "with the eggs", "good")]},
        "people": {"count": "", "cards": [
            _card("Store managers", f"-{money(before['cents'])}", "sales that vanished, in every store", "bad")]},
    }  # fmt: skip
    text = {
        "happened": f"The price of {name} changes on 2026-08-10. The buying team closes the old "
        f"price on 08-09, as they should. Then they type the start of the new price wrong: "
        f"{late}. For two days the product has no price at all. This fault is planted in the "
        "lab: the real price list is right on every day.",
        "files_title": "The price list has two days with no price",
        "files": f"The old price of {name} ends on 08-09. The new one starts on {late}. Both "
        "rows are valid rows. The fault is not in a row. It is between two rows.",
        "load": "The task load copies the price list as it is.",
        "clean": f"The task clean joins every sales line to the price valid on its day. On "
        f"08-10 and 08-11 {name} has no price. {before['lines']} lines find no partner, and an "
        "inner join drops them. No error. No row is left to look at.",
        "standard": "The five standard checks run, and all five are green. A row that is not "
        "there cannot be empty, or twice, or not allowed.",
        "publish": f"Airflow starts the task publish. For those two days daily_store_sales "
        f"says {money(after['published'])} for the six stores. The receipts say "
        f"{money(before['published'])}. {money(before['cents'])} of eggs is gone.",
        "people_title": "Every store is paid a little too little",
        "people": f"{money(before['cents'])} of sales vanished, spread over every store that "
        f"sold {name}. Nobody typed a wrong sale. One date in a table of about 40 rows was wrong.",
        "prepare": "For every price that ends, look for the price that starts the next day. "
        "A price list is checked row against neighbouring row.",
    }
    catches = [
        (BY_KEY["no_gap_between_prices"], found["no_gap_between_prices"],
         "This check takes each price that ends and looks for the one that starts the day "
         "after. It names the product and the first day with no price."),
        (BY_KEY["every_sale_has_a_product"], found["every_sale_has_a_product"],
         "A check from the sales card sees the damage too. It goes back to the raw lines and "
         "finds the ones with no product valid on their day."),
    ]  # fmt: skip
    steps = _steps(
        word="Validity dates", words={}, clocks=_lab_clocks(), text=text, full=full,
        catches=catches,
        silent=(quiet, "This check looks for two prices on one day. Here no day has two. Two "
                "days have none."),
        all_caught=list(found),
        outcome=f"daily_store_sales is still {money(before['cents'])} too low for those two days.",
    )  # fmt: skip
    return {
        "id": "L2", "label": "Lab", "hint": "planted in the lab: a day with no price",
        "name": "A day with no price", "day": "planted in the lab", "store": "products",
        "where": "Planted in the lab · four clean weeks · table products",
        "table": "products", "output": "daily_store_sales", "lab": True,
        "says_label": "The pipeline says", "key_label": "The answer key says",
        "pipeline_says": money(after["published"]), "answer_key_says": money(before["published"]),
        "table_is_right": False, "note": "Net sales of all six stores on 08-10 and 08-11.",
        "caught_by": _caught_by(found), "steps": steps,
    }  # fmt: skip


def _store_cards(rows: list[dict], bad: tuple = ()) -> list[dict]:
    return [
        _card(r["store_id"], r["town"], f"{str(r['opens_at'])[:5]} to {str(r['closes_at'])[:5]}",
              "bad" if r["store_id"] in bad else "")
        for r in rows
    ]  # fmt: skip


def _lab_store_twice(folder: Path, base: Path) -> dict:
    twice, gone = "S01", "S06"
    newest = NEWEST.format(table="stores")

    def probe(wh):
        return {
            "stores": wh.query("SELECT * FROM clean.stores ORDER BY store_id"),
            "twice_rows": wh.value("SELECT count(*) FROM clean.sales_lines WHERE store_id = ?", (twice,)),
            "gone_rows": wh.value("SELECT count(*) FROM clean.sales_lines WHERE store_id = ?", (gone,)),
            "gone_cents": int(wh.value("SELECT coalesce(sum(amount_cents), 0) FROM clean.sales_lines WHERE store_id = ?", (gone,))),
        }  # fmt: skip

    lab = _lab(folder, base, "store_twice", [
        f"DELETE FROM raw.stores WHERE store_id = '{gone}' AND _file_name = {newest}",
        f"INSERT INTO raw.stores SELECT * FROM raw.stores WHERE store_id = '{twice}' AND _file_name = {newest}",
    ], probe)  # fmt: skip
    before, after, found = lab["before"], lab["after"], lab["found"]
    assert len(after["stores"]) == len(before["stores"]) == 6
    assert after["twice_rows"] == 2 * before["twice_rows"] and after["gone_rows"] == 0
    assert lab["failed"] == 3 and {"six_stores_once", "every_store_is_known"} <= set(found)
    dup = next(r["bad_rows"] for r in lab["standard"] if r["check_number"] == 3)
    quiet = BY_KEY["store_values_allowed"]
    assert quiet.key not in found
    cards = _store_cards(after["stores"], (twice,))[:5]
    full = {
        "files": {"count": "the stores table, as typed", "cards": cards},
        "raw": {"count": "6 rows", "cards": cards},
        "clean": {"count": "6 rows, 5 stores", "cards": [
            _card(f"{twice} lines", str(after["twice_rows"]), f"were {before['twice_rows']}: every line twice", "bad"),
            _card(f"{gone} lines", str(after["gone_rows"]), f"were {before['gone_rows']}: all dropped", "bad")]},
        "outputs": {"count": "daily_store_sales", "cards": [
            _card("Not published", "old numbers", "the check failed, so publish did not run", "changed")]},
        "people": {"count": "", "cards": [
            _card("Head of retail operations", "no report", "tonight's numbers are missing", "bad")]},
    }  # fmt: skip
    text = {
        "happened": f"The stores table has six rows. It was typed once and is rarely changed. "
        f"Somebody edits it and makes two slips in one go: the row of {gone} is deleted, and the "
        f"row of {twice} is saved twice. This fault is planted in the lab: the real stores "
        "table is right on every day.",
        "files_title": "The stores table still has six rows",
        "files": f"Six rows arrive, as always. {twice} is there twice, and {gone} is not there. "
        "A plain row count says: six, correct.",
        "load": "The task load copies six rows.",
        "clean": f"The task clean joins every sales line to its store. Every line of {twice} "
        f"finds two partners: {before['twice_rows']} lines become {after['twice_rows']} rows. "
        f"Every line of {gone} finds none: {before['gone_rows']} lines, "
        f"{money(before['gone_cents'])}, are dropped. Wrong here is wrong everywhere.",
        "standard": f"Standard check 3 fails: {dup} line ids are there twice. That is the loud "
        f"half of the fault, and it was caught by luck. The silent half is not seen at all: "
        f"no standard check knows that {gone} has vanished.",
        "publish": "The check task failed, so Airflow does not start publish. The outputs keep "
        "the numbers of the night before.",
        "people_title": "No report, and only half of the reason",
        "people": f"The message says: {dup} keys are not unique in sales_lines. It does not say "
        f"that a store is there twice. It does not say that {gone} is gone. If only the "
        f"doubled row were fixed, the night would be green, and {gone} would be paid nothing.",
        "prepare": "Nothing but a count, and a list of the store ids in every store file. "
        "For a table this small, the exact number of rows is a fact you can know: six, "
        "each once.",
    }
    catches = [
        (BY_KEY["six_stores_once"], found["six_stores_once"],
         "This check counts the rows, and it counts each store_id. Six rows is right. "
         f"{twice} twice is not."),
        (BY_KEY["every_store_is_known"], found["every_store_is_known"],
         f"This check collects the store_id of every store file and joins it to the stores "
         f"table. Files still come from {gone}, and the table does not know it."),
    ]  # fmt: skip
    steps = _steps(
        word="Duplicate", words={}, clocks=_lab_clocks(), text=text, full=full, catches=catches,
        silent=(quiet, "This check tests each row: a name, a town, a manager, hours that make "
                "sense. Every row that is there is a perfectly good row."),
        all_caught=list(found), failed_check=3,
        outcome=f"The stores table still has {twice} twice and no {gone}.",
    )  # fmt: skip
    return {
        "id": "L3", "label": "Lab", "hint": "planted in the lab: a store there twice",
        "name": "A store there twice, so six rows again", "day": "planted in the lab",
        "store": "stores", "where": "Planted in the lab · four clean weeks · table stores",
        "table": "stores", "output": "daily_store_sales", "lab": True,
        "says_label": "The table has", "key_label": "Lotus has",
        "pipeline_says": "6 rows, 5 stores", "answer_key_says": "6 stores, each once",
        "table_is_right": False,
        "note": f"{twice} is doubled and {gone} is dropped. A row count of six is right.",
        "caught_by": _caught_by(found), "steps": steps,
    }  # fmt: skip


def _lab_hours_changed(folder: Path, base: Path) -> dict:
    store, wrong = "S02", "20:00"

    def probe(wh):
        return {
            "stores": wh.query("SELECT * FROM clean.stores ORDER BY store_id"),
            "published": int(wh.value("SELECT sum(net_sales_cents) FROM output.daily_store_sales")),
        }

    lab = _lab(folder, base, "hours_changed", [
        f"UPDATE raw.stores SET closes_at = '{wrong}' WHERE store_id = '{store}' "
        f"AND _file_name = {NEWEST.format(table='stores')}"
    ], probe)  # fmt: skip
    before, after, found = lab["before"], lab["after"], lab["found"]
    was = str(next(r for r in before["stores"] if r["store_id"] == store)["closes_at"])[:5]
    assert lab["failed"] is None and after["published"] == before["published"]
    assert set(found) == {"stores_did_not_change", "inside_opening_hours"}
    alarms = len(found["inside_opening_hours"])
    assert {r["store_id"] for r in found["inside_opening_hours"]} == {store}
    quiet = BY_KEY["store_values_allowed"]
    cards = _store_cards(after["stores"], (store,))[:4]
    full = {
        "files": {"count": "the stores table, as typed", "cards": cards},
        "raw": {"count": "6 rows", "cards": cards},
        "clean": {"count": "6 rows", "cards": cards},
        "outputs": {"count": "daily_store_sales", "cards": [
            _card("All stores, four weeks", money(after["published"]), "published, and right", "good")]},
        "people": {"count": "", "cards": [
            _card("Whoever reads the alarms", f"{alarms} bad rows", f"right sales of {store}, after {wrong}", "bad")]},
    }  # fmt: skip
    text = {
        "happened": f"Store {store} closes at {was}. Somebody edits the stores table and types "
        f"{wrong}. Nobody tells the data team. The stores table is reference data: other data "
        "is judged against it. This fault is planted in the lab: the real stores table is the "
        "same on every day.",
        "files_title": "The stores table arrives, with one hour changed",
        "files": f"Six rows, six stores. {store} now closes at {wrong}. That is a perfectly "
        "allowed hour for a store to close.",
        "load": "The task load copies six rows.",
        "clean": "The task clean gives the hours their type. Every sales line still finds its "
        "store. No row is lost and none is doubled.",
        "standard": "The five standard checks run, and all five are green. They are right: "
        "no number in sales_lines is wrong.",
        "publish": f"Airflow starts the task publish. The outputs are published, and they are "
        f"right: {money(after['published'])}, as before. This fault does not change a number.",
        "people_title": f"{alarms} right sales look wrong",
        "people": f"The opening-hours check from the sales card reads these hours. It now "
        f"returns {alarms} sales of {store}: every real sale after {wrong} in four weeks. "
        "All of them are right. The reference table is wrong, so the check cries wolf, and "
        "a check that cries wolf gets switched off.",
        "prepare": "Keep yesterday's file, and compare it with today's, row by row. A table "
        "that should stand still is checked against its own past.",
    }
    catches = [
        (BY_KEY["stores_did_not_change"], found["stores_did_not_change"],
         "This check compares the newest stores file with the one before it. One row "
         "differs. It does not say the change is wrong. It says: ask a person."),
        (BY_KEY["inside_opening_hours"], found["inside_opening_hours"],
         "The opening-hours check does return rows. Every one is a false alarm, and that "
         "is the damage."),
    ]  # fmt: skip
    steps = _steps(
        word="Reference data", words={}, clocks=_lab_clocks(), text=text, full=full,
        catches=catches,
        silent=(quiet, f"This check tests each store row: it opens before it closes, it has a "
                f"till. {wrong} is a perfectly allowed hour. A valid value can be a wrong value."),
        all_caught=list(found),
        outcome=f"The stores table still says {store} closes at {wrong}.",
    )  # fmt: skip
    return {
        "id": "L4", "label": "Lab", "hint": "planted in the lab: the hours changed, silently",
        "name": "The opening hours changed and nobody said so", "day": "planted in the lab",
        "store": "stores", "where": "Planted in the lab · four clean weeks · table stores",
        "table": "stores", "output": "daily_store_sales", "lab": True,
        "says_label": "Sales that look wrong", "key_label": "Sales that are wrong",
        "pipeline_says": str(alarms), "answer_key_says": "0", "table_is_right": True,
        "note": f"Every output is right. {alarms} right sales fail a check, because its "
        "reference table is wrong.",
        "caught_by": _caught_by(found), "steps": steps,
    }  # fmt: skip


# ---------------------------------------------------------------- the whole week


def _mentions(row: dict, case: dict) -> bool:
    """Is this bad row about the planted case?"""
    day = case["day"]
    days = (
        {day.isoformat(), (day + timedelta(days=1)).isoformat()}
        if case["case"] == 5
        else {day.isoformat()}
    )
    if "store" in case and "store_id" in row:
        return row["store_id"] == case["store"] and str(row.get("sales_date")) in days
    if "supplier" in case:
        text = " ".join(str(v) for v in row.values())
        return case["supplier"] in text and day.isoformat() in text
    if case["case"] == 13:
        return case["second"]["member_id"] in row.values()
    return False


def _planted_cases_caught(nightly: dict) -> list[int]:
    """The planted cases that at least one check returns, on the night of the case."""
    caught = []
    for case in planted.CASES:
        found = nightly[case["day"]]
        if any(_mentions(row, case) for rows in found.values() for row in rows):
            caught.append(case["case"])
    return caught


def _false_alarms(found: dict, world) -> int:
    """Bad rows on the three things that look wrong and are not."""
    refit_store, refit_day = sorted(CLOSED)[0]
    stores = {r.store_id for r in world.receipts}
    not_wrong = {(s, HOLIDAY.isoformat()) for s in stores} | {(refit_store, refit_day.isoformat())}
    alarms = sum(
        (r.get("store_id"), str(r.get("sales_date"))) in not_wrong
        for rows in found.values()
        for r in rows
    )
    big = world.planted["big_sale"]
    alarms += sum(
        r["store_id"] == big.store_id and r["quantity"] == 400 for r in found["believable_quantity"]
    )
    return alarms


def count_tests() -> int:
    """How many tests this milestone has, counted by pytest itself."""
    listing = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q"],
        cwd=MILESTONE_DIR,
        capture_output=True,
        text=True,
        check=False,
    ).stdout
    found = re.search(r"(\d+) tests? collected", listing)
    return int(found.group(1)) if found else 0


def dag_tasks() -> list[str]:
    """The task ids, read from the DAG file itself."""
    return re.findall(r'task_id="(\w+)"', DAG_FILE.read_text(encoding="utf-8"))


def _where(check, rows: list[dict]) -> list[str]:
    places = inspect.Finding(check, rows).store_days()
    shown = [f"{thing} {day[5:]}".strip() for thing, day in places]
    return shown[:4] + ([f"and {len(shown) - 4} more"] if len(shown) > 4 else [])


@lru_cache(maxsize=1)
def _built() -> str:
    world = build_world()
    saved = (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING)
    with tempfile.TemporaryDirectory() as temp:
        folder = Path(temp)
        try:
            config.WAREHOUSE = "duckdb"
            config.LANDING = folder / "landing"
            files.deliver_through(LAST_DAY, config.LANDING)

            # The lab: four clean weeks, on their own file.
            base = folder / "lab_base.duckdb"
            config.DUCKDB_PATH = base
            pipeline.run(LAB_DAY, say=lambda text: None)
            with connect() as wh:
                assert not any(_findings(wh).values()), "the four clean weeks are not clean"
            labs = [
                _lab_two_prices(folder, base),
                _lab_no_price(folder, base),
                _lab_store_twice(folder, base),
                _lab_hours_changed(folder, base),
            ]

            # The bonus week, night by night.
            config.DUCKDB_PATH = folder / "flow.duckdb"
            nightly = {}
            for day in WEEK:
                pipeline.run(day, say=lambda text: None)
                with connect() as wh:
                    nightly[day] = _findings(wh)
            found = nightly[WEEK[-1]]
            with connect() as wh:
                green = wh.query("SELECT passed FROM ops.check_results")
                tables = {t: wh.value(f"SELECT count(*) FROM clean.{t}") for t in load.TABLES}
                raw_tables = {t: wh.value(f"SELECT count(*) FROM raw.{t}") for t in load.TABLES}
                file_columns = wh.value("SELECT count(*) FROM raw.file_columns")
                files_loaded = wh.value("SELECT count(*) FROM raw.loaded_files")
                planted_cases = [
                    _case_10(wh, world, config.LANDING, found),
                    _case_11(wh, world, config.LANDING, found),
                    _case_12(wh, world, config.LANDING, found),
                    _case_13(wh, world, config.LANDING, found),
                ]
                sales_case = _case_9(wh, world, found)
            at_the_door = contract.check_landing(config.LANDING)
        finally:
            config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING = saved

    cases = planted_cases + labs + [sales_case]
    tasks = dag_tasks()
    caught = _planted_cases_caught(nightly)
    refused = [r["file"] for r in at_the_door if r["verdict"] == "refuse"]

    reasoned = [
        {
            "number": c.number, "key": c.key, "name": c.name, "lens": c.lens,
            "question": c.question, "table": inspect.table_of(c), "family": c.family,
            "new": inspect.table_of(c) in NEW_TABLES,
            "protects": c.protects, "prepare": c.prepare, "limit": c.limit,
            "bad_rows": len(found[c.key]), "where": _where(c, found[c.key]),
        }
        for c in ALL_CHECKS
    ]  # fmt: skip
    reasoned_tables = [
        {"table": table, "new": table in NEW_TABLES, "checks": len(table_checks)}
        for table, table_checks in CHECKS_BY_TABLE.items()
    ]
    lenses = [
        {
            **lens,
            "checks": sum(c.lens == lens["id"] for c in ALL_CHECKS),
            "standard": len(checks.STANDARD_CHECKS) if lens["id"] == "data" else 0,
        }
        for lens in LENSES
    ]
    side = [
        {**row, "why": ONLY_ONE_BECAUSE.get(row["family"], "") if row["tables"] == 1 else ""}
        for row in inspect.side_by_side()
    ]
    assert {r["family"] for r in side if r["tables"] == 1} == set(ONLY_ONE_BECAUSE)
    by_id = {c["id"]: c for c in cases}
    c10, c11, c12, c13 = (by_id[k] for k in ("10", "11", "12", "13"))
    stores_family = next(r for r in side if r["family"] == "the key is unique")

    def brief(c: dict) -> dict:
        return {
            "case": c["id"], "name": c["name"], "day": c["day"], "store": c["store"],
            "table": c["table"], "says_label": c["says_label"],
            "pipeline_says": c["pipeline_says"], "answer_key_says": c["answer_key_says"],
            "caught_by": [f"{x['number']}. {x['name']}" for x in c["caught_by"]],
            "at_the_door": c.get("at_the_door", ""),
        }  # fmt: skip

    recap = {
        "question": QUESTION,
        "own_question": OWN_QUESTION,
        "week": {
            "nights": len(WEEK),
            "checks_green": sum(r["passed"] for r in green),
            "checks_run": len(green),
            "checks_with_bad_rows": sum(c["bad_rows"] > 0 for c in reasoned),
            "new_checks_with_bad_rows": sum(c["bad_rows"] > 0 and c["new"] for c in reasoned),
            "bad_rows": sum(c["bad_rows"] for c in reasoned),
        },
        "checks": reasoned,
        "tables": reasoned_tables,
        "lenses": lenses,
        "cases": [brief(c) for c in planted_cases],
        "lab_cases": [brief(c) for c in labs],
        "side_by_side": side,
        "table_names": list(CHECKS_BY_TABLE),
        "contract": {
            "name": contract.load_contract()["name"],
            "version": contract.load_contract()["version"],
            "files": len(at_the_door),
            "refused": len(refused),
            "refused_files": refused,
            "file_columns": file_columns,
            "files_loaded": files_loaded,
        },
        "quiz": [
            {
                "question": "A teammate copies the checks of sales_lines onto the stores "
                "table: not empty, key not null, key unique. All pass. Is the stores table safe?",
                "answer": f"No. In the lab {by_id['L3']['pipeline_says']} passed a row count: "
                "one store was there twice and another was gone. For a table this small the "
                "exact count and the exact list are facts you can know, so check those. And "
                "check that it is the same as yesterday: this table should stand still.",
            },
            {
                "question": f"On {c11['day']} the file of {c11['store']} has every column, "
                "no empty cell, and only valid numbers. The schema check is green and the "
                "not-null check is green. Finance says tomorrow's order looks strange. "
                "Where do you look?",
                "answer": f"At the meaning, not the shape. The order list says "
                f"{c11['pipeline_says']}, and {c11['answer_key_says']} is right: the supplier "
                "sent cents where dollars were agreed. A value can only be judged beside "
                "something else, so compare each cost with the supplier's last file and with "
                f"the shelf price. Checks {' and '.join(str(x['number']) for x in c11['caught_by'])} "
                "do that, and the contract would refuse the file at the door.",
            },
            {
                "question": f"The order list says: {c12['pipeline_says'].replace('order', 'order')} "
                f"bottles of milk for {c12['store']}. Every stock count in the table is a "
                "believable number. Which check could still be wrong for this table?",
                "answer": f"The one that looks for rows that are not there. {c12['note']} "
                f"The right answer is {c12['answer_key_says']}. A check on values cannot see "
                "a missing row. You must first build the rows you expected: every open store, "
                "every product on sale, every day. A missing day is a gap, not a zero.",
            },
        ],
        "words": [{"term": term, "meaning": meaning} for term, meaning in WORDS.items()],
        "still_wrong": STILL_WRONG,
        "not_in_the_run": NOT_IN_THE_RUN,
        "example": {
            "case_10": c10["note"], "case_11": c11["note"], "case_12": c12["note"],
            "case_13": f"{c13['pipeline_says']}, {c13['answer_key_says']}",
            "key_unique_on": stores_family["tables"],
        },
        "planted_cases_caught": len(caught),
        "planted_cases": len(planted.CASES),
        "false_alarms": _false_alarms(found, world),
    }  # fmt: skip

    flow = {
        "recap": recap,
        "milestone": 3,
        "of": 5,
        "title": "Every table is different",
        "lede": "Customers of Lotus Retails pay at a till: the machine at the counter, also "
        "called a cash register. The sales that the tills write are one table of seven. The "
        "other tables are typed by people, counted by hand, or sent by another company, so "
        f"they fail in other ways. Now all seven have a check card: {len(ALL_CHECKS)} checks. "
        "Pick a fault, and watch which check its own table needs.",
        "question": QUESTION,
        "dag": {"id": re.search(r'dag_id="(\w+)"', DAG_FILE.read_text()).group(1), "tasks": tasks},
        "stages": STAGES,
        "gates": GATES,
        "quarantine": {
            "state": "not_yet",
            "text": "No quarantine yet. A bad row has nowhere to be set aside. Milestone 4.",
        },
        "readers": READERS,
        "tables": [
            {"name": t, "raw_rows": raw_tables[t], "clean_rows": rows} for t, rows in tables.items()
        ],
        "outputs": ["daily_store_sales", "replenishment_list"],
        "checks": [
            {"number": c.number, "name": c.name, "table": c.table, "lens": "data"}
            for c in checks.STANDARD_CHECKS
        ],
        "reasoned": reasoned,
        "reasoned_tables": reasoned_tables,
        "reasoned_note": {
            "checks": len(ALL_CHECKS),
            "tables": len(CHECKS_BY_TABLE),
            "new_checks": len(OTHER_CHECKS),
            "command": "python -m lotus_pipeline inspect",
        },
        "lenses": lenses,
        "side_by_side": side,
        "table_names": list(CHECKS_BY_TABLE),
        "words": recap["words"],
        "cases": cases,
        "still_wrong": STILL_WRONG,
        "not_in_the_run": NOT_IN_THE_RUN,
        "next": "Milestone 4: gates and quarantine.",
    }
    return json.dumps(flow, default=str)


def build() -> dict:
    """Everything the flow page shows, from a real run."""
    return json.loads(_built())


def build_recap(flow: dict | None = None) -> dict:
    """The numbers shown on milestone_3_recap.html."""
    flow = flow or build()
    recap = dict(flow["recap"])
    cards = sorted(p.stem for p in (MILESTONE_DIR / "checks").glob("*.md"))
    recap["numbers"] = {
        "notebooks": len(list((MILESTONE_DIR / "notebooks").glob("0*.ipynb"))),
        "tests": count_tests(),
        "tables": len(load.TABLES),
        "tables_with_a_card": len(cards),
        "checks_with_a_reason": len(ALL_CHECKS),
        "new_checks": len(OTHER_CHECKS),
        "standard_checks": len(checks.STANDARD_CHECKS),
        "tasks": len(flow["dag"]["tasks"]),
        "planted_cases_caught": recap["planted_cases_caught"],
        "planted_cases": recap["planted_cases"],
        "files_refused": recap["contract"]["refused"],
        "supplier_files": recap["contract"]["files"],
        "false_alarms": recap["false_alarms"],
        "gates": sum(g["state"] != "not_yet" for g in flow["gates"]),
        "rows_set_aside": 0,  # there is no quarantine yet
        "words": len(WORDS),
    }
    recap["cards"] = cards
    recap["dag"] = flow["dag"]
    return recap


def _read(page: Path) -> dict:
    return json.loads(MARKER.search(page.read_text(encoding="utf-8")).group(2))


def _write(page: Path, data: dict) -> None:
    payload = json.dumps(data, indent=1, ensure_ascii=False).replace("</", "<\\/")
    text = page.read_text(encoding="utf-8")
    page.write_text(
        MARKER.sub(lambda m: m.group(1) + "\n" + payload + "\n" + m.group(3), text),
        encoding="utf-8",
    )


def page_data() -> dict:
    """The data currently written inside the flow page."""
    return _read(PAGE)


def recap_page_data() -> dict:
    """The data currently written inside the recap page."""
    return _read(RECAP_PAGE)


def main() -> int:
    flow = build()
    recap = build_recap(flow)
    if "--check" in sys.argv:
        ok = page_data() == flow and recap_page_data() == recap
        print(
            "The pages match the code."
            if ok
            else "The pages are out of date. Run: uv run python -m lotus_pipeline.flow_data"
        )
        return 0 if ok else 1
    _write(PAGE, flow)
    _write(RECAP_PAGE, recap)
    print(
        f"Wrote {len(flow['cases'])} cases into {PAGE.name}, and the numbers into {RECAP_PAGE.name}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
