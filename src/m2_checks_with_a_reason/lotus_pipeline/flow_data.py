"""Build the numbers shown on milestone_2_flow.html and milestone_2_recap.html.

The pages must never show a number typed by hand. This script runs the real
pipeline for the seven nights of the bonus week, on a private warehouse
(one DuckDB file in a temporary folder). After each night it also runs this
milestone's checks, the way `python -m lotus_pipeline inspect` does, and writes
what happened into the pages.

    uv run python -m lotus_pipeline.flow_data          write the pages' data
    uv run python -m lotus_pipeline.flow_data --check  fail if a page is out of date

It never touches your own warehouse, Docker or Airflow.
"""

import json
import re
import subprocess
import sys
import tempfile
import textwrap
from datetime import date, timedelta
from functools import lru_cache
from pathlib import Path

from lotus_pipeline import checks, config, inspect, load, pipeline, report
from lotus_pipeline.inspect import ALL_CHECKS
from lotus_pipeline.report import money
from lotus_pipeline.sales_checks import MAX_KILOGRAMS, SALES_CHECKS, VOLUME_HIGH, VOLUME_LOW
from lotus_pipeline.till_checks import TILL_CHECKS
from lotus_pipeline.warehouse import connect, plain
from store_systems import files, planted, truth
from store_systems.catalog import BONUS_WEEK_START, CLOSED, HOLIDAY, HOLIDAY_NAME, LAST_DAY
from store_systems.generate import build as build_world

MILESTONE_DIR = config.MILESTONE_DIR
PAGE = MILESTONE_DIR / "milestone_2_flow.html"
RECAP_PAGE = MILESTONE_DIR / "milestone_2_recap.html"
DAG_FILE = MILESTONE_DIR / "dags" / "lotus_nightly_dag.py"
MARKER = re.compile(
    r'(<script id="page-data" type="application/json">)(.*?)(</script>)', flags=re.DOTALL
)
WEEK = [BONUS_WEEK_START + timedelta(days=n) for n in range(7)]
SHOWN_CASES = (1, 2, 3, 4, 5, 6, 7, 8, 9)  # every planted case in sales_lines

QUESTION = (
    "Can I pay the bonus from last week's net sales, store by store? "
    "And if I cannot trust a number, which one, and why?"
)
OWN_QUESTION = "Which check would have caught each wrong number, and why does that check exist?"
STILL_WRONG = "One table is done well. There are five more, and they are nothing like it."
NOT_IN_THE_RUN = (
    "And the checks are not part of the nightly run. You start them by hand, "
    "so a failed check changes nothing yet."
)

# The words this milestone teaches. The meanings are the same as in GLOSSARY.md.
WORDS = {
    "Not-null check": "A check that a cell is filled. The skill is saying in which rows "
    "an empty cell is wrong.",
    "Unique check": "A check that one thing is stored once.",
    "Composite key": "A key made of more than one column, such as `receipt_id` + `line_no`.",
    "Range check": "A check that a number is between a smallest and a largest believable value.",
    "Format check": "A check that a text value has the right shape, such as a receipt id.",
    "Business rule": 'A rule of how the company works, such as "a refund gives money back". '
    "The data cannot tell you the rule. A person must.",
    "Reference data": "A small list that people keep by hand, and that other data is judged "
    "against: prices, opening hours, holidays.",
    "Validity and accuracy": "A valid value is allowed by the rules. An accurate value matches "
    "the real world. A value can be valid and wrong.",
    "Reconciliation": "Comparing your numbers with another source that saw the same thing.",
    "Control total": "A total counted a second way, kept only to be compared. "
    "The till's own total is one.",
    "Referential integrity": "Every row that points to another table finds a row there.",
    "Orphan row": "A row that points to something that does not exist, such as a sale "
    "for a product nobody typed.",
    "Baseline": "The number you compare with. It is what normal looks like.",
    "Limit": "The line between good and bad in a check. Also called a threshold.",
    "Anomaly": "A value far from the baseline. It is a reason to look, not a proof of a fault.",
    "False positive": "An alarm when nothing is wrong. Also called a false alarm.",
    "False negative": "A fault with no alarm.",
    "Seasonality": "A pattern that repeats with the calendar: Saturdays are busy, "
    "holidays are quiet.",
}

# The word each check teaches when it catches a row.
CHECK_WORD = {
    "required_cells": "Not-null check",
    "true_key_unique": "Composite key",
    "right_shape": "Format check",
    "line_adds_up": "Validity and accuracy",
    "every_sale_has_a_product": "Orphan row",
    "refund_gives_money_back": "Business rule",
    "refund_not_larger_than_sale": "Referential integrity",
    "price_of_the_day": "Reference data",
    "inside_opening_hours": "Reference data",
    "believable_quantity": "Range check",
    "till_total": "Reconciliation",
    "every_store_reported": "False negative",
    "normal_volume": "Baseline",
    "one_total_per_till_per_day": "Control total",
    "till_adds_up": "Control total",
    "every_till_reported": "Control total",
}

# A store's day against the same weekday before, with NO calendar. The careless version
# of the volume check: it judges a holiday as if it were a normal day.
NAIVE_NORMAL_DAY = """
    SELECT today.store_id, today.sales_date, today.lines, avg(before.lines) AS normal_lines
    FROM (SELECT store_id, sales_date, count(*) AS lines FROM clean.sales_lines
          GROUP BY store_id, sales_date) today
    JOIN (SELECT store_id, sales_date, count(*) AS lines FROM clean.sales_lines
          GROUP BY store_id, sales_date) before
      ON before.store_id = today.store_id
     AND before.sales_date IN (today.sales_date - 7, today.sales_date - 14,
                               today.sales_date - 21, today.sales_date - 28)
    GROUP BY today.store_id, today.sales_date, today.lines
    HAVING count(before.lines) >= 3
"""

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


def _day(day: date) -> str:
    """2026-08-24 -> Mon 08-24"""
    return day.strftime("%a %m-%d")


def _signed(cents: int) -> str:
    return ("+" if cents >= 0 else "-") + money(abs(cents))


def _qty(text) -> str:
    """'2.000' -> 2, '1.180' -> 1.18, '1180.000' -> 1180"""
    return f"{float(text):g}"


# ---------------------------------------------------------------- run the real pipeline


def _snapshot(wh, day: date) -> dict:
    """What the warehouse holds right after the run for `day`."""
    moment = load.run_moment(day)
    return {
        "files": wh.query(
            "SELECT file_name, table_name, row_count, received_at FROM raw.loaded_files "
            "WHERE loaded_at = ? ORDER BY file_name",
            (moment,),
        ),
        "raw": wh.query(
            "SELECT * FROM raw.sales_lines WHERE _loaded_at = ? ORDER BY line_id", (moment,)
        ),
        "clean": {
            r["line_id"]: r
            for r in wh.query(
                "SELECT c.* FROM clean.sales_lines c JOIN raw.sales_lines r "
                "ON r.line_id = c.line_id WHERE r._loaded_at = ?",
                (moment,),
            )
        },
        "clean_total": wh.value("SELECT count(*) FROM clean.sales_lines"),
        "lines_of_the_day": wh.value(
            "SELECT count(*) FROM clean.sales_lines WHERE sales_date = ?", (day,)
        ),
        "sales": {
            (r["sales_date"], r["store_id"]): int(r["net_sales_cents"])
            for r in wh.query("SELECT * FROM output.daily_store_sales")
        },
        "counts": {
            (r["sales_date"], r["store_id"]): (int(r["receipts"]), int(r["lines"]))
            for r in wh.query("SELECT * FROM output.daily_store_sales")
        },
        "checks": wh.query(
            "SELECT * FROM ops.check_results WHERE run_day = ? ORDER BY check_number", (day,)
        ),
        "bonus": report.bonus_report(wh, day),
        "to_order": {
            r["store_id"]: int(r["products"])
            for r in wh.query(
                "SELECT store_id, sum(CASE WHEN order_qty > 0 THEN 1 ELSE 0 END) AS products "
                "FROM output.replenishment_list WHERE for_date = CAST(? AS DATE) + 1 "
                "GROUP BY store_id",
                (day,),
            )
        },
        "tills": {
            (r["business_date"], r["store_id"]): int(r["net"])
            for r in wh.query(
                "SELECT business_date, store_id, sum(net_total_cents) AS net "
                "FROM clean.till_totals GROUP BY business_date, store_id"
            )
        },
        "tables": {table: wh.value(f"SELECT count(*) FROM clean.{table}") for table in load.TABLES},
        "raw_tables": {
            table: wh.value(f"SELECT count(*) FROM raw.{table}") for table in load.TABLES
        },
        "products": {
            r["product_id"]: r["product_name"]
            for r in wh.query("SELECT DISTINCT product_id, product_name FROM clean.products")
        },
        "stores": {r["store_id"]: r for r in wh.query("SELECT * FROM clean.stores")},
        # New in milestone 2: what the checks with a reason say after this night.
        "findings": {
            # A query gives its rows in no promised order. Sort them, so the page never changes.
            f.check.key: sorted(
                ({k: plain(v) for k, v in row.items()} for row in f.bad_rows),
                key=lambda row: json.dumps(row, default=str, sort_keys=True),
            )
            for f in inspect.run_all(wh)
        },
        "price_list": {
            (r["product_id"], r["valid_from"]): r for r in wh.query("SELECT * FROM clean.products")
        },
        "calendar": wh.query("SELECT * FROM clean.calendar ORDER BY calendar_date, store_id"),
        "naive_normal": {
            (r["sales_date"], r["store_id"]): (int(r["lines"]), float(r["normal_lines"]))
            for r in wh.query(NAIVE_NORMAL_DAY)
        },
        "lines_per_day": {
            (r["sales_date"], r["store_id"]): int(r["lines"])
            for r in wh.query(
                "SELECT sales_date, store_id, count(*) AS lines FROM clean.sales_lines "
                "GROUP BY sales_date, store_id"
            )
        },
        "pieces_over_50": wh.value(
            "SELECT count(*) FROM clean.sales_lines WHERE unit = 'each' AND quantity > 50"
        ),
        "largest_real_weight": wh.value(
            "SELECT max(quantity) FROM clean.sales_lines WHERE unit = 'kg' AND sales_date < ?",
            (BONUS_WEEK_START,),
        ),
    }


def _run_the_week() -> dict[date, dict]:
    """Run the seven nights on a private warehouse. Your own settings are put back after."""
    saved = (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING)
    with tempfile.TemporaryDirectory() as folder:
        try:
            config.WAREHOUSE = "duckdb"
            config.DUCKDB_PATH = Path(folder) / "flow.duckdb"
            config.LANDING = Path(folder) / "landing"
            files.deliver_through(LAST_DAY, config.LANDING)
            nights = {}
            for day in WEEK:
                pipeline.run(day, say=lambda text: None)
                with connect() as wh:
                    nights[day] = _snapshot(wh, day)
            return nights
        finally:
            config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING = saved


# ---------------------------------------------------------------- cards


def _line_card(row: dict, names: dict, tone: str = "", sub: str | None = None) -> dict:
    """One sales line as a small card. `row` may be a raw row (text) or a clean row."""
    name = names.get(row["product_id"], f"{row['product_id']} (not in products)")
    if sub is None:
        sub = f"{row['line_type']} · qty {_qty(row['quantity'])} · {str(row['sold_at'])[11:16]}"
    return {"id": name, "amount": money(int(row["amount_cents"])), "sub": sub, "tone": tone}


def _note_card(title: str, amount: str, sub: str, tone: str = "") -> dict:
    return {"id": title, "amount": amount, "sub": sub, "tone": tone}


# ---------------------------------------------------------------- one case, start to end


def _case(number: int, nights: dict, world, key: dict) -> dict:
    """Everything the page shows when the reader follows one planted case."""
    case = planted.case(number)
    day, store = case["day"], case["store"]
    night = nights[day]
    names = night["products"]
    manager = night["stores"][store]["manager_name"]
    store_name = night["stores"][store]["store_name"]
    label = _day(day)
    true_day = key[(day, store)]["net_sales_cents"]
    got_day = night["sales"].get((day, store))
    till = night["tills"].get((day, store))
    file_date = day.isoformat()
    in_file = [r for r in night["raw"] if r["store_id"] == store and file_date in r["_file_name"]]
    ordinary = [r for r in in_file if r["line_type"] == "sale"][:2]
    bonus = next(s for s in night["bonus"]["stores"] if s["store_id"] == store)
    week_true = truth.week_net_sales(world, BONUS_WEEK_START, day)[store]
    bonus_true = truth.bonus_cents(week_true)
    files_night = len(night["files"])
    rows_night = sum(int(f["row_count"]) for f in night["files"])
    lines_day = night["lines_of_the_day"]

    def clean_of(rows):
        return [night["clean"][r["line_id"]] for r in rows if r["line_id"] in night["clean"]]

    def cards(rows, bad=(), sub=None):
        bad_ids = {r["line_id"] for r in bad}
        return [
            _line_card(r, names, "bad" if r["line_id"] in bad_ids else "", sub(r) if sub else None)
            for r in rows
        ]

    # Defaults, used by most cases. A case below changes what is special about it.
    why = {
        1: f"{label} has {lines_day:,} lines in clean.sales_lines. The table is not empty.",
        2: "Every line has a line_id. None is empty.",
        3: f"All {night['clean_total']:,} line_ids are different.",
        4: "Every line_type is sale or refund.",
        5: "Every store's day adds up to more than zero.",
    }
    out_tone = "bad"
    out_cards = None
    people_cards = None
    fault_rows: list[dict] = []
    shown: list[dict] = []
    sub = None
    files_count = f"{len(in_file)} lines from {store}"
    clean_count = None
    hint = ""
    special = 0  # the standard check worth a sentence in this case
    people_text = None

    if number == 1:
        fault_rows = [
            r for r in in_file if r["line_type"] == "refund" and int(r["amount_cents"]) > 0
        ]
        normal = [r for r in in_file if r["line_type"] == "refund" and int(r["amount_cents"]) < 0]
        shown = fault_rows + normal[:1] + ordinary
        cents = int(fault_rows[0]["amount_cents"])
        hint = "a refund stored as a positive amount"
        special = 4
        happened = (
            f"On {label} a customer brings one bottle of olive oil back to store {store}. "
            f"The till gives {money(cents)} back. The till's own total for the day is "
            f"{money(till)}. That is the true number."
        )
        files_text = (
            f"The export program in the store writes the night's file: {len(in_file)} lines. "
            f"On the refund line it loses the minus sign. The line says refund, quantity -1, "
            f"and amount +{cents}. It should be -{cents}."
        )
        raw_text = "The refund line is copied with its wrong sign."
        clean_text = (
            f"The text '{cents}' becomes the number {cents}. A type is not a rule: "
            f"+{cents} is a perfectly good number."
        )
        why[4] = (
            "The line says refund. That is an allowed value. "
            "This check does not look at the amount."
        )
        why[5] = (
            f"{store}'s day adds up to {money(got_day)}. It is above zero. "
            "A total that is too high is still positive."
        )
        publish_text = (
            f"daily_store_sales says {money(got_day)} for {store}. The refund was added, "
            f"not taken away, so the day is {money(2 * cents)} too high: two times {money(cents)}."
        )
        people_title = f"The manager of {store} is paid on a wrong number"

    elif number == 2:
        receipt = world.planted[case["receipt"]].receipt_id
        both = [r for r in in_file if r["receipt_id"] == receipt]
        half = len(both) // 2
        fault_rows = both[half:]
        shown = both[:2] + fault_rows[:2]
        total = sum(int(r["amount_cents"]) for r in both[:half])
        hint = "one receipt in the file twice"
        special = 3

        def sub(r):
            return f"{r['line_id']} · {r['receipt_id']} line {r['line_no']}"

        happened = (
            f"On {label} a customer at store {store} buys {half} things for {money(total)}. "
            f"It is one ordinary receipt: {receipt}."
        )
        files_text = (
            f"The export program writes that receipt twice. The second copy gets new line_ids. "
            f"So the file has {len(both)} lines for one receipt of {half} lines."
        )
        raw_text = f"All {len(both)} lines are copied."
        clean_text = "Both copies get their types. Nothing removes a copy."
        why[3] = (
            f"The second copy has new line_ids, so all {night['clean_total']:,} line_ids are "
            "different. The check looks at line_id. The true key is receipt_id + line_no."
        )
        publish_text = (
            f"daily_store_sales says {money(got_day)} for {store}. The receipt is counted "
            f"twice, so the day is {money(total)} too high."
        )
        people_title = f"The manager of {store} is paid for one sale twice"

    elif number == 3:
        fault_rows = [r for r in in_file if float(r["quantity"]) >= 1000]
        row = fault_rows[0]
        true_line = world.planted[case["receipt"]].lines[0]
        shown = fault_rows + ordinary
        hint = "a weight in grams, not kilograms"
        special = 5
        happened = (
            f"On {label} a customer at store {store} buys {_qty(true_line.quantity)} kg of "
            f"bananas at {money(true_line.unit_price_cents)} a kilogram. "
            f"They pay {money(true_line.amount_cents)}."
        )
        files_text = (
            f"The export program writes the scale's grams where kilograms belong: quantity "
            f"{_qty(row['quantity'])}. Then it works the amount out again: "
            f"{money(int(row['amount_cents']))} for one line of bananas."
        )
        raw_text = "The line is copied as it is."
        clean_text = (
            f"{_qty(row['quantity'])} becomes a number. It is a valid number. "
            "Nothing asks whether it is a believable weight."
        )
        why[5] = (
            f"{store}'s day adds up to {money(got_day)}. It is far above zero, "
            "so this check is happy."
        )
        publish_text = (
            f"daily_store_sales says {money(got_day)} for {store}. The true day is "
            f"{money(true_day)}. One line of bananas is larger than the whole day."
        )
        people_title = f"The manager of {store} gets the largest bonus error of the week"

    elif number == 4:
        new_price = night["price_list"][(case["product"], day)]["unit_price_cents"]
        fault_rows = [
            r
            for r in in_file
            if r["product_id"] == case["product"] and int(r["unit_price_cents"]) != new_price
        ]
        old_price = int(fault_rows[0]["unit_price_cents"])
        # One ordinary line beside them, with another price, so the butter stands out.
        other = [
            r
            for r in in_file
            if r["line_type"] == "sale"
            and r["product_id"] != case["product"]
            and int(r["unit_price_cents"]) != old_price
        ]
        shown = fault_rows[:3] + other[:1]
        packs = sum(int(float(r["quantity"])) for r in fault_rows)
        took = sum(int(r["amount_cents"]) for r in fault_rows)
        short = packs * new_price - took
        hint = "yesterday's price, charged today"
        special = 5
        out_tone = ""

        def sub(r):
            return f"qty {_qty(r['quantity'])} at {money(int(r['unit_price_cents']))} · {r['sold_at'][11:16]}"

        happened = (
            f"From {label} butter costs {money(new_price)}, not {money(old_price)}. The buying "
            f"team typed the new price the day before. The tills of store {store} still have "
            f"the old one. They sell {packs} packs at {money(old_price)}."
        )
        files_text = (
            f"The file is honest. It says what the tills did: {len(fault_rows)} lines of butter "
            f"at {money(old_price)}. Each line adds up, and the tills' own total agrees."
        )
        raw_text = "The lines are copied as they are."
        clean_text = (
            f"Each butter line joins to the price row that is valid on {label}. The join "
            f"finds it. Nothing compares the price on the line with the price on that row."
        )
        why[5] = (
            f"{store}'s day adds up to {money(got_day)}. None of the five checks looks at a price."
        )
        publish_text = (
            f"daily_store_sales says {money(got_day)} for {store}. That is what the tills "
            f"took, so it equals the answer key. The table is right. The money is not: "
            f"the store took {money(short)} less than the price list says."
        )
        people_title = f"Finance is {money(short)} short, and no report shows it"
        people_text = (
            f"Store {store} should have taken {money(packs * new_price)} for {packs} packs of "
            f"butter. It took {money(took)}. Finance plans with the price list, so the money "
            f"in the bank will be {money(short)} less than planned. Net sales are right, "
            "the bonus is right, and 5 of 5 checks passed. No output shows this fault at all."
        )
        people_cards = [
            _note_card(f"{store} took", money(took), f"{packs} packs at {money(old_price)}", "bad"),
            _note_card(
                "The price list says", money(packs * new_price),
                f"{packs} packs at {money(new_price)}", "good",
            ),
        ]  # fmt: skip

    elif number == 5:
        fault_rows = [r for r in in_file if r["sold_at"] > f"{file_date} 23:59:59"]
        shown = fault_rows[:3] + ordinary[:1]
        total = sum(int(r["amount_cents"]) for r in fault_rows)
        stamped = fault_rows[0]["sold_at"][:16]
        next_day = day + timedelta(days=1)
        true_time = world.planted[case["receipt"]].sold_at
        hint = "a sale at 03:10 in a store that closes at 22:00"
        special = 5
        happened = (
            f"On {label} at {true_time:%H:%M} a customer at store {store} buys "
            f"{len(fault_rows)} things for {money(total)}. The till had a power cut "
            "just before, and its clock is wrong."
        )
        files_text = (
            f"The till stamps the receipt {stamped}. That is the next day, at an hour "
            f"when the store is closed. The lines are in {label}'s file."
        )
        raw_text = "The lines are copied with the wrong time."
        clean_text = (
            f"The pipeline takes the day of a sale from sold_at. So these "
            f"{len(fault_rows)} lines get sales_date {next_day.isoformat()}."
        )
        why[1] = (
            f"{label} still has {lines_day:,} lines. The table is not empty. "
            "Nobody counts whether a few lines left for another day."
        )
        why[5] = (
            f"{store} now has a day on {_day(next_day)} that adds up to {money(total)}. "
            "It is above zero. This check does not ask whether that day has happened."
        )
        publish_text = (
            f"daily_store_sales has two rows that are wrong. {store} on {label} is "
            f"{money(total)} too low. And {store} has a row for {_day(next_day)}, "
            "a day that has not happened yet."
        )
        out_cards = [
            _note_card(f"{store} · {label}", money(got_day), f"right: {money(true_day)}", "bad"),
            _note_card(
                f"{store} · {_day(next_day)}",
                money(night["sales"][(next_day, store)]),
                "a day that has not happened",
                "bad",
            ),
        ]
        people_title = f"The sale falls out of {store}'s bonus week"

    elif number == 6:
        fault_rows = [r for r in in_file if r["product_id"] not in names]
        shown = fault_rows + ordinary[:1]
        total = sum(int(r["amount_cents"]) for r in fault_rows)
        product = fault_rows[0]["product_id"]
        hint = "a product that is not in products"
        special = 2
        happened = (
            f"On {label} store {store} sells {len(fault_rows)} cartons of a new soup, "
            f"product {product}, for {money(total)} in all. The buying team has not typed "
            "the product into the price list yet."
        )
        files_text = (
            f"The file is right: {len(in_file)} lines, and {len(fault_rows)} of them are "
            f"for {product}. The till knows the product. The products table does not."
        )
        raw_text = f"All {len(in_file)} lines are copied, the soup too."
        clean_text = (
            f"A line with no product has "
            f"nothing to join to, so it is dropped. {len(in_file)} lines go in and "
            f"{len(clean_of(in_file))} come out. No error, no warning."
        )
        clean_count = f"{len(clean_of(in_file))} of {len(in_file)} lines"
        why[2] = "The dropped lines are not in the table. A row that is not there cannot fail."
        why[4] = "Every line that is left is a sale or a refund."
        publish_text = (
            f"daily_store_sales says {money(got_day)} for {store}. The true day is "
            f"{money(true_day)}. The soup is missing: {money(total)}."
        )
        people_title = f"The manager of {store} is paid too little"

    elif number == 7:
        hint = "one store sends no file"
        special = 1
        arrived = sorted(
            f["file_name"][12:15]
            for f in night["files"]
            if f["table_name"] == "sales_lines" and file_date in f["file_name"]
        )
        files_count = f"no file from {store}"
        happened = (
            f"On {label} store {store} is open as usual. Its tills take {money(till)}. "
            "At night the export program in the back office crashes."
        )
        files_text = (
            f"No sales file comes from {store}. The other stores send theirs: "
            f"{', '.join(arrived)}. The tills' own totals do arrive, in another file."
        )
        raw_text = f"There is nothing to copy for {store}. The task still succeeds."
        clean_text = f"clean.sales_lines has no line for {store} on {label}."
        clean_count = f"0 lines from {store}"
        why[1] = (
            f"The other {len(arrived)} stores sent {lines_day:,} lines for {label}. "
            "The table is not empty."
        )
        why[5] = (
            f"{store} has no row for {label}. A total that does not exist cannot be zero or below."
        )
        publish_text = (
            f"daily_store_sales has no row for {store} on {label}. "
            f"The true day is {money(true_day)}. A whole day is missing."
        )
        out_cards = [_note_card(f"{store} · {label}", "no row", f"right: {money(true_day)}", "bad")]
        people_title = f"The manager of {store} loses a whole day"

    elif number == 8:
        hint = "one store's file arrives a day late"
        special = 1
        next_night = nights[day + timedelta(days=1)]
        arrives = case["arrives_at"]
        healed = next_night["sales"][(day, store)]
        to_order = night["to_order"][store]
        right_orders = sum(
            1
            for (s, _p), row in truth.replenishment_list(world, day).items()
            if s == store and row["order_qty"] > 0
        )
        files_count = f"no file from {store} yet"
        happened = (
            f"On {label} store {store} is open as usual. Its tills take {money(till)}. "
            "The back-office computer is slow to send the file."
        )
        files_text = (
            f"Files are due by 01:00, and the run is at 02:00. {store}'s file arrives at "
            f"{arrives:%H:%M} on {_day(arrives.date())}: a day late."
        )
        raw_text = f"Tonight there is nothing to copy for {store}. The task still succeeds."
        clean_text = f"clean.sales_lines has no line for {store} on {label}. Not yet."
        clean_count = f"0 lines from {store}"
        why[1] = f"The other stores sent {lines_day:,} lines for {label}. The table is not empty."
        why[5] = f"{store} has no row for {label} yet. No row cannot fail."
        publish_text = (
            f"Tonight daily_store_sales has no row for {store} on {label}. One night later "
            f"the file is loaded, and the row says {money(healed)}. "
            "The same day gives two different answers."
        )
        out_cards = [
            _note_card(f"{store} · {label}", "no row", "after tonight's run", "bad"),
            _note_card(f"{store} · {label}", money(healed), "one night later", "changed"),
        ]
        people_title = f"The order list for {store} is made from an empty day"

    else:  # 9
        hint = "one store's whole file sent twice"
        special = 3
        first = [r for r in in_file if r["_file_name"].endswith(f"{file_date}.csv")]
        second = [r for r in in_file if r["_file_name"].endswith("_2.csv")]
        fault_rows = second
        shown = first[:2] + second[:2]
        files_count = f"{len(first)} + {len(second)} lines from {store}"

        def sub(r):
            return f"{r['line_id']} · {r['_file_name'][12:]}"

        happened = (
            f"On {label} store {store} takes {money(till)}. At night the back office "
            "sends the file. It is not sure the file arrived, so it sends it again."
        )
        files_text = (
            f"Two files arrive from {store}, twelve minutes apart: {len(first)} lines each. "
            "The receipts are the same. The export program gave the second file new line_ids."
        )
        raw_text = f"Both files are copied: {len(in_file)} lines for one day."
        clean_text = "Every line gets its type, in both copies. Nothing compares the two files."
        why[3] = (
            f"The second file has new line_ids, so all {night['clean_total']:,} line_ids are "
            "different. Twice the rows, and not one key is stored twice."
        )
        why[5] = f"{store}'s day adds up to {money(got_day)}. Double is still positive."
        publish_text = (
            f"daily_store_sales says {money(got_day)} for {store}. The true day is "
            f"{money(true_day)}. The whole day is counted twice."
        )
        people_title = f"The manager of {store} is paid for Friday twice"

    # ---- What every case shares ------------------------------------------------
    week_error = bonus["net_sales_cents"] - week_true
    bonus_error = bonus["bonus_cents"] - bonus_true
    if number == 4:
        pass  # the people step was written above: finance, not a manager
    elif number == 8:
        people_text = (
            f"The replenishment team orders tomorrow's stock from tonight's list. For {store} "
            f"it says nothing sold, so it has {to_order} products to order. The true list has "
            f"{right_orders}. The shelves will be empty tomorrow, and the manager's number "
            "changes the day after."
        )
        people_cards = [
            _note_card(
                "Replenishment team", f"{to_order} products", f"to order for {store}", "bad"
            ),
            _note_card("Answer key", f"{right_orders} products", f"to order for {store}", "good"),
        ]
    else:
        own_error = (got_day or 0) - true_day
        other = (
            f" This fault alone is {_signed(own_error)}. The rest comes from another "
            f"fault at {store} this week."
            if own_error != week_error
            else ""
        )
        people_text = (
            f"{manager} manages {store}. The bonus is 1% of the week's net sales. "
            f"Up to {label} the report says {money(bonus['net_sales_cents'])}. The answer key "
            f"says {money(week_true)}. That is {_signed(week_error)} in sales and "
            f"{_signed(bonus_error)} in bonus.{other} The report also says: "
            "5 of 5 checks passed."
        )
        people_cards = [
            _note_card(
                f"{manager}, {store}",
                money(bonus["bonus_cents"]),
                f"bonus on {money(bonus['net_sales_cents'])}",
                "bad",
            ),
            _note_card("Answer key", money(bonus_true), f"bonus on {money(week_true)}", "good"),
        ]
    if out_cards is None:
        got_receipts, got_lines = night["counts"][(day, store)]
        out_cards = [
            _note_card(
                f"{store} · {label}", money(got_day), f"{got_receipts} receipts · {got_lines} lines",
                out_tone,
            ),
            _note_card("Answer key", money(true_day), "from the till journal", "good"),
        ]  # fmt: skip
    else:
        out_cards.append(_note_card("Answer key", money(true_day), f"{store} · {label}", "good"))

    passed = [c["passed"] for c in night["checks"]]
    assert passed == [True] * 5, f"a standard check failed on {day}"

    file_cards = (
        cards(shown, fault_rows, sub)
        if shown
        else [_note_card(f"{store} · {label}", "no file", "nothing arrived by 01:00", "bad")]
    )
    raw_cards = cards(shown, fault_rows, sub)
    clean_rows = clean_of(shown)
    clean_cards = [
        _line_card(
            r, names, "bad" if r["line_id"] in {f["line_id"] for f in fault_rows} else "",
            (sub(next(x for x in shown if x["line_id"] == r["line_id"])) if sub
             else f"{r['line_type']} · qty {_qty(r['quantity'])} · sales_date {r['sales_date']:%m-%d}"),
        )
        for r in clean_rows
    ]  # fmt: skip

    full = {
        "files": {"count": files_count, "cards": file_cards},
        "raw": {"count": f"{len(in_file)} lines from {store}", "cards": raw_cards},
        "clean": {
            "count": clean_count or f"{len(clean_of(in_file))} lines from {store}",
            "cards": clean_cards,
        },
        "outputs": {"count": "daily_store_sales", "cards": out_cards},
        "people": {"count": "", "cards": people_cards},
    }
    order = ["files", "raw", "clean", "checks", "outputs", "people"]

    def stages(upto: str, done: int = 0, active: int = 0) -> dict:
        reach = order.index(upto)
        shown_stages = {}
        for name in order:
            if name == "checks":
                shown_stages[name] = {"done": done, "active": active}
            elif order.index(name) <= reach:
                shown_stages[name] = full[name]
            else:
                shown_stages[name] = {"count": "", "cards": []}
        return shown_stages

    def word(term: str) -> dict:
        return {"term": term, "meaning": WORDS[term]}

    night_label = f"Night after {label}, 02:00"
    desk = f"{_day(day + timedelta(days=1))}, 09:00, at your desk"
    first_run = " This first run also loads the four weeks before." if day == WEEK[0] else ""

    # ---- This milestone's checks, on the same run ------------------------------
    days = {day.isoformat()}
    if number == 5:  # the wrong clock moved the lines to the next day
        days.add((day + timedelta(days=1)).isoformat())

    def mine(rows: list[dict]) -> list[dict]:
        return [r for r in rows if r.get("store_id") == store and str(r.get("sales_date")) in days]

    found = {check.key: mine(night["findings"][check.key]) for check in ALL_CHECKS}
    catchers = [check for check in ALL_CHECKS if found[check.key]]
    assert catchers, f"no check catches case {number}"
    silent_check, silent_why = _silent(number, night, store, day, fault_rows, world)
    assert not found[silent_check.key], f"{silent_check.key} is not silent on case {number}"

    def reasoned(caught=(), silent=(), now=None, all_done=False) -> dict:
        caught, silent = list(caught), list(silent)
        if all_done:
            silent = [c.key for c in ALL_CHECKS if c.key not in caught]
        return {
            "run": bool(caught or silent or now),
            "caught": caught,
            "silent": silent,
            "now": now,
        }

    not_run = reasoned()
    steps = [
        {
            "id": "happened", "active": "files", "task": None, "clock": f"{label}, in the store",
            "title": "What really happened", "text": happened,
            "word": word("Control total"),
            "stages": stages("files") | {"files": {"count": "", "cards": []}}, "moves": [],
        },
        {
            "id": "files", "active": "files", "task": "stores_send_files",
            "clock": f"{label}, 23:00", "title": "The store sends its file", "text": files_text,
            "word": word("Validity and accuracy"), "stages": stages("files"), "moves": [],
            "plant": True, "problem": True,
        },
        {
            "id": "load", "active": "raw", "task": "load", "clock": night_label,
            "title": "Task load: copy the files, as received",
            "text": f"Airflow starts the task load. It copies {files_night} new files into the "
                    f"raw tables: {rows_night:,} rows.{first_run} Every value stays text. "
                    f"Nothing looks at its shape, and nothing is judged. {raw_text}",
            "word": word("Format check"), "stages": stages("raw"),
            "moves": [{"from": "files", "to": "raw"}],
        },
        {
            "id": "clean", "active": "clean", "task": "clean", "clock": night_label,
            "title": "Task clean: give types, and join",
            "text": f"The task clean turns the text into numbers, dates and times, and joins "
                    f"each line to its product and its store. {clean_text}",
            "word": word("Referential integrity"), "stages": stages("clean"),
            "moves": [{"from": "raw", "to": "clean"}],
        },
        {
            "id": "standard", "active": "checks", "task": "check", "clock": night_label,
            "title": "Task check: the five standard checks, all green",
            "text": "The task check asks five questions: is the table empty, is a key empty, "
                    "is a key there twice, is a status not allowed, is a total not above zero. "
                    f"Five queries, and each returns no rows. {why[special]}",
            "sql": textwrap.dedent(checks.STANDARD_CHECKS[special - 1].sql).strip(),
            "word": word("Not-null check"), "stages": stages("clean", done=5),
            "moves": [{"from": "clean", "to": "checks"}],
        },
        {
            "id": "publish", "active": "outputs", "task": "publish", "clock": night_label,
            "title": "Task publish: the check passed, so it runs",
            "text": f"All five checks are green, so Airflow starts the task publish. {publish_text}",
            "word": word("False negative"), "stages": stages("outputs", done=5),
            "moves": [{"from": "checks", "to": "outputs"}], "problem": True,
        },
        {
            "id": "people", "active": "people", "task": None,
            "clock": f"{_day(day + timedelta(days=1))}, morning",
            "title": people_title, "text": people_text,
            "word": word("False negative"), "stages": stages("people", done=5),
            "moves": [{"from": "outputs", "to": "people"}], "problem": True,
        },
    ]  # fmt: skip
    for step in steps:
        step["reasoned"] = not_run

    caught_so_far: list[str] = []
    for position, check in enumerate(catchers):
        caught_so_far = caught_so_far + [check.key]
        term = CHECK_WORD[check.key]
        if check.key == "true_key_unique" and number == 9:
            term = "Unique check"
        rows = found[check.key]
        intro = (
            "Now run the same night again, with this milestone's checks. You start them "
            "yourself: python -m lotus_pipeline inspect. "
            if position == 0
            else ""
        )
        steps.append(
            {
                "id": f"catch_{check.key}", "active": "reasoned", "task": None, "clock": desk,
                "title": f"Check {check.number} catches it: {check.name.lower()}",
                "text": f"{intro}This check does its work first, then tests its rule. "
                        f"It returns {_count(len(rows), 'bad row')} for store {store}.",
                "facts": [
                    ["Lens", check.lens],
                    ["Protects", check.protects],
                    ["Prepare first", check.prepare],
                    ["The limit", check.limit],
                ],
                "sql": textwrap.dedent(check.sql).strip(),
                "rows": _table(rows),
                "word": word(term), "stages": stages("people", done=5), "moves": [],
                "reasoned": reasoned(caught_so_far, now=check.key), "caught": True,
            }
        )  # fmt: skip
    steps.append(
        {
            "id": "silent", "active": "reasoned", "task": None, "clock": desk,
            "title": f"Check {silent_check.number} stays silent: {silent_check.name.lower()}",
            "text": f"{silent_why} A check answers only the question it asks. That is why "
                    "one fault needs more than one check, and why each check needs a reason.",
            "facts": [["Lens", silent_check.lens], ["The limit", silent_check.limit]],
            "word": word("Limit"), "stages": stages("people", done=5), "moves": [],
            "reasoned": reasoned(caught_so_far, [silent_check.key], now=silent_check.key),
        }
    )  # fmt: skip
    names_of = _and([c.name for c in catchers])
    if number == 4:
        outcome = (
            f"The table was never wrong, so nothing in it changes. The store is still "
            f"{money(short)} short, and nobody was told."
        )
    elif got_day is None:
        outcome = f"daily_store_sales still has no row for {store} on {label}."
    else:
        outcome = f"daily_store_sales still says {money(got_day)} for {store} on {label}."
    steps.append(
        {
            "id": "still_wrong", "active": "outputs", "task": None, "clock": desk,
            "title": "Still wrong: the checks found it, and nothing changed",
            "text": f"{_count(len(catchers), 'check')} of {len(ALL_CHECKS)} caught this fault: "
                    f"{names_of}. But you ran them by hand, the morning after. The nightly run "
                    f"does not know them. {outcome} There is no gate to stop a bad night, and "
                    "no quarantine to set a bad row aside. The dashed lines show where they "
                    "will stand.",
            "word": word("False negative"), "stages": stages("people", done=5), "moves": [],
            "reasoned": reasoned(caught_so_far, all_done=True), "problem": True,
        }
    )  # fmt: skip
    finished = {"happened": 0, "files": 0, "load": 1, "clean": 2, "standard": 3, "publish": 4}
    for step in steps:
        step["tasks_done"] = finished.get(step["id"], 5)
    same = got_day == true_day
    return {
        "id": str(number),
        "label": f"Case {number}",
        "hint": hint,
        "name": case["name"],
        "day": label,
        "store": store,
        "store_name": store_name,
        "caught_by": [{"number": c.number, "name": c.name, "lens": c.lens} for c in catchers],
        "pipeline_says": money(got_day) if got_day is not None else "no row",
        "answer_key_says": money(true_day),
        "table_is_right": same,
        "note": f"The table is right. The store is {money(short)} short." if number == 4 else "",
        "steps": steps,
    }


def _count(n: int, thing: str) -> str:
    return f"{n} {thing}" if n == 1 else f"{n} {thing}s"


def _and(things: list[str]) -> str:
    """['A', 'B', 'C'] -> "A", "B" and "C" """
    named = [f'"{t}"' for t in things]
    return named[0] if len(named) == 1 else ", ".join(named[:-1]) + " and " + named[-1]


def _table(rows: list[dict], limit: int = 4) -> dict:
    """The bad rows a check returned, ready to show: money as dollars, at most four rows."""
    columns = [c for c in rows[0] if c != "store_id"]  # the store is known: it is the case's

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


def _silent(number: int, night: dict, store: str, day: date, fault_rows: list, world):
    """One check that says nothing about this fault, and the reason, with the run's numbers."""
    by_key = {c.key: c for c in ALL_CHECKS}
    lines = night["lines_per_day"].get((day, store), 0)
    normal = night["naive_normal"].get((day, store))
    band = f"between {VOLUME_LOW:.0%} and {VOLUME_HIGH:.0%}"
    percent = round(100 * lines / normal[1]) if normal else None
    if number == 1:
        cents = abs(int(fault_rows[0]["amount_cents"]))
        return by_key["refund_not_larger_than_sale"], (
            f"This check joins the refund to the receipt it names. The customer paid "
            f"{money(cents)} for the oil, and {money(cents)} went back. The size is inside "
            "the limit. Only the sign is wrong, and this check does not look at the sign."
        )
    if number in (2, 5, 6):
        what = {
            2: f"The receipt that came twice adds {len(fault_rows)} lines",
            5: f"The {len(fault_rows)} lines with the wrong time left for another day",
            6: f"The {len(fault_rows)} dropped lines are missing",
        }[number]
        return by_key["normal_volume"], (
            f"{what}. {store} still has {lines} lines on {_day(day)}. Its normal day is "
            f"{round(normal[1])} lines, so this is {percent}% of normal. The limit is "
            f"{band}. A few lines never move a whole day past it."
        )
    if number == 3:
        row = fault_rows[0]
        return by_key["line_adds_up"], (
            f"The export program wrote {_qty(row['quantity'])} and then worked the amount out "
            f"again: {_qty(row['quantity'])} x {row['unit_price_cents']} is "
            f"{int(row['amount_cents']):,} cents. The line adds up. It is wrong and consistent."
        )
    if number == 4:
        till = night["tills"][(day, store)]
        return by_key["till_total"], (
            f"The tills charged the old price themselves. So the tills' own total and the "
            f"lines agree: both say {money(till)}. Two counts of the same mistake are equal. "
            "The second count only helps when the fault is born after the till."
        )
    if number == 7:
        return by_key["normal_volume"], (
            f"This check counts the lines of each store's day and compares the count with a "
            f"normal day. {store} has no lines on {_day(day)}, so there is no count at all. "
            "A day with no rows is not a day with zero lines. It is not in the list."
        )
    if number == 8:
        return by_key["every_till_reported"], (
            f"Both tills of {store} sent their own totals on time. Only the sales file "
            "is late. This check looks at the tills' file, and that file is complete."
        )
    return by_key["every_store_reported"], (
        f"{store} did report. It reported twice. This check asks only whether an open "
        "store has at least one line, and it has many."
    )


# ---------------------------------------------------------------- what is not a fault


def _not_a_fault(nights: dict, world) -> dict:
    """Three things that look wrong and are not. Every check must stay silent on them."""
    last = nights[WEEK[-1]]
    stores = last["stores"]
    holiday = HOLIDAY
    refit_store, refit_day = sorted(CLOSED)[0]
    big = world.planted["big_sale"]
    big_day, big_store = big.business_date, big.store_id
    big_line = big.lines[0]
    everywhere = {
        (r.get("store_id"), str(r.get("sales_date"))): key
        for key, rows in last["findings"].items()
        for r in rows
    }
    for where in [(s, holiday.isoformat()) for s in stores] + [
        (refit_store, refit_day.isoformat()),
    ]:
        assert where not in everywhere, f"{everywhere.get(where)} fires on {where}"
    assert not [r for r in last["findings"]["believable_quantity"] if r["store_id"] == big_store]

    quiet = {s: last["naive_normal"][(holiday, s)] for s in sorted(stores)}
    low = round(min(100 * lines / normal for lines, normal in quiet.values()))
    high = round(max(100 * lines / normal for lines, normal in quiet.values()))
    naive_alarms = sum(lines < VOLUME_LOW * normal for lines, normal in quiet.values())
    opens = next(c for c in last["calendar"] if c["kind"] == "public_holiday")
    all_lines = [n for (d, _s), n in last["lines_per_day"].items() if d < BONUS_WEEK_START]
    small = sorted(
        stores,
        key=lambda s: sum(
            n for (d, st), n in last["lines_per_day"].items() if st == s and d < BONUS_WEEK_START
        ),
    )[0]
    small_days = [
        n for (d, s), n in last["lines_per_day"].items() if s == small and d < BONUS_WEEK_START
    ]
    fixed = 100
    under = sum(n < fixed for n in small_days)
    big_pct = round(
        100
        * last["lines_per_day"][(big_day, big_store)]
        / last["naive_normal"][(big_day, big_store)][1]
    )

    def word(term: str) -> dict:
        return {"term": term, "meaning": WORDS[term]}

    empty = {"count": "", "cards": []}

    def stages(clean_cards: list[dict], count: str) -> dict:
        return {
            "files": empty, "raw": empty, "clean": {"count": count, "cards": clean_cards},
            "checks": {"done": 5, "active": 0}, "outputs": empty, "people": empty,
        }  # fmt: skip

    holiday_cards = [
        _note_card(
            f"{s} · {_day(holiday)}", f"{lines} lines", f"normal: {round(normal)}", "changed"
        )
        for s, (lines, normal) in list(quiet.items())[:4]
    ]
    refit_cards = [
        _note_card(f"{refit_store} · {_day(refit_day)}", "no file", "closed for a refit", "changed")
    ]
    big_cards = [
        _note_card(
            "Still water 500ml", money(big_line.amount_cents),
            f"sale · qty {_qty(big_line.quantity)} · {big.sold_at:%H:%M}", "changed",
        )
    ]  # fmt: skip
    none = {"run": False, "caught": [], "silent": [], "now": None}

    def run(*silent: str, now: str | None = None, done: bool = False) -> dict:
        keys = [c.key for c in ALL_CHECKS] if done else list(silent)
        return {"run": True, "caught": [], "silent": keys, "now": now}

    by_key = {c.key: c for c in ALL_CHECKS}
    volume, reported, quantity = (
        by_key["normal_volume"], by_key["every_store_reported"], by_key["believable_quantity"],
    )  # fmt: skip
    steps = [
        {
            "id": "holiday", "active": "clean", "task": None, "clock": f"{_day(holiday)}, in the stores",
            "title": f"A quiet Wednesday: {HOLIDAY_NAME}",
            "text": f"{_day(holiday)} is {HOLIDAY_NAME}, a public holiday. The stores open only "
                    f"from {opens['opens_at']:%H:%M} to {opens['closes_at']:%H:%M}. Every store "
                    f"sells between {low}% and {high}% of a normal Wednesday. Nothing is wrong. "
                    "Sales follow the calendar: Saturdays are busy, holidays are quiet.",
            "word": word("Seasonality"), "stages": stages(holiday_cards, "lines per store"),
            "moves": [], "reasoned": none,
        },
        {
            "id": "false_alarm", "active": "reasoned", "task": None, "clock": f"{_day(holiday)}, night",
            "title": f"A careless check would raise {_count(naive_alarms, 'alarm')}",
            "text": f"Take a volume check that knows no calendar. It compares {_day(holiday)} "
                    f"with the four Wednesdays before, and its limit is {VOLUME_LOW:.0%} of "
                    f"normal. It fails for {naive_alarms} of {len(quiet)} stores, on a day "
                    "when nothing is wrong. People who get such alarms stop reading alarms.",
            "sql": textwrap.dedent(NAIVE_NORMAL_DAY).strip(),
            "word": word("False positive"), "stages": stages(holiday_cards, "lines per store"),
            "moves": [], "reasoned": none, "problem": True,
        },
        {
            "id": "calendar", "active": "reasoned", "task": None, "clock": f"{_day(holiday)}, night",
            "title": f"Check {volume.number} knows the calendar, and stays silent",
            "text": f"Head office keeps a small list of days that are not normal: "
                    f"{len(last['calendar'])} rows in clean.calendar. Check {volume.number} "
                    "leaves those days out. It does not judge a holiday as a normal day, and "
                    "it does not use a holiday to work out a normal day. The price: a real "
                    "fault on a holiday would get no alarm from this check.",
            "facts": [["Lens", volume.lens], ["Prepare first", volume.prepare],
                      ["The limit", volume.limit]],
            "word": word("Reference data"), "stages": stages(holiday_cards, "lines per store"),
            "moves": [], "reasoned": run(volume.key, now=volume.key),
        },
        {
            "id": "refit", "active": "reasoned", "task": None, "clock": f"{_day(refit_day)}, night",
            "title": f"Store {refit_store} sends nothing, and that is correct",
            "text": f"On {_day(refit_day)} store {refit_store} is closed for a planned refit. "
                    f"No file comes. On Tuesday store S04 also sent no file, and that was a "
                    f"fault. The files look the same: there are none. Only the calendar tells "
                    f"them apart. Check {reported.number} asks for a file from every OPEN store, "
                    f"so it fails for S04 and stays silent for {refit_store}.",
            "facts": [["Lens", reported.lens], ["Prepare first", reported.prepare],
                      ["The limit", reported.limit]],
            "word": word("False negative"), "stages": stages(refit_cards, f"0 lines from {refit_store}"),
            "moves": [], "reasoned": run(volume.key, reported.key, now=reported.key),
        },
        {
            "id": "big_sale", "active": "reasoned", "task": None, "clock": f"{_day(big_day)}, {big.sold_at:%H:%M}",
            "title": f"A real sale of {_qty(big_line.quantity)} bottles",
            "text": f"A café buys {_qty(big_line.quantity)} bottles of still water at store "
                    f"{big_store}: one line of {money(big_line.amount_cents)}. It is far from "
                    f"a normal line, so it is worth a look. It is also true. Check "
                    f"{quantity.number} stays silent: water is sold by the piece, and "
                    f"{_qty(big_line.quantity)} is a whole number. A limit of 50 pieces would "
                    f"have failed on {_count(last['pieces_over_50'], 'line')} this week: this one.",
            "facts": [["Lens", quantity.lens], ["The limit", quantity.limit]],
            "word": word("Anomaly"), "stages": stages(big_cards, f"1 line from {big_store}"),
            "moves": [], "reasoned": run(volume.key, reported.key, quantity.key, now=quantity.key),
        },
        {
            "id": "limit", "active": "reasoned", "task": None, "clock": f"{_day(big_day)}, night",
            "title": "Where does a limit come from? From the data",
            "text": f"\"At least {fixed} lines a day\" sounds safe. In the four clean weeks a "
                    f"store's day has between {min(all_lines)} and {max(all_lines)} lines. "
                    f"Store {small} is small: {under} of its {len(small_days)} clean days are "
                    f"under {fixed}. One fixed number is wrong for somebody. So check "
                    f"{volume.number} compares each store with its own normal day, and its "
                    f"limit is a share: {VOLUME_LOW:.0%} to {VOLUME_HIGH:.0%}. On the day of the "
                    f"café, {big_store} had {big_pct}% of its normal lines. The largest real "
                    f"weight in those weeks is {_qty(last['largest_real_weight'])} kg, and the "
                    f"weight limit is {MAX_KILOGRAMS} kg.",
            "word": word("Limit"), "stages": stages(big_cards, f"1 line from {big_store}"),
            "moves": [], "reasoned": run(volume.key, reported.key, quantity.key, now=volume.key),
        },
        {
            "id": "silence", "active": "reasoned", "task": None, "clock": "Mon 08-31, morning",
            "title": f"{len(ALL_CHECKS)} checks, three strange days, no false alarm",
            "text": f"After the whole week, none of the {len(ALL_CHECKS)} checks has a bad row "
                    f"on the holiday, on the closed store, or on the {_qty(big_line.quantity)} "
                    "bottles. A check that cries wolf gets switched off. So every check is "
                    "tested twice: once on its fault, and once on things that only look wrong.",
            "word": word("False positive"), "stages": stages(big_cards, f"1 line from {big_store}"),
            "moves": [], "reasoned": run(done=True),
        },
    ]  # fmt: skip
    for step in steps:
        step["tasks_done"] = 5
    return {
        "id": "ok",
        "label": "Not a fault",
        "hint": "a holiday, a closed store, 400 bottles",
        "name": "Three things that look wrong and are not",
        "day": f"{_day(holiday)} to {_day(big_day)}",
        "store": "all",
        "store_name": "the six stores",
        "caught_by": [],
        "pipeline_says": "right",
        "answer_key_says": "right",
        "table_is_right": True,
        "note": "No check may fire on these.",
        "false_alarms": 0,
        "careless_alarms": naive_alarms,
        "big_sale": f"{_day(big_day)}, {big_store}",
        "bottles": _qty(big_line.quantity),
        "steps": steps,
    }


# ---------------------------------------------------------------- the pages' data


def _week_table(nights: dict, world) -> list[dict]:
    """Each store at the end of the bonus week: the report against the answer key."""
    last = nights[WEEK[-1]]
    week_true = truth.week_net_sales(world, BONUS_WEEK_START)
    rows = []
    for s in last["bonus"]["stores"]:
        right = week_true[s["store_id"]]
        rows.append(
            {
                "store": s["store_id"],
                "store_name": s["store_name"],
                "report": money(s["net_sales_cents"]),
                "answer_key": money(right),
                "error": _signed(s["net_sales_cents"] - right),
                "wrong": s["net_sales_cents"] != right,
            }
        )
    return rows


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


def _store_days(rows: list[dict]) -> list[str]:
    found = sorted({(str(r["sales_date"]), r["store_id"]) for r in rows})
    return [f"{store} {day[5:]}" for day, store in found]


@lru_cache(maxsize=1)
def _built() -> str:
    world = build_world()
    key = truth.daily_store_sales(world)
    nights = _run_the_week()
    last = nights[WEEK[-1]]
    tasks = dag_tasks()
    week = _week_table(nights, world)
    cases = [_case(n, nights, world, key) for n in SHOWN_CASES]
    not_a_fault = _not_a_fault(nights, world)
    green = sum(c["passed"] for night in nights.values() for c in night["checks"])
    total_checks = sum(len(night["checks"]) for night in nights.values())
    by_case = {c["id"]: c for c in cases}

    reasoned = [
        {
            "number": c.number, "key": c.key, "name": c.name, "lens": c.lens,
            "question": c.question, "table": c.table.split(".")[1],
            "protects": c.protects, "prepare": c.prepare, "limit": c.limit,
            "bad_rows": len(last["findings"][c.key]),
            "where": _store_days(last["findings"][c.key]),
            "cases": [int(case["id"]) for case in cases
                      if c.number in [x["number"] for x in case["caught_by"]]],
        }
        for c in ALL_CHECKS
    ]  # fmt: skip
    lenses = [
        {
            **lens,
            "checks": sum(c.lens == lens["id"] for c in ALL_CHECKS),
            "standard": len(checks.STANDARD_CHECKS) if lens["id"] == "data" else 0,
        }
        for lens in LENSES
    ]
    c1, c3, c4, c7 = by_case["1"], by_case["3"], by_case["4"], by_case["7"]
    till = next(c for c in reasoned if c["key"] == "till_total")
    unique = next(c for c in reasoned if c["key"] == "true_key_unique")
    caught_cases = sum(bool(case["caught_by"]) for case in cases)

    recap = {
        "question": QUESTION,
        "own_question": OWN_QUESTION,
        "week": {
            "stores": week,
            "stores_wrong": sum(r["wrong"] for r in week),
            "checks_green": green,
            "checks_run": total_checks,
            "nights": len(nights),
            "checks_with_bad_rows": sum(c["bad_rows"] > 0 for c in reasoned),
            "bad_rows": sum(c["bad_rows"] for c in reasoned),
        },
        "checks": reasoned,
        "lenses": lenses,
        "cases": [
            {
                "case": int(c["id"]), "name": c["name"], "day": c["day"], "store": c["store"],
                "pipeline_says": c["pipeline_says"], "answer_key_says": c["answer_key_says"],
                "caught_by": [f"{x['number']}. {x['name']}" for x in c["caught_by"]],
                "lenses": sorted({x["lens"] for x in c["caught_by"]}),
            }
            for c in cases
        ],
        "not_a_fault": {
            "things": [
                {"what": f"{HOLIDAY_NAME}, a public holiday", "where": f"{_day(HOLIDAY)}, all stores"},
                {"what": "A store closed for a planned refit",
                 "where": f"{_day(sorted(CLOSED)[0][1])}, {sorted(CLOSED)[0][0]}"},
                {"what": f"A café buys {not_a_fault['bottles']} bottles of water",
                 "where": not_a_fault["big_sale"]},
            ],
            "false_alarms": not_a_fault["false_alarms"],
            "careless_alarms": not_a_fault["careless_alarms"],
            "calendar_rows": len(last["calendar"]),
        },
        "quiz": [
            {
                "question": "A teammate says: we already have a unique check on line_id, and "
                "it passes every night. So no sale is counted twice. What do you show them?",
                "answer": f"The check is on the wrong columns. line_id is new on every export, "
                f"so a copy always gets a new one. Grouped by the true key, receipt_id + line_no, "
                f"the week has {unique['bad_rows']} lines stored twice: "
                f"{' and '.join(unique['where'])}. Unique on which columns is the whole question.",
            },
            {
                "question": f"Store {c4['store']} sells butter at yesterday's price on "
                f"{c4['day']}. The receipts add up to the tills' own total, to the cent. "
                "Is the day right?",
                "answer": f"{c4['note']} The tills "
                "made the mistake themselves, so both counts agree. A second count only catches "
                "a fault that is born after the till. This one needs the price list: "
                f"check {c4['caught_by'][0]['number']}, {c4['caught_by'][0]['name'].lower()}.",
            },
            {
                "question": f"On {_day(HOLIDAY)} every store sells less than half of a normal "
                "Wednesday. Your volume check stays silent. A colleague calls that a bug. Is it?",
                "answer": f"No. {_day(HOLIDAY)} is {HOLIDAY_NAME}, and the calendar says so. "
                f"Without the calendar the check would raise an alarm for every store, on a day "
                f"when nothing is wrong. A check that cries wolf gets switched off. The price is "
                "honest too: a real fault on a holiday gets no alarm from this check.",
            },
        ],
        "words": [{"term": term, "meaning": meaning} for term, meaning in WORDS.items()],
        "still_wrong": STILL_WRONG,
        "not_in_the_run": NOT_IN_THE_RUN,
        "example": {
            "case_1": {"store": c1["store"], "day": c1["day"], "says": c1["pipeline_says"],
                       "right": c1["answer_key_says"],
                       "caught_by": [x["name"] for x in c1["caught_by"]]},
            "case_3": {"store": c3["store"], "says": c3["pipeline_says"], "right": c3["answer_key_says"]},
            "case_7": {"store": c7["store"], "day": c7["day"], "right": c7["answer_key_says"]},
            "till_total_cases": till["cases"],
            "till_total_rows": till["bad_rows"],
        },
        "planted_cases_caught": caught_cases,
        "planted_cases_shown": len(cases),
    }  # fmt: skip

    flow = {
        "recap": recap,
        "milestone": 2,
        "of": 5,
        "title": "Checks with a reason",
        "lede": "Customers of Lotus Retails pay at a till: the machine at the counter, also "
        "called a cash register. Last time a wrong number walked through five green checks. "
        f"Now you have {len(ALL_CHECKS)} checks that can each say who they protect. Pick a "
        "fault, watch it pass the old checks, then watch the new ones catch it.",
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
            {"name": table, "raw_rows": last["raw_tables"][table], "clean_rows": rows}
            for table, rows in last["tables"].items()
        ],
        "outputs": ["daily_store_sales", "replenishment_list"],
        "checks": [
            {"number": c.number, "name": c.name, "table": c.table, "lens": "data"}
            for c in checks.STANDARD_CHECKS
        ],
        "reasoned": reasoned,
        "reasoned_note": {
            "sales": len(SALES_CHECKS),
            "tills": len(TILL_CHECKS),
            "command": "python -m lotus_pipeline inspect",
        },
        "lenses": lenses,
        "words": recap["words"],
        "cases": cases + [not_a_fault],
        "still_wrong": STILL_WRONG,
        "not_in_the_run": NOT_IN_THE_RUN,
        "next": "Milestone 3: every table is different.",
    }
    return json.dumps(flow, default=str)


def build() -> dict:
    """Everything the flow page shows, from a real run."""
    return json.loads(_built())


def build_recap(flow: dict | None = None) -> dict:
    """The numbers shown on milestone_2_recap.html."""
    flow = flow or build()
    recap = dict(flow["recap"])
    cards = sorted(p.stem for p in (MILESTONE_DIR / "checks").glob("*.md"))
    recap["numbers"] = {
        "notebooks": len(list((MILESTONE_DIR / "notebooks").glob("0*.ipynb"))),
        "tests": count_tests(),
        "tables": len(load.TABLES),
        "tables_with_a_card": len(cards),
        "checks_with_a_reason": len(ALL_CHECKS),
        "checks_on_sales_lines": len(SALES_CHECKS),
        "checks_on_till_totals": len(TILL_CHECKS),
        "standard_checks": len(checks.STANDARD_CHECKS),
        "tasks": len(flow["dag"]["tasks"]),
        "planted_cases_caught": recap["planted_cases_caught"],
        "planted_cases_in_sales": recap["planted_cases_shown"],
        "false_alarms": recap["not_a_fault"]["false_alarms"],
        "stores_wrong": recap["week"]["stores_wrong"],
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
