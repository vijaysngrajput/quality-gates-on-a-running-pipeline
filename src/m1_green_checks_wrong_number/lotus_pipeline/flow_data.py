"""Build the numbers shown on milestone_1_flow.html and milestone_1_recap.html.

The pages must never show a number typed by hand. This script runs the real
starter pipeline for the seven nights of the bonus week, on a private warehouse
(one DuckDB file in a temporary folder), and writes what happened into the pages.

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

from lotus_pipeline import checks, config, load, pipeline, report
from lotus_pipeline.report import money
from lotus_pipeline.warehouse import connect
from store_systems import files, key_files, planted, truth
from store_systems.catalog import BONUS_WEEK_START, LAST_DAY
from store_systems.generate import build as build_world

MILESTONE_DIR = config.MILESTONE_DIR
PAGE = MILESTONE_DIR / "milestone_1_flow.html"
RECAP_PAGE = MILESTONE_DIR / "milestone_1_recap.html"
DAG_FILE = MILESTONE_DIR / "dags" / "lotus_nightly_dag.py"
MARKER = re.compile(
    r'(<script id="page-data" type="application/json">)(.*?)(</script>)', flags=re.DOTALL
)
WEEK = [BONUS_WEEK_START + timedelta(days=n) for n in range(7)]
SHOWN_CASES = (1, 2, 3, 5, 6, 7, 8, 9)  # the sales cases this milestone's pipeline can show

QUESTION = (
    "Can I pay the bonus from last week's net sales, store by store? "
    "And if I cannot trust a number, which one, and why?"
)
STILL_WRONG = (
    "We found one error by luck, and we have a list of worries. "
    "We have no checks that would have caught it."
)

# The words this milestone teaches. The meanings are the same as in GLOSSARY.md.
WORDS = {
    "Data quality": "Data quality means the data is good enough to trust.",
    "Check": "A check is a query that looks for bad rows. No rows means the data passes.",
    "DAG": "The list of a pipeline's tasks and the order they run in. "
    "Airflow draws it as boxes and arrows.",
    "Task": "One step of the pipeline that Airflow starts, such as `load`.",
    "Dependency": "An arrow between two tasks. The second waits for the first, "
    "and does not run if the first fails.",
    "Silent failure": "Wrong data with no error. Every task succeeds, "
    "and a wrong number reaches the reader.",
    "Consumer": "Anything that reads a table: a person, a report or another table.",
    "Impact": "Everything a wrong table touches: the tables built from it "
    "and the people who read them. Also called blast radius.",
    "Critical table": "A table where a wrong value costs someone money or a decision. "
    "It gets the most checks.",
    "Grain": "What one row of a table means.",
    "Profiling": "Reading a table before you check it: count the rows, "
    "find the smallest and largest values, count the empty cells.",
    "The six dimensions": "Six questions for any table: is it complete, unique, valid, "
    "accurate, consistent and fresh?",
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

# The three places a gate will stand. None exists in this milestone.
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
    caught = key_files.DETAILS[number]["caught_by"]

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

    if number == 1:
        fault_rows = [
            r for r in in_file if r["line_type"] == "refund" and int(r["amount_cents"]) > 0
        ]
        normal = [r for r in in_file if r["line_type"] == "refund" and int(r["amount_cents"]) < 0]
        shown = fault_rows + normal[:1] + ordinary
        cents = int(fault_rows[0]["amount_cents"])
        hint = "a refund stored as a positive amount"
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

    elif number == 5:
        fault_rows = [r for r in in_file if r["sold_at"] > f"{file_date} 23:59:59"]
        shown = fault_rows[:3] + ordinary[:1]
        total = sum(int(r["amount_cents"]) for r in fault_rows)
        stamped = fault_rows[0]["sold_at"][:16]
        next_day = day + timedelta(days=1)
        true_time = world.planted[case["receipt"]].sold_at
        hint = "a sale at 03:10 in a store that closes at 22:00"
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
    if number == 8:
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
    first_run = " This first run also loads the four weeks before." if day == WEEK[0] else ""
    steps = [
        {
            "id": "happened", "active": "files", "task": None, "clock": f"{label}, in the store",
            "title": "What really happened", "text": happened,
            "word": word("Data quality"), "stages": stages("files") | {"files": {"count": "", "cards": []}},
            "moves": [],
        },
        {
            "id": "files", "active": "files", "task": "stores_send_files",
            "clock": f"{label}, 23:00", "title": "The store sends its file", "text": files_text,
            "word": word("Grain"), "stages": stages("files"), "moves": [], "plant": True,
            "problem": True,
        },
        {
            "id": "load", "active": "raw", "task": "load", "clock": night_label,
            "title": "Task load: copy the files, as received",
            "text": f"Airflow starts the task load. It copies {files_night} new files into the "
                    f"raw tables: {rows_night:,} rows.{first_run} Every value stays text, and "
                    f"nothing is judged. {raw_text}",
            "word": word("DAG"), "stages": stages("raw"), "moves": [{"from": "files", "to": "raw"}],
        },
        {
            "id": "clean", "active": "clean", "task": "clean", "clock": night_label,
            "title": "Task clean: give types, and join",
            "text": f"The task clean turns the text into numbers, dates and times, and joins "
                    f"each line to its product and its store. {clean_text}",
            "word": word("Task"), "stages": stages("clean"),
            "moves": [{"from": "raw", "to": "clean"}],
        },
    ]  # fmt: skip
    check_words = ["Check", "Dependency", "Profiling", "The six dimensions", "Critical table"]
    for check, term in zip(checks.STANDARD_CHECKS, check_words):
        steps.append(
            {
                "id": f"check_{check.number}", "active": "checks", "task": "check",
                "clock": night_label,
                "title": f"Check {check.number} of 5: {check.name.lower()}",
                "text": f"{why[check.number]} The query returns no rows, so the check passes.",
                "sql": textwrap.dedent(check.sql).strip(),
                "word": word(term),
                "stages": stages("clean", done=check.number - 1, active=check.number),
                "moves": [{"from": "clean", "to": "checks"}] if check.number == 1 else [],
            }
        )  # fmt: skip
    steps += [
        {
            "id": "publish", "active": "outputs", "task": "publish", "clock": night_label,
            "title": "Task publish: the check passed, so it runs",
            "text": f"All five checks are green, so Airflow starts the task publish. {publish_text}",
            "word": word("Silent failure"), "stages": stages("outputs", done=5),
            "moves": [{"from": "checks", "to": "outputs"}], "problem": True,
        },
        {
            "id": "people", "active": "people", "task": None, "clock": f"{_day(day + timedelta(days=1))}, morning",
            "title": people_title, "text": people_text,
            "word": word("Consumer"), "stages": stages("people", done=5),
            "moves": [{"from": "outputs", "to": "people"}], "problem": True,
        },
        {
            "id": "still_wrong", "active": "checks", "task": None, "clock": f"{_day(day + timedelta(days=1))}, morning",
            "title": "Still wrong: no check catches it yet",
            "text": f"What would catch it: {_and(caught)}. Milestone 1 has no such check. "
                    "It has no gate to stop a bad row, and no quarantine to set it aside. "
                    "The dashed lines show where they will stand. The next milestones "
                    "write these checks.",
            "word": word("Impact"), "stages": stages("people", done=5), "moves": [],
            "problem": True,
        },
    ]  # fmt: skip
    finished = {"happened": 0, "files": 0, "load": 1, "clean": 2, "publish": 4}
    for step in steps:
        step["tasks_done"] = finished.get(step["id"], 3 if step["id"].startswith("check") else 5)
    return {
        "id": str(number),
        "label": f"Case {number}",
        "hint": hint,
        "name": case["name"],
        "day": label,
        "store": store,
        "store_name": store_name,
        "lens_of_the_check_that_will_catch_it": case["lens"],
        "caught_by": caught,
        "pipeline_says": money(got_day) if got_day is not None else "no row",
        "answer_key_says": money(true_day),
        "steps": steps,
    }


def _and(things: list[str]) -> str:
    """['Refund rule', 'Till total'] -> a check called "Refund rule", and one called "Till total" """
    named = [f'a check called "{things[0]}"'] + [f'one called "{t}"' for t in things[1:]]
    return ", and ".join(named)


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
                "manager": s["manager_name"],
                "report": money(s["net_sales_cents"]),
                "answer_key": money(right),
                "error": _signed(s["net_sales_cents"] - right),
                "bonus_error": _signed(s["bonus_cents"] - truth.bonus_cents(right)),
                "report_cents": s["net_sales_cents"],
                "answer_key_cents": right,
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


@lru_cache(maxsize=1)
def _built() -> str:
    world = build_world()
    key = truth.daily_store_sales(world)
    nights = _run_the_week()
    monday = nights[WEEK[0]]
    tasks = dag_tasks()
    week = _week_table(nights, world)
    c1 = planted.case(1)
    s03 = next(s for s in monday["bonus"]["stores"] if s["store_id"] == c1["store"])
    s03_true = key[(c1["day"], c1["store"])]["net_sales_cents"]
    refund = next(
        r
        for r in monday["raw"]
        if r["store_id"] == c1["store"]
        and r["line_type"] == "refund"
        and int(r["amount_cents"]) > 0
    )
    monday_lines = [r for r in monday["raw"] if "2026-08-24" in r["_file_name"]]
    no_card = sum(1 for r in monday_lines if not r["member_id"])
    c7 = planted.case(7)
    c7_true = key[(c7["day"], c7["store"])]["net_sales_cents"]
    green = sum(c["passed"] for night in nights.values() for c in night["checks"])
    total_checks = sum(len(night["checks"]) for night in nights.values())

    recap = {
        "question": QUESTION,
        "monday": {
            "day": _day(c1["day"]),
            "files": len(monday["files"]),
            "rows": f"{sum(int(f['row_count']) for f in monday['files']):,}",
            "clean_lines": f"{monday['clean_total']:,}",
            "store": c1["store"],
            "manager": s03["manager_name"],
            "report": money(s03["net_sales_cents"]),
            "by_hand": money(s03_true),
            "difference": money(s03["net_sales_cents"] - s03_true),
            "refund": money(int(refund["amount_cents"])),
            "refund_line": refund["line_id"],
            "bonus_report": money(s03["bonus_cents"]),
            "bonus_right": money(truth.bonus_cents(s03_true)),
            "stores": [
                {
                    "store": s["store_id"],
                    "store_name": s["store_name"],
                    "report": money(s["net_sales_cents"]),
                    "answer_key": money(key[(c1["day"], s["store_id"])]["net_sales_cents"]),
                    "wrong": s["net_sales_cents"]
                    != key[(c1["day"], s["store_id"])]["net_sales_cents"],
                }
                for s in monday["bonus"]["stores"]
            ],
        },
        "week": {
            "stores": week,
            "stores_wrong": sum(r["wrong"] for r in week),
            "checks_green": green,
            "checks_run": total_checks,
            "nights": len(nights),
        },
        "quiz": [
            {
                "question": f"It is {_day(c1['day'])} night. All five checks pass. The head of "
                "retail operations asks: so the numbers are right? What do you say?",
                "answer": f"No. Green checks only answer the five questions they ask. None of "
                f"them asks whether a refund is negative. Store {c1['store']} is "
                f"{money(s03['net_sales_cents'] - s03_true)} too high tonight, "
                "with five green checks.",
            },
            {
                "question": "A teammate wants a not-null check on member_id in sales_lines, "
                "because empty cells are bad. What do you ask first?",
                "answer": f"Who uses member_id, and what goes wrong for them when it is empty? "
                f"A customer may pay without a loyalty card. On {_day(c1['day'])}, "
                f"{no_card} of {len(monday_lines)} lines have no member, and all of them "
                "are right. A check with no answer to that question is deleted.",
            },
            {
                "question": f"Store {c7['store']} sends no sales file on {_day(c7['day'])} night. "
                "Which of the five standard checks fails?",
                "answer": f"None. The other stores sent their lines, so the table is not empty. "
                f"{c7['store']} has no rows, and a row that is not there cannot fail a check. "
                f"A whole day of {money(c7_true)} is missing, and the run is green.",
            },
        ],
        "words": [{"term": term, "meaning": meaning} for term, meaning in WORDS.items()],
        "still_wrong": STILL_WRONG,
    }

    flow = {
        "recap": recap,
        "milestone": 1,
        "of": 5,
        "title": "Green checks, wrong number",
        "lede": "Customers of Lotus Retails pay at a till: the machine at the counter, also "
        "called a cash register. The pipeline is given to you, and it runs every night on Airflow. "
        "Pick a fault, and watch real rows from a Lotus store walk through five green "
        "checks and reach a person.",
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
            {"name": table, "raw_rows": monday["raw_tables"][table], "clean_rows": rows}
            for table, rows in monday["tables"].items()
        ],
        "outputs": ["daily_store_sales", "replenishment_list"],
        "checks": [
            {"number": c.number, "name": c.name, "table": c.table, "lens": "data"}
            for c in checks.STANDARD_CHECKS
        ],
        "lenses": [
            {**lens, "checks": len(checks.STANDARD_CHECKS) if lens["id"] == "data" else 0}
            for lens in LENSES
        ],
        "words": recap["words"],
        "cases": [_case(n, nights, world, key) for n in SHOWN_CASES],
        "still_wrong": STILL_WRONG,
        "next": "Milestone 2: checks with a reason.",
    }
    return json.dumps(flow, default=str)


def build() -> dict:
    """Everything the flow page shows, from a real run."""
    return json.loads(_built())


def build_recap(flow: dict | None = None) -> dict:
    """The numbers shown on milestone_1_recap.html."""
    flow = flow or build()
    recap = dict(flow["recap"])
    recap["numbers"] = {
        "notebooks": len(list((MILESTONE_DIR / "notebooks").glob("0*.ipynb"))),
        "tests": count_tests(),
        "tables": len(load.TABLES),
        "outputs": len(flow["outputs"]),
        "checks": len(checks.STANDARD_CHECKS),
        "tasks": len(flow["dag"]["tasks"]),
        "planted_cases": len(planted.CASES),
        "stores_wrong": recap["week"]["stores_wrong"],
        "gates": sum(g["state"] != "not_yet" for g in flow["gates"]),
        "rows_set_aside": 0,  # there is no quarantine yet
        "words": len(WORDS),
    }
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
        f"Wrote {len(flow['cases'])} cases of {len(flow['cases'][0]['steps'])} steps into "
        f"{PAGE.name}, and the numbers into {RECAP_PAGE.name}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
