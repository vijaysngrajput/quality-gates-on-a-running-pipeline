"""Build the data shown on understand_business/lotus_retails.html.

Every receipt, row and amount on that page comes from the till journal, the files
the stores send, and the answer key. No number is typed by hand.

    uv run python -m store_systems.business_page          write the page's data
    uv run python -m store_systems.business_page --check  fail if the page is out of date
"""

import json
import re
import sys
from datetime import date, datetime, time, timedelta
from pathlib import Path

from store_systems import files, key_files, planted, truth
from store_systems.catalog import (
    BONUS_RATE_PERCENT,
    BONUS_WEEK_END,
    BONUS_WEEK_START,
    PRODUCTS,
    RUN_HOUR,
    SIGN_OFF_DAY,
    STORES,
    SUPPLIERS,
)
from store_systems.generate import Receipt, World, build

PAGE = Path(__file__).resolve().parent.parent / "understand_business" / "lotus_retails.html"
MARKER = re.compile(
    r'(<script id="page-data" type="application/json">)(.*?)(</script>)', flags=re.DOTALL
)
NO_CHANGES = {"faults": [], "fixed": []}  # the files as the stores really send them

STAGES = [
    {"id": "till", "name": "Till", "sub": "in the store"},
    {"id": "file", "name": "Store file", "sub": "sent every night"},
    {"id": "raw", "name": "Raw tables", "sub": "as received"},
    {"id": "clean", "name": "Clean tables", "sub": "typed, joined"},
    {"id": "output", "name": "daily_store_sales", "sub": "what people read"},
    {"id": "bonus", "name": "Bonus", "sub": "signed on Monday"},
]

# The seven tables. "why" is why each one needs checks of its own.
TABLES = [
    {
        "id": "sales_lines",
        "one_row": "One item line on one receipt",
        "made": "Each till writes it. Each store sends a file every night",
        "why": "Money. Refunds. Goods sold by weight. A store's file can be late, missing or sent twice",
    },
    {
        "id": "till_totals",
        "one_row": "One till's own total for one day",
        "made": "The till adds it up when it is closed",
        "why": "A second count of the same money. It exists to be compared with sales_lines",
    },
    {
        "id": "products",
        "one_row": "One product at one price, with the dates that price is valid",
        "made": "Typed by the buying team",
        "why": "It changes. A price has a start and an end. A product can be retired",
    },
    {
        "id": "stores",
        "one_row": "One store, with its opening hours",
        "made": "Typed once, rarely changed",
        "why": "Tiny. Wrong here is wrong everywhere",
    },
    {
        "id": "stock_counts",
        "one_row": "One product in one store on one day",
        "made": "Counted by hand or by scanner at closing time",
        "why": "A count of one moment. A missing day is a gap, not a zero",
    },
    {
        "id": "supplier_prices",
        "one_row": "One product's cost from one supplier",
        "made": "A file the supplier sends once a week",
        "why": "Lotus does not control it. Columns move, units change, files repeat",
    },
    {
        "id": "members",
        "one_row": "One loyalty member",
        "made": "Typed by the customer in an app",
        "why": "People. The same person twice, half-filled rows, personal data",
    },
]


def dollars(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) // 100:,}.{abs(cents) % 100:02d}"


def when(moment: datetime | date) -> str:
    if isinstance(moment, datetime):
        return f"{moment:%a} {moment.day} {moment:%b}, {moment:%H:%M}"
    return f"{moment:%a} {moment.day} {moment:%b}"


def _run(day: date) -> datetime:
    """The nightly run for a business day: 02:00 the next morning."""
    return datetime.combine(day + timedelta(days=1), time(RUN_HOUR, 0))


def quantity(value) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _items(receipt: Receipt) -> str:
    return ", ".join(
        f"{quantity(abs(line.quantity))} × {PRODUCTS[line.product_id][0]} at "
        f"{dollars(line.unit_price_cents)}"
        for line in receipt.lines
    )


def _table(columns: list[str], rows: list[dict], mark: dict | None = None) -> dict:
    """A small table for the page. `mark` names one cell to point at: {"row": 0, "col": name}."""
    return {
        "columns": columns,
        "rows": [[str(row[c]) for c in columns] for row in rows],
        "mark": [mark["row"], columns.index(mark["col"])] if mark else None,
    }


def _file_lines(world: World, receipt: Receipt) -> list[dict]:
    """The rows of one receipt, exactly as they are in the store's nightly file."""
    day = receipt.business_date
    rows = files.sales_rows(world, receipt.store_id, day, NO_CHANGES)
    numbered = files._with_line_ids(rows, receipt.store_id, day, 1)
    return [row for row in numbered if row["receipt_id"] == receipt.receipt_id]


def _file_total(world: World, store_id: str, day: date) -> int:
    """What the lines of a store's file add up to. This is the number the tables will show."""
    return sum(row["amount_cents"] for row in files.sales_rows(world, store_id, day, NO_CHANGES))


def _till(world: World, receipt: Receipt) -> dict:
    rows = files.till_rows(world, receipt.store_id, receipt.business_date)
    return next(row for row in rows if row["till_id"] == receipt.till_id)


def _steps(world: World) -> list[dict]:
    sale, refund = world.planted["c1_sale"], world.planted["c1_refund"]
    store_id = sale.store_id
    store_name, town = STORES[store_id][0], STORES[store_id][1]
    sales = truth.daily_store_sales(world)
    oil = refund.lines[0]
    oil_name = PRODUCTS[oil.product_id][0]

    sale_lines = _file_lines(world, sale)
    refund_lines = _file_lines(world, refund)
    line_columns = [
        "line_id", "receipt_id", "line_no", "product_id", "quantity", "unit_price_cents",
        "amount_cents", "line_type",
    ]  # fmt: skip
    till_columns = [
        "business_date", "store_id", "till_id", "receipts", "gross_sales_cents",
        "refunds_cents", "net_total_cents",
    ]  # fmt: skip

    sale_day, refund_day = sale.business_date, refund.business_date
    sale_day_true = sales[(sale_day, store_id)]["net_sales_cents"]
    refund_day_true = sales[(refund_day, store_id)]["net_sales_cents"]
    refund_day_file = _file_total(world, store_id, refund_day)
    refund_tills = files.till_rows(world, store_id, refund_day)
    tills_say = sum(row["net_total_cents"] for row in refund_tills)

    week_true = truth.week_net_sales(world, BONUS_WEEK_START)[store_id]
    week_file = week_true + (refund_day_file - refund_day_true)
    manager = STORES[store_id][4]

    def output_row(day: date, net: int) -> dict:
        return {"sales_date": day.isoformat(), "store_id": store_id, "net_sales_cents": net}

    return [
        {
            "stage": "till",
            "title": "A customer pays at the till",
            "when": when(sale.sold_at),
            "gap": "",
            "what": f"At {store_name}, in {town}, a customer buys: {_items(sale)}. "
            f"Till {sale.till_id} prints receipt {sale.receipt_id}. "
            f"The total is {dollars(sale.total_cents)}.",
            "tables": [],
            "show": None,
            "note": "",
            "number": f"The till took {dollars(sale.total_cents)}.",
            "wrong": False,
        },
        {
            "stage": "file",
            "title": "The store sends its file",
            "when": f"{when(sale_day)}, after closing",
            "gap": "",
            "what": f"Every night a program in the store's back office writes the day's receipts "
            f"into a file and sends it. One row is one item line. So this receipt is "
            f"{len(sale_lines)} rows of the table sales_lines.",
            "tables": ["sales_lines"],
            "show": _table(line_columns, sale_lines),
            "note": "Money is in whole cents: 949 is $9.49.",
            "number": f"{len(sale_lines)} rows, {dollars(sale.total_cents)} in all.",
            "wrong": False,
        },
        {
            "stage": "file",
            "title": "The till counts its own day",
            "when": f"{when(sale_day)}, after closing",
            "gap": "",
            "what": f"When till {sale.till_id} is closed, it adds up its own day. This total "
            f"never passes through the back-office program. It is a second count of the "
            f"same money, in a second table: till_totals.",
            "tables": ["till_totals"],
            "show": _table(till_columns, [_till(world, sale)]),
            "note": "Nobody reads this table yet. Remember it.",
            "number": f"Till {sale.till_id} took {dollars(_till(world, sale)['net_total_cents'])} "
            f"that day.",
            "wrong": False,
        },
        {
            "stage": "raw",
            "title": "The pipeline copies the file",
            "when": when(_run(sale_day)),
            "gap": "",
            "what": "A pipeline is a program that moves data in steps, every night. Its first "
            "step copies each file into a raw table, exactly as it arrived. Every value "
            "stays text. Nothing is judged.",
            "tables": ["sales_lines", "till_totals", "products", "stores"],
            "show": None,
            "note": "",
            "number": "The same rows, with the name of the file they came from.",
            "wrong": False,
        },
        {
            "stage": "clean",
            "title": "The pipeline cleans the rows",
            "when": when(_run(sale_day)),
            "gap": "",
            "what": "The second step gives every column its type: a number, a date, a time. "
            "It also joins each line to the tables products and stores, to add the "
            "product's name and the store's name.",
            "tables": ["sales_lines", "products", "stores"],
            "show": _table(
                ["receipt_id", "product_name", "quantity", "amount_cents", "sales_date"],
                [
                    {
                        "receipt_id": row["receipt_id"],
                        "product_name": PRODUCTS[row["product_id"]][0],
                        "quantity": row["quantity"],
                        "amount_cents": row["amount_cents"],
                        "sales_date": sale_day.isoformat(),
                    }
                    for row in sale_lines
                ],
            ),
            "note": "",
            "number": "Clean rows, ready to be added up.",
            "wrong": False,
        },
        {
            "stage": "output",
            "title": "One number for the store's day",
            "when": when(_run(sale_day)),
            "gap": "",
            "what": "The last step adds up every line of the day, store by store. The result "
            "is the table daily_store_sales: one row per store per day. Net sales means "
            "the money taken for sales, less the money given back.",
            "tables": ["sales_lines"],
            "show": _table(
                ["sales_date", "store_id", "net_sales_cents"],
                [output_row(sale_day, sale_day_true)],
            ),
            "note": "",
            "number": f"{store_id} on {when(sale_day)}: {dollars(sale_day_true)}. That is right.",
            "wrong": False,
        },
        {
            "stage": "till",
            "title": "The customer brings the oil back",
            "when": when(refund.sold_at),
            "gap": f"{(refund_day - sale_day).days} days later",
            "what": f"The {oil_name} goes back to the store. Till {refund.till_id} gives the "
            f"customer {dollars(abs(refund.total_cents))}. This is a refund: money the "
            f"store gives back. Receipt {refund.receipt_id} points to the first receipt.",
            "tables": [],
            "show": None,
            "note": "",
            "number": f"The till gave back {dollars(abs(refund.total_cents))}.",
            "wrong": False,
        },
        {
            "stage": "file",
            "title": "The refund in the store's file",
            "when": f"{when(refund_day)}, after closing",
            "gap": "",
            "what": "The back-office program writes the refund as one row. A refund must take "
            "money away, so its amount must be below zero. Read the row.",
            "tables": ["sales_lines"],
            "show": _table(line_columns, refund_lines, {"row": 0, "col": "amount_cents"}),
            "note": f"The quantity is -1. The amount is +{refund_lines[0]['amount_cents']}. "
            f"The minus sign of the money is lost.",
            "number": f"The file says +{dollars(refund_lines[0]['amount_cents'])}. "
            f"The till gave back {dollars(abs(refund.total_cents))}.",
            "wrong": True,
        },
        {
            "stage": "file",
            "title": "The tills still know the truth",
            "when": f"{when(refund_day)}, after closing",
            "gap": "",
            "what": f"The two tills of {store_id} add up their own day, as every night. A till "
            f"knows that money went out. Their totals are right.",
            "tables": ["till_totals"],
            "show": _table(till_columns, refund_tills),
            "note": "",
            "number": f"The tills say {dollars(tills_say)} for the day.",
            "wrong": False,
        },
        {
            "stage": "output",
            "title": "A wrong number, and nothing fails",
            "when": when(_run(refund_day)),
            "gap": "",
            "what": "The pipeline copies, cleans and adds up, as every night. The row with the "
            "lost minus sign is a valid row: a number, a known product, a known store. "
            "So the refund is added, not taken away.",
            "tables": ["sales_lines"],
            "show": _table(
                ["sales_date", "store_id", "net_sales_cents"],
                [output_row(refund_day, refund_day_file)],
                {"row": 0, "col": "net_sales_cents"},
            ),
            "note": f"The table says {dollars(refund_day_file)}. The tills say "
            f"{dollars(tills_say)}. The table is {dollars(refund_day_file - tills_say)} too "
            f"high. It added {dollars(abs(refund.total_cents))} where it should have taken "
            f"{dollars(abs(refund.total_cents))} away.",
            "number": f"{store_id} on {when(refund_day)}: {dollars(refund_day_file)}. "
            f"The truth is {dollars(refund_day_true)}.",
            "wrong": True,
        },
        {
            "stage": "bonus",
            "title": "The bonus is paid from that number",
            "when": when(SIGN_OFF_DAY),
            "gap": f"{(SIGN_OFF_DAY - refund_day).days} days later",
            "what": f"Every Monday the head of retail operations signs the store managers' "
            f"bonus. It is {BONUS_RATE_PERCENT}% of the store's net sales for the week, "
            f"{when(BONUS_WEEK_START)} to {when(BONUS_WEEK_END)}. {manager} manages "
            f"{store_id}.",
            "tables": ["sales_lines"],
            "show": _table(
                ["", "week's net sales", f"bonus at {BONUS_RATE_PERCENT}%"],
                [
                    {
                        "": "The truth",
                        "week's net sales": dollars(week_true),
                        f"bonus at {BONUS_RATE_PERCENT}%": dollars(truth.bonus_cents(week_true)),
                    },
                    {
                        "": "With this one row",
                        "week's net sales": dollars(week_file),
                        f"bonus at {BONUS_RATE_PERCENT}%": dollars(truth.bonus_cents(week_file)),
                    },
                ],
            ),
            "note": "One wrong row in one file reached a person's pay. No program stopped. "
            "No error was shown.",
            "number": f"The bonus of {store_id} is "
            f"{dollars(truth.bonus_cents(week_file) - truth.bonus_cents(week_true))} too high.",
            "wrong": True,
        },
    ]


def _groups(world: World) -> list[dict]:
    """Who reads which table, and one real example of what a wrong value does to them."""
    cases = {c["case"]: c for c in key_files.planted_cases(world)}
    c1, c3, c4, c11, c12, c13 = (cases[n] for n in (1, 3, 4, 11, 12, 13))

    def day(case: dict) -> str:
        return when(date.fromisoformat(case["day"]))

    supplier = c11["supplier"]
    cost_day = date.fromisoformat(c11["day"])
    example_product = next(iter(world.costs[(supplier, cost_day)]))
    true_cost = int(world.costs[(supplier, cost_day)][example_product] * 100)
    milk = PRODUCTS[planted.case(12)["product"]][0]
    butter = PRODUCTS[planted.case(4)["product"]][0]
    first, second = planted.case(13)["first"], planted.case(13)["second"]
    bananas_bonus = truth.bonus_cents(c3["week_error_cents"])

    return [
        {
            "id": "managers",
            "name": "Store managers",
            "decide": f"They are paid from the data: a weekly bonus of {BONUS_RATE_PERCENT}% of "
            f"their store's net sales.",
            "hurt": "A wrong number is wrong pay.",
            "tables": ["sales_lines", "products", "stores"],
            "reads": "daily_store_sales",
            "examples": [
                f"{day(c1)}, store {c1['store']}: a refund is stored as a positive amount. "
                f"Net sales are {dollars(c1['week_error_cents'])} too high.",
                f"{day(c3)}, store {c3['store']}: a weight is written in grams, not kilograms. "
                f"One line of bananas says {dollars(c3['file_line_cents'])} where the customer "
                f"paid {dollars(c3['true_line_cents'])}. The manager's bonus is "
                f"{dollars(bananas_bonus)} too high.",
            ],
        },
        {
            "id": "replenishment",
            "name": "The replenishment team",
            "decide": "They order tomorrow's stock from yesterday's sales and from what is "
            "counted on the shelf.",
            "hurt": "Too high wastes food. Too low leaves empty shelves.",
            "tables": ["sales_lines", "stock_counts", "products", "stores"],
            "reads": "replenishment_list",
            "examples": [
                f"{day(c12)}, store {c12['store']}: nobody counts the {milk}. The store sold "
                f"{quantity(c12['sold'])} and has {quantity(c12['true_stock'])} left. The right "
                f"order is {quantity(c12['true_order'])}. With no count, the shelf is read as "
                f"empty and the order is {quantity(c12['order_with_no_count'])}.",
            ],
        },
        {
            "id": "finance",
            "name": "Finance",
            "decide": "They pay suppliers from the suppliers' price files, and they watch that "
            "the tills charge the price on the price list.",
            "hurt": "A wrong price is wrong money leaving the company.",
            "tables": ["supplier_prices", "products"],
            "reads": "replenishment_list",
            "examples": [
                f"{day(c11)}, supplier {supplier}: the file gives the cost in cents, not "
                f"dollars. {c11['products_100_times_too_dear']} products are 100 times too "
                f"dear. {PRODUCTS[example_product][0]} costs {dollars(true_cost)}, and the file "
                f"says {dollars(true_cost * 100)}.",
                f"{day(c4)}, store {c4['store']}: the price of {butter} went up that day. The "
                f"tills still charge the old price. {c4['packs']} packs are sold, and the "
                f"store is {dollars(c4['store_short_cents'])} short.",
            ],
        },
        {
            "id": "marketing",
            "name": "Marketing",
            "decide": "They email loyalty members: customers who signed up in the Lotus app.",
            "hurt": "One person stored twice gets two emails. A wrong address reaches a stranger.",
            "tables": ["members"],
            "reads": "the clean members table",
            "examples": [
                f"{day(c13)}: one person signs up a second time. Member {first['member_id']} "
                f"is {first['email']}. Member {second['member_id']} is {second['email']}. "
                f"A computer sees two different emails. It is one person.",
            ],
        },
    ]


def _file_sizes(world: World) -> dict[str, str]:
    """How big each table's file is on the first night of the bonus week."""
    day = BONUS_WEEK_START
    lines = sum(len(files.sales_rows(world, s, day, NO_CHANGES)) for s in STORES)
    stock = sum(len(files.stock_rows(world, s, day, NO_CHANGES)) for s in STORES)
    supplier = next(s for s, weekday in SUPPLIERS.items() if weekday == day.weekday())
    costs = len(world.costs[(supplier, day)])
    return {
        "sales_lines": f"{lines:,} rows from {len(STORES)} stores",
        "till_totals": f"{2 * len(STORES)} rows: 2 tills in each of {len(STORES)} stores",
        "products": f"{len(files.product_rows(day))} rows",
        "stores": f"{len(files.store_rows())} rows",
        "stock_counts": f"{stock:,} rows from {len(STORES)} stores",
        "supplier_prices": f"{costs} rows from supplier {supplier}",
        "members": f"{len(files.member_rows(world, day)):,} rows",
    }


def build_data() -> dict:
    world = build()
    sizes = _file_sizes(world)
    return {
        "stages": STAGES,
        "steps": _steps(world),
        "groups": _groups(world),
        "tables": [{**table, "size": sizes[table["id"]]} for table in TABLES],
        "size_day": when(BONUS_WEEK_START),
        "stores": len(STORES),
        "towns": len({store[1] for store in STORES.values()}),
        "sign_off": when(SIGN_OFF_DAY),
    }


def page_data() -> dict:
    return json.loads(MARKER.search(PAGE.read_text(encoding="utf-8")).group(2))


def main() -> int:
    data = build_data()
    if "--check" in sys.argv:
        ok = page_data() == data
        print(
            "lotus_retails.html matches the till journal."
            if ok
            else "lotus_retails.html is out of date."
        )
        return 0 if ok else 1
    text = PAGE.read_text(encoding="utf-8")
    payload = json.dumps(data, indent=1, ensure_ascii=False).replace("</", "<\\/")
    PAGE.write_text(
        MARKER.sub(lambda m: m.group(1) + "\n" + payload + "\n" + m.group(3), text),
        encoding="utf-8",
    )
    print(f"Wrote {len(data['steps'])} steps and {len(data['groups'])} groups into {PAGE.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
