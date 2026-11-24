"""Write the answer key and the list of planted cases as files.

    uv run python -m store_systems.key_files

    answer_key/answer_key.json      the true number for every store and day
    answer_key/planted_cases.json   the thirteen cases, with dates and amounts

Notebooks, tests, pages and the D8Loop website read these two files.
A test fails when they are out of date.
"""

import json
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

from store_systems import planted, truth
from store_systems.catalog import BONUS_WEEK_END, BONUS_WEEK_START, PRODUCTS, STORES
from store_systems.generate import World, amount, build, order_quantity, units_sold

FOLDER = Path(__file__).resolve().parent.parent / "answer_key"

# What each case does to a person, and what catches it. The amounts are worked out below.
DETAILS = {
    1: {
        "who": "Store managers",
        "harm": "Net sales are too high for one store, so its manager is overpaid.",
        "caught_by": ["Refund rule", "Till total"],
        "born_in": "export program",
    },
    2: {
        "who": "Store managers",
        "harm": "The key is still unique. One receipt is counted twice.",
        "caught_by": ["Unique on the right columns", "Till total"],
        "born_in": "export program",
    },
    3: {
        "who": "Store managers",
        "harm": "One line of bananas costs more than the store's whole day.",
        "caught_by": ["Believable range per product"],
        "born_in": "export program",
    },
    4: {
        "who": "Finance",
        "harm": "Each line is valid. The store took less money than the price list says.",
        "caught_by": ["Price valid on that date"],
        "born_in": "till",
    },
    5: {
        "who": "Store managers",
        "harm": "The sale lands on Monday, in the next bonus week.",
        "caught_by": ["Store hours"],
        "born_in": "till",
    },
    6: {
        "who": "Store managers",
        "harm": "The join drops the lines silently, and sales are too low.",
        "caught_by": ["Every sale has a product"],
        "born_in": "buying team",
    },
    7: {
        "who": "Store managers",
        "harm": "Every check passes: there are no rows to fail. A whole day is missing.",
        "caught_by": ["Volume against a normal day", "Every store reported"],
        "born_in": "store back office",
    },
    8: {
        "who": "The replenishment team",
        "harm": "Yesterday's number is low, and changes tomorrow.",
        "caught_by": ["Freshness", "Run history"],
        "born_in": "store back office",
    },
    9: {
        "who": "Store managers",
        "harm": "Sales for that store double for the day.",
        "caught_by": ["Volume against a normal day", "Till total"],
        "born_in": "export program",
    },
    10: {
        "who": "Finance",
        "harm": "The load runs. The cost column is empty, so the order has no cost.",
        "caught_by": ["Expected columns"],
        "born_in": "supplier",
    },
    11: {
        "who": "Finance",
        "harm": "Every cost is 100 times too high, and still a valid number.",
        "caught_by": ["Change against the last file"],
        "born_in": "supplier",
    },
    12: {
        "who": "The replenishment team",
        "harm": "The gap is read as zero stock, and the store over-orders.",
        "caught_by": ["A snapshot has a row for every day"],
        "born_in": "store",
    },
    13: {
        "who": "Marketing",
        "harm": "Two emails to one person, and two sets of points.",
        "caught_by": ["Uniqueness after cleaning"],
        "born_in": "loyalty app",
    },
}


def _sales_errors(world: World) -> dict[int, dict]:
    """What the starter pipeline gets wrong in net sales, case by case, in cents."""
    sales = truth.daily_store_sales(world)

    def day_total(number: int) -> int:
        case = planted.case(number)
        return sales[(case["day"], case["store"])]["net_sales_cents"]

    c3 = world.planted["c3"].lines[0]
    in_grams = amount(c3.quantity * 1000, c3.unit_price_cents)
    butter = sum(
        line.quantity
        for key, receipt in world.planted.items()
        if key.startswith("c4_")
        for line in receipt.lines
        if line.product_id == "P-003"
    )
    return {
        1: {"week_error_cents": 2 * abs(world.planted["c1_refund"].total_cents)},
        2: {"week_error_cents": world.planted["c2"].total_cents},
        3: {"week_error_cents": in_grams - c3.amount_cents,
            "true_line_cents": c3.amount_cents, "file_line_cents": in_grams},
        4: {"week_error_cents": 0, "packs": int(butter),
            "store_short_cents": int(butter) * (399 - PRODUCTS["P-003"][3])},
        5: {"week_error_cents": -world.planted["c5"].total_cents},
        6: {"week_error_cents": -3 * 599},
        7: {"week_error_cents": -day_total(7)},
        8: {"week_error_cents": 0, "error_for_one_night_cents": -day_total(8)},
        9: {"week_error_cents": day_total(9)},
    }  # fmt: skip


def _other_errors(world: World) -> dict[int, dict]:
    c12 = planted.case(12)
    sold = units_sold(world.receipts).get((c12["day"], c12["store"], c12["product"]), Decimal(0))
    stock = world.stock[(c12["day"], c12["store"], c12["product"])]["counted_qty"]
    true_order = order_quantity(sold, stock)
    wrong_order = order_quantity(sold, Decimal(0))
    c10, c11 = planted.case(10), planted.case(11)
    return {
        10: {"products_without_cost": len(world.costs[(c10["supplier"], c10["day"])])},
        11: {"products_100_times_too_dear": len(world.costs[(c11["supplier"], c11["day"])])},
        12: {"sold": float(sold), "true_stock": float(stock), "true_order": float(true_order),
             "order_with_no_count": float(wrong_order),
             "over_order": float(wrong_order - true_order)},
        13: {"first": planted.case(13)["first"]["member_id"],
             "second": planted.case(13)["second"]["member_id"]},
    }  # fmt: skip


def planted_cases(world: World) -> list[dict]:
    amounts = {**_sales_errors(world), **_other_errors(world)}
    cases = []
    for case in planted.CASES:
        number = case["case"]
        cases.append(
            {
                "case": number,
                "name": case["name"],
                "lens": case["lens"],
                "table": case["table"],
                "day": case["day"].isoformat(),
                "store": case.get("store"),
                "supplier": case.get("supplier"),
                "passes_the_five_standard_checks": True,
                **DETAILS[number],
                **amounts[number],
            }
        )
    return cases


def week_key(world: World) -> dict:
    """The bonus week: each store's true net sales and bonus, and the day by day totals."""
    totals = truth.week_net_sales(world, BONUS_WEEK_START)
    running = {}
    day = BONUS_WEEK_START
    while day <= BONUS_WEEK_END:
        running[day.isoformat()] = truth.week_net_sales(world, BONUS_WEEK_START, day)
        day += timedelta(days=1)
    return {
        "week_start": BONUS_WEEK_START.isoformat(),
        "week_end": BONUS_WEEK_END.isoformat(),
        "stores": {
            store_id: {
                "store_name": STORES[store_id][0],
                "net_sales_cents": totals[store_id],
                "bonus_cents": truth.bonus_cents(totals[store_id]),
            }
            for store_id in STORES
        },
        "net_sales_cents_up_to": running,
    }


def build_files(world: World | None = None) -> dict[str, str]:
    world = world or build()
    key = {**truth.answer_key(world), "bonus_week": week_key(world)}
    cases = {
        "cases": planted_cases(world),
        "not_wrong": [{**n, "day": n["day"].isoformat()} for n in planted.NOT_WRONG],
    }
    return {
        "answer_key.json": json.dumps(key, indent=1) + "\n",
        "planted_cases.json": json.dumps(cases, indent=1) + "\n",
    }


def main() -> None:
    FOLDER.mkdir(exist_ok=True)
    for name, text in build_files().items():
        (FOLDER / name).write_text(text)
        print(f"wrote {FOLDER / name}")


if __name__ == "__main__":
    main()
