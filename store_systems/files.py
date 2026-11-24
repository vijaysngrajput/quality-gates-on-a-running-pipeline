"""Write the files the stores, the suppliers and the loyalty app send.

The till journal is the truth. A file is what arrives. The thirteen planted cases
are the differences between the two, and every one of them is in this file.

    landing/
      sales_lines/sales_lines_S03_2026-08-24.csv      one per store, per night
      till_totals/till_totals_S03_2026-08-24.csv      one per store, per night
      stock_counts/stock_counts_S03_2026-08-24.csv    one per store, per night
      products/products_2026-08-24.csv                the whole table, every night
      stores/stores_2026-08-24.csv                    the whole table, every night
      members/members_2026-08-24.csv                  the whole table, every night
      supplier_prices/supplier_prices_SUP-B_2026-08-25.csv   one per supplier, per week
      _arrivals.csv                                   when each file arrived
"""

import csv
import json
from datetime import date, datetime, time, timedelta
from pathlib import Path

from store_systems import planted
from store_systems.catalog import (
    PRICE_CHANGES,
    PRODUCTS,
    RETIRED,
    STORES,
    SUPPLIERS,
    all_days,
    is_open,
    on_sale,
)
from store_systems.generate import Receipt, World, amount, build

SALES_COLUMNS = [
    "line_id", "receipt_id", "line_no", "store_id", "till_id", "sold_at", "product_id",
    "quantity", "unit_price_cents", "amount_cents", "line_type", "refund_of_receipt_id",
    "member_id",
]  # fmt: skip
TILL_COLUMNS = [
    "business_date", "store_id", "till_id", "receipts", "gross_sales_cents", "refunds_cents",
    "net_total_cents", "closed_at",
]  # fmt: skip
PRODUCT_COLUMNS = [
    "product_id", "product_name", "category", "unit", "unit_price_cents", "valid_from",
    "valid_to", "status",
]  # fmt: skip
STORE_COLUMNS = [
    "store_id", "store_name", "town", "opens_at", "closes_at", "manager_name", "tills",
]  # fmt: skip
STOCK_COLUMNS = [
    "count_date", "store_id", "product_id", "counted_qty", "counted_by", "counted_at",
]  # fmt: skip
SUPPLIER_COLUMNS = ["supplier_id", "product_id", "unit_cost", "unit", "price_date"]
MEMBER_COLUMNS = [
    "member_id", "full_name", "email", "phone", "home_store_id", "joined_on", "marketing_opt_in",
]  # fmt: skip

PRICE_LIST_START = date(2026, 1, 5)  # the day the current price list began
FAULT_KINDS = ("late-file", "file-twice", "rename-column", "bad-prices", "no-file")
STATE_FILE = "_source_state.json"
ARRIVALS_FILE = "_arrivals.csv"


def _stamp(moment: datetime) -> str:
    return moment.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------- what the learner switched on


def read_state(landing: Path) -> dict:
    path = landing / STATE_FILE
    if path.exists():
        return json.loads(path.read_text())
    return {"faults": [], "fixed": []}


def write_state(landing: Path, state: dict) -> None:
    landing.mkdir(parents=True, exist_ok=True)
    (landing / STATE_FILE).write_text(json.dumps(state, indent=2))


def _switched_on(state: dict, kind: str, store_id: str, day: date) -> dict | None:
    for fault in state["faults"]:
        if (fault["kind"], fault["store"], fault["day"]) == (kind, store_id, day.isoformat()):
            return fault
    return None


def _planted(state: dict, number: int, day: date, who: str | None = None) -> dict | None:
    """The planted case, if it applies here and nobody has fixed it."""
    case = planted.case(number)
    if number in state["fixed"] or case["day"] != day:
        return None
    if who is not None and who not in (case.get("store"), case.get("supplier")):
        return None
    return case


# ---------------------------------------------------------------- sales lines


def _receipt_rows(receipt: Receipt, state: dict) -> list[dict]:
    """One receipt as the export program writes it. Cases 1, 3 and 5 happen here."""
    day = receipt.business_date
    refund_sign_lost = _planted(state, 1, day) and receipt.key == planted.case(1)["receipt"]
    grams = _planted(state, 3, day) and receipt.key == planted.case(3)["receipt"]
    wrong_clock = _planted(state, 5, day) and receipt.key == planted.case(5)["receipt"]

    rows = []
    for line in receipt.lines:
        quantity, cents = line.quantity, line.amount_cents
        if refund_sign_lost:
            cents = abs(cents)  # case 1: the minus sign of the refund is lost
        if grams and line.product_id == planted.case(3)["product"]:
            quantity = quantity * 1000  # case 3: the scale's grams are written as kilograms
            cents = amount(quantity, line.unit_price_cents)
        sold_at = planted.case(5)["stamped_at"] if wrong_clock else receipt.sold_at
        rows.append(
            {
                "receipt_id": receipt.receipt_id,
                "line_no": line.line_no,
                "store_id": receipt.store_id,
                "till_id": receipt.till_id,
                "sold_at": _stamp(sold_at),
                "product_id": line.product_id,
                "quantity": f"{quantity:.3f}",
                "unit_price_cents": line.unit_price_cents,
                "amount_cents": cents,
                "line_type": line.line_type,
                "refund_of_receipt_id": receipt.refund_of.receipt_id if receipt.refund_of else "",
                "member_id": receipt.member_id or "",
            }
        )
    return rows


def sales_rows(world: World, store_id: str, day: date, state: dict) -> list[dict]:
    """Every line of one store's file for one day, without line ids."""
    receipts = [r for r in world.receipts if r.store_id == store_id and r.business_date == day]
    rows = [row for receipt in receipts for row in _receipt_rows(receipt, state)]
    if _planted(state, 2, day, store_id):  # case 2: one receipt is exported twice
        twice = world.planted[planted.case(2)["receipt"]]
        rows.extend(_receipt_rows(twice, state))
    bad = _switched_on(state, "bad-prices", store_id, day)
    if bad:  # a batch of impossible prices: every tenth line is 100 times too dear
        for row in rows[::10]:
            row["unit_price_cents"] = row["unit_price_cents"] * 100
            row["amount_cents"] = row["amount_cents"] * 100
    return rows


def _with_line_ids(rows: list[dict], store_id: str, day: date, first: int) -> list[dict]:
    """The export program gives every line a new id, on every export."""
    return [
        {"line_id": f"{store_id}-{day:%m%d}-{number:06d}", **row}
        for number, row in enumerate(rows, start=first)
    ]


# ---------------------------------------------------------------- the other tables


def till_rows(world: World, store_id: str, day: date) -> list[dict]:
    """Each till adds up its own day. It never passes through the export program."""
    rows = []
    for till_id in ("T1", "T2"):
        receipts = [
            r
            for r in world.receipts
            if (r.store_id, r.till_id, r.business_date) == (store_id, till_id, day)
        ]
        lines = [line for r in receipts for line in r.lines]
        gross = sum(x.amount_cents for x in lines if x.line_type == "sale")
        refunds = -sum(x.amount_cents for x in lines if x.line_type == "refund")
        closes = STORES[store_id][3]
        closed_at = datetime.combine(day, closes) + timedelta(minutes=6 if till_id == "T1" else 9)
        rows.append(
            {
                "business_date": day.isoformat(),
                "store_id": store_id,
                "till_id": till_id,
                "receipts": len(receipts),
                "gross_sales_cents": gross,
                "refunds_cents": refunds,
                "net_total_cents": gross - refunds,
                "closed_at": _stamp(closed_at),
            }
        )
    return rows


def product_rows(day: date) -> list[dict]:
    """The price list as the buying team has typed it by the end of `day`."""
    rows = []
    for product_id, (name, category, unit, price, _supplier) in PRODUCTS.items():
        row = {
            "product_id": product_id,
            "product_name": name,
            "category": category,
            "unit": unit,
            "unit_price_cents": price,
            "valid_from": PRICE_LIST_START.isoformat(),
            "valid_to": "",
            "status": "active",
        }
        last_day = RETIRED.get(product_id)
        if last_day and day >= last_day:
            row.update(valid_to=last_day.isoformat(), status="retired")
        change = PRICE_CHANGES.get(product_id)
        if change and day >= change[0] - timedelta(days=1):  # typed the day before it starts
            starts, new_price = change
            row["valid_to"] = (starts - timedelta(days=1)).isoformat()
            rows.append(row)
            row = {**row, "unit_price_cents": new_price, "valid_from": starts.isoformat(),
                   "valid_to": ""}  # fmt: skip
        rows.append(row)
    return rows


def store_rows() -> list[dict]:
    return [
        {
            "store_id": store_id,
            "store_name": name,
            "town": town,
            "opens_at": f"{opens:%H:%M}",
            "closes_at": f"{closes:%H:%M}",
            "manager_name": manager,
            "tills": 2,
        }
        for store_id, (name, town, opens, closes, manager, _size) in STORES.items()
    ]


def stock_rows(world: World, store_id: str, day: date, state: dict) -> list[dict]:
    missing = _planted(state, 12, day, store_id)
    rows = []
    for product_id in PRODUCTS:
        count = world.stock.get((day, store_id, product_id))
        if count is None:
            continue
        if missing and product_id == missing["product"]:
            continue  # case 12: nobody counted the milk
        rows.append(
            {
                "count_date": day.isoformat(),
                "store_id": store_id,
                "product_id": product_id,
                "counted_qty": f"{count['counted_qty']:.3f}",
                "counted_by": count["counted_by"],
                "counted_at": _stamp(count["counted_at"]),
            }
        )
    return rows


def supplier_file(world: World, supplier_id: str, day: date, state: dict) -> tuple[list, list]:
    """The columns and the rows of one supplier's weekly price file."""
    columns = list(SUPPLIER_COLUMNS)
    in_cents = _planted(state, 11, day, supplier_id)
    rows = []
    for product_id, cost in world.costs[(supplier_id, day)].items():
        rows.append(
            {
                "supplier_id": supplier_id,
                "product_id": product_id,
                # case 11: 2.10 dollars is written as 210
                "unit_cost": f"{int(cost * 100)}" if in_cents else f"{cost:.2f}",
                "unit": PRODUCTS[product_id][2],
                "price_date": day.isoformat(),
            }
        )
    renamed = _planted(state, 10, day, supplier_id)
    if renamed:  # case 10: the supplier gives the cost column a new name
        old, new = renamed["old_name"], renamed["new_name"]
        columns = [new if c == old else c for c in columns]
        rows = [{(new if k == old else k): v for k, v in row.items()} for row in rows]
    return columns, rows


def member_rows(world: World, day: date) -> list[dict]:
    return [
        {
            **{k: member[k] for k in MEMBER_COLUMNS},
            "joined_on": member["joined_on"].isoformat(),
            "marketing_opt_in": "true" if member["marketing_opt_in"] else "false",
        }
        for member in world.members
        if member["joined_on"] <= day
    ]


# ---------------------------------------------------------------- delivering a day


class _Landing:
    """The folder the files arrive in, and the list of when each one arrived."""

    def __init__(self, folder: Path):
        self.folder = folder
        self.arrivals: dict[str, dict] = {}
        path = folder / ARRIVALS_FILE
        if path.exists():
            with path.open(newline="") as f:
                self.arrivals = {row["file_name"]: row for row in csv.DictReader(f)}
        self.written: list[Path] = []

    def write(self, table: str, name: str, columns: list, rows: list, received: datetime) -> None:
        path = self.folder / table / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(rows)
        self.arrivals[name] = {
            "file_name": name, "table_name": table, "received_at": _stamp(received),
        }  # fmt: skip
        self.written.append(path)

    def close(self) -> None:
        self.folder.mkdir(parents=True, exist_ok=True)
        with (self.folder / ARRIVALS_FILE).open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["file_name", "table_name", "received_at"])
            writer.writeheader()
            writer.writerows(sorted(self.arrivals.values(), key=lambda r: r["received_at"]))


def _send_sales(world, landing: _Landing, store_id: str, day: date, state, received: datetime):
    rows = sales_rows(world, store_id, day, state)
    columns = list(SALES_COLUMNS)
    renamed = _switched_on(state, "rename-column", store_id, day)
    first = _with_line_ids(rows, store_id, day, 1)
    second = _with_line_ids(rows, store_id, day, len(rows) + 1)
    if renamed:  # the export program gives the amount column a new name
        columns = ["amount" if c == "amount_cents" else c for c in columns]
        for row in first + second:
            row["amount"] = row.pop("amount_cents")
    name = f"sales_lines_{store_id}_{day.isoformat()}"
    landing.write("sales_lines", f"{name}.csv", columns, first, received)
    twice = _planted(state, 9, day, store_id) or _switched_on(state, "file-twice", store_id, day)
    if twice:  # case 9: the whole file again, with new line ids
        later = received + timedelta(minutes=12)
        landing.write("sales_lines", f"{name}_2.csv", columns, second, later)


def _is_late(state: dict, store_id: str, day: date) -> datetime | None:
    """When a late sales file arrives, or None when it is on time."""
    case = _planted(state, 8, day, store_id)
    if case:
        return case["arrives_at"]
    if _switched_on(state, "late-file", store_id, day):
        return datetime.combine(day + timedelta(days=2), time(0, 40))
    return None


def deliver(day: date, landing_folder: Path, world: World | None = None) -> list[Path]:
    """Write every file that arrives during the night after business day `day`.

    Files are due by 01:00. The pipeline runs at 02:00.
    """
    world = world or build()
    state = read_state(landing_folder)
    landing = _Landing(landing_folder)
    evening = datetime.combine(day, time(23, 0))

    landing.write("products", f"products_{day}.csv", PRODUCT_COLUMNS, product_rows(day), evening)
    landing.write("stores", f"stores_{day}.csv", STORE_COLUMNS, store_rows(), evening)
    landing.write("members", f"members_{day}.csv", MEMBER_COLUMNS, member_rows(world, day), evening)

    for supplier_id, weekday in SUPPLIERS.items():
        if day.weekday() == weekday:
            columns, rows = supplier_file(world, supplier_id, day, state)
            name = f"supplier_prices_{supplier_id}_{day}.csv"
            landing.write("supplier_prices", name, columns, rows, datetime.combine(day, time(9, 0)))

    for number, store_id in enumerate(STORES):
        received = evening + timedelta(minutes=10 + 4 * number)
        yesterday = day - timedelta(days=1)
        late = _is_late(state, store_id, yesterday)
        if late:  # yesterday's file arrives now, a day late
            _send_sales(world, landing, store_id, yesterday, state, late)
        if not is_open(store_id, day):
            continue  # a closed store sends nothing, and that is correct
        landing.write(
            "till_totals", f"till_totals_{store_id}_{day}.csv", TILL_COLUMNS,
            till_rows(world, store_id, day), received + timedelta(minutes=1),
        )  # fmt: skip
        landing.write(
            "stock_counts", f"stock_counts_{store_id}_{day}.csv", STOCK_COLUMNS,
            stock_rows(world, store_id, day, state), received + timedelta(minutes=2),
        )  # fmt: skip
        no_file = _planted(state, 7, day, store_id) or _switched_on(state, "no-file", store_id, day)
        if no_file or _is_late(state, store_id, day):
            continue  # case 7: no sales file at all. Case 8: not tonight
        _send_sales(world, landing, store_id, day, state, received)

    landing.close()
    return landing.written


def deliver_through(last_day: date, landing_folder: Path) -> int:
    """Every night from the first day up to `last_day`. Returns the number of files."""
    world = build()
    return sum(len(deliver(d, landing_folder, world)) for d in all_days() if d <= last_day)


__all__ = ["deliver", "deliver_through", "read_state", "write_state", "on_sale", "FAULT_KINDS"]
