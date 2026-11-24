"""Build the world of Lotus Retails: what really happened in five weeks.

The most important part is the TILL JOURNAL: every receipt, as the till really printed it.
The journal is the truth. The files a store sends are written from it (see files.py),
and the answer key is worked out from it (see truth.py).

The same seed always gives the same world.
"""

import random
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import ROUND_HALF_UP, Decimal
from functools import lru_cache

from store_systems import planted
from store_systems.catalog import (
    FIRST_DAY,
    HOLIDAY,
    HOLIDAY_FACTOR,
    PRODUCTS,
    STORES,
    SUPPLIERS,
    TILLS,
    WEEKDAY_FACTOR,
    all_days,
    is_open,
    on_sale,
    opening_hours,
    price_on,
)

SEED = 20260727
FIRST_RECEIPT_NUMBER = 4001
THOUSANDTH = Decimal("0.001")


@dataclass
class Line:
    line_no: int
    product_id: str
    quantity: Decimal  # pieces, or kilograms. Negative on a refund
    unit_price_cents: int
    amount_cents: int  # negative on a refund
    line_type: str  # sale or refund


@dataclass
class Receipt:
    store_id: str
    till_id: str
    sold_at: datetime  # the true moment of the sale
    lines: list[Line]
    member_id: str | None = None
    refund_of: "Receipt | None" = None
    key: str | None = None  # the name of a planted receipt, or None
    receipt_id: str = ""

    @property
    def business_date(self) -> date:
        return self.sold_at.date()

    @property
    def total_cents(self) -> int:
        return sum(line.amount_cents for line in self.lines)


@dataclass
class World:
    receipts: list[Receipt]
    members: list[dict]
    stock: dict[tuple[date, str, str], dict]  # (day, store, product) -> the true count
    costs: dict[tuple[str, date], dict[str, Decimal]]  # (supplier, day) -> product -> dollars
    planted: dict[str, Receipt] = field(default_factory=dict)  # planted key -> its receipt


def amount(quantity: Decimal, unit_price_cents: int) -> int:
    """quantity × price, rounded to a whole cent."""
    return int((quantity * unit_price_cents).quantize(Decimal(1), rounding=ROUND_HALF_UP))


def _line(line_no: int, product_id: str, quantity: Decimal, price: int) -> Line:
    kind = "refund" if quantity < 0 else "sale"
    return Line(line_no, product_id, quantity, price, amount(quantity, price), kind)


# ---------------------------------------------------------------- members

FIRST_NAMES = (
    "Amara Ben Carla Dev Elena Farid Grace Hugo Imani Jonas Keiko Liam Maya Noor Omar Petra "
    "Quinn Rosa Sven Talia Uma Viktor Wren Xavier Yara Zane Anika Bruno Chloe Dario Esme Felix "
    "Gita Hana Ivo Jade Kofi Lena Mateo Nadia"
).split()
LAST_NAMES = (
    "Abbott Bauer Castillo Dlamini Eriksen Fontaine Gupta Hoffman Ibrahim Jansen Kowalski Larsen "
    "Mbeki Novak Ortega Petrov Quintero Rossi Suzuki Thorne Usman Varga Whitlock Xu Yilmaz Zubiri "
    "Acheampong Bianchi Cohen Duarte Engel Fraser Grant Hale Ito Jovanovic Khan Lund Moreau Nkosi"
).split()

BASE_MEMBERS = 280  # joined before the first day
JOINS_BEFORE_CASE_13 = 23  # so the planted second member is M-0304


def _members(rng: random.Random) -> list[dict]:
    c13 = planted.case(13)
    used_emails = {c13["first"]["email"]}
    stores = list(STORES)

    def person(number: int, joined_on: date) -> dict:
        while True:
            first, last = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
            email = f"{first}.{last}@example.com".lower()
            if email not in used_emails:
                break
        used_emails.add(email)
        return {
            "member_id": f"M-{number:04d}",
            "full_name": f"{first} {last}",
            "email": email,
            "phone": f"555-01{rng.randint(0, 99):02d}" if rng.random() < 0.6 else "",
            "home_store_id": rng.choice(stores),
            "joined_on": joined_on,
            "marketing_opt_in": rng.random() < 0.7,
        }

    members = []
    for number in range(1, BASE_MEMBERS + 1):
        joined_on = FIRST_DAY - timedelta(days=rng.randint(1, 540))
        members.append(person(number, joined_on))
    first = int(c13["first"]["member_id"][2:])
    members[first - 1].update(c13["first"], phone="555-0142", marketing_opt_in=True)

    # New members during the five weeks. The planted duplicate joins on its own day.
    days_before = [d for d in all_days() if d < c13["day"]]
    join_days = sorted(rng.choices(days_before, k=JOINS_BEFORE_CASE_13))
    number = BASE_MEMBERS
    for day in join_days:
        number += 1
        members.append(person(number, day))
    number += 1
    assert f"M-{number:04d}" == c13["second"]["member_id"]
    members.append(
        {**c13["second"], "phone": "", "joined_on": c13["day"], "marketing_opt_in": True}
    )
    for day in [d for d in all_days() if d > c13["day"]]:
        number += 1
        members.append(person(number, day))
    return members


# ---------------------------------------------------------------- receipts


def _ordinary_receipts(rng: random.Random, members: list[dict]) -> list[Receipt]:
    receipts: list[Receipt] = []
    by_store_day: dict[tuple[str, date], list[Receipt]] = {}
    for day in all_days():
        joined = [m["member_id"] for m in members if m["joined_on"] <= day]
        on_shelf = [p for p in PRODUCTS if on_sale(p, day)]
        for store_id, store in STORES.items():
            if not is_open(store_id, day):
                continue
            busy = WEEKDAY_FACTOR[day.weekday()] * (HOLIDAY_FACTOR if day == HOLIDAY else 1)
            count = round(34 * store[5] * busy * rng.uniform(0.9, 1.1))
            opens, closes = opening_hours(store_id, day)
            first = opens.hour * 60 + opens.minute + 5
            last = closes.hour * 60 + closes.minute - 5
            choices = [p for p in on_shelf if (store_id, day, p) not in planted.NO_ORDINARY_SALES]
            todays = []
            for _ in range(count):
                minute = rng.randint(first, last)
                sold_at = datetime.combine(day, time(minute // 60, minute % 60, rng.randint(0, 59)))
                lines = []
                products = rng.sample(choices, rng.choice([1, 2, 2, 3, 3, 4, 4, 5, 6]))
                for line_no, product_id in enumerate(products, start=1):
                    if PRODUCTS[product_id][2] == "kg":
                        quantity = Decimal(rng.randint(200, 2000)) / 1000
                    else:
                        quantity = Decimal(rng.choice([1, 1, 1, 2, 2, 3]))
                    lines.append(_line(line_no, product_id, quantity, price_on(product_id, day)))
                member_id = rng.choice(joined) if rng.random() < 0.35 else None
                todays.append(Receipt(store_id, rng.choice(TILLS), sold_at, lines, member_id))

            # About every second day, a customer brings one thing back from an earlier day.
            earlier = [
                r
                for back in (1, 2, 3)
                for r in by_store_day.get((store_id, day - timedelta(days=back)), [])
            ]
            if earlier and rng.random() < 0.5:
                original = rng.choice(earlier)
                line = rng.choice(original.lines)
                if PRODUCTS[line.product_id][2] == "each":
                    minute = rng.randint(first, last)
                    sold_at = datetime.combine(day, time(minute // 60, minute % 60, 30))
                    back_line = _line(1, line.product_id, -line.quantity, line.unit_price_cents)
                    todays.append(
                        Receipt(store_id, rng.choice(TILLS), sold_at, [back_line], None, original)
                    )
            by_store_day[(store_id, day)] = [r for r in todays if r.refund_of is None]
            receipts.extend(todays)
    return receipts


def _planted_receipts() -> list[Receipt]:
    made: dict[str, Receipt] = {}
    for p in planted.RECEIPTS:
        day = p["sold_at"].date()
        lines = []
        for line_no, (product_id, quantity) in enumerate(p["items"], start=1):
            price = price_on(product_id, day)
            if p.get("old_price") == product_id:
                price = PRODUCTS[product_id][3]  # the till still has yesterday's price
            lines.append(_line(line_no, product_id, Decimal(quantity), price))
        refund_of = made[p["refund_of"]] if "refund_of" in p else None
        made[p["key"]] = Receipt(
            p["store"], p["till"], p["sold_at"], lines, None, refund_of, key=p["key"]
        )
    return list(made.values())


def _number_the_receipts(receipts: list[Receipt]) -> None:
    """Each till numbers its own receipts, in the order it printed them."""
    receipts.sort(key=lambda r: (r.sold_at, r.store_id, r.till_id))
    counters: dict[tuple[str, str], int] = {}
    for receipt in receipts:
        till = (receipt.store_id, receipt.till_id)
        counters[till] = counters.get(till, FIRST_RECEIPT_NUMBER - 1) + 1
        receipt.receipt_id = f"{receipt.store_id}-{receipt.till_id}-{counters[till]:06d}"


# ---------------------------------------------------------------- stock and costs


def units_sold(receipts: list[Receipt]) -> dict[tuple[date, str, str], Decimal]:
    """(day, store, product) -> units really sold. A refund takes units away."""
    sold: dict[tuple[date, str, str], Decimal] = {}
    for receipt in receipts:
        for line in receipt.lines:
            key = (receipt.business_date, receipt.store_id, line.product_id)
            sold[key] = sold.get(key, Decimal(0)) + line.quantity
    return sold


def order_quantity(sold_yesterday: Decimal, stock_counted: Decimal) -> Decimal:
    """Lotus's replenishment rule: keep two days of sales on the shelf."""
    return max(Decimal(0), 2 * sold_yesterday - stock_counted)


def _stock(rng: random.Random, receipts: list[Receipt]) -> dict:
    """What is really on each shelf at closing time, every day."""
    sold = units_sold(receipts)
    stock = {}
    for store_id, store in STORES.items():
        for product_id, product in PRODUCTS.items():
            by_weight = product[2] == "kg"
            on_shelf = (
                Decimal(rng.randint(8000, 25000)) / 1000
                if by_weight
                else Decimal(rng.randint(10, 30))
            )
            delivery = Decimal(0)
            for day in all_days():
                if not on_sale(product_id, day):
                    break
                sold_today = sold.get((day, store_id, product_id), Decimal(0))
                on_shelf = max(on_shelf + delivery - sold_today, Decimal(0))
                delivery = order_quantity(sold_today, on_shelf)  # arrives tomorrow morning
                if not is_open(store_id, day):
                    continue  # nobody is there to count
                closes = opening_hours(store_id, day)[1]
                stock[(day, store_id, product_id)] = {
                    "counted_qty": on_shelf.quantize(THOUSANDTH),
                    "counted_by": "hand" if by_weight else "scanner",
                    "counted_at": datetime.combine(day, closes) - timedelta(minutes=45),
                }
    return stock


def _costs(rng: random.Random) -> dict:
    """What each supplier charges Lotus, in every weekly price file."""
    cost = {
        product_id: (Decimal(product[3]) * Decimal(rng.randint(55, 70)) / 10000).quantize(
            Decimal("0.01")
        )
        for product_id, product in PRODUCTS.items()
    }
    # A few costs really change during the five weeks. A change is not a fault.
    changes = {
        ("P-005", date(2026, 8, 10)): Decimal("0.05"),
        ("P-021", date(2026, 8, 11)): Decimal("0.20"),
        ("P-034", date(2026, 8, 14)): Decimal("-0.04"),
    }
    files = {}
    for day in all_days():
        for supplier_id, weekday in SUPPLIERS.items():
            if day.weekday() != weekday:
                continue
            for (product_id, change_day), step in changes.items():
                if change_day == day:
                    cost[product_id] += step
            files[(supplier_id, day)] = {
                p: cost[p] for p in PRODUCTS if PRODUCTS[p][4] == supplier_id and on_sale(p, day)
            }
    return files


# ---------------------------------------------------------------- the world


@lru_cache(maxsize=2)
def build(seed: int = SEED) -> World:
    """Everything that really happened. Read it, never change it."""
    members = _members(random.Random(f"{seed}-members"))
    receipts = _ordinary_receipts(random.Random(f"{seed}-receipts"), members)
    planted_receipts = _planted_receipts()
    receipts.extend(planted_receipts)
    _number_the_receipts(receipts)
    return World(
        receipts=receipts,
        members=members,
        stock=_stock(random.Random(f"{seed}-stock"), receipts),
        costs=_costs(random.Random(f"{seed}-costs")),
        planted={r.key: r for r in planted_receipts},
    )
