"""The planted cases: hand-chosen, on known dates, so each one can be checked with a pencil.

Every case passes all five standard checks of the starter pipeline. That is the point.

Two kinds of thing are planted:

1. RECEIPTS that really happened, with hand-chosen items. They are in the till journal.
2. FAULTS: what goes wrong between the till and the file a store sends. They are not in
   the journal. The journal is the truth.

An item is (product_id, quantity as text). A negative quantity is a refund.
"""

from datetime import date, datetime


def D(month: int, day: int) -> date:
    return date(2026, month, day)


def T(month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(2026, month, day, hour, minute)


# Receipts that really happened. `key` is how the rest of this file names them.
RECEIPTS = [
    # Case 1. A customer buys olive oil on Saturday and brings it back on Monday.
    {
        "key": "c1_sale",
        "store": "S03",
        "till": "T2",
        "sold_at": T(8, 22, 16, 30),
        "items": [("P-024", "1"), ("P-008", "1")],
    },
    {
        "key": "c1_refund",
        "store": "S03",
        "till": "T1",
        "sold_at": T(8, 24, 11, 5),
        "items": [("P-024", "-1")],
        "refund_of": "c1_sale",
    },
    # Case 2. An ordinary receipt. The export program writes it twice.
    {
        "key": "c2",
        "store": "S01",
        "till": "T2",
        "sold_at": T(8, 25, 14, 12),
        "items": [("P-001", "2"), ("P-008", "1"), ("P-004", "1"), ("P-013", "0.640")],
    },
    # Case 3. Bananas, 1.180 kg. The export program writes 1180.
    {
        "key": "c3",
        "store": "S05",
        "till": "T1",
        "sold_at": T(8, 27, 10, 20),
        "items": [("P-012", "1.180"), ("P-001", "1")],
    },
    # Case 4. Butter is 399 from 08-28. Store S04 still charges 349: 12 packs on 6 receipts.
    {
        "key": "c4_1",
        "store": "S04",
        "till": "T1",
        "sold_at": T(8, 28, 8, 40),
        "items": [("P-003", "2"), ("P-009", "1")],
        "old_price": "P-003",
    },
    {
        "key": "c4_2",
        "store": "S04",
        "till": "T2",
        "sold_at": T(8, 28, 10, 15),
        "items": [("P-003", "2"), ("P-031", "1")],
        "old_price": "P-003",
    },
    {
        "key": "c4_3",
        "store": "S04",
        "till": "T1",
        "sold_at": T(8, 28, 12, 50),
        "items": [("P-003", "2"), ("P-006", "1")],
        "old_price": "P-003",
    },
    {
        "key": "c4_4",
        "store": "S04",
        "till": "T2",
        "sold_at": T(8, 28, 15, 5),
        "items": [("P-003", "2"), ("P-010", "1")],
        "old_price": "P-003",
    },
    {
        "key": "c4_5",
        "store": "S04",
        "till": "T1",
        "sold_at": T(8, 28, 17, 35),
        "items": [("P-003", "2"), ("P-001", "1")],
        "old_price": "P-003",
    },
    {
        "key": "c4_6",
        "store": "S04",
        "till": "T2",
        "sold_at": T(8, 28, 19, 20),
        "items": [("P-003", "2"), ("P-030", "1")],
        "old_price": "P-003",
    },
    # Case 5. Sold at 21:40 on Sunday. The till's clock is wrong after a power cut.
    {
        "key": "c5",
        "store": "S02",
        "till": "T2",
        "sold_at": T(8, 30, 21, 40),
        "items": [("P-028", "1"), ("P-002", "2"), ("P-006", "1"), ("P-030", "1")],
    },
    # Case 6. Three receipts with pumpkin soup, a product that is not in `products` yet.
    {
        "key": "c6_1",
        "store": "S06",
        "till": "T1",
        "sold_at": T(8, 29, 10, 15),
        "items": [("P-041", "1"), ("P-009", "1")],
    },
    {
        "key": "c6_2",
        "store": "S06",
        "till": "T2",
        "sold_at": T(8, 29, 13, 40),
        "items": [("P-041", "1"), ("P-035", "1")],
    },
    {
        "key": "c6_3",
        "store": "S06",
        "till": "T1",
        "sold_at": T(8, 29, 18, 5),
        "items": [("P-041", "1"), ("P-025", "2")],
    },
    # Case 12. Friday is a busy day for milk at store S05. That night, nobody counts the milk.
    {
        "key": "c12_1",
        "store": "S05",
        "till": "T1",
        "sold_at": T(8, 28, 9, 10),
        "items": [("P-001", "3"), ("P-009", "1")],
    },
    {
        "key": "c12_2",
        "store": "S05",
        "till": "T2",
        "sold_at": T(8, 28, 13, 25),
        "items": [("P-001", "3"), ("P-006", "1")],
    },
    {
        "key": "c12_3",
        "store": "S05",
        "till": "T1",
        "sold_at": T(8, 28, 17, 45),
        "items": [("P-001", "2"), ("P-029", "1")],
    },
    # Not a fault: a café really buys 400 bottles of water.
    {
        "key": "big_sale",
        "store": "S02",
        "till": "T1",
        "sold_at": T(8, 29, 9, 30),
        "items": [("P-032", "400")],
    },
]

# Store S04 sells butter only on the six planted receipts that day, so the count is exactly 12.
NO_ORDINARY_SALES = {("S04", D(8, 28), "P-003")}

# The faults. Each is one difference between the truth and the files.
CASES = [
    {
        "case": 1,
        "name": "A refund stored as a positive amount",
        "lens": "business",
        "day": D(8, 24),
        "store": "S03",
        "table": "sales_lines",
        "receipt": "c1_refund",
    },
    {
        "case": 2,
        "name": "One receipt in the file twice, with different line ids",
        "lens": "data",
        "day": D(8, 25),
        "store": "S01",
        "table": "sales_lines",
        "receipt": "c2",
    },
    {
        "case": 3,
        "name": "A weight entered in grams, not kilograms",
        "lens": "business",
        "day": D(8, 27),
        "store": "S05",
        "table": "sales_lines",
        "receipt": "c3",
        "product": "P-012",
    },
    {
        "case": 4,
        "name": "A price that was changed yesterday, charged at the old price",
        "lens": "business",
        "day": D(8, 28),
        "store": "S04",
        "table": "sales_lines",
        "product": "P-003",
    },
    {
        "case": 5,
        "name": "A sale at 03:10 in a store that closes at 22:00",
        "lens": "business",
        "day": D(8, 30),
        "store": "S02",
        "table": "sales_lines",
        "receipt": "c5",
        "stamped_at": T(8, 31, 3, 10),
    },
    {
        "case": 6,
        "name": "A sale for a product that is not in products",
        "lens": "data",
        "day": D(8, 29),
        "store": "S06",
        "table": "sales_lines",
        "product": "P-041",
    },
    {
        "case": 7,
        "name": "One store sends no file",
        "lens": "operational",
        "day": D(8, 25),
        "store": "S04",
        "table": "sales_lines",
    },
    {
        "case": 8,
        "name": "One store's file arrives a day late",
        "lens": "operational",
        "day": D(8, 27),
        "store": "S06",
        "table": "sales_lines",
        "arrives_at": T(8, 29, 0, 40),
    },
    {
        "case": 9,
        "name": "One store's whole file sent twice",
        "lens": "operational",
        "day": D(8, 28),
        "store": "S02",
        "table": "sales_lines",
    },
    {
        "case": 10,
        "name": "The supplier renames a column",
        "lens": "data",
        "day": D(8, 25),
        "supplier": "SUP-B",
        "table": "supplier_prices",
        "old_name": "unit_cost",
        "new_name": "cost_per_unit",
    },
    {
        "case": 11,
        "name": "The supplier sends cost in cents",
        "lens": "business",
        "day": D(8, 28),
        "supplier": "SUP-C",
        "table": "supplier_prices",
    },
    {
        "case": 12,
        "name": "A stock count is missing for one day",
        "lens": "data",
        "day": D(8, 28),
        "store": "S05",
        "table": "stock_counts",
        "product": "P-001",
    },
    {
        "case": 13,
        "name": "The same member twice, with different letter case in the email",
        "lens": "data",
        "day": D(8, 26),
        "table": "members",
        "first": {
            "member_id": "M-0117",
            "full_name": "Priya Nair",
            "email": "priya.nair@example.com",
            "home_store_id": "S03",
        },
        "second": {
            "member_id": "M-0304",
            "full_name": "Priya Nair",
            "email": "Priya.Nair@Example.com",
            "home_store_id": "S03",
        },
    },
]


def case(number: int) -> dict:
    return next(c for c in CASES if c["case"] == number)


# Things that look wrong and are not. No check may fire on these.
NOT_WRONG = [
    {
        "name": "A public holiday with low sales",
        "day": D(8, 26),
        "store": "all",
        "why": "Founders' Day. The stores open from 10:00 to 16:00",
    },
    {
        "name": "A store closed for one day",
        "day": D(8, 27),
        "store": "S01",
        "why": "A planned refit. No files, and that is correct",
    },
    {
        "name": "A real sale of 400 items",
        "day": D(8, 29),
        "store": "S02",
        "why": "A café buys 400 bottles of still water at $0.59",
        "receipt": "big_sale",
    },
]
