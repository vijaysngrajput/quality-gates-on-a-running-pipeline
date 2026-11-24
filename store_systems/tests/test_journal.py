"""The till journal: the truth. Every hand-chosen amount is checked here with plain numbers."""

from datetime import date
from decimal import Decimal

from store_systems import planted, truth
from store_systems.catalog import BONUS_WEEK_START, CLOSED, HOLIDAY, PRODUCTS, STORES, all_days
from store_systems.generate import build

WORLD = build()
SALES = truth.daily_store_sales(WORLD)


def test_the_same_seed_gives_the_same_world():
    build.cache_clear()
    again = build()
    assert [(r.receipt_id, r.total_cents) for r in again.receipts] == [
        (r.receipt_id, r.total_cents) for r in WORLD.receipts
    ]


def test_the_size_of_the_world():
    assert len(all_days()) == 35
    assert len({r.receipt_id for r in WORLD.receipts}) == len(WORLD.receipts)
    assert 20_000 < sum(len(r.lines) for r in WORLD.receipts) < 30_000


def test_every_line_adds_up():
    for receipt in WORLD.receipts:
        for line in receipt.lines:
            expected = (line.quantity * line.unit_price_cents).quantize(Decimal(1), "ROUND_HALF_UP")
            assert line.amount_cents == int(expected)
            assert (line.line_type == "refund") == (line.amount_cents < 0)


def test_a_refund_is_never_larger_than_its_sale():
    for receipt in WORLD.receipts:
        if receipt.refund_of:
            assert receipt.refund_of.sold_at < receipt.sold_at
            assert abs(receipt.total_cents) <= receipt.refund_of.total_cents


def test_planted_amounts_by_hand():
    p = WORLD.planted
    assert p["c1_refund"].total_cents == -949  # one olive oil, brought back
    assert p["c1_refund"].refund_of is p["c1_sale"]
    assert p["c2"].total_cents == 578 + 525 + 449 + 255 == 1807
    assert p["c3"].lines[0].amount_cents == 152  # 1.180 kg of bananas at 129
    assert p["c5"].total_cents == 899 + 698 + 519 + 279 == 2395
    assert p["big_sale"].total_cents == 400 * 59 == 23600
    assert [p[k].lines[0].unit_price_cents for k in ("c6_1", "c6_2", "c6_3")] == [599] * 3


def test_case_4_one_store_charges_the_old_butter_price():
    day = date(2026, 8, 28)
    butter = [
        (r.store_id, line)
        for r in WORLD.receipts
        if r.business_date == day
        for line in r.lines
        if line.product_id == "P-003"
    ]
    at_s04 = [line for store, line in butter if store == "S04"]
    assert sum(line.quantity for line in at_s04) == 12
    assert {line.unit_price_cents for line in at_s04} == {349}
    assert {line.unit_price_cents for store, line in butter if store != "S04"} == {399}


def test_things_that_look_wrong_and_are_not():
    assert all((HOLIDAY, s) in SALES for s in STORES)
    for store_id in STORES:  # the holiday is far below the same weekday a week earlier
        normal = SALES[(date(2026, 8, 19), store_id)]["net_sales_cents"]
        assert SALES[(HOLIDAY, store_id)]["net_sales_cents"] < 0.6 * normal
    for store_id, day in CLOSED:
        assert (day, store_id) not in SALES
        assert (day, store_id, "P-001") not in WORLD.stock


def test_a_normal_day_is_small_enough_to_add_up_by_hand():
    normal = [v["net_sales_cents"] for (d, _s), v in SALES.items() if d < BONUS_WEEK_START]
    assert 40_000 < min(normal) and max(normal) < 150_000


def test_the_bananas_in_grams_cost_more_than_the_stores_whole_day():
    case = planted.case(3)
    assert 1180 * 129 > SALES[(case["day"], case["store"])]["net_sales_cents"]


def test_case_13_the_same_person_twice():
    emails = [m["email"] for m in WORLD.members]
    assert len(set(emails)) == len(emails)  # every email is different, as typed
    assert len({e.lower() for e in emails}) == len(emails) - 1  # one person is there twice
    assert WORLD.members[116]["member_id"] == "M-0117"
    assert WORLD.members[303]["member_id"] == "M-0304"


def test_every_product_has_a_supplier_file_each_week():
    assert len(WORLD.costs) == 15  # three suppliers, five weeks
    priced = {p for costs in WORLD.costs.values() for p in costs}
    assert priced == set(PRODUCTS)
