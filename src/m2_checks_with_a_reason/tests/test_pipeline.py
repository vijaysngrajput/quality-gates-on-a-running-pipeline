"""The starter pipeline, run for the whole bonus week, held up against the answer key.

This file proves the sentence the project is built on:
all five checks are green on all seven nights, and all six stores are wrong.
"""

from datetime import date
from decimal import Decimal

from lotus_pipeline import pipeline, report
from lotus_pipeline.warehouse import connect
from store_systems import planted, truth
from store_systems.catalog import BONUS_WEEK_END, BONUS_WEEK_START, PRODUCTS, STORES

D = date
LAST = BONUS_WEEK_END


def net(night: dict, day: date, store: str) -> int | None:
    row = night["sales"].get((day, store))
    return row["net_sales_cents"] if row else None


def true_net(key: dict, day: date, store: str) -> int:
    return key[(day, store)]["net_sales_cents"]


def test_every_planted_case_passes_all_five_checks(week):
    for day, night in week["nights"].items():
        assert [c["passed"] for c in night["checks"]] == [True] * 5, f"a check failed on {day}"


def test_the_four_clean_weeks_equal_the_answer_key(week, key):
    last_night = week["nights"][LAST]["sales"]
    clean_days = {k: v for k, v in key.items() if k[0] < BONUS_WEEK_START}
    assert len(clean_days) == 28 * 6
    for (day, store), truth_row in clean_days.items():
        assert last_night[(day, store)]["net_sales_cents"] == truth_row["net_sales_cents"]
        assert last_night[(day, store)]["receipts"] == truth_row["receipts"]
        assert last_night[(day, store)]["lines"] == truth_row["lines"]


def test_case_1_refund_as_a_positive_amount(week, key):
    night = week["nights"][D(2026, 8, 24)]
    assert net(night, D(2026, 8, 24), "S03") - true_net(key, D(2026, 8, 24), "S03") == 2 * 949
    for store in set(STORES) - {"S03"}:  # the only wrong number of that night
        assert net(night, D(2026, 8, 24), store) == true_net(key, D(2026, 8, 24), store)


def test_case_2_one_receipt_twice(week, key):
    night = week["nights"][LAST]
    assert net(night, D(2026, 8, 25), "S01") - true_net(key, D(2026, 8, 25), "S01") == 1807


def test_case_3_grams(week, key):
    night = week["nights"][LAST]
    assert net(night, D(2026, 8, 27), "S05") - true_net(key, D(2026, 8, 27), "S05") == 152_068


def test_case_4_the_old_price_is_not_an_error_in_the_table(week, key):
    night = week["nights"][LAST]
    assert net(night, D(2026, 8, 28), "S04") == true_net(key, D(2026, 8, 28), "S04")


def test_case_5_the_sale_lands_on_monday(week, key):
    night = week["nights"][LAST]
    assert net(night, D(2026, 8, 30), "S02") - true_net(key, D(2026, 8, 30), "S02") == -2395
    assert net(night, D(2026, 8, 31), "S02") == 2395  # a day that has not happened yet


def test_case_6_the_join_drops_the_soup(week, key):
    night = week["nights"][LAST]
    assert net(night, D(2026, 8, 29), "S06") - true_net(key, D(2026, 8, 29), "S06") == -3 * 599


def test_case_7_no_file_no_row_no_failure(week, key):
    assert net(week["nights"][LAST], D(2026, 8, 25), "S04") is None
    assert true_net(key, D(2026, 8, 25), "S04") > 0


def test_case_8_the_late_file_heals_one_night_later(week, key):
    day = D(2026, 8, 27)
    assert net(week["nights"][day], day, "S06") is None
    assert net(week["nights"][D(2026, 8, 28)], day, "S06") == true_net(key, day, "S06")


def test_case_9_the_whole_file_twice(week, key):
    day = D(2026, 8, 28)
    assert net(week["nights"][LAST], day, "S02") == 2 * true_net(key, day, "S02")


def test_things_that_look_wrong_and_are_not_are_right(week, key):
    night = week["nights"][LAST]
    for store in STORES:  # the holiday
        assert net(night, D(2026, 8, 26), store) == true_net(key, D(2026, 8, 26), store)
    assert net(night, D(2026, 8, 27), "S01") is None  # the planned refit
    assert net(night, D(2026, 8, 29), "S02") == true_net(key, D(2026, 8, 29), "S02")  # 400 waters


def test_all_six_stores_are_wrong_at_the_end_of_the_week(week, world):
    truth_week = truth.week_net_sales(world, BONUS_WEEK_START)
    with connect() as wh:
        bonus = report.bonus_report(wh, LAST)
    got = {s["store_id"]: s["net_sales_cents"] for s in bonus["stores"]}
    errors = {store: got[store] - truth_week[store] for store in STORES}
    day_7 = truth.daily_store_sales(world)[(D(2026, 8, 25), "S04")]["net_sales_cents"]
    day_9 = truth.daily_store_sales(world)[(D(2026, 8, 28), "S02")]["net_sales_cents"]
    assert errors == {
        "S01": 1807,
        "S02": day_9 - 2395,
        "S03": 1898,
        "S04": -day_7,
        "S05": 152_068,
        "S06": -1797,
    }
    assert bonus["checks_passed"] == bonus["checks_total"] == 5


def test_the_order_list_of_a_clean_night_equals_the_answer_key(week, world):
    day = D(2026, 8, 24)
    orders = week["nights"][day]["orders"]
    expected = truth.replenishment_list(world, day)
    assert len(orders) == len(expected) == 6 * 39  # 40 products, less the retired bagels
    for where, row in expected.items():
        assert orders[where]["order_qty"] == row["order_qty"], where
        assert orders[where]["unit_cost_cents"] == row["unit_cost_cents"], where


def test_case_10_the_cost_column_is_empty(week):
    orders = week["nights"][D(2026, 8, 25)]["orders"]
    sup_b = [p for p in PRODUCTS if PRODUCTS[p][4] == "SUP-B"]
    assert all(orders[("S03", p)]["unit_cost_cents"] is None for p in sup_b)
    assert all(orders[("S03", p)]["order_cost_cents"] is None for p in sup_b)
    assert orders[("S03", "P-001")]["unit_cost_cents"] is not None


def test_case_11_cost_is_100_times_too_high(week, world):
    day = D(2026, 8, 28)
    orders = week["nights"][day]["orders"]
    costs = truth.true_costs(world, day)
    sup_c = [p for p in PRODUCTS if PRODUCTS[p][4] == "SUP-C"]
    for product in sup_c:
        assert orders[("S01", product)]["unit_cost_cents"] == int(costs[product] * 100) * 100


def test_case_12_a_missing_count_is_read_as_zero(week, world):
    case = planted.case(12)
    order = week["nights"][case["day"]]["orders"][(case["store"], case["product"])]
    right = truth.replenishment_list(world, case["day"])[(case["store"], case["product"])]
    assert order["stock_counted"] == 0 and right["stock_counted"] == 3
    assert (order["order_qty"], right["order_qty"]) == (Decimal(18), Decimal(15))


def test_case_13_both_members_are_in_the_clean_table(week):
    with connect() as wh:
        twice = wh.query(
            "SELECT member_id FROM clean.members WHERE lower(email) = ? ORDER BY 1",
            ("priya.nair@example.com",),
        )
        ids = wh.value("SELECT count(DISTINCT member_id) FROM clean.members")
        rows = wh.value("SELECT count(*) FROM clean.members")
    assert [r["member_id"] for r in twice] == ["M-0117", "M-0304"]
    assert ids == rows  # the key is unique, and the person is still there twice


def test_running_a_night_again_changes_nothing(week):
    before = week["nights"][LAST]["sales"]
    said = []
    pipeline.run(LAST, say=said.append)
    assert said[0].startswith("load:    0 new files, 0 rows")
    with connect() as wh:
        after = wh.query(
            "SELECT sales_date, store_id, net_sales_cents FROM output.daily_store_sales"
        )
        lists = wh.value("SELECT count(DISTINCT for_date) FROM output.replenishment_list")
    assert {(r["sales_date"], r["store_id"]): r["net_sales_cents"] for r in after} == {
        k: v["net_sales_cents"] for k, v in before.items()
    }
    assert lists == 7  # one order list per night, never two
