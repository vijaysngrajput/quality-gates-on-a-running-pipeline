"""The gates, run for the whole bonus week, held up against the answer key.

Test the tests: every planted fault is caught by its gate, the right thing happens to it,
and nothing that is not wrong is touched.
"""

from datetime import date

import pytest

from conftest import WEEK, plant, rows, snapshot
from lotus_pipeline import pipeline, report
from lotus_pipeline.gates import GateBlocked
from lotus_pipeline.warehouse import connect
from store_systems import truth
from store_systems.catalog import BONUS_WEEK_START, PRODUCTS, STORES

D = date
MON, TUE, WED, THU, FRI, SAT, SUN = WEEK


def failed(night: dict) -> dict[str, int]:
    return {name: r["bad_rows"] for name, r in night["gates"].items() if r["status"] != "pass"}


# ---------------------------------------------------------------- clean data: nothing happens


def test_four_clean_weeks_pass_every_gate_and_every_store_can_be_paid(clean_weeks):
    night = clean_weeks["night"]
    assert failed(night) == {}
    assert len(night["gates"]) == 47
    assert all(s["pay"] for s in night["report"]["stores"])
    assert all(not found for found in night["set_aside"].values()) and night["refused"] == []


def test_nothing_that_is_not_wrong_is_touched(week):
    last = week["last"]
    for store in STORES:  # the public holiday
        assert last["status"][("2026-08-26", store)]["status"] == "ok"
    assert ("2026-08-27", "S01") not in last["status"]  # the planned refit: no day is expected
    assert last["status"][("2026-08-29", "S02")]["status"] == "ok"  # the real sale of 400 bottles
    aside = {r["file_day"] for r in last["set_aside"]["sales_lines"]}
    assert "2026-08-26" not in aside


# ---------------------------------------------------------------- the answer to the Monday question


def test_the_week_night_by_night(week):
    can_be_paid = [sum(s["pay"] for s in week["nights"][day]["report"]["stores"]) for day in WEEK]
    assert can_be_paid == [5, 4, 4, 2, 3, 2, 1]


def test_on_monday_one_store_can_be_paid_and_five_cannot_each_with_its_reason(week):
    stores = {s["store_id"]: s for s in week["last"]["report"]["stores"]}
    assert [s for s in stores if stores[s]["pay"]] == ["S01"]
    why = {s: stores[s]["why_not"] for s in stores if not stores[s]["pay"]}
    assert why == {
        "S02": ["Sun 08-30: 4 rows set aside ($23.95): Nothing is sold while the store is closed"],
        "S03": ["Mon 08-24: 1 row set aside ($9.49): A line adds up; A refund gives money back"],
        "S04": ["Tue 08-25: no sales file arrived. The tills counted $587.42"],
        "S05": ["Thu 08-27: 1 row set aside ($1,522.20): A weight is a believable weight"],
        "S06": ["Sat 08-29: 3 rows set aside ($17.97): Every sale is for a product that exists"],
    }


def test_what_is_published_is_the_truth_less_what_was_set_aside(week, world):
    right = truth.week_net_sales(world, BONUS_WEEK_START)
    got = {s["store_id"]: s["net_sales_cents"] for s in week["last"]["report"]["stores"]}
    day_7 = truth.daily_store_sales(world)[(TUE, "S04")]["net_sales_cents"]
    assert {store: got[store] - right[store] for store in STORES} == {
        "S01": 0,  # the extra copies were removed. The number is right, and it can be paid
        "S02": -2395,  # the receipt with the wrong clock is set aside. The doubled file is repaired
        "S03": 949,  # the bad refund is set aside, so it is not taken away yet
        "S04": -day_7,  # a day with no file
        "S05": -152,  # the bananas are set aside: $1.52 of real sales, not $1,522.20
        "S06": -1797,  # the three lines with no product
    }


# ---------------------------------------------------------------- each planted case, at its gate


def test_case_1_the_refund_is_set_aside_on_the_first_night(week, world):
    night = week["nights"][MON]
    assert failed(night) == {
        "line_adds_up": 1, "refund_gives_money_back": 1, "store_is_not_ready_to_pay": 1,
    }  # fmt: skip
    line = [r for r in night["set_aside"]["sales_lines"]]
    assert {(r["store_id"], r["file_day"]) for r in line} == {("S03", "2026-08-24")}
    assert len({r["line_id"] for r in line}) == 1 and len(line) == 2  # one row, two reasons
    assert night["status"][("2026-08-24", "S03")]["problem"] == "rows_set_aside"


def test_case_2_and_9_extra_copies_are_removed_and_the_day_is_right_again(week, key):
    last = week["last"]
    copies = [r for r in last["set_aside"]["sales_lines"] if r["check_key"] == "true_key_unique"]
    per_day = {}
    for r in copies:
        per_day[(r["store_id"], r["file_day"])] = per_day.get((r["store_id"], r["file_day"]), 0) + 1
    assert per_day == {("S01", "2026-08-25"): 4, ("S02", "2026-08-28"): 167}
    for day, store in ((TUE, "S01"), (FRI, "S02")):
        published = last["sales"][(day.isoformat(), store)]["net_sales_cents"]
        assert published == key[(day, store)]["net_sales_cents"]
        assert last["status"][(day.isoformat(), store)]["status"] == "ok"


def test_case_3_5_and_6_are_set_aside_with_their_reason(week):
    last = week["last"]
    by_check = {}
    for r in last["set_aside"]["sales_lines"]:
        by_check.setdefault(r["check_key"], set()).add((r["store_id"], r["file_day"]))
    assert by_check["believable_quantity"] == {("S05", "2026-08-27")}
    assert by_check["inside_opening_hours"] == {("S02", "2026-08-30")}
    soup = last["set_aside"]["raw_sales_lines"]
    assert [(r["store_id"], r["file_day"], r["product_id"], r["amount_cents"]) for r in soup] == [
        ("S06", "2026-08-29", "P-041", 599)
    ] * 3


def test_case_4_is_a_warning_and_the_store_can_still_be_paid_for_that_day(week):
    night = week["nights"][FRI]
    assert night["gates"]["price_of_the_day"]["bad_rows"] == 6
    assert night["gates"]["price_of_the_day"]["action"] == "warn"
    assert night["status"][("2026-08-28", "S04")]["status"] == "ok"


def test_case_7_no_file_and_case_8_a_late_file(week):
    assert week["last"]["status"][("2026-08-25", "S04")]["problem"] == "no_sales_file"
    assert week["nights"][THU]["status"][("2026-08-27", "S06")]["problem"] == "no_sales_file"
    assert week["nights"][FRI]["status"][("2026-08-27", "S06")]["status"] == "ok"  # it arrived


def test_case_10_and_11_two_supplier_files_are_refused_and_the_last_good_cost_is_used(week, world):
    assert week["nights"][MON]["refused"] == []
    refused = {r["file_name"]: r for r in week["last"]["refused"]}
    assert sorted(refused) == [
        "supplier_prices_SUP-B_2026-08-25.csv", "supplier_prices_SUP-C_2026-08-28.csv",
    ]  # fmt: skip
    assert "unit_cost" in refused["supplier_prices_SUP-B_2026-08-25.csv"]["reason"]
    assert "0 decimals" in refused["supplier_prices_SUP-C_2026-08-28.csv"]["reason"]
    for day in (TUE, FRI, SUN):  # every order has a cost, and it is the true one
        costs = truth.true_costs(world, day)
        for (store, product), order in week["nights"][day]["orders"].items():
            assert order["unit_cost_cents"] == int(costs[product] * 100), (day, product)
    assert set(PRODUCTS) - {"P-011"} == {p for _s, p in week["last"]["orders"]}


def test_case_12_a_missing_count_is_not_read_as_an_empty_shelf(week):
    night = week["nights"][FRI]
    order = night["orders"][("S05", "P-001")]
    assert (order["stock_counted"], order["order_qty"]) == (None, None)
    assert order["note"] == "no count: order by hand"
    assert night["gates"]["a_count_for_every_day"]["bad_rows"] == 1
    assert night["gates"]["order_without_a_count"]["bad_rows"] == 1
    others = [o for where, o in night["orders"].items() if where != ("S05", "P-001")]
    assert all(o["note"] is None and o["order_qty"] is not None for o in others)


def test_case_13_marketing_gets_one_member_and_no_personal_data_is_set_aside(week):
    assert week["nights"][TUE]["set_aside"]["members"] == []
    aside = week["last"]["set_aside"]["members"]
    assert [r["member_id"] for r in aside] == ["M-0304"]
    assert set(aside[0]) == {
        "member_id",
        "check_key",
        "reason",
        "set_aside_on",
    }  # no name, no email
    members = {r["member_id"] for r in week["last"]["members"]}
    assert "M-0117" in members and "M-0304" not in members


# ---------------------------------------------------------------- block, in the lab


def run_from(step: str, day: date) -> None:
    steps = pipeline.STEPS[pipeline.STEPS.index(step) :]
    pipeline.run(day, steps, say=lambda text: None)


def published() -> list[dict]:
    return rows("SELECT * FROM output.daily_store_sales ORDER BY sales_date, store_id")


def test_a_broken_reference_table_blocks_and_the_outputs_keep_their_last_good_numbers(lab):
    before = published()
    plant("DELETE FROM raw.stores WHERE store_id = 'S06' "
          "AND _file_name = (SELECT max(_file_name) FROM raw.stores)")  # fmt: skip
    with pytest.raises(GateBlocked) as blocked:
        run_from("clean", lab)
    assert "BLOCK" in str(blocked.value) and "There are six stores, each once" in str(blocked.value)
    assert published() == before  # publish did not run
    with connect() as wh:  # and the report still lists all six stores, with their old numbers
        assert len(report.bonus_report(wh, lab)["stores"]) == 6
    assert snapshot(lab)["gates"]["six_stores_once"]["status"] == "fail"


def test_two_prices_on_one_day_block(lab):
    plant(
        "INSERT INTO raw.products SELECT product_id, product_name, category, unit, '319', "
        "'2026-08-01', NULL, status, _file_name, _received_at, _loaded_at FROM raw.products "
        "WHERE product_id = 'P-001' AND _file_name = (SELECT max(_file_name) FROM raw.products)"
    )
    with pytest.raises(GateBlocked) as blocked:
        run_from("clean", lab)
    assert "A product has one price on any day" in str(blocked.value)


def test_a_difference_nobody_can_explain_blocks_at_the_publish_gate(lab):
    """A valid row with a wrong quantity: every row check passes, and the tills disagree."""
    before = published()
    plant(
        "UPDATE raw.sales_lines SET quantity = '2.000', "
        "amount_cents = CAST(2 * CAST(unit_price_cents AS INTEGER) AS TEXT) "
        "WHERE line_id = (SELECT min(line_id) FROM raw.sales_lines WHERE quantity = '1.000' "
        "AND line_type = 'sale' AND _file_name = 'sales_lines_S03_2026-08-20.csv')"
    )
    with pytest.raises(GateBlocked) as blocked:
        run_from("clean", lab)
    assert "or we can say why not" in str(blocked.value)
    gates = snapshot(lab)["gates"]
    assert gates["unexplained_difference"]["bad_rows"] == 1
    assert [g for g in gates.values() if g["gate"] == "clean" and g["status"] != "pass"] == []
    assert published() == before


def test_a_file_with_a_missing_column_is_refused_and_its_store_cannot_be_paid(lab):
    plant("DELETE FROM raw.file_columns WHERE file_name = 'sales_lines_S03_2026-08-20.csv' "
          "AND column_name = 'amount_cents'")  # fmt: skip
    run_from("gate-landing", lab)
    night = snapshot(lab)
    assert [r["file_name"] for r in night["refused"]] == ["sales_lines_S03_2026-08-20.csv"]
    status = night["status"][("2026-08-20", "S03")]
    assert (status["problem"], status["lines_cents"]) == ("file_refused", 0)
    assert "amount_cents" in status["reasons"]
    assert sum(s["status"] != "ok" for s in night["status"].values()) == 1  # the other stores go on


def test_more_than_five_percent_bad_rows_and_the_whole_file_is_refused(lab):
    plant(
        "UPDATE raw.sales_lines SET quantity = '-' || quantity "
        "WHERE _file_name = 'sales_lines_S03_2026-08-20.csv' AND line_type = 'sale' AND line_no = 1"
    )
    run_from("clean", lab)
    night = snapshot(lab)
    aside = [r for r in night["set_aside"]["sales_lines"] if r["file_day"] == "2026-08-20"]
    total = rows(
        "SELECT count(*) AS n FROM raw.sales_lines WHERE _file_name = 'sales_lines_S03_2026-08-20.csv'"
    )
    assert len({r["line_id"] for r in aside}) == total[0]["n"]  # every line of the file
    assert "too_many_bad_rows" in {r["check_key"] for r in aside}
    assert rows("SELECT count(*) AS n FROM clean.sales_lines WHERE store_id = 'S03' "
                "AND file_day = DATE '2026-08-20'")[0]["n"] == 0  # fmt: skip
    assert night["status"][("2026-08-20", "S03")]["status"] == "do not pay yet"


def test_a_few_bad_rows_do_not_refuse_the_file(lab):
    plant(
        "UPDATE raw.sales_lines SET quantity = '-' || quantity WHERE line_id = "
        "(SELECT min(line_id) FROM raw.sales_lines WHERE _file_name = 'sales_lines_S03_2026-08-20.csv' "
        "AND line_type = 'sale')"
    )
    run_from("clean", lab)
    aside = snapshot(lab)["set_aside"]["sales_lines"]
    assert len({r["line_id"] for r in aside}) == 1


def test_running_a_night_again_changes_nothing(week):
    before = week["last"]
    pipeline.run(SUN, say=lambda text: None)
    again = snapshot(SUN)
    assert again["sales"] == before["sales"] and again["status"] == before["status"]
    assert again["report"] == before["report"]
