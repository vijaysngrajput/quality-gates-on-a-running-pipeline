"""The checks with a reason.

Every check has two kinds of test:
  1. its fault is there, and the check returns exactly the bad rows;
  2. the data is clean, and the check stays silent.

"Clean" is four whole weeks, and it includes the three things that look wrong and are not.
"""

from datetime import date

import pytest

from lotus_pipeline import checks, inspect
from lotus_pipeline.inspect import ALL_CHECKS, BY_KEY
from lotus_pipeline.sales_checks import SALES_CHECKS
from lotus_pipeline.till_checks import TILL_CHECKS

D = date


def store_days(finding) -> set:
    return {(r["store_id"], str(r["sales_date"])) for r in finding.bad_rows}


def bad(wh, key: str) -> list[dict]:
    return inspect.run_one(wh, key).bad_rows


def the_five_standard_checks_pass(wh) -> bool:
    return all(r.passed for r in checks.run_checks(wh, D(2026, 8, 23)))


# ---------------------------------------------------------------- clean data: silence


@pytest.mark.parametrize("key", list(BY_KEY))
def test_every_check_is_silent_on_four_clean_weeks(lab, key):
    assert bad(lab, key) == []


def test_no_check_fires_on_the_things_that_look_wrong_and_are_not(week):
    found = week["findings"]
    holiday, refit = "2026-08-26", ("S01", "2026-08-27")
    for key, finding in found.items():
        assert holiday not in {day for _store, day in store_days(finding)}, key  # Founders' Day
        assert refit not in store_days(finding), key  # the planned refit
    assert not [r for r in found["believable_quantity"].bad_rows if r["quantity"] == 400]
    assert ("S02", "2026-08-29") not in store_days(found["normal_volume"])  # the 400 waters


# ---------------------------------------------------------------- the planted cases


def test_each_check_catches_exactly_its_planted_cases(week):
    found = {key: store_days(f) for key, f in week["findings"].items()}
    assert found == {
        "required_cells": set(),
        "true_key_unique": {("S01", "2026-08-25"), ("S02", "2026-08-28")},  # cases 2 and 9
        "right_shape": set(),
        "line_adds_up": {("S03", "2026-08-24")},  # case 1
        "every_sale_has_a_product": {("S06", "2026-08-29")},  # case 6
        "refund_gives_money_back": {("S03", "2026-08-24")},  # case 1
        "refund_not_larger_than_sale": set(),
        "price_of_the_day": {("S04", "2026-08-28")},  # case 4
        "inside_opening_hours": {("S02", "2026-08-31")},  # case 5
        "believable_quantity": {("S05", "2026-08-27")},  # case 3
        "till_total": {
            ("S03", "2026-08-24"), ("S01", "2026-08-25"), ("S04", "2026-08-25"),
            ("S05", "2026-08-27"), ("S02", "2026-08-28"), ("S06", "2026-08-29"),
            ("S02", "2026-08-30"), ("S02", "2026-08-31"),
        },  # cases 1, 2, 7, 3, 9, 6 and 5 twice
        "every_store_reported": {("S04", "2026-08-25")},  # case 7
        "normal_volume": {("S02", "2026-08-28")},  # case 9
        "one_total_per_till_per_day": set(),  # the tills' own totals are right all week
        "till_adds_up": set(),
        "every_till_reported": set(),
    }  # fmt: skip


def test_the_catches_named_on_each_check_are_the_truth(week):
    case_at = {1: ("S03", "2026-08-24"), 2: ("S01", "2026-08-25"), 3: ("S05", "2026-08-27"),
               4: ("S04", "2026-08-28"), 6: ("S06", "2026-08-29"), 7: ("S04", "2026-08-25"),
               9: ("S02", "2026-08-28")}  # fmt: skip
    for check in SALES_CHECKS:
        found = store_days(week["findings"][check.key])
        for case in check.catches:
            if case in case_at:
                assert case_at[case] in found, (check.key, case)


def test_the_amounts_the_checks_report(week, key):
    found = week["findings"]
    twice = [r for r in found["true_key_unique"].bad_rows if r["store_id"] == "S01"]
    assert sum(r["counted_too_much_cents"] for r in twice) == 1807  # case 2
    assert sum(r["store_short_cents"] for r in found["price_of_the_day"].bad_rows) == 600  # case 4
    assert sum(r["amount_cents"] for r in found["inside_opening_hours"].bad_rows) == 2395  # case 5
    assert sum(r["lost_cents"] for r in found["every_sale_has_a_product"].bad_rows) == 3 * 599
    grams = found["believable_quantity"].bad_rows[0]
    assert (grams["quantity"], grams["amount_cents"]) == (1180, 152220)  # case 3
    doubled = found["normal_volume"].bad_rows[0]
    assert doubled["percent_of_normal"] > 200  # case 9


def test_the_till_total_says_how_far_apart_the_two_counts_are(week, key):
    rows = week["findings"]["till_total"].bad_rows
    apart = {
        (r["store_id"], r["till_id"], str(r["sales_date"])): r["difference_cents"] for r in rows
    }
    assert apart[("S03", "T1", "2026-08-24")] == 1898  # case 1: twice the refund
    assert apart[("S01", "T2", "2026-08-25")] == 1807  # case 2
    assert apart[("S05", "T1", "2026-08-27")] == 152_068  # case 3
    assert apart[("S02", "T2", "2026-08-30")] == -2395  # case 5: missing on Sunday...
    assert apart[("S02", "T2", "2026-08-31")] == 2395  # ...and there on Monday, with no till
    assert apart[("S06", "T1", "2026-08-29")] + apart[("S06", "T2", "2026-08-29")] == -1797  # 6
    missing = key[(D(2026, 8, 25), "S04")]["net_sales_cents"]  # case 7: a till with no lines
    assert apart[("S04", "T1", "2026-08-25")] + apart[("S04", "T2", "2026-08-25")] == -missing
    doubled = key[(D(2026, 8, 28), "S02")]["net_sales_cents"]  # case 9
    assert apart[("S02", "T1", "2026-08-28")] + apart[("S02", "T2", "2026-08-28")] == doubled


def test_case_8_the_late_file_is_seen_on_its_night_and_gone_the_next(week):
    night = week["nights"][D(2026, 8, 27)]["findings"]["every_store_reported"]
    assert ("S06", "2026-08-27") in store_days(night)
    after = week["nights"][D(2026, 8, 28)]["findings"]["every_store_reported"]
    assert ("S06", "2026-08-27") not in store_days(after)


# ---------------------------------------------------------------- faults planted in the lab
# Three checks catch no standing case. Their fault is planted here, by hand.
# Each test also shows the five standard checks staying green on the same fault.

A_LINE = "S03-0805-000001"


def test_required_cells_an_empty_amount(lab):
    lab.execute("UPDATE clean.sales_lines SET amount_cents = NULL WHERE line_id = ?", (A_LINE,))
    assert [r["line_id"] for r in bad(lab, "required_cells")] == [A_LINE]
    assert the_five_standard_checks_pass(lab)


def test_required_cells_empty_in_which_rows(lab):
    refund = lab.value("SELECT min(line_id) FROM clean.sales_lines WHERE line_type = 'refund'")
    lab.execute(
        "UPDATE clean.sales_lines SET refund_of_receipt_id = NULL WHERE line_id = ?", (refund,)
    )
    lab.execute(
        "UPDATE clean.sales_lines SET refund_of_receipt_id = 'S03-T1-004001' WHERE line_id = ?",
        (A_LINE,),
    )
    assert {r["problem"] for r in bad(lab, "required_cells")} == {
        "a refund that names no receipt", "a sale that names a refunded receipt",
    }  # fmt: skip
    assert the_five_standard_checks_pass(lab)


@pytest.mark.parametrize("wrong", ["S03T1004493", "S04-T1-004493", "S03-T1-00449X", "S03-T1-4493"])
def test_right_shape(lab, wrong):
    lab.execute("UPDATE clean.sales_lines SET receipt_id = ? WHERE line_id = ?", (wrong, A_LINE))
    assert [r["line_id"] for r in bad(lab, "right_shape")] == [A_LINE]
    assert the_five_standard_checks_pass(lab)


def test_a_refund_larger_than_the_sale(lab):
    refund = lab.value("SELECT min(line_id) FROM clean.sales_lines WHERE line_type = 'refund'")
    lab.execute(
        "UPDATE clean.sales_lines SET amount_cents = amount_cents * 3 WHERE line_id = ?", (refund,)
    )
    assert [r["line_id"] for r in bad(lab, "refund_not_larger_than_sale")] == [refund]
    assert bad(lab, "refund_gives_money_back") == []  # the sign is right. Only the size is wrong
    assert the_five_standard_checks_pass(lab)


def test_a_refund_of_a_receipt_that_does_not_exist(lab):
    refund = lab.value("SELECT min(line_id) FROM clean.sales_lines WHERE line_type = 'refund'")
    lab.execute(
        "UPDATE clean.sales_lines SET refund_of_receipt_id = 'S01-T1-999999' WHERE line_id = ?",
        (refund,),
    )
    found = bad(lab, "refund_not_larger_than_sale")
    assert [(r["line_id"], r["sold_cents"]) for r in found] == [(refund, None)]


def test_half_a_bottle(lab):
    lab.execute(
        "UPDATE clean.sales_lines SET quantity = 1.5 WHERE line_id = ? AND unit = 'each'", (A_LINE,)
    )
    changed = lab.value(
        "SELECT count(*) FROM clean.sales_lines WHERE line_id = ? AND quantity = 1.5", (A_LINE,)
    )
    if changed:
        assert [r["line_id"] for r in bad(lab, "believable_quantity")] == [A_LINE]


def test_a_day_that_is_far_too_small(lab):
    """Half a file: the store's day has a third of its lines, and every other check is happy."""
    lab.execute(
        "DELETE FROM clean.sales_lines WHERE store_id = 'S03' AND sales_date = DATE '2026-08-21' "
        "AND line_no > 1"
    )
    found = bad(lab, "normal_volume")
    assert [(r["store_id"], str(r["sales_date"])) for r in found] == [("S03", "2026-08-21")]
    assert found[0]["percent_of_normal"] < 60
    assert bad(lab, "every_store_reported") == []  # the store did report. Just far too little
    assert the_five_standard_checks_pass(lab)


def test_a_sale_before_the_holiday_opening_hour(lab, landing):
    """On Founders' Day the stores open at 10:00. The calendar knows. The stores table does not."""
    from lotus_pipeline import pipeline

    lab.commit()
    pipeline.run(D(2026, 8, 26), say=lambda text: None)  # load up to the holiday
    line = lab.value(
        "SELECT min(line_id) FROM clean.sales_lines WHERE sales_date = DATE '2026-08-26'"
    )
    lab.execute(
        "UPDATE clean.sales_lines SET sold_at = TIMESTAMP '2026-08-26 09:00:00' WHERE line_id = ?",
        (line,),
    )
    assert [r["line_id"] for r in bad(lab, "inside_opening_hours")] == [line]


# ---------------------------------------------------------------- the checks themselves


def test_every_check_can_say_why_it_exists():
    assert [c.number for c in ALL_CHECKS] == list(range(1, 17))
    assert len(SALES_CHECKS) == 13 and len(TILL_CHECKS) == 3
    for check in ALL_CHECKS:
        assert check.lens in inspect.LENSES
        assert check.question in {
            "Complete?",
            "Unique?",
            "Valid?",
            "Accurate?",
            "Consistent?",
            "Fresh?",
        }
        assert check.protects and check.limit and check.prepare and check.key
    assert [c.lens for c in SALES_CHECKS].count("business") == 6


def test_the_five_standard_checks_cannot_say_why_they_exist():
    assert all(not c.protects for c in checks.STANDARD_CHECKS)


def test_the_report_and_the_command_line(lab, capsys):
    from lotus_pipeline import pipeline

    text = inspect.format_results(inspect.run_all(lab))
    assert (
        "0 of 16 checks found bad rows" in text
        and "13 on sales_lines, 3 on till_totals" in text
        and "Through the business lens" in text
    )
    lab.commit()
    assert pipeline.main(["inspect", "--check", "till_total"]) == 0
    assert "The check passes." in capsys.readouterr().out
    assert pipeline.main(["inspect", "--check", "no_such_check"]) == 1
    assert "There is no check 'no_such_check'" in capsys.readouterr().out


# ---------------------------------------------------------------- the control table itself


def test_a_till_total_stored_twice(lab):
    lab.execute(
        "INSERT INTO clean.till_totals SELECT * FROM clean.till_totals "
        "WHERE store_id = 'S03' AND till_id = 'T1' AND business_date = DATE '2026-08-05'"
    )
    assert [r["times"] for r in bad(lab, "one_total_per_till_per_day")] == [2]
    assert the_five_standard_checks_pass(lab)


def test_a_till_total_that_does_not_add_up(lab):
    lab.execute(
        "UPDATE clean.till_totals SET net_total_cents = gross_sales_cents "
        "WHERE store_id = 'S03' AND till_id = 'T1' AND business_date = DATE '2026-08-05' "
        "AND refunds_cents > 0"
    )
    lab.execute(
        "UPDATE clean.till_totals SET refunds_cents = -100, net_total_cents = gross_sales_cents + 100 "
        "WHERE store_id = 'S04' AND till_id = 'T2' AND business_date = DATE '2026-08-05'"
    )
    assert ("S04", "2026-08-05") in {
        (r["store_id"], str(r["sales_date"])) for r in bad(lab, "till_adds_up")
    }


def test_a_till_that_sent_no_total(lab):
    lab.execute(
        "DELETE FROM clean.till_totals "
        "WHERE store_id = 'S05' AND till_id = 'T2' AND business_date = DATE '2026-08-05'"
    )
    found = bad(lab, "every_till_reported")
    assert [(r["store_id"], str(r["sales_date"]), r["tills_reported"]) for r in found] == [
        ("S05", "2026-08-05", 1)
    ]
    # The money check notices too: lines with no till to compare with also fail.
    assert [(r["till_id"], r["till_cents"]) for r in bad(lab, "till_total")] == [("T2", None)]
