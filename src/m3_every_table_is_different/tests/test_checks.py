"""Each standard check has two tests: one where its fault is planted and it fails,
and one on clean data where it stays silent."""

from datetime import date

import pytest

from lotus_pipeline import checks, pipeline

DAY = date(2026, 8, 24)
CHECK = {c.number: c for c in checks.STANDARD_CHECKS}

GOOD_LINES = [
    # line_id, receipt_id, store_id, sales_date, amount_cents, line_type
    ("L1", "R1", "S03", DAY, 949, "sale"),
    ("L2", "R1", "S03", DAY, 525, "sale"),
    ("L3", "R2", "S03", DAY, -525, "refund"),
    ("L4", "R3", "S04", DAY, 289, "sale"),
]


def make_sales_lines(wh, rows):
    wh.execute("DROP TABLE IF EXISTS clean.sales_lines")
    wh.execute(
        "CREATE TABLE clean.sales_lines (line_id TEXT, receipt_id TEXT, store_id TEXT, "
        "sales_date DATE, amount_cents BIGINT, line_type TEXT)"
    )
    for row in rows:
        wh.execute("INSERT INTO clean.sales_lines VALUES (?, ?, ?, ?, ?, ?)", row)


def bad_rows(wh, number, rows):
    make_sales_lines(wh, rows)
    return checks.run_check(wh, CHECK[number], DAY).bad_rows


@pytest.mark.parametrize("number", [1, 2, 3, 4, 5])
def test_every_check_is_silent_on_clean_data(empty_warehouse, number):
    assert bad_rows(empty_warehouse, number, GOOD_LINES) == []


def test_check_1_fails_when_the_day_has_no_lines(empty_warehouse):
    other_day = [(i, r, s, date(2026, 8, 23), a, t) for i, r, s, _d, a, t in GOOD_LINES]
    assert len(bad_rows(empty_warehouse, 1, other_day)) == 1


def test_check_2_fails_on_a_null_key(empty_warehouse):
    rows = GOOD_LINES + [(None, "R4", "S04", DAY, 100, "sale")]
    assert len(bad_rows(empty_warehouse, 2, rows)) == 1


def test_check_3_fails_on_a_key_stored_twice(empty_warehouse):
    rows = GOOD_LINES + [("L1", "R1", "S03", DAY, 949, "sale")]
    assert bad_rows(empty_warehouse, 3, rows) == [{"line_id": "L1", "times": 2}]


def test_check_4_fails_on_a_status_that_is_not_allowed(empty_warehouse):
    rows = GOOD_LINES + [("L5", "R4", "S04", DAY, 100, "void")]
    assert len(bad_rows(empty_warehouse, 4, rows)) == 1


def test_check_5_fails_when_a_stores_day_is_not_above_zero(empty_warehouse):
    rows = GOOD_LINES + [("L5", "R4", "S04", DAY, -289, "refund")]
    assert [r["store_id"] for r in bad_rows(empty_warehouse, 5, rows)] == ["S04"]


def test_what_the_five_checks_do_not_catch(empty_warehouse):
    """The planted refund: a refund with a positive amount. All five stay silent."""
    rows = GOOD_LINES + [("L5", "R4", "S03", DAY, 949, "refund")]
    make_sales_lines(empty_warehouse, rows)
    results = checks.run_checks(empty_warehouse, DAY)
    assert all(r.passed for r in results)
    assert "5 of 5 checks passed." in checks.format_results(results)


def test_a_failed_check_stops_the_run_before_publish(empty_warehouse, monkeypatch):
    make_sales_lines(empty_warehouse, GOOD_LINES + [("L1", "R1", "S03", DAY, 949, "sale")])
    with pytest.raises(pipeline.ChecksFailed) as failed:
        pipeline.step_check(empty_warehouse, DAY)
    assert "FAIL (1 bad rows)" in str(failed.value)
