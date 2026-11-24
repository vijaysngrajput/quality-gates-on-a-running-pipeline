"""Profiling: reading a table before checking it."""

from datetime import date

import pytest

from lotus_pipeline import pipeline, profile
from lotus_pipeline.warehouse import connect

DAY = date(2026, 8, 24)


@pytest.fixture
def one_night(own_warehouse):
    pipeline.run(DAY, say=lambda text: None)
    with connect() as wh:
        yield wh


def test_the_profile_of_sales_lines(one_night):
    rows = {r["column"]: r for r in profile.profile_columns(one_night, "clean.sales_lines")}
    assert rows["line_id"]["empty_cells"] == 0
    assert rows["line_id"]["different_values"] == rows["line_id"]["rows"]  # unique, as check 3 says
    assert rows["store_id"]["different_values"] == 6
    assert rows["line_type"]["different_values"] == 2
    assert 60 < rows["member_id"]["empty_percent"] < 70  # most customers have no card: normal
    assert rows["sales_date"]["smallest"] == "2026-07-27"
    assert rows["sales_date"]["largest"] == "2026-08-24"


def test_profiling_finds_the_planted_refund_without_luck(one_night):
    """Read sales and refunds apart. A refund should never be above zero."""
    by_type = {r["line_type"]: r for r in profile.amounts_by_line_type(one_night)}
    assert by_type["sale"]["smallest_cents"] > 0
    assert by_type["refund"]["largest_cents"] == 949  # the surprise
    assert by_type["refund"]["largest_quantity"] < 0


def test_rows_per_store_per_day(one_night, key):
    rows = profile.rows_per_store_per_day(one_night, date(2026, 8, 17), DAY)
    assert len(rows) == 8 * 6  # eight days, six stores, no hole
    for r in rows:
        assert r["lines"] == key[(r["sales_date"], r["store_id"])]["lines"]


def test_no_row_is_lost_on_a_clean_night(one_night):
    assert profile.raw_against_clean(one_night)["lost_rows"] == 0


def test_the_join_loses_the_soup_lines(own_warehouse):
    pipeline.run(date(2026, 8, 29), say=lambda text: None)
    with connect() as wh:
        assert profile.raw_against_clean(wh)["lost_rows"] == 3  # case 6


def test_a_table_that_is_not_there(one_night):
    with pytest.raises(ValueError, match="Run the pipeline first"):
        profile.profile_columns(one_night, "clean.no_such_table")


def test_the_command_line(own_warehouse, capsys):
    pipeline.run(DAY, say=lambda text: None)
    assert pipeline.main(["profile", "--table", "clean.sales_lines"]) == 0
    assert "member_id" in capsys.readouterr().out
