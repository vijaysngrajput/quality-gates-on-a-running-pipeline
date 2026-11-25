"""The checks for products, stores, stock counts, supplier prices and members.

Every check has two kinds of test: its fault is there and it returns exactly the bad rows,
and the data is clean and it stays silent. Four planted cases live in these tables
(10, 11, 12 and 13). Every other fault is planted here, on a private copy.
"""

from datetime import date

import pytest

from lotus_pipeline import checks, inspect
from lotus_pipeline.other_checks import (
    MEMBER_CHECKS,
    OTHER_CHECKS,
    PERSONAL_COLUMNS,
    PRODUCT_CHECKS,
    STOCK_CHECKS,
    STORE_CHECKS,
    SUPPLIER_CHECKS,
)

KEYS = [c.key for c in OTHER_CHECKS]


def bad(wh, key: str) -> list[dict]:
    return inspect.run_one(wh, key).bad_rows


def things(wh, key: str) -> list[str]:
    return sorted(str(r.get("thing", r.get("store_id"))) for r in bad(wh, key))


def the_five_standard_checks_pass(wh) -> bool:
    return all(r.passed for r in checks.run_checks(wh, date(2026, 8, 23)))


# ---------------------------------------------------------------- clean data: silence


@pytest.mark.parametrize("key", KEYS)
def test_every_check_is_silent_on_four_clean_weeks(lab, key):
    assert bad(lab, key) == []


def test_there_are_21_checks_on_five_tables():
    sizes = [
        len(c) for c in (PRODUCT_CHECKS, STORE_CHECKS, STOCK_CHECKS, SUPPLIER_CHECKS, MEMBER_CHECKS)
    ]
    assert sizes == [5, 4, 3, 5, 4]
    assert [c.number for c in OTHER_CHECKS] == list(range(17, 38))
    for check in OTHER_CHECKS:
        assert check.protects and check.limit and check.prepare and check.family
        assert check.lens in inspect.LENSES


# ---------------------------------------------------------------- the four planted cases


def test_the_bonus_week_exactly_the_planted_cases_and_nothing_else(week):
    found = {key: f.bad_rows for key, f in week["findings"].items() if key in KEYS}
    silent = [key for key, rows in found.items() if not rows]
    assert sorted(k for k in found if k not in silent) == sorted(
        ["a_count_for_every_day", "files_have_their_columns", "cost_is_filled",
         "cost_near_the_last_file", "cost_below_the_shelf_price", "one_person_once"]
    )  # fmt: skip


def test_case_10_the_renamed_column(week):
    rows = week["findings"]["files_have_their_columns"].bad_rows
    assert sorted((r["thing"], r["column_name"], r["kind_of_change"]) for r in rows) == [
        ("supplier_prices_SUP-B_2026-08-25.csv", "cost_per_unit", "additive"),
        ("supplier_prices_SUP-B_2026-08-25.csv", "unit_cost", "breaking"),
    ]
    empty = week["findings"]["cost_is_filled"].bad_rows
    assert len(empty) == 12 and {(r["supplier_id"], str(r["day"])) for r in empty} == {
        ("SUP-B", "2026-08-25")
    }


def test_case_10_is_seen_the_night_it_arrives(week):
    night = week["nights"][date(2026, 8, 25)]["findings"]
    assert len(night["files_have_their_columns"].bad_rows) == 2
    before = week["nights"][date(2026, 8, 24)]["findings"]
    assert before["files_have_their_columns"].bad_rows == []


def test_case_11_cost_in_cents_is_caught_two_ways(week):
    for key in ("cost_near_the_last_file", "cost_below_the_shelf_price"):
        rows = week["findings"][key].bad_rows
        assert len(rows) == 15, key
        assert {(r["supplier_id"], str(r["day"])) for r in rows} == {("SUP-C", "2026-08-28")}
    for row in week["findings"]["cost_near_the_last_file"].bad_rows:
        assert row["new_cost_cents"] == 100 * row["last_cost_cents"]
    assert week["findings"]["cost_is_filled"].bad_rows  # ...and not by the not-null check:
    assert all(r["supplier_id"] == "SUP-B" for r in week["findings"]["cost_is_filled"].bad_rows)


def test_case_12_the_missing_count(week):
    rows = week["findings"]["a_count_for_every_day"].bad_rows
    assert [(r["store_id"], str(r["sales_date"]), r["product_id"]) for r in rows] == [
        ("S05", "2026-08-28", "P-001")
    ]


def test_case_13_the_same_person_twice(week):
    rows = week["findings"]["one_person_once"].bad_rows
    assert [(r["first_member_id"], r["second_member_id"]) for r in rows] == [("M-0117", "M-0304")]


def test_no_member_check_ever_returns_personal_data(week, lab):
    lab.execute("UPDATE clean.members SET email = 'not an email' WHERE member_id = 'M-0005'")
    lab.execute("UPDATE clean.members SET full_name = NULL WHERE member_id = 'M-0006'")
    lab.execute("UPDATE clean.members SET home_store_id = 'S09' WHERE member_id = 'M-0007'")
    for check in MEMBER_CHECKS:
        for rows in (week["findings"][check.key].bad_rows, bad(lab, check.key)):
            for row in rows:
                assert not set(row) & set(PERSONAL_COLUMNS), check.key
                assert "@" not in " ".join(str(v) for v in row.values()), check.key


# ---------------------------------------------------------------- products, planted in the lab


def test_two_prices_on_one_day(lab):
    lab.execute(
        "INSERT INTO clean.products SELECT product_id, product_name, category, unit, 319, "
        "DATE '2026-08-01', NULL, status FROM clean.products WHERE product_id = 'P-001'"
    )
    assert things(lab, "one_price_at_a_time") == ["P-001"]
    assert bad(lab, "no_gap_between_prices") == []
    assert the_five_standard_checks_pass(lab)


def test_a_day_with_no_price(lab):
    lab.execute(
        "UPDATE clean.products SET valid_from = DATE '2026-08-12' "
        "WHERE product_id = 'P-006' AND valid_to IS NULL"
    )
    rows = bad(lab, "no_gap_between_prices")
    assert [(r["thing"], str(r["day"])) for r in rows] == [("P-006", "2026-08-10")]
    assert bad(lab, "one_price_at_a_time") == []


@pytest.mark.parametrize(
    "change",
    ["unit_price_cents = 0", "unit = 'box'", "status = 'gone'", "valid_to = DATE '2025-12-31'",
     "product_name = NULL"],
)  # fmt: skip
def test_a_product_row_that_makes_no_sense(lab, change):
    lab.execute(f"UPDATE clean.products SET {change} WHERE product_id = 'P-020'")
    assert things(lab, "product_values_allowed") == ["P-020"]


def test_a_price_typed_ten_times_too_high(lab):
    lab.execute(
        "UPDATE clean.products SET unit_price_cents = 5190 WHERE product_id = 'P-006' AND valid_to IS NULL"
    )
    rows = bad(lab, "price_change_is_believable")
    assert [(r["thing"], r["old_price_cents"], r["new_price_cents"]) for r in rows] == [
        ("P-006", 489, 5190)
    ]
    assert bad(lab, "product_values_allowed") == []  # 5190 is a perfectly allowed price


def test_a_retired_product_with_no_last_day(lab):
    lab.execute("UPDATE clean.products SET valid_to = NULL WHERE product_id = 'P-011'")
    assert things(lab, "retired_means_ended") == ["P-011"]


# ---------------------------------------------------------------- stores


def test_five_stores_and_a_store_twice(lab):
    lab.execute("DELETE FROM clean.stores WHERE store_id = 'S06'")
    assert [(r["stores_found"], r["stores_expected"]) for r in bad(lab, "six_stores_once")] == [
        (5, 6)
    ]
    lab.execute("INSERT INTO clean.stores SELECT * FROM clean.stores WHERE store_id = 'S01'")
    assert things(lab, "six_stores_once") == ["S01"]  # six rows again, and still wrong


def test_a_store_that_closes_before_it_opens(lab):
    lab.execute("UPDATE clean.stores SET closes_at = opens_at WHERE store_id = 'S02'")
    lab.execute("UPDATE clean.stores SET tills = 0 WHERE store_id = 'S03'")
    assert things(lab, "store_values_allowed") == ["S02", "S03"]


def test_a_file_from_a_store_nobody_knows(lab):
    lab.execute(
        "UPDATE raw.till_totals SET store_id = 'S07' WHERE store_id = 'S04' AND business_date = '2026-08-05'"
    )
    assert [(r["thing"], r["found_in"]) for r in bad(lab, "every_store_is_known")] == [
        ("S07", "till_totals")
    ]


def test_someone_changed_the_opening_hours(lab):
    lab.execute(
        "UPDATE raw.stores SET closes_at = '23:00' WHERE store_id = 'S02' "
        "AND _file_name = (SELECT max(_file_name) FROM raw.stores)"
    )
    rows = bad(lab, "stores_did_not_change")
    assert [(r["thing"], r["closes_at"]) for r in rows] == [("S02", "23:00")]
    assert bad(lab, "store_values_allowed") == []  # 23:00 is a perfectly allowed hour


# ---------------------------------------------------------------- stock counts


def test_a_count_that_is_not_there(lab):
    lab.execute(
        "DELETE FROM clean.stock_counts WHERE store_id = 'S02' AND product_id = 'P-024' "
        "AND count_date = DATE '2026-08-20'"
    )
    rows = bad(lab, "a_count_for_every_day")
    assert [(r["store_id"], str(r["sales_date"]), r["product_id"]) for r in rows] == [
        ("S02", "2026-08-20", "P-024")
    ]
    assert bad(lab, "count_values_allowed") == []  # a row that is not there cannot be wrong
    assert the_five_standard_checks_pass(lab)


def test_a_product_counted_twice(lab):
    lab.execute(
        "INSERT INTO clean.stock_counts SELECT * FROM clean.stock_counts WHERE store_id = 'S02' "
        "AND product_id = 'P-024' AND count_date = DATE '2026-08-20'"
    )
    assert [r["times"] for r in bad(lab, "one_count_per_day")] == [2]


@pytest.mark.parametrize("change", ["counted_qty = -3", "counted_qty = 2.5", "counted_by = 'guess'",
                                    "counted_at = TIMESTAMP '2026-08-22 09:00:00'"])  # fmt: skip
def test_a_count_that_cannot_be_true(lab, change):
    lab.execute(
        f"UPDATE clean.stock_counts SET {change} WHERE store_id = 'S02' AND product_id = 'P-024' "
        "AND count_date = DATE '2026-08-20'"
    )
    assert [r["product_id"] for r in bad(lab, "count_values_allowed")] == ["P-024"]


# ---------------------------------------------------------------- supplier prices


def test_a_new_column_is_reported_and_is_not_breaking(lab):
    lab.execute(
        "INSERT INTO raw.file_columns VALUES ('members_2026-08-23.csv', 'members', 'nickname', 8)"
    )
    rows = bad(lab, "files_have_their_columns")
    assert [(r["column_name"], r["kind_of_change"]) for r in rows] == [("nickname", "additive")]


def test_a_cost_per_kilogram_for_a_product_sold_by_the_piece(lab):
    lab.execute(
        "UPDATE clean.supplier_prices SET unit = 'kg' WHERE product_id = 'P-024' "
        "AND price_date = DATE '2026-08-21'"
    )
    rows = bad(lab, "supplier_row_fits_the_product")
    assert [(r["product_id"], r["supplier_unit"], r["product_unit"]) for r in rows] == [
        ("P-024", "kg", "each")
    ]


def test_a_supplier_row_twice(lab):
    lab.execute(
        "INSERT INTO clean.supplier_prices SELECT * FROM clean.supplier_prices "
        "WHERE product_id = 'P-024' AND price_date = DATE '2026-08-21'"
    )
    assert [r["problem"] for r in bad(lab, "supplier_row_fits_the_product")] == [
        "the same row is there more than once"
    ]


def test_a_cost_that_halves_is_not_an_alarm_and_a_cost_that_drops_to_a_tenth_is(lab):
    lab.execute(
        "UPDATE clean.supplier_prices SET unit_cost_cents = unit_cost_cents / 10 "
        "WHERE product_id = 'P-024' AND price_date = DATE '2026-08-21'"
    )
    assert [r["product_id"] for r in bad(lab, "cost_near_the_last_file")] == ["P-024"]
    assert bad(lab, "cost_below_the_shelf_price") == []  # a low cost is below the shelf price


# ---------------------------------------------------------------- members


def test_the_same_email_with_spaces_and_capitals(lab):
    lab.execute(
        "UPDATE clean.members SET email = '  ' || upper(email) || ' ' WHERE member_id = 'M-0010'"
    )
    assert bad(lab, "one_person_once") == []  # still one person: only one row has that email
    lab.execute(
        "INSERT INTO clean.members SELECT 'M-9999', full_name, lower(trim(email)), phone, "
        "home_store_id, joined_on, marketing_opt_in FROM clean.members WHERE member_id = 'M-0010'"
    )
    rows = bad(lab, "one_person_once")
    assert [(r["first_member_id"], r["second_member_id"]) for r in rows] == [("M-0010", "M-9999")]
    ids = lab.value("SELECT count(*) - count(DISTINCT member_id) FROM clean.members")
    emails = lab.value("SELECT count(*) - count(DISTINCT email) FROM clean.members")
    assert (ids, emails) == (0, 0)  # the common checks see nothing: every id and email is unique


@pytest.mark.parametrize(
    "email", ["no-at-sign.example.com", "@example.com", "two@@example.com", "a b@example.com",
              "name@nodot", "name@example."],
)  # fmt: skip
def test_an_email_with_the_wrong_shape(lab, email):
    lab.execute("UPDATE clean.members SET email = ? WHERE member_id = 'M-0010'", (email,))
    assert things(lab, "email_has_the_right_shape") == ["M-0010"]


def test_an_empty_phone_is_fine_and_an_empty_name_is_not(lab):
    assert lab.value("SELECT count(*) FROM clean.members WHERE phone IS NULL") > 50
    lab.execute("UPDATE clean.members SET full_name = NULL WHERE member_id = 'M-0010'")
    lab.execute("UPDATE clean.members SET marketing_opt_in = NULL WHERE member_id = 'M-0011'")
    assert things(lab, "member_cells_filled") == ["M-0010", "M-0011"]


def test_a_member_of_a_store_that_does_not_exist_and_a_member_from_the_future(lab):
    lab.execute("UPDATE clean.members SET home_store_id = 'S09' WHERE member_id = 'M-0010'")
    lab.execute("UPDATE clean.members SET joined_on = DATE '2027-01-01' WHERE member_id = 'M-0011'")
    assert things(lab, "member_fits_the_real_world") == ["M-0010", "M-0011"]


# ---------------------------------------------------------------- side by side


def test_the_cards_side_by_side():
    rows = {r["family"]: r for r in inspect.side_by_side()}
    assert sum(sum(r[t] for t in inspect.CHECKS_BY_TABLE) for r in rows.values()) == 37
    assert rows["the key is unique"]["tables"] == 5  # a habit worth having
    only_one = sorted(f for f, r in rows.items() if r["tables"] == 1)
    assert only_one == [
        "a row agrees with its neighbours", "it has the columns we expect",
        "it looks like a normal day", "two counts of one thing agree",
    ]  # fmt: skip
    assert rows["it looks like a normal day"]["sales_lines"] == 1
    assert rows["a row agrees with its neighbours"]["products"] == 2
    assert "5 of 7" in inspect.format_side_by_side()
