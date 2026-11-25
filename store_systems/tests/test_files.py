"""The files the stores send. Each planted case is one difference from the truth."""

import csv
from datetime import date

import pytest

from store_systems import files, key_files, planted
from store_systems.__main__ import main
from store_systems.catalog import LAST_DAY, STORES
from store_systems.generate import build

WORLD = build()


def rows(folder, table, name):
    with (folder / table / name).open(newline="") as f:
        return list(csv.DictReader(f))


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    folder = tmp_path_factory.mktemp("landing")
    files.deliver_through(LAST_DAY, folder)
    return folder


def sales(landing, store, day):
    return rows(landing, "sales_lines", f"sales_lines_{store}_{day}.csv")


def test_a_clean_file_equals_the_journal(landing):
    day = date(2026, 8, 5)
    in_file = sum(int(r["amount_cents"]) for r in sales(landing, "S03", day))
    in_journal = sum(
        r.total_cents for r in WORLD.receipts if r.store_id == "S03" and r.business_date == day
    )
    assert in_file == in_journal


def test_case_1_the_refund_is_positive_in_the_file(landing):
    refund = WORLD.planted["c1_refund"]
    line = [r for r in sales(landing, "S03", "2026-08-24") if r["receipt_id"] == refund.receipt_id]
    assert [(r["line_type"], r["quantity"], r["amount_cents"]) for r in line] == [
        ("refund", "-1.000", "949")
    ]


def test_case_2_one_receipt_twice_with_new_line_ids(landing):
    twice = WORLD.planted["c2"].receipt_id
    lines = [r for r in sales(landing, "S01", "2026-08-25") if r["receipt_id"] == twice]
    assert len(lines) == 8  # four lines, twice
    assert len({r["line_id"] for r in lines}) == 8


def test_case_3_grams(landing):
    line = [r for r in sales(landing, "S05", "2026-08-27") if r["quantity"] == "1180.000"]
    assert [(r["product_id"], r["amount_cents"]) for r in line] == [("P-012", "152220")]


def test_case_5_the_wrong_clock(landing):
    late = [r for r in sales(landing, "S02", "2026-08-30") if r["sold_at"] > "2026-08-31"]
    assert {r["sold_at"] for r in late} == {"2026-08-31 03:10:00"}
    assert sum(int(r["amount_cents"]) for r in late) == 2395


def test_case_6_a_product_nobody_typed(landing):
    products = {r["product_id"] for r in rows(landing, "products", "products_2026-08-30.csv")}
    soup = [r for r in sales(landing, "S06", "2026-08-29") if r["product_id"] not in products]
    assert [(r["product_id"], r["amount_cents"]) for r in soup] == [("P-041", "599")] * 3


def test_cases_7_8_and_9_files_missing_late_and_twice(landing):
    names = {p.name for p in (landing / "sales_lines").iterdir()}
    assert "sales_lines_S04_2026-08-25.csv" not in names  # case 7
    assert "till_totals_S04_2026-08-25.csv" in {p.name for p in (landing / "till_totals").iterdir()}
    arrivals = {r["file_name"]: r["received_at"] for r in rows(landing, "", "_arrivals.csv")}
    assert arrivals["sales_lines_S06_2026-08-27.csv"] == "2026-08-29 00:40:00"  # case 8
    first, second = sales(landing, "S02", "2026-08-28"), sales(landing, "S02", "2026-08-28_2")
    assert len(first) == len(second) > 0  # case 9
    assert not {r["line_id"] for r in first} & {r["line_id"] for r in second}
    assert [r["receipt_id"] for r in first] == [r["receipt_id"] for r in second]


def test_every_line_id_is_unique_across_all_files(landing):
    ids = [
        r["line_id"]
        for path in sorted((landing / "sales_lines").iterdir())
        for r in rows(landing, "sales_lines", path.name)
    ]
    assert len(ids) == len(set(ids))


def test_a_closed_store_sends_nothing(landing):
    for table in ("sales_lines", "till_totals", "stock_counts"):
        assert not (landing / table / f"{table}_S01_2026-08-27.csv").exists()


def test_cases_10_and_11_the_suppliers(landing):
    renamed = rows(landing, "supplier_prices", "supplier_prices_SUP-B_2026-08-25.csv")
    assert "unit_cost" not in renamed[0] and "cost_per_unit" in renamed[0]
    before = rows(landing, "supplier_prices", "supplier_prices_SUP-C_2026-08-21.csv")
    after = rows(landing, "supplier_prices", "supplier_prices_SUP-C_2026-08-28.csv")
    assert [round(float(r["unit_cost"]) * 100) for r in before] == [
        int(r["unit_cost"]) for r in after
    ]


def test_case_12_one_count_is_missing(landing):
    counted = {
        r["product_id"] for r in rows(landing, "stock_counts", "stock_counts_S05_2026-08-28.csv")
    }
    assert (
        "P-001" not in counted and len(counted) == 38
    )  # 40 products, bagels retired, milk missing


def test_case_13_both_members_are_in_the_file(landing):
    members = rows(landing, "members", "members_2026-08-26.csv")
    assert [m["member_id"] for m in members if m["email"].lower() == "priya.nair@example.com"] == [
        "M-0117", "M-0304",
    ]  # fmt: skip


def test_the_tills_own_total_is_the_truth(landing):
    for store_id in STORES:
        tills = rows(landing, "till_totals", f"till_totals_{store_id}_2026-08-24.csv")
        journal = sum(
            r.total_cents
            for r in WORLD.receipts
            if r.store_id == store_id and r.business_date == date(2026, 8, 24)
        )
        assert sum(int(t["net_total_cents"]) for t in tills) == journal


def test_a_fault_you_switch_on_and_a_fix(tmp_path, monkeypatch):
    monkeypatch.setenv("LOTUS_LANDING", str(tmp_path))
    assert main(["fault", "file-twice", "--store", "S03", "--day", "2026-08-05"]) == 0
    assert main(["fault", "rename-column", "--store", "S04", "--day", "2026-08-05"]) == 0
    assert main(["fault", "no-file", "--store", "S05", "--day", "2026-08-05"]) == 0
    assert main(["fault", "late-file", "--store", "S06", "--day", "2026-08-05"]) == 0
    assert main(["fault", "bad-prices", "--store", "S01", "--day", "2026-08-05"]) == 0
    assert main(["deliver", "--day", "2026-08-05"]) == 0
    names = {p.name for p in (tmp_path / "sales_lines").iterdir()}
    assert "sales_lines_S03_2026-08-05_2.csv" in names
    assert "sales_lines_S05_2026-08-05.csv" not in names
    assert "sales_lines_S06_2026-08-05.csv" not in names
    assert "amount" in sales(tmp_path, "S04", "2026-08-05")[0]
    assert max(int(r["unit_price_cents"]) for r in sales(tmp_path, "S01", "2026-08-05")) > 10_000
    assert main(["deliver", "--day", "2026-08-06"]) == 0  # the late file arrives a day late
    assert (tmp_path / "sales_lines" / "sales_lines_S06_2026-08-05.csv").exists()

    assert main(["fix", "--all-faults"]) == 0
    assert main(["fix", "--case", "1"]) == 0
    assert main(["deliver", "--day", "2026-08-24"]) == 0
    refund = WORLD.planted["c1_refund"].receipt_id
    fixed = [r for r in sales(tmp_path, "S03", "2026-08-24") if r["receipt_id"] == refund]
    assert fixed[0]["amount_cents"] == "-949"
    assert main(["status"]) == 0 and main(["reset"]) == 0
    assert list(tmp_path.iterdir()) == []  # the folder is empty, and still there


def test_the_key_files_are_up_to_date():
    for name, text in key_files.build_files(WORLD).items():
        assert (key_files.FOLDER / name).read_text() == text, (
            f"answer_key/{name} is out of date. Run: uv run python -m store_systems.key_files"
        )


def test_every_case_has_its_details():
    assert [c["case"] for c in planted.CASES] == list(range(1, 14)) == sorted(key_files.DETAILS)


def test_fixing_case_6_means_the_buying_team_types_the_product(tmp_path, monkeypatch):
    monkeypatch.setenv("LOTUS_LANDING", str(tmp_path))
    assert main(["deliver", "--day", "2026-08-29"]) == 0
    assert "P-041" not in {
        r["product_id"] for r in rows(tmp_path, "products", "products_2026-08-29.csv")
    }
    assert main(["fix", "--case", "6"]) == 0 and main(["deliver", "--day", "2026-08-29"]) == 0
    soup = [
        r
        for r in rows(tmp_path, "products", "products_2026-08-29.csv")
        if r["product_id"] == "P-041"
    ]
    assert [(r["unit_price_cents"], r["valid_from"], r["status"]) for r in soup] == [
        ("599", "2026-08-29", "active")
    ]
