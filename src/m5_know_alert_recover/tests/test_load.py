"""The load step: files in, raw rows out, nothing changed and nothing judged."""

from datetime import date, datetime

from lotus_pipeline import load, pipeline
from lotus_pipeline.warehouse import connect


def test_a_run_only_sees_files_that_had_arrived(empty_warehouse, landing):
    done = load.load(empty_warehouse, date(2026, 8, 27), landing)
    names = {
        r["file_name"] for r in empty_warehouse.query("SELECT file_name FROM raw.loaded_files")
    }
    assert done["files"] == len(names)
    assert "sales_lines_S05_2026-08-27.csv" in names
    assert "sales_lines_S06_2026-08-27.csv" not in names  # case 8: it arrives a day late
    assert not any("2026-08-28" in n for n in names)

    load.load(empty_warehouse, date(2026, 8, 28), landing)
    late = empty_warehouse.query(
        "SELECT received_at, loaded_at FROM raw.loaded_files WHERE file_name = ?",
        ("sales_lines_S06_2026-08-27.csv",),
    )
    assert late == [
        {"received_at": datetime(2026, 8, 29, 0, 40), "loaded_at": datetime(2026, 8, 29, 2, 0)}
    ]


def test_every_raw_row_says_where_it_came_from(empty_warehouse, landing):
    load.load(empty_warehouse, date(2026, 7, 27), landing)
    row = empty_warehouse.query("SELECT * FROM raw.sales_lines LIMIT 1")[0]
    assert row["_file_name"].startswith("sales_lines_S0")
    assert row["_loaded_at"] == datetime(2026, 7, 28, 2, 0)
    assert isinstance(row["amount_cents"], str)  # raw values stay text


def test_a_column_the_file_does_not_have_becomes_null(empty_warehouse, landing):
    """Case 10: the supplier renamed unit_cost. The load runs, and the column is empty."""
    load.load(empty_warehouse, date(2026, 8, 25), landing)
    costs = empty_warehouse.query(
        "SELECT count(*) AS rows, count(unit_cost) AS with_cost FROM raw.supplier_prices "
        "WHERE _file_name = ?",
        ("supplier_prices_SUP-B_2026-08-25.csv",),
    )
    assert costs == [{"rows": 12, "with_cost": 0}]


def test_an_empty_cell_becomes_null(tmp_path):
    path = tmp_path / "members.csv"
    path.write_text("member_id,phone\nM-0001,\nM-0002,555-0101\n")
    assert load.read_file(path, ["member_id", "phone"]) == [
        ("M-0001", None),
        ("M-0002", "555-0101"),
    ]


def test_the_command_line(own_warehouse, capsys):
    assert pipeline.main(["run", "--day", "2026-08-24"]) == 0
    out = capsys.readouterr().out
    assert "gate landing: 4 tests, 4 passed." in out and "gate clean: 40 tests, 38 passed." in out
    assert "alerts:       1 sent (0 page, 1 ticket)." in out
    assert "S03  Lotus Calder Bridge" in out and "DO NOT PAY YET" in out
    assert "5 of 6 stores can be paid." in out
    with connect() as wh:
        assert wh.value("SELECT count(*) FROM output.daily_store_sales") == 29 * 6
