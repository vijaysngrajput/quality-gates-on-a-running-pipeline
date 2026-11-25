"""Who is hurt when a table is wrong."""

import re
from pathlib import Path

from lotus_pipeline import clean, consumers, pipeline, publish


def test_the_map_matches_the_sql():
    """BUILT_FROM is typed by hand. It must agree with the tables the SQL really reads."""

    def tables_read(sql: str) -> set[str]:
        return set(re.findall(r"\b(?:raw|clean)\.[a-z_]+", sql.split(" AS", 1)[1]))

    for name, sql in clean.STEPS.items():
        assert tables_read(sql) == set(consumers.BUILT_FROM[f"clean.{name}"]), name
    assert tables_read(publish.DAILY_STORE_SALES) == {"clean.sales_lines"}
    in_list = set(re.findall(r"\bclean\.[a-z_]+", publish.REPLENISHMENT_LIST))
    assert in_list == set(consumers.BUILT_FROM["output.replenishment_list"])


def test_a_wrong_sales_line_reaches_both_outputs_and_four_groups():
    found = consumers.impact("sales_lines")
    assert found["outputs"] == ["output.daily_store_sales", "output.replenishment_list"]
    assert found["people"] == [
        "Store managers", "Head of retail operations", "The replenishment team", "Finance",
    ]  # fmt: skip


def test_a_wrong_store_is_wrong_everywhere():
    assert consumers.impact("stores")["people"] == consumers.impact("sales_lines")["people"]


def test_nobody_reads_till_totals_and_only_marketing_reads_members():
    assert consumers.impact("till_totals")["people"] == []
    assert consumers.impact("till_totals")["outputs"] == []
    assert consumers.impact("members")["people"] == ["Marketing"]


def test_the_ranking_by_harm():
    ranked = consumers.rank_tables()
    assert [r["table"] for r in ranked] == [
        "sales_lines", "products", "stores", "stock_counts", "supplier_prices", "members",
        "till_totals",
    ]  # fmt: skip
    assert [r["table"] for r in ranked if r["critical"]] == ["sales_lines", "products", "stores"]


def test_every_source_table_is_in_every_map():
    assert set(consumers.MADE_BY) == set(consumers.SOURCE_TABLES)
    assert len(consumers.SOURCE_TABLES) == 7 and len(consumers.CONSUMERS) == 5


def test_the_command_line(capsys):
    assert pipeline.main(["impact"]) == 0
    assert pipeline.main(["impact", "--table", "members"]) == 0
    out = capsys.readouterr().out
    assert "1. sales_lines     CRITICAL" in out and "7. till_totals" in out
    assert "Marketing" in out


def test_the_check_card_names_the_same_people():
    card = (Path(__file__).parent.parent / "checks" / "sales_lines.md").read_text()
    for heading in range(1, 9):
        assert f"\n## {heading}. " in card
    for who in ("Store managers", "Head of retail operations", "The replenishment team"):
        assert who in card
