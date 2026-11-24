"""The check card page and its data file show exactly what the Markdown cards say."""

import importlib.util
from pathlib import Path

FOLDER = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("build_page", FOLDER / "build_page.py")
build_page = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_page)

HOW = "Run: uv run python checks/build_page.py"


def test_the_json_file_is_up_to_date():
    expected = build_page.as_json(build_page.build_data())
    assert build_page.JSON_FILE.read_text(encoding="utf-8") == expected, HOW


def test_the_page_is_up_to_date():
    assert build_page.page_data() == build_page.build_data(), HOW


def test_every_table_is_on_the_page():
    cards = {c["table"]: c for c in build_page.build_data()["cards"]}
    assert list(cards) == [
        "sales_lines", "till_totals", "products", "stores", "stock_counts", "supplier_prices",
        "members",
    ]  # fmt: skip
    for path in FOLDER.glob("*.md"):
        assert cards[path.stem]["has_card"]


def test_a_card_has_the_eight_questions_in_order():
    for card in build_page.build_data()["cards"]:
        if card["has_card"]:
            assert [q["number"] for q in card["questions"]] == list(range(1, 9))
            assert [q["question"] for q in card["questions"]] == build_page.QUESTIONS


def test_the_sales_lines_card_after_milestone_1():
    card = build_page.build_data()["cards"][0]
    answered = [q["number"] for q in card["questions"] if q["answered"]]
    assert answered[:5] == [1, 2, 3, 4, 5]
    assert card["answered"] == len(answered)
    worries = card["questions"][3]["blocks"]
    assert any(block["type"] == "table" for block in worries)


def test_markdown_is_read_into_blocks():
    blocks = build_page._blocks(
        ["One line.", "Same paragraph.", "", "| A | B |", "|---|---|", "| 1 | `x` |", "", "Last."]
    )
    assert blocks == [
        {"type": "p", "text": "One line. Same paragraph."},
        {"type": "table", "head": ["A", "B"], "rows": [["1", "`x`"]]},
        {"type": "p", "text": "Last."},
    ]
