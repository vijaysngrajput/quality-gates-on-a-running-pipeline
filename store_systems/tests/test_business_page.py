"""The business page shows only what the till journal, the files and the answer key say."""

from store_systems import business_page

DATA = business_page.build_data()


def test_the_page_holds_exactly_what_the_journal_says():
    assert business_page.page_data() == DATA, (
        "lotus_retails.html is out of date. Run: uv run python -m store_systems.business_page"
    )


def test_the_receipt_travels_from_the_till_to_the_bonus():
    stages = [s["id"] for s in DATA["stages"]]
    assert stages == ["till", "file", "raw", "clean", "output", "bonus"]
    steps = DATA["steps"]
    assert steps[0]["stage"] == "till" and steps[-1]["stage"] == "bonus"
    assert {s["stage"] for s in steps} == set(stages)
    assert "$14.74" in steps[0]["what"]  # olive oil 9.49 + sourdough 5.25


def test_the_refund_row_is_shown_as_the_file_has_it():
    step = next(s for s in DATA["steps"] if s["title"] == "The refund in the store's file")
    row = dict(zip(step["show"]["columns"], step["show"]["rows"][0]))
    assert (row["line_type"], row["quantity"], row["amount_cents"]) == ("refund", "-1.000", "949")
    marked = step["show"]["columns"][step["show"]["mark"][1]]
    assert marked == "amount_cents" and step["wrong"]


def test_the_wrong_number_and_the_true_number():
    step = next(s for s in DATA["steps"] if s["title"] == "A wrong number, and nothing fails")
    assert "$796.18" in step["number"] and "$777.20" in step["number"]
    assert "$18.98 too high" in step["note"]
    assert "$0.19 too high" in DATA["steps"][-1]["number"]


def test_four_groups_each_with_a_real_example():
    groups = {g["id"]: g for g in DATA["groups"]}
    assert list(groups) == ["managers", "replenishment", "finance", "marketing"]
    tables = {t["id"] for t in DATA["tables"]}
    assert len(tables) == 7
    for group in groups.values():
        assert group["examples"] and set(group["tables"]) <= tables
    assert "$1,522.20" in groups["managers"]["examples"][1]
    assert "the order is 18" in groups["replenishment"]["examples"][0]
    assert "$6.00 short" in groups["finance"]["examples"][1]
    assert "M-0117" in groups["marketing"]["examples"][0]


def test_the_page_is_before_milestone_1():
    text = business_page.PAGE.read_text(encoding="utf-8").lower()
    assert "quarantine" not in text and "gate" not in text
    assert "http://" not in text and "https://" not in text  # nothing is fetched
