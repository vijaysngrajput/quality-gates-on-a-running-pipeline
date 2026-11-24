"""The flow page and the recap page must never show a number that the code did not produce."""

import re

from lotus_pipeline import checks, flow_data
from store_systems import planted


def test_the_flow_page_holds_exactly_what_the_code_produces_now():
    assert flow_data.page_data() == flow_data.build(), (
        "milestone_1_flow.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_the_recap_page_holds_exactly_what_the_code_produces_now():
    assert flow_data.recap_page_data() == flow_data.build_recap(), (
        "milestone_1_recap.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_building_the_pages_leaves_your_own_settings_alone():
    from lotus_pipeline import config

    before = (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING)
    flow_data._built.cache_clear()
    flow_data.build()
    assert (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING) == before


def test_every_stage_of_the_pipeline_is_drawn():
    data = flow_data.page_data()
    assert [s["id"] for s in data["stages"]] == [
        "files", "raw", "clean", "checks", "outputs", "people",
    ]  # fmt: skip
    assert data["dag"]["id"] == "lotus_nightly_m1"
    assert data["dag"]["tasks"] == ["stores_send_files", "load", "clean", "check", "publish"]
    assert [s["task"] for s in data["stages"]][:5] == data["dag"]["tasks"]
    assert len(data["tables"]) == 7 and len(data["outputs"]) == 2 and len(data["readers"]) == 5


def test_the_gates_and_the_quarantine_are_drawn_as_not_built_yet():
    data = flow_data.page_data()
    assert [g["state"] for g in data["gates"]] == ["not_yet"] * 3
    assert [g["before"] for g in data["gates"]] == ["raw", "checks", "outputs"]
    assert data["quarantine"]["state"] == "not_yet"


def test_the_five_checks_are_named_with_their_lens():
    data = flow_data.page_data()
    assert [c["name"] for c in data["checks"]] == [c.name for c in checks.STANDARD_CHECKS]
    assert {c["lens"] for c in data["checks"]} == {"data"}
    assert {lens["id"]: lens["checks"] for lens in data["lenses"]} == {
        "data": 5, "business": 0, "operational": 0,
    }  # fmt: skip


def test_the_reader_can_follow_eight_planted_cases():
    data = flow_data.page_data()
    assert [c["id"] for c in data["cases"]] == ["1", "2", "3", "5", "6", "7", "8", "9"]
    for case in data["cases"]:
        assert case["name"] == planted.case(int(case["id"]))["name"]
        ids = [s["id"] for s in case["steps"]]
        assert ids[:4] == ["happened", "files", "load", "clean"]
        assert ids[4:9] == [f"check_{n}" for n in range(1, 6)]
        assert ids[9:] == ["publish", "people", "still_wrong"]
        assert sum(bool(s.get("plant")) for s in case["steps"]) == 1  # "plant the fault again"
        for step in case["steps"]:
            assert step["word"]["term"] and step["word"]["meaning"] and step["text"]
        for step in case["steps"][4:9]:  # the row walks through five green checks
            assert "SELECT" in step["sql"] and "the check passes" in step["text"]
        assert case["steps"][-1]["stages"]["checks"]["done"] == 5
        assert case["pipeline_says"] != case["answer_key_says"]


def test_the_refund_reaches_the_manager_with_five_green_checks():
    case = flow_data.page_data()["cases"][0]
    assert (case["pipeline_says"], case["answer_key_says"]) == ("$796.18", "$777.20")
    refund = case["steps"][1]["stages"]["files"]["cards"][0]
    assert (refund["id"], refund["amount"], refund["tone"]) == ("Olive oil 500ml", "$9.49", "bad")
    assert "refund · qty -1" in refund["sub"]
    people = case["steps"][10]
    assert "5 of 5 checks passed" in people["text"] and "+$18.98" in people["text"]


def test_every_word_of_the_milestone_is_on_the_page_and_taught_at_a_step():
    data = flow_data.page_data()
    words = {w["term"]: w["meaning"] for w in data["words"]}
    assert len(words) == 12 and words == flow_data.WORDS
    taught = {step["word"]["term"] for step in data["cases"][0]["steps"]}
    assert taught == set(words)


def test_the_page_ends_with_what_is_still_wrong():
    wrong = (
        "We found one error by luck, and we have a list of worries. "
        "We have no checks that would have caught it."
    )
    assert flow_data.page_data()["still_wrong"] == wrong
    assert flow_data.recap_page_data()["still_wrong"] == wrong


def test_the_recap_counts_are_real(request):
    recap = flow_data.recap_page_data()
    numbers = recap["numbers"]
    assert numbers["tables"] == 7 and numbers["outputs"] == 2
    assert numbers["checks"] == 5 and numbers["tasks"] == 5
    assert numbers["planted_cases"] == 13 and numbers["stores_wrong"] == 6
    assert numbers["gates"] == 0 and numbers["rows_set_aside"] == 0
    assert numbers["notebooks"] == len(
        list((flow_data.MILESTONE_DIR / "notebooks").glob("0*.ipynb"))
    )
    assert recap["week"]["checks_green"] == recap["week"]["checks_run"] == 35
    assert len(recap["quiz"]) == 3 and all(q["question"] and q["answer"] for q in recap["quiz"])
    if not request.config.args or request.config.args == ["tests"]:  # the whole suite is running
        assert numbers["tests"] == request.session.testscollected


def test_each_page_is_one_self_contained_file():
    for page in (flow_data.PAGE, flow_data.RECAP_PAGE):
        html = page.read_text(encoding="utf-8")
        assert not re.search(r'(src|href)="https?://(?!docs\.astral\.sh)', html), page.name
        assert "<script src" not in html and "<link " not in html, page.name
