"""The flow page and the recap page must never show a number that the code did not produce."""

import re

from lotus_pipeline import checks, flow_data
from lotus_pipeline.inspect import ALL_CHECKS
from store_systems import planted

STANDARD = ["happened", "files", "load", "clean", "standard", "publish", "people"]


def test_the_flow_page_holds_exactly_what_the_code_produces_now():
    assert flow_data.page_data() == flow_data.build(), (
        "milestone_2_flow.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_the_recap_page_holds_exactly_what_the_code_produces_now():
    assert flow_data.recap_page_data() == flow_data.build_recap(), (
        "milestone_2_recap.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_building_the_pages_leaves_your_own_settings_alone():
    from lotus_pipeline import config

    before = (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING)
    flow_data._built.cache_clear()
    flow_data.build()
    assert (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING) == before


def test_the_layout_is_the_same_as_in_milestone_1():
    data = flow_data.page_data()
    assert [s["id"] for s in data["stages"]] == [
        "files", "raw", "clean", "checks", "outputs", "people",
    ]  # fmt: skip
    assert data["dag"]["id"] == "lotus_nightly_m2"
    assert data["dag"]["tasks"] == ["stores_send_files", "load", "clean", "check", "publish"]
    assert len(data["tables"]) == 7 and len(data["outputs"]) == 2 and len(data["readers"]) == 5
    assert [c["name"] for c in data["checks"]] == [c.name for c in checks.STANDARD_CHECKS]


def test_the_gates_and_the_quarantine_are_still_not_built():
    data = flow_data.page_data()
    assert [g["state"] for g in data["gates"]] == ["not_yet"] * 3
    assert data["quarantine"]["state"] == "not_yet"


def test_the_new_checks_are_drawn_with_their_lens_and_their_reason():
    data = flow_data.page_data()
    assert [c["key"] for c in data["reasoned"]] == [c.key for c in ALL_CHECKS]
    for shown, check in zip(data["reasoned"], ALL_CHECKS):
        assert (shown["lens"], shown["protects"], shown["prepare"], shown["limit"]) == (
            check.lens, check.protects, check.prepare, check.limit,
        )  # fmt: skip
    assert {lens["id"]: lens["checks"] for lens in data["lenses"]} == {
        "data": 6, "business": 7, "operational": 3,
    }  # fmt: skip
    assert data["reasoned_note"] == {
        "sales": 13, "tills": 3, "command": "python -m lotus_pipeline inspect",
    }  # fmt: skip


def test_every_planted_sales_case_passes_the_old_checks_and_is_caught_by_the_new():
    data = flow_data.page_data()
    cases = data["cases"][:-1]
    assert [c["id"] for c in cases] == [str(n) for n in range(1, 10)]
    for case in cases:
        assert case["name"] == planted.case(int(case["id"]))["name"]
        ids = [s["id"] for s in case["steps"]]
        assert ids[:7] == STANDARD and ids[-2:] == ["silent", "still_wrong"]
        catches = [s for s in case["steps"] if s["id"].startswith("catch_")]
        assert catches and len(catches) == len(case["caught_by"])
        assert sum(bool(s.get("plant")) for s in case["steps"]) == 1  # "plant the fault again"
        standard = case["steps"][4]
        assert "SELECT" in standard["sql"] and "returns no rows" in standard["text"]
        assert standard["stages"]["checks"]["done"] == 5
        for step in case["steps"][:7]:  # the new checks are not part of the nightly run
            assert step["reasoned"] == {"run": False, "caught": [], "silent": [], "now": None}
        for step in catches:
            labels = [fact[0] for fact in step["facts"]]
            assert labels == ["Lens", "Protects", "Prepare first", "The limit"]
            assert "SELECT" in step["sql"] and step["rows"]["rows"] and step["caught"]
        silent = case["steps"][-2]
        assert silent["reasoned"]["silent"] and "stays silent" in silent["title"]
        last = case["steps"][-1]
        assert last["problem"] and "by hand" in last["text"]
        done = last["reasoned"]
        assert len(done["caught"]) + len(done["silent"]) == len(ALL_CHECKS)
        for step in case["steps"]:
            assert step["word"]["term"] and step["word"]["meaning"] and step["text"]


def test_the_refund_is_caught_three_ways_and_the_number_is_still_wrong():
    case = flow_data.page_data()["cases"][0]
    assert (case["pipeline_says"], case["answer_key_says"]) == ("$796.18", "$777.20")
    assert [c["number"] for c in case["caught_by"]] == [4, 6, 11]
    assert {c["lens"] for c in case["caught_by"]} == {"data", "business"}
    assert "still says $796.18" in case["steps"][-1]["text"]


def test_case_4_the_table_is_right_and_the_store_is_short():
    case = flow_data.page_data()["cases"][3]
    assert case["table_is_right"] and case["pipeline_says"] == case["answer_key_says"]
    assert case["note"] == "The table is right. The store is $6.00 short."
    assert [c["number"] for c in case["caught_by"]] == [8]
    silent = case["steps"][-2]
    assert "Check 11 stays silent" in silent["title"]  # the till total cannot see it


def test_the_three_things_that_are_not_faults_raise_no_alarm():
    case = flow_data.page_data()["cases"][-1]
    assert case["id"] == "ok" and case["caught_by"] == [] and case["false_alarms"] == 0
    assert case["careless_alarms"] == 6  # a check with no calendar: one alarm per store
    assert [s["id"] for s in case["steps"]] == [
        "holiday", "false_alarm", "calendar", "refit", "big_sale", "limit", "silence",
    ]  # fmt: skip
    for step in case["steps"]:
        assert step["reasoned"]["caught"] == []
    assert len(case["steps"][-1]["reasoned"]["silent"]) == len(ALL_CHECKS)


def test_every_word_of_the_milestone_is_on_the_page_and_taught_at_a_step():
    data = flow_data.page_data()
    words = {w["term"]: w["meaning"] for w in data["words"]}
    assert len(words) == 18 and words == flow_data.WORDS
    taught = {step["word"]["term"] for case in data["cases"] for step in case["steps"]}
    assert taught == set(words)


def test_the_page_ends_with_what_is_still_wrong():
    wrong = "One table is done well. There are five more, and they are nothing like it."
    for data in (flow_data.page_data(), flow_data.recap_page_data()):
        assert data["still_wrong"] == wrong
        assert "not part of the nightly run" in data["not_in_the_run"]


def test_the_recap_counts_are_real(request):
    recap = flow_data.recap_page_data()
    numbers = recap["numbers"]
    assert numbers["checks_with_a_reason"] == 16 and numbers["standard_checks"] == 5
    assert (numbers["checks_on_sales_lines"], numbers["checks_on_till_totals"]) == (13, 3)
    assert numbers["planted_cases_caught"] == numbers["planted_cases_in_sales"] == 9
    assert numbers["false_alarms"] == 0 and numbers["stores_wrong"] == 6
    assert numbers["gates"] == 0 and numbers["rows_set_aside"] == 0 and numbers["words"] == 18
    assert numbers["tables"] == 7
    assert (
        numbers["tables_with_a_card"]
        == len(recap["cards"])
        == len(list((flow_data.MILESTONE_DIR / "checks").glob("*.md")))
    )
    assert numbers["notebooks"] == len(
        list((flow_data.MILESTONE_DIR / "notebooks").glob("0*.ipynb"))
    )
    assert recap["week"]["checks_green"] == recap["week"]["checks_run"] == 35
    assert recap["week"]["checks_with_bad_rows"] == 10
    assert recap["example"]["till_total_cases"] == [1, 2, 3, 5, 6, 7, 8, 9]
    assert len(recap["quiz"]) == 3 and all(q["question"] and q["answer"] for q in recap["quiz"])
    if not request.config.args or request.config.args == ["tests"]:  # the whole suite is running
        assert numbers["tests"] == request.session.testscollected


def test_each_page_is_one_self_contained_file():
    for page in (flow_data.PAGE, flow_data.RECAP_PAGE):
        html = page.read_text(encoding="utf-8")
        assert not re.search(r'(src|href)="https?://(?!docs\.astral\.sh)', html), page.name
        assert "<script src" not in html and "<link " not in html, page.name
