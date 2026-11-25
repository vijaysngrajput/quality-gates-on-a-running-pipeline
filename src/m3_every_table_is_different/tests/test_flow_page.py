"""The flow page and the recap page show real numbers.

`flow_data.build()` runs the real pipeline for the bonus week on a private warehouse, plants
four faults in a private lab copy, and runs this milestone's checks and the contract.
These tests fail when a page holds anything else.
"""

import json
import re

import pytest

from lotus_pipeline import config, flow_data
from lotus_pipeline.inspect import ALL_CHECKS, CHECKS_BY_TABLE
from lotus_pipeline.other_checks import OTHER_CHECKS
from store_systems.generate import build as build_world

STEP_KEYS = {"id", "active", "task", "clock", "title", "text", "word", "stages", "moves",
             "reasoned", "tasks_done", "failed_task"}  # fmt: skip


@pytest.fixture(scope="module")
def flow():
    before = (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING)
    data = flow_data.build()
    assert (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING) == before  # put back
    return data


@pytest.fixture(scope="module")
def recap(flow):
    return flow_data.build_recap(flow)


def case(flow, case_id):
    return next(c for c in flow["cases"] if c["id"] == case_id)


def test_the_flow_page_is_up_to_date(flow):
    assert flow_data.page_data() == flow, (
        "milestone_3_flow.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_the_recap_page_is_up_to_date(recap):
    assert flow_data.recap_page_data() == recap, (
        "milestone_3_recap.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_the_same_layout_as_every_milestone(flow):
    assert [s["id"] for s in flow["stages"]] == [
        "files",
        "raw",
        "clean",
        "checks",
        "outputs",
        "people",
    ]
    assert flow["dag"]["id"] == "lotus_nightly_m3"
    assert flow["dag"]["tasks"] == ["stores_send_files", "load", "clean", "check", "publish"]
    assert [g["state"] for g in flow["gates"]] == ["not_yet"] * 3  # still no gate
    assert flow["quarantine"]["state"] == "not_yet"
    assert len(flow["checks"]) == 5


def test_all_37_checks_are_on_the_page_grouped_by_table(flow):
    assert [c["key"] for c in flow["reasoned"]] == [c.key for c in ALL_CHECKS]
    assert [t["table"] for t in flow["reasoned_tables"]] == list(CHECKS_BY_TABLE)
    assert sum(t["checks"] for t in flow["reasoned_tables"]) == 37
    new = [t["table"] for t in flow["reasoned_tables"] if t["new"]]
    assert new == ["products", "stores", "stock_counts", "supplier_prices", "members"]
    assert sum(c["new"] for c in flow["reasoned"]) == len(OTHER_CHECKS) == 21
    assert sum(lens["checks"] for lens in flow["lenses"]) == 37


def test_the_nine_picks(flow):
    assert [c["id"] for c in flow["cases"]] == ["10", "11", "12", "13", "L1", "L2", "L3", "L4", "9"]
    assert [c["id"] for c in flow["cases"] if c["lab"]] == ["L1", "L2", "L3", "L4"]
    for c in flow["cases"]:
        assert c["caught_by"], c["id"]
        assert c["table"] in CHECKS_BY_TABLE
        ids = [s["id"] for s in c["steps"]]
        assert ids[:7] == ["happened", "files", "load", "clean", "standard", "publish", "people"]
        assert ids[7] == "prepare" and ids[-1] == "still_wrong" and "silent" in ids
        for step in c["steps"]:
            assert STEP_KEYS <= set(step), (c["id"], step["id"])
            assert step["word"]["term"] in flow_data.ALL_WORDS
        last = c["steps"][-1]["reasoned"]
        assert set(last["caught"]) | set(last["silent"]) == {x.key for x in ALL_CHECKS}


def test_the_rows_in_the_stages_are_rows_of_the_cases_own_table(flow):
    assert case(flow, "10")["steps"][1]["stages"]["files"]["cards"][0]["sub"].startswith(
        "cost_per_unit"
    )
    assert case(flow, "12")["output"] == case(flow, "11")["output"] == "replenishment_list"
    assert case(flow, "13")["output"] is None  # marketing reads the clean table itself
    milk = case(flow, "12")["steps"][1]["stages"]["files"]["cards"][-1]
    assert (milk["id"], milk["amount"]) == ("Whole milk 2L", "no row")


def test_the_four_planted_cases(flow):
    assert [x["number"] for x in case(flow, "10")["caught_by"]] == [29, 30]
    assert [x["number"] for x in case(flow, "11")["caught_by"]] == [31, 32]
    assert [x["number"] for x in case(flow, "12")["caught_by"]] == [26]
    assert [x["number"] for x in case(flow, "13")["caught_by"]] == [34]
    assert (case(flow, "12")["pipeline_says"], case(flow, "12")["answer_key_says"]) == (
        "order 18", "order 15",
    )  # fmt: skip
    assert (case(flow, "13")["pipeline_says"], case(flow, "13")["answer_key_says"]) == (
        "308 members", "307 people",
    )  # fmt: skip


def test_cases_10_and_11_are_also_caught_at_the_door(flow):
    for case_id in ("10", "11"):
        chosen = case(flow, case_id)
        door = next(s for s in chosen["steps"] if s["id"] == "door")
        assert chosen["at_the_door"] == "refuse" and door["rows"]["rows"]
    kinds = [
        r[0]
        for r in next(s for s in case(flow, "10")["steps"] if s["id"] == "door")["rows"]["rows"]
    ]
    assert kinds == ["breaking", "additive"]
    assert "door" not in [s["id"] for s in case(flow, "12")["steps"]]


def test_case_11_the_schema_check_and_the_not_null_check_stay_silent(flow):
    chosen = case(flow, "11")
    silent = next(s for s in chosen["steps"] if s["id"] == "silent")
    assert silent["reasoned"]["now"] == "files_have_their_columns"
    last = chosen["steps"][-1]["reasoned"]
    assert {"files_have_their_columns", "cost_is_filled"} <= set(last["silent"])


def test_in_the_lab_a_standard_check_fails_twice_and_points_at_the_wrong_table(flow):
    for case_id in ("L1", "L3"):
        steps = {s["id"]: s for s in case(flow, case_id)["steps"]}
        assert steps["standard"]["stages"]["checks"]["failed"] == [3]
        assert steps["publish"]["failed_task"] == "check" and steps["publish"]["task"] is None
        assert steps["still_wrong"]["tasks_done"] == 3  # publish never ran
    for case_id in ("L2", "L4", "10", "11", "12", "13", "9"):
        steps = {s["id"]: s for s in case(flow, case_id)["steps"]}
        assert steps["standard"]["stages"]["checks"] == {"done": 5, "active": 0, "failed": []}


def test_the_lab_numbers(flow):
    assert (case(flow, "L1")["pipeline_says"], case(flow, "L1")["answer_key_says"]) == (
        "828 rows", "414 lines",
    )  # fmt: skip
    assert case(flow, "L3")["pipeline_says"] == "6 rows, 5 stores"
    assert case(flow, "L4")["table_is_right"] is True and case(flow, "L4")["answer_key_says"] == "0"
    assert int(case(flow, "L4")["pipeline_says"]) > 100  # right sales that now look wrong


def test_no_personal_data_reaches_the_pages(flow, recap):
    text = (json.dumps(flow, ensure_ascii=False) + json.dumps(recap, ensure_ascii=False)).lower()
    for member in build_world().members:
        assert member["email"].lower() not in text
        assert member["full_name"].lower() not in text
    assert "555-01" not in text
    assert "m-0117" in text and "p•••@example.com" in text  # ids and masked emails only


def test_the_side_by_side_view(flow):
    side = {r["family"]: r for r in flow["side_by_side"]}
    assert sum(sum(r[t] for t in flow["table_names"]) for r in side.values()) == 37
    only_one = [f for f, r in side.items() if r["tables"] == 1]
    assert sorted(only_one) == sorted(flow_data.ONLY_ONE_BECAUSE)
    assert all(side[f]["why"] for f in only_one)
    assert all(not r["why"] for r in side.values() if r["tables"] > 1)


def test_the_recap_numbers_come_from_the_code(recap):
    n = recap["numbers"]
    assert (n["checks_with_a_reason"], n["new_checks"], n["tables"], n["tables_with_a_card"]) == (
        37, 21, 7, 7,
    )  # fmt: skip
    assert (n["planted_cases_caught"], n["planted_cases"]) == (13, 13)
    assert (n["files_refused"], n["supplier_files"]) == (2, 15)
    assert (n["false_alarms"], n["gates"], n["rows_set_aside"], n["words"]) == (0, 0, 0, 9)
    assert n["tests"] == flow_data.count_tests() > 200
    assert recap["week"]["checks_green"] == recap["week"]["checks_run"] == 35
    assert [c["case"] for c in recap["cases"]] == ["10", "11", "12", "13"]
    assert len(recap["lab_cases"]) == 4 and len(recap["quiz"]) == 3


def test_the_words_and_the_last_line(flow, recap):
    assert [w["term"] for w in flow["words"]] == list(flow_data.WORDS)
    assert len(flow_data.WORDS) == 9
    taught = {s["word"]["term"] for c in flow["cases"] for s in c["steps"]}
    assert set(flow_data.WORDS) <= taught
    wrong = "The checks exist in notebooks. The pipeline does not run them, and a failure changes nothing."
    assert flow["still_wrong"] == recap["still_wrong"] == wrong


def test_the_pages_make_no_request_and_type_no_number_by_hand():
    for page in (flow_data.PAGE, flow_data.RECAP_PAGE):
        html = page.read_text(encoding="utf-8")
        outside = flow_data.MARKER.sub("", html)
        assert not re.search(r'(src|href)="https?://(?!docs\.astral\.sh)', outside), page.name
        assert "$" not in re.sub(
            r"<script>.*?</script>|<style>.*?</style>", "", outside, flags=re.S
        ), f"{page.name} has a dollar amount typed by hand"
