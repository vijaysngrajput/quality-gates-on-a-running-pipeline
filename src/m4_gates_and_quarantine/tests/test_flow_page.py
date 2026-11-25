"""The flow page and the recap page show real numbers.

`flow_data.build()` runs the real pipeline, gates and all, for the bonus week on a private
warehouse, and plants two blocking faults in a private lab copy. These tests fail when a
page holds anything else.
"""

import pytest

from lotus_pipeline import config, flow_data
from store_systems.generate import build as build_world

TASKS = ["stores_send_files", "load", "gate_landing", "clean", "gate_clean", "build",
         "gate_publish", "publish"]  # fmt: skip


@pytest.fixture(scope="module")
def flow():
    before = (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING, config.DBT_TARGET)
    data = flow_data.build()
    assert (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING, config.DBT_TARGET) == before
    return data


@pytest.fixture(scope="module")
def recap(flow):
    return flow_data.build_recap(flow)


def case(flow, case_id):
    return next(c for c in flow["cases"] if c["id"] == case_id)


def step(flow, case_id, step_id):
    return next(s for s in case(flow, case_id)["steps"] if s["id"] == step_id)


def test_the_flow_page_is_up_to_date(flow):
    assert flow_data.page_data() == flow, (
        "milestone_4_flow.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_the_recap_page_is_up_to_date(recap):
    assert flow_data.recap_page_data() == recap, (
        "milestone_4_recap.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_the_same_layout_and_the_dashed_lines_are_real_gates_now(flow):
    assert [s["id"] for s in flow["stages"]] == [
        "files",
        "raw",
        "clean",
        "checks",
        "outputs",
        "people",
    ]
    assert flow["dag"] == {"id": "lotus_nightly_m4", "tasks": TASKS}
    assert [(g["before"], g["task"], g["state"]) for g in flow["gates"]] == [
        ("raw", "gate_landing", "new"), ("checks", "gate_clean", "new"),
        ("outputs", "gate_publish", "new"),
    ]  # fmt: skip
    for c in flow["cases"]:
        for s in c["steps"]:
            assert set(s["stages"]) == {"files", "raw", "clean", "checks", "outputs", "people"}
            assert [line["gate"] for line in s["stages"]["checks"]["lines"]] == [
                "gate_landing", "gate_clean", "gate_publish",
            ]  # fmt: skip
            assert set(s["tasks"]) == set(TASKS)
            assert {"quarantine", "dead", "gates", "tests", "word", "clock"} <= set(s)


def test_every_test_of_the_three_gates_is_on_the_page(flow):
    per_gate = {}
    for t in flow["tests"]:
        per_gate[t["gate"]] = per_gate.get(t["gate"], 0) + 1
        assert t["action"] in ("block", "quarantine", "refuse_file", "warn")
    assert per_gate == {"landing": 3, "clean": 39, "publish": 5}
    assert flow["tests_note"]["dbt"] == 46  # the 47th is the supplier's contract, in Python


def test_the_fourteen_picks(flow):
    ids = [c["id"] for c in flow["cases"]]
    assert ids == ["1", "2", "3", "4", "5", "6", "7", "9", "10", "11", "12", "13", "L1", "L2"]
    assert [c["id"] for c in flow["cases"] if c["lab"]] == ["L1", "L2"]
    for c in flow["cases"]:
        assert c["steps"][0]["plant"] and c["steps"][-1]["id"] == "still_wrong"
        assert c["steps"][1]["id"] == "before" and c["steps"][1]["tone"] == "problem"
        assert c["caught_by"] and c["outcome"]


def test_every_word_is_taught_and_means_what_the_glossary_says(flow):
    assert len(flow_data.WORDS) == 14
    used = {s["word"]["term"] for c in flow["cases"] for s in c["steps"]}
    assert used == set(flow_data.WORDS)
    for c in flow["cases"]:
        for s in c["steps"]:
            assert s["word"]["meaning"] == flow_data.WORDS[s["word"]["term"]]


def test_case_1_the_refund_is_set_aside_and_the_store_is_held_back(flow):
    c = case(flow, "1")
    assert (c["pipeline_says"], c["answer_key_says"]) == ("$796.18", "$777.20")
    aside = step(flow, "1", "aside")
    assert aside["active"] == "quarantine" and len(aside["quarantine"]["cards"]) == 1
    card = aside["quarantine"]["cards"][0]
    assert card["amount"] == "$9.49" and "A refund gives money back" in card["sub"]
    caught = step(flow, "1", "caught")
    assert caught["facts"][0][1] == "dbt_project/tests/clean/refund_gives_money_back.sql"
    assert "severity='warn'" in caught["facts"][1][1] and "quarantine" in caught["facts"][1][1]
    assert "SELECT" in caught["sql"] and "{{" not in caught["sql"]
    assert caught["rows"]["rows"] and caught["gates"]["clean"] == "acted"
    people = step(flow, "1", "people")
    assert any(
        "DO NOT PAY YET" in card["sub"] and "S03" in card["id"]
        for card in people["stages"]["people"]["cards"]
    )
    assert c["outcome"] == "1 row set aside. S03: do not pay yet"


def test_cases_2_and_9_removing_a_copy_repairs_the_day(flow):
    assert "4 rows set aside. The day is right again" in case(flow, "2")["outcome"]
    assert "167 rows set aside. The day is right again" in case(flow, "9")["outcome"]
    for cid, store in (("2", "S01"), ("9", "S02")):
        people = step(flow, cid, "people")["stages"]["people"]["cards"]
        assert any(store in card["id"] and card["sub"] == "Pay? yes" for card in people)


def test_case_4_is_a_warning_and_nothing_is_set_aside(flow):
    c = case(flow, "4")
    assert c["table_is_right"] and c["pipeline_says"] == c["answer_key_says"]
    assert "$6.00" in c["note"]
    assert [s["tone"] for s in c["steps"] if s["id"] in ("caught", "warn")] == ["warn", "warn"]
    assert all(not s["quarantine"]["cards"] for s in c["steps"])


def test_case_7_no_gate_before_the_last_can_see_a_file_that_did_not_come(flow):
    c = case(flow, "7")
    assert all(not s["quarantine"]["cards"] for s in c["steps"])
    caught = step(flow, "7", "caught")
    assert caught["tests"]["now"] == "store_is_not_ready_to_pay"
    assert ["S04", "2026-08-25", "no_sales_file", "0"] in caught["rows"]["rows"]
    assert "no sales file arrived" in step(flow, "7", "people")["text"]


def test_cases_10_and_11_the_file_goes_to_the_dead_letter_table(flow):
    for cid, name in (
        ("10", "supplier_prices_SUP-B_2026-08-25.csv"),
        ("11", "supplier_prices_SUP-C_2026-08-28.csv"),
    ):
        refused = step(flow, cid, "refused")
        assert refused["active"] == "dead"
        assert name in [card["id"] for card in refused["dead"]["cards"]]
        assert all(not s["quarantine"]["cards"] for s in case(flow, cid)["steps"])
    assert (
        step(flow, "10", "caught")["facts"][0][1]
        == "dbt_project/tests/landing/file_is_missing_a_column.sql"
    )
    assert step(flow, "11", "caught")["facts"][0][1] == "contracts/supplier_prices.yml"
    assert "12 products" in case(flow, "10")["pipeline_says"]
    assert "100 times" in case(flow, "11")["pipeline_says"]


def test_case_12_the_order_list_says_what_it_does_not_know(flow):
    c = case(flow, "12")
    assert (c["pipeline_says"], c["answer_key_says"]) == ("18 bottles", "15 bottles")
    assert "no count: order by hand" in c["outcome"]
    assert step(flow, "12", "audit")["tests"]["now"] == "order_without_a_count"


def test_no_personal_data_on_either_page(flow):
    c = case(flow, "13")
    aside = step(flow, "13", "aside")["quarantine"]["cards"][0]
    assert aside["id"] == "M-0304" and "member_id, check_key, reason, set_aside_on" in aside["sub"]
    assert (c["pipeline_says"], c["answer_key_says"]) == ("304 members", "303 members")
    pages = flow_data.PAGE.read_text() + flow_data.RECAP_PAGE.read_text()
    for member in build_world().members:
        assert member["email"] not in pages
        assert member["full_name"] not in pages


def test_the_two_lab_cases_block_and_airflow_marks_what_comes_after(flow):
    one = step(flow, "L1", "upstream")["tasks"]
    assert one["gate_clean"] == "failed"
    assert [one[t] for t in ("build", "gate_publish", "publish")] == ["upstream_failed"] * 3
    assert [one[t] for t in ("stores_send_files", "load", "gate_landing", "clean")] == [
        "success"
    ] * 4
    two = step(flow, "L2", "upstream")["tasks"]
    assert (two["gate_publish"], two["publish"], two["gate_clean"]) == (
        "failed",
        "upstream_failed",
        "success",
    )
    assert step(flow, "L1", "blocked")["tests"]["now"] == "six_stores_once"
    assert step(flow, "L2", "blocked")["tests"]["now"] == "unexplained_difference"
    assert step(flow, "L2", "clean")["gates"]["clean"] in ("passed", "acted")
    for cid in ("L1", "L2"):
        blocked = step(flow, cid, "blocked")
        assert blocked["tone"] == "blocked" and "severity='error'" in blocked["facts"][1][1]
        assert "last good numbers" in step(flow, cid, "upstream")["stages"]["outputs"]["count"]


def test_the_recap_numbers_come_from_the_runs(recap):
    n = recap["numbers"]
    assert (n["dbt_tests"], n["unit_tests"], n["gates"], n["tasks"]) == (46, 2, 3, 8)
    assert (n["can_be_paid"], n["stores"], n["files_refused"], n["false_alarms"]) == (1, 6, 2, 0)
    assert (n["planted_cases_handled"], n["planted_cases"], n["words"]) == (13, 13, 14)
    assert n["rows_set_aside"] == sum(recap["set_aside"].values()) == 181
    assert n["tests"] > 80
    assert [w["can_be_paid"] for w in recap["week"]] == [5, 4, 4, 2, 3, 2, 1]
    assert recap["clean_weeks_failed"] == 0
    assert recap["dbt"]["files"] == 42 and recap["dbt"]["models"] == 4


def test_the_recap_answers_both_sentences_of_the_monday_question(recap):
    stores = recap["report"]["stores"]
    assert [s["store"][:3] for s in stores if s["pay"]] == ["S01"]
    assert all(s["why_not"] for s in stores if not s["pay"])
    assert len(recap["cases"]) == 12 and len(recap["lab_cases"]) == 2
    assert len(recap["quiz"]) == 3 and all(q["question"] and q["answer"] for q in recap["quiz"])
    assert (
        recap["still_wrong"]
        == "A gate fails at 02:00, and nobody knows until a store manager complains."
    )
    assert [g["tests"] for g in recap["gates"]] == [3, 39, 5]
