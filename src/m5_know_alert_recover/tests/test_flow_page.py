"""The flow page and the recap page show real numbers.

`flow_data.build()` runs the real pipeline for the bonus week on a private warehouse: the
gates, the alerts, the recovery of four stores, four drills and one fault that blocks. These
tests fail when a page holds anything else.
"""

import re

import pytest

from lotus_pipeline import config, flow_data
from store_systems.generate import build as build_world

TASKS = ["stores_send_files", "load", "gate_landing", "clean", "gate_clean", "build",
         "gate_publish", "publish"]  # fmt: skip
PROJECT = flow_data.PROJECT_DIR


@pytest.fixture(scope="module")
def flow():
    before = (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING, config.DBT_TARGET,
              config.ALERTS_LOG)  # fmt: skip
    data = flow_data.build()
    assert (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING, config.DBT_TARGET,
            config.ALERTS_LOG) == before  # fmt: skip
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
        "milestone_5_flow.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_the_recap_page_is_up_to_date(recap):
    assert flow_data.recap_page_data() == recap, (
        "milestone_5_recap.html is out of date. Run: uv run python -m lotus_pipeline.flow_data"
    )


def test_the_same_layout_as_milestone_4_and_the_new_lanes(flow):
    assert [s["id"] for s in flow["stages"]] == [
        "files", "raw", "clean", "checks", "outputs", "people",
    ]  # fmt: skip
    assert flow["dag"] == {"id": "lotus_nightly_m5", "tasks": TASKS}
    assert [(g["before"], g["task"]) for g in flow["gates"]] == [
        ("raw", "gate_landing"), ("checks", "gate_clean"), ("outputs", "gate_publish"),
    ]  # fmt: skip
    for c in flow["cases"]:
        for s in c["steps"]:
            assert set(s["stages"]) == {"files", "raw", "clean", "checks", "outputs", "people"}
            assert [line["gate"] for line in s["stages"]["checks"]["lines"]] == [
                "gate_landing", "gate_clean", "gate_publish",
            ]  # fmt: skip
            assert set(s["tasks"]) == set(TASKS)
            assert {"quarantine", "dead", "gates", "tests", "word", "clock", "alerts",
                    "history", "health"} <= set(s)  # fmt: skip
    page = flow_data.PAGE.read_text(encoding="utf-8")
    for lane in ("band-alerts", "band-history", "band-quarantine", "band-dead"):
        assert f'id="{lane}"' in page


def test_every_test_of_the_three_gates_is_on_the_page(flow):
    per_gate = {}
    for t in flow["tests"]:
        per_gate[t["gate"]] = per_gate.get(t["gate"], 0) + 1
        assert t["action"] in ("block", "quarantine", "refuse_file", "warn")
    assert per_gate == {"landing": 4, "clean": 40, "publish": 5}
    assert flow["tests_note"]["dbt"] == 48  # the 49th is the supplier's contract, in Python
    assert {t["name"] for t in flow["tests"] if t["new"]} == {
        "file_arrived_on_time", "impossible_price",
    }  # fmt: skip


def test_the_twelve_picks(flow):
    ids = [c["id"] for c in flow["cases"]]
    assert ids == ["1", "3", "5", "6", "8", "7", "D1", "D2", "D3", "D4", "B", "M"]
    assert [c["id"] for c in flow["cases"] if c["lab"]] == ["D1", "D2", "D3", "D4", "B"]
    for c in flow["cases"]:
        assert c["steps"][0]["plant"] and c["caught_by"] and c["outcome"]
        for s in c["steps"]:
            assert s["active"] in {"files", "raw", "clean", "tests", "quarantine", "dead",
                                   "outputs", "people", "alerts", "history"}  # fmt: skip
    for cid in ("1", "3", "5", "6"):  # the faults the stores can fix
        assert c_ids(flow, cid)[-4:] == ["told", "fix", "rerun", "right"]


def c_ids(flow, cid):
    return [s["id"] for s in case(flow, cid)["steps"]]


def test_every_word_is_taught_and_means_what_the_glossary_says(flow):
    assert len(flow_data.WORDS) == 16
    used = {s["word"]["term"] for c in flow["cases"] for s in c["steps"]}
    assert set(flow_data.WORDS) <= used  # every new word is taught somewhere
    assert used <= set(flow_data.WORD_BOOK)
    for c in flow["cases"]:
        for s in c["steps"]:
            assert s["word"]["meaning"] == flow_data.WORD_BOOK[s["word"]["term"]]
            assert s["word"]["new"] == (s["word"]["term"] in flow_data.WORDS)
    glossary = (PROJECT / "GLOSSARY.md").read_text(encoding="utf-8")
    for word, meaning in flow_data.WORDS.items():
        assert f"| **{word}** | {meaning}" in glossary


def test_case_1_told_fixed_and_right_again(flow):
    c = case(flow, "1")
    assert (c["pipeline_says"], c["answer_key_says"]) == ("$796.18", "$777.20")
    assert c["recovered"] == {"published": "$777.20", "answer_key": "$777.20", "equal": True}
    aside = step(flow, "1", "aside")
    assert (
        len(aside["quarantine"]["cards"]) == 1
        and aside["quarantine"]["cards"][0]["amount"] == "$9.49"
    )
    told = step(flow, "1", "told")
    assert told["active"] == "alerts" and told["callback"] == "publish"
    assert told["alerts"]["counts"].startswith("1 sent")
    assert told["alerts"]["cards"][0]["level"] == "ticket" and told["alerts"]["cards"][0]["do"]
    assert ["Level", "ticket"] in told["facts"]
    people = step(flow, "1", "people")
    assert any(
        "DO NOT PAY YET" in x["sub"] and "S03" in x["id"]
        for x in people["stages"]["people"]["cards"]
    )
    rerun = step(flow, "1", "rerun")
    assert "lotus_nightly_m5" in rerun["sql"] and "2026-08-24" in rerun["sql"]
    assert "sales_lines_S03_2026-08-24.csv" in rerun["text"]
    right = step(flow, "1", "right")
    assert "$777.20, the same as the answer key" in right["title"] and "RESOLVED" in right["text"]
    assert "1 resolved" in right["alerts"]["counts"]
    assert any(x["state"] == "resolved" for x in right["alerts"]["cards"])
    assert any("Pay? yes" in x["sub"] for x in right["stages"]["people"]["cards"])


def test_cases_3_5_and_6_come_back_equal_to_the_answer_key(flow):
    expected = {"3": ("S05", "$2,101.52", "$580.84"), "5": ("S02", "$881.35", "$905.30"),
                "6": ("S06", "$978.26", "$996.23")}  # fmt: skip
    for cid, (store, says, key) in expected.items():
        c = case(flow, cid)
        assert (c["pipeline_says"], c["answer_key_says"]) == (says, key)
        assert c["recovered"]["published"] == key and c["recovered"]["equal"]
        assert store in step(flow, cid, "right")["title"]
    assert "Sat 08-29 and Sun 08-30" in step(flow, "6", "rerun")["title"]  # the price list


def test_case_8_the_late_file_heals_by_itself(flow):
    c = case(flow, "8")
    assert "Resolved by itself" in c["outcome"] and c["recovered"]["equal"]
    late = step(flow, "8", "late")
    assert late["tests"]["now"] == "file_arrived_on_time" and late["tone"] == "warn"
    assert "S06" in late["rows"]["rows"][0][0]
    right = step(flow, "8", "right")
    assert "RESOLVED" in right["title"] and "1 resolved" in right["alerts"]["counts"]
    history = step(flow, "8", "history")
    assert [h["mark"] for h in history["health"][:2]] == ["ok", "miss"]
    assert "39 of 41" in history["health"][1]["text"] and "95.1%" in history["health"][1]["text"]
    assert any("S06 08-27" in h["text"] and "23.7" in h["text"] for h in history["health"])


def test_case_7_the_file_never_comes_and_the_ticket_stays_open(flow):
    c = case(flow, "7")
    assert c["answer_key_says"] == "$587.42" and "recovered" not in c
    monday = step(flow, "7", "monday")
    assert monday["alerts"]["counts"].endswith("1 ticket still open")
    assert monday["alerts"]["cards"][0]["state"] == "still open"
    assert any("DO NOT PAY YET" in x["sub"] and "S04" in x["id"] and "587.42" in x["sub"]
               for x in monday["stages"]["people"]["cards"])  # fmt: skip
    fatigue = step(flow, "7", "fatigue")
    assert "10 alerts" in fatigue["text"] and "70" in fatigue["text"]
    assert "7 of 7" in step(flow, "7", "history")["text"]
    assert [h["published"] for h in step(flow, "7", "history")["history"]] == [True] * 7


def test_the_four_drills_on_thursday_the_20th(flow):
    names = {
        "D1": ("gate_clean", "true_key_unique", "S03"),
        "D2": ("gate_landing", "file_is_missing_a_column", "S04"),
        "D3": ("gate_publish", "store_is_not_ready_to_pay", "S05"),
        "D4": ("gate_clean", "impossible_price", "S06"),
    }
    for cid, (gate, test, store) in names.items():
        c = case(flow, cid)
        assert c["lab"] and c["recovered"]["equal"] and store in c["where"]
        assert "2026-08-20" not in c["where"] and "Thu 08-20" in c["where"]
        assert step(flow, cid, "caught")["tests"]["now"] == test
        assert c["caught_by"][0].startswith(gate)
        assert step(flow, cid, "right")["title"].startswith("Right again")
    assert "Nobody had to act" in case(flow, "D1")["caught_by"][1]
    assert step(flow, "D2", "caught")["active"] == "tests"


def test_the_block_case_pages_someone(flow):
    c = case(flow, "B")
    assert c["pipeline_says"] != c["answer_key_says"]
    blocked = step(flow, "B", "blocked")
    assert blocked["tone"] == "blocked" and blocked["tasks"]["gate_clean"] == "failed"
    assert [blocked["tasks"][t] for t in ("build", "gate_publish", "publish")] == [
        "upstream_failed"
    ] * 3
    paged = step(flow, "B", "paged")
    assert paged["callback"] == "gate_clean" and paged["active"] == "alerts"
    assert paged["alerts"]["cards"][0]["level"] == "page"
    assert ["Level", "page"] in paged["facts"]
    nights = step(flow, "B", "history")["history"]
    assert any(n["blocked_at"] == "gate-clean" and not n["published"] for n in nights)


def test_the_monday_answer_before_and_after(flow):
    c = case(flow, "M")
    assert (c["pipeline_says"], c["answer_key_says"]) == (
        "1 of 6 can be paid",
        "5 of 6 can be paid",
    )
    before, after = step(flow, "M", "before"), step(flow, "M", "after")
    assert before["stages"]["people"]["count"].endswith("1 of 6 stores can be paid")
    assert after["stages"]["people"]["count"].endswith("5 of 6 stores can be paid")
    cards = after["stages"]["people"]["cards"]
    assert sum("equal to the answer key" in x["sub"] for x in cards) == 5
    assert any(
        x["sub"].startswith("DO NOT PAY YET") and "S04" in x["id"] and "$587.42" in x["sub"]
        for x in cards
    )
    assert step(flow, "M", "done")["title"] == "Nothing is still wrong"
    assert step(flow, "M", "alerts")["title"].endswith("10 alerts, not 70")


def test_no_personal_data_on_either_page(flow):
    pages = flow_data.PAGE.read_text(encoding="utf-8") + flow_data.RECAP_PAGE.read_text(
        encoding="utf-8"
    )
    for member in build_world().members:
        assert member["email"] not in pages
        assert member["full_name"] not in pages
    assert not re.search(r"[\w.+-]+@[\w-]+\.[\w.]+", pages.replace("•••@", ""))


def test_the_pages_load_nothing_from_outside():
    for page in (flow_data.PAGE, flow_data.RECAP_PAGE):
        text = page.read_text(encoding="utf-8")
        assert not re.search(
            r'(src|href)="https?://', text.replace("https://docs.astral.sh/uv/", "")
        )
        assert "@import" not in text and "fetch(" not in text


def test_the_recap_numbers_come_from_the_runs(recap):
    n = recap["numbers"]
    assert n["notebooks"] == len(list((flow_data.MILESTONE_DIR / "notebooks").glob("*.ipynb")))
    assert n["tests"] > 100
    assert (n["alerts_sent"], n["alerts_shouted"]) == (10, 70)
    assert (n["files_on_time"], n["files_expected"]) == (39, 41)
    assert (n["can_be_paid_before"], n["can_be_paid_after"], n["stores"]) == (1, 5, 6)
    assert (n["drills"], n["words"], n["planted_cases"]) == (4, 16, 13)
    assert n["dbt_tests"] == sum(recap["dbt"].values()) == 48
    assert recap["dbt"] == {"landing": 3, "clean": 40, "publish": 5}
    glossary = (PROJECT / "GLOSSARY.md").read_text(encoding="utf-8")
    assert n["all_words"] == len([x for x in glossary.splitlines() if x.startswith("| **")]) == 69
    assert recap["dag"]["id"] == "lotus_nightly_m5"


def test_the_recap_week_night_by_night(recap):
    week = recap["week"]
    assert [w["sent"] for w in week] == [1, 2, 0, 2, 3, 1, 1]
    assert sum(w["shouted"] for w in week) == 70
    assert [w["resolved"] for w in week][4] == 1  # S06's missing file, closed on Friday night
    health = recap["health"]
    assert (health["nights_published"], health["nights"]) == (7, 7)
    assert (health["files_on_time_percent"], health["files_on_time_target"]) == (95.1, 98)
    assert (health["store_days_ok"], health["store_days"]) == (36, 41)
    assert recap["late"] == "S06 08-27, 23.7 hours late" and recap["missing"] == "S04 08-25"
    assert [r["case"] for r in recap["recovery"]] == [
        "Case 1",
        "Case 3",
        "Case 6",
        "Case 5",
    ]  # in the order of the nights
    assert all(r["loaded_again"] for r in recap["recovery"])


def test_the_recap_answers_the_monday_question(recap):
    assert [s["store"][:3] for s in recap["before"]["stores"] if s["pay"]] == ["S01"]
    after = recap["after"]["stores"]
    paid = [s for s in after if s["pay"]]
    assert len(paid) == 5 and all(s["equal"] for s in paid)
    (held,) = [s for s in after if not s["pay"]]
    assert held["store"].startswith("S04") and "587.42" in " ".join(held["why_not"])
    assert recap["after"]["open_tickets"] == ["S04 Tue 08-25: No sales file arrived"]
    assert len(recap["drills"]) == 4 and all(d["right_again"] for d in recap["drills"])
    assert len(recap["quiz"]) == 3 and all(q["question"] and q["answer"] for q in recap["quiz"])
    assert recap["nothing_wrong"].startswith("Nothing is still wrong")


def test_the_look_back_reads_the_readmes(recap):
    rows = recap["journey"]
    assert [r[1] for r in rows] == ["Green checks, wrong number", "Checks with a reason",
                                    "Every table is different", "Gates and quarantine",
                                    "Know, alert, recover"]  # fmt: skip
    for number, folder in enumerate(sorted(p.name for p in PROJECT.glob("src/m[1-4]_*")), start=1):
        text = (PROJECT / "src" / folder / "README.md").read_text(encoding="utf-8")
        assert rows[number - 1][2] in text
    if not (flow_data.MILESTONE_DIR / "README.md").exists():
        assert rows[4][2] == "Nothing"  # milestone 5 has nothing left, and may have no README yet
