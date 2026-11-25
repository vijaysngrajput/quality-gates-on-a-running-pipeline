"""Know, alert, recover.

know      the pipeline remembers its runs, and measures itself
alert     the right person is told once, in words they can act on
recover   fix the source, run one night again, and the numbers equal the answer key
"""

from datetime import date

import pytest

from conftest import WEEK, plant, rows, snapshot
from lotus_pipeline import alerts, health, load, pipeline
from lotus_pipeline.gates import GateBlocked
from lotus_pipeline.warehouse import connect
from store_systems import truth
from store_systems.catalog import BONUS_WEEK_START

MON, TUE, WED, THU, FRI, SAT, SUN = WEEK


# ---------------------------------------------------------------- know


def test_every_step_of_every_night_is_in_the_run_history(week):
    history = rows("SELECT run_day, step, status FROM ops.run_history ORDER BY started_at")
    nights = {r["run_day"] for r in history}
    assert nights == {d.isoformat() for d in WEEK}
    monday = [r["step"] for r in history if r["run_day"] == "2026-08-24"][:9]
    assert monday == list(pipeline.STEPS)
    assert {r["status"] for r in history} == {"ok"}


def test_the_four_health_numbers_of_the_bonus_week(week):
    with connect() as wh:
        h = health.health(wh, SUN)
    assert (h["nights_published"], h["nights"]) == (7, 7)
    assert (h["files_on_time"], h["files_expected"]) == (39, 41)  # 6 stores, 7 days, less one refit
    assert (
        h["files_on_time_percent"] == 95.1 and h["files_on_time_target"] == 98
    )  # the promise is missed
    assert h["late_files"] == [
        {"store_id": "S06", "day": "2026-08-27", "hours_late": 23.7}
    ]  # case 8
    assert h["missing_files"] == [{"store_id": "S04", "day": "2026-08-25"}]  # case 7
    assert (h["store_days_ok"], h["store_days"]) == (36, 41)
    text = health.format_health(h)
    assert "MISS Is the data fresh?" in text and "ok   Did it run?" in text


def test_case_8_freshness_sees_the_late_file_only_when_it_has_arrived(week):
    assert "file_arrived_on_time" not in {
        n for n, r in week["nights"][THU]["gates"].items() if r["status"] != "pass"
    }  # on Thursday night the file is missing, not late: there is no row to be late
    friday = week["nights"][FRI]["gates"]["file_arrived_on_time"]
    assert (friday["status"], friday["bad_rows"], friday["action"]) == ("fail", 1, "warn")


# ---------------------------------------------------------------- alert


def sent(night: dict) -> list[dict]:
    return [a for a in night["alert_log"] if a["event"] == "new" and a["level"] != "message"]


def test_one_ticket_for_one_problem_sent_once(week):
    per_night = [len(sent(week["nights"][day])) for day in WEEK]
    assert per_night == [1, 2, 0, 2, 3, 1, 1]
    everything = [a for day in WEEK for a in sent(week["nights"][day])]
    assert len({a["fingerprint"] for a in everything}) == len(everything) == 10  # never twice


def test_shouting_sends_many_times_more(week):
    """One alert per failed test per night, to everyone. The same refund, seven nights running."""
    shouted = sum(
        sum(r["status"] != "pass" for r in week["nights"][day]["gates"].values()) for day in WEEK
    )
    assert shouted == 70  # against 10 tickets: seven times as many


def test_each_alert_goes_to_the_person_who_can_act(week):
    to = {}
    for day in WEEK:
        for a in week["nights"][day]["alert_log"]:
            if a["event"] == "new":
                to.setdefault(a["to_whom"], []).append((a["level"], a["title"]))
    assert sorted(to) == [
        "Finance", "Finance, and the buying team", "Head of retail operations", "Marketing",
        "Store operations", "The data engineer on call", "The replenishment team",
    ]  # fmt: skip
    assert len(to["Head of retail operations"]) == 7  # six store days, and one day far from normal
    assert ("message", "M-0304 is the same person as an older member") in to["Marketing"]
    assert [level for level, _t in to["Finance, and the buying team"]] == ["ticket", "ticket"]
    assert {level for people in to.values() for level, _t in people} == {"ticket", "message"}


def test_the_late_file_is_told_on_its_night_and_closed_the_next(week):
    thursday = [a["title"] for a in sent(week["nights"][THU])]
    assert "S06 Thu 08-27: No sales file arrived" in thursday
    closed = [a["title"] for a in week["nights"][FRI]["alert_log"] if a["event"] == "resolved"]
    assert "S06 Thu 08-27: No sales file arrived" in closed


def test_on_monday_five_tickets_are_open_for_the_five_stores_that_wait(week):
    open_tickets = [
        a["title"] for a in week["last"]["open_alerts"]
        if a["level"] == "ticket" and a["to_whom"] == "Head of retail operations"
    ]  # fmt: skip
    assert sorted(t[:3] for t in open_tickets) == ["S02", "S03", "S04", "S05", "S06"]


def test_an_alert_says_what_how_bad_and_what_to_do(week):
    with connect() as wh:
        found = {a.fingerprint: a for a in alerts.tonight(wh, SUN)}
    a = found["store_day|S04|2026-08-25"]
    assert (a.level, a.to_whom) == ("ticket", "Head of retail operations")
    assert a.title == "S04 Tue 08-25: No sales file arrived"
    assert "$587.42" in a.how_bad and "Ines Marchetti" in a.how_bad
    assert a.what_to_do.startswith("Ask the store's back office to send the file")
    assert a.runbook == "ask-a-store-for-its-file"


def test_no_alert_carries_personal_data(week, world):
    text = " ".join(
        " ".join(str(v) for v in a.values())
        for day in WEEK for a in week["nights"][day]["alert_log"]
    )  # fmt: skip
    assert "@" not in text and "Priya" not in text
    assert not [m for m in world.members if m["full_name"] in text]


def test_running_a_night_again_sends_nothing_twice(week):
    with connect() as wh:
        again = alerts.send(wh, SUN)
    assert again["sent"] == [] and again["resolved"] == [] and len(again["still_open"]) == 14


def test_every_runbook_section_an_alert_names_exists(week):
    from pathlib import Path

    runbook = (Path(__file__).resolve().parent.parent / "RUNBOOK.md").read_text().lower()
    with connect() as wh:
        named = {a.runbook for a in alerts.tonight(wh, SUN)} | {"a-gate-blocked", "a-task-failed"}
    headings = {
        line[3:].strip().replace(" ", "-")
        for line in runbook.splitlines()
        if line.startswith("## ")
    }
    assert named <= headings, named - headings


def test_a_blocked_gate_pages_the_engineer_on_call_and_the_run_still_stops(lab):
    plant("DELETE FROM raw.stores WHERE store_id = 'S06' "
          "AND _file_name = (SELECT max(_file_name) FROM raw.stores)")  # fmt: skip
    said = []
    with pytest.raises(GateBlocked):
        pipeline.run(lab, pipeline.STEPS[2:], say=said.append)
    assert "2 sent (2 page, 0 ticket)" in said[-1]  # two tests see the missing store
    night = snapshot(lab)
    pages = [a for a in night["open_alerts"] if a["level"] == "page"]
    assert len(pages) == 2 and {a["to_whom"] for a in pages} == {"The data engineer on call"}
    titles = " ".join(a["title"] for a in pages)
    assert "BLOCKED: There are six stores, each once" in titles
    history = rows("SELECT step, status FROM ops.run_history ORDER BY started_at")[-2:]
    assert history == [
        {"step": "clean", "status": "ok"}, {"step": "gate-clean", "status": "blocked"},
    ]  # fmt: skip


# ---------------------------------------------------------------- recover: reprocessing


def test_a_file_that_did_not_change_is_skipped_and_a_corrected_one_is_loaded_again(drill):
    drill.stores("deliver", "--through", "2026-08-24")
    first = drill.night("2026-08-24")
    assert first["report"]["stores"][2]["pay"] is False  # S03, the planted refund
    with connect() as wh:
        assert load.load(wh, MON)["files"] == 0  # nothing changed: nothing is loaded
    drill.stores("fix", "--case", "1")
    drill.stores("deliver", "--day", "2026-08-24")
    with connect() as wh:
        done = load.load(wh, MON)
    assert done["loaded_again"] == ["sales_lines_S03_2026-08-24.csv"] and done["files"] == 1
    files = rows(
        "SELECT count(*) AS n FROM raw.loaded_files WHERE file_name = 'sales_lines_S03_2026-08-24.csv'"
    )
    assert files[0]["n"] == 1  # its old rows are gone. It is not there twice


# ---------------------------------------------------------------- recover: the four drills
# Each drill: a fault on Thursday 2026-08-20, a clean week. Which gate caught it, what was
# set aside, what the manager saw. Then fix the source, run that one night again, and the
# numbers equal the answer key.

DRILL_DAY = date(2026, 8, 20)


def start_drill(drill, kind: str, store: str) -> dict:
    drill.stores("fault", kind, "--store", store, "--day", "2026-08-20")
    drill.stores("deliver", "--through", "2026-08-21")
    return drill.night("2026-08-20")


def recover(drill) -> dict:
    drill.stores("fix", "--all-faults")
    drill.stores("deliver", "--day", "2026-08-20")
    return drill.night("2026-08-20")  # only that night is run again


def is_right_again(night: dict, store: str, key: dict) -> bool:
    published = night["sales"][("2026-08-20", store)]["net_sales_cents"]
    status = night["status"][("2026-08-20", store)]["status"]
    return published == key[(DRILL_DAY, store)]["net_sales_cents"] and status == "ok"


def test_drill_1_a_stores_file_arrives_twice(drill, key):
    night = start_drill(drill, "file-twice", "S03")
    copies = [r for r in night["set_aside"]["sales_lines"] if r["check_key"] == "true_key_unique"]
    assert len(copies) == key[(DRILL_DAY, "S03")]["lines"]  # every line of the second file
    assert is_right_again(night, "S03", key)  # the manager saw the right number, and "yes"
    assert sent(night) == []  # nobody needs to act. It is a message in the digest
    # Nothing needs fixing. A store cannot take a file back, so the second copy stays in the
    # landing folder for ever. Every night the gate removes its lines again, and the day is right.
    again = drill.night("2026-08-20")
    assert len(again["set_aside"]["sales_lines"]) == len(copies)
    assert is_right_again(again, "S03", key)


def test_drill_2_a_column_is_renamed(drill, key):
    night = start_drill(drill, "rename-column", "S04")
    assert (
        night["gates"]["file_is_missing_a_column"]["status"] == "fail"
    )  # caught at the landing gate
    assert [r["file_name"] for r in night["refused"]] == ["sales_lines_S04_2026-08-20.csv"]
    assert night["status"][("2026-08-20", "S04")]["problem"] == "file_refused"
    stores = {s["store_id"]: s for s in night["report"]["stores"]}
    assert stores["S04"]["pay"] is False and "amount_cents" in stores["S04"]["why_not"][0]
    assert [a["title"] for a in sent(night)] == ["S04 Thu 08-20: The sales file was refused"]
    after = recover(drill)
    assert after["refused"] == [] and is_right_again(after, "S04", key)
    closed = [a for a in after["alert_log"] if a["event"] == "resolved" and a["level"] == "ticket"]
    assert [a["title"] for a in closed] == ["S04 Thu 08-20: The sales file was refused"]


def test_drill_3_a_file_is_a_day_late(drill, key):
    night = start_drill(drill, "late-file", "S05")
    assert night["status"][("2026-08-20", "S05")]["problem"] == "no_sales_file"
    assert [a["title"] for a in sent(night)] == ["S05 Thu 08-20: No sales file arrived"]
    next_night = drill.night("2026-08-21")  # no fix is needed. The file arrives by itself
    assert is_right_again(next_night, "S05", key)
    assert next_night["gates"]["file_arrived_on_time"]["bad_rows"] == 1
    assert "S05 Thu 08-20: No sales file arrived" in [
        a["title"] for a in next_night["alert_log"] if a["event"] == "resolved"
    ]
    with connect() as wh:
        late = health.health(wh, date(2026, 8, 21))["late_files"]
    assert late == [{"store_id": "S05", "day": "2026-08-20", "hours_late": 23.7}]


def test_drill_4_a_batch_of_impossible_prices(drill, key):
    night = start_drill(drill, "bad-prices", "S06")
    assert night["gates"]["impossible_price"]["status"] == "fail"
    aside = [r for r in night["set_aside"]["sales_lines"] if r["store_id"] == "S06"]
    assert "too_many_bad_rows" in {
        r["check_key"] for r in aside
    }  # a tenth of the file: all of it goes
    assert len({r["line_id"] for r in aside}) == key[(DRILL_DAY, "S06")]["lines"]
    assert night["status"][("2026-08-20", "S06")]["problem"] == "rows_set_aside"
    assert not {s["store_id"]: s for s in night["report"]["stores"]}["S06"]["pay"]
    after = recover(drill)
    assert after["set_aside"]["sales_lines"] == [] and is_right_again(after, "S06", key)


# ---------------------------------------------------------------- the Monday answer


def test_after_the_recovery_five_stores_can_be_paid_and_one_cannot(drill, world):
    """The end of the project: yes for five stores, no for one, and the row that says why."""
    drill.stores("deliver", "--through", "2026-08-30")
    for day in WEEK:
        night = drill.night(day.isoformat())
    assert [s["store_id"] for s in night["report"]["stores"] if s["pay"]] == ["S01"]

    for case in (1, 3, 5, 6):  # four stores send it right. S04's Tuesday file never comes
        drill.stores("fix", "--case", str(case))
    drill.stores("deliver", "--through", "2026-08-30")
    for day in (MON, THU, SAT, SUN):  # only the four nights that were wrong
        night = drill.night(day.isoformat())

    stores = {s["store_id"]: s for s in night["report"]["stores"]}
    assert [s for s in stores if stores[s]["pay"]] == ["S01", "S02", "S03", "S05", "S06"]
    assert stores["S04"]["why_not"] == [
        "Tue 08-25: no sales file arrived. The tills counted $587.42"
    ]
    right = truth.week_net_sales(world, BONUS_WEEK_START)
    for store in ("S01", "S02", "S03", "S05", "S06"):
        assert stores[store]["net_sales_cents"] == right[store], store
        assert stores[store]["bonus_cents"] == truth.bonus_cents(right[store])
    open_tickets = [a["title"] for a in night["open_alerts"] if a["to_whom"] == "Head of retail operations"
                    and a["level"] == "ticket"]  # fmt: skip
    assert open_tickets == ["S04 Tue 08-25: No sales file arrived"]


def test_every_test_that_can_fail_reaches_a_person():
    """A test with no alert is a check nobody hears about. Each one is routed, or covered."""
    import re
    from pathlib import Path

    project = Path(__file__).resolve().parent.parent / "dbt_project"
    for path in project.glob("tests/*/*.sql"):
        action = re.search(r"'action': '(\w+)'", path.read_text()).group(1)
        if action == "block":
            continue  # every block pages the engineer on call
        assert path.stem in alerts.WARNINGS or path.stem in alerts.COVERED, path.stem
    assert not set(alerts.WARNINGS) & alerts.COVERED


def test_the_website_data_says_what_the_pipeline_really_does_with_each_case(week):
    """answer_key/planted_cases.json is read by the D8Loop website. Hold it against real runs."""
    import json
    from pathlib import Path

    path = Path(__file__).resolve().parents[3] / "answer_key" / "planted_cases.json"
    cases = {c["case"]: c["finished_pipeline"] for c in json.loads(path.read_text())["cases"]}
    caught_by = {
        1: "refund_gives_money_back", 2: "true_key_unique", 3: "believable_quantity",
        4: "price_of_the_day", 5: "inside_opening_hours", 6: "every_sale_has_a_product",
        7: "store_is_not_ready_to_pay", 8: "store_is_not_ready_to_pay", 9: "true_key_unique",
        10: "file_is_missing_a_column", 11: "supplier_contract", 12: "a_count_for_every_day",
        13: "one_person_once",
    }  # fmt: skip
    for case, test in caught_by.items():
        result = week["last"]["gates"][test]
        assert result["status"] == "fail", (case, test)  # the test really fires in the bonus week
        assert cases[case]["gate"] == f"gate_{result['gate']}", case
        assert cases[case]["action"] == result["action"], case
