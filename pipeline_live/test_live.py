"""The live page's server: it must run the real pipeline and tell the truth about it."""

import json
import re
import shutil
import subprocess
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import server  # noqa: E402


@pytest.fixture(scope="module")
def page(tmp_path_factory):
    """The real server on a free port, with its own data folder. Returns a function that calls it."""
    live = server.create_server(0, tmp_path_factory.mktemp("live"))
    port = live.server_address[1]
    threading.Thread(target=live.serve_forever, daemon=True).start()

    def call(path: str, body: dict | None = None, raw: bool = False):
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}",
            data=None if body is None else json.dumps(body).encode(),
            method="GET" if body is None else "POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request) as answer:
                text = answer.read().decode()
                return answer.status, text if raw else json.loads(text)
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read())

    yield call
    live.shutdown()


def stores(state: dict) -> dict:
    return {s["store_id"]: s for s in state["report"]["stores"]}


def test_it_starts_with_four_clean_weeks_and_six_stores_that_can_be_paid(page):
    status, state = page("/api/reset", {})
    assert status == 200 and state["clock"]["night"] == "2026-08-23"
    assert state["clock"]["next"] == "2026-08-24"
    assert state["report"]["can_be_paid"] == 6 and state["quarantine"]["total"] == 0
    assert all(s["equals_the_key"] for s in state["report"]["stores"])
    assert [t["state"] for t in state["tasks"]] == ["ok"] * 9


def test_monday_one_store_waits_with_its_reason_and_one_ticket_is_sent(page):
    status, state = page("/api/run", {"night": "next"})
    assert status == 200 and state["clock"]["night"] == "2026-08-24"
    s03 = stores(state)["S03"]
    assert s03["pay"] is False and not s03["equals_the_key"]
    assert s03["why_not"] == [
        "Mon 08-24: 1 row set aside ($9.49): A line adds up; A refund gives money back"
    ]
    assert state["report"]["can_be_paid"] == 5 and state["quarantine"]["total"] == 1
    assert state["alerts"]["sent"] == 1 and len(state["alerts"]["shouted"]) == 3
    ticket = state["alerts"]["open"][0]
    assert (ticket["level"], ticket["to_whom"]) == ("ticket", "Head of retail operations")
    assert ticket["runbook_url"].endswith("RUNBOOK.md#rows-were-set-aside")
    assert state["gate_summary"]["clean"]["set_aside"] == 2


def test_a_fault_you_switch_on_is_caught_at_its_gate(page):
    status, state = page(
        "/api/fault", {"kind": "rename-column", "store": "S01", "day": "2026-08-25"}
    )
    assert status == 200
    assert state["stores"]["faults"] == [
        {"kind": "rename-column", "store": "S01", "day": "2026-08-25"}
    ]
    _status, state = page("/api/run", {"night": "next"})
    refused = {r["file_name"]: r["reason"] for r in state["refused"]}
    assert (
        "amount_cents" in refused["sales_lines_S01_2026-08-25.csv"]
    )  # the landing gate refused it
    s01 = stores(state)["S01"]
    assert s01["pay"] is False and "the sales file was refused" in s01["why_not"][0]
    assert state["gate_summary"]["landing"]["refused"] >= 1
    assert "S01 Tue 08-25: The sales file was refused" in [
        a["title"] for a in state["alerts"]["open"]
    ]


def test_switch_the_fault_off_run_the_night_again_and_the_number_equals_the_key(page):
    assert page("/api/fix", {"all_faults": True})[0] == 200
    _status, state = page("/api/run", {"night": "2026-08-25"})
    s01 = stores(state)["S01"]
    assert (
        s01["pay"] is True and s01["equals_the_key"]
    )  # the extra copies of case 2 are removed too
    assert "sales_lines_S01_2026-08-25.csv" not in {r["file_name"] for r in state["refused"]}
    assert "S01 Tue 08-25: The sales file was refused" in [
        a["title"] for a in state["alerts"]["resolved"]
    ]
    assert "1 changed file loaded again" in state["tasks"][0]["text"]


def test_fix_case_1_run_monday_again_and_s03_can_be_paid(page):
    _status, state = page("/api/fix", {"case": 1})
    assert state["stores"]["fixed"] == [1]
    _status, state = page("/api/run", {"night": "2026-08-24"})
    assert state["last_run"]["day"] == "2026-08-24"
    s03 = stores(state)["S03"]
    assert s03["pay"] is True and s03["equals_the_key"]
    closed = [a["title"] for a in state["alerts"]["resolved"]]
    assert "S03 Mon 08-24: Rows were set aside, and the day does not add up" in closed
    assert any("RESOLVED" in line and "S03 Mon 08-24" in line for line in state["alerts"]["log"])
    assert stores(state)["S04"]["pay"] is False  # its Tuesday file never came. Nothing fixes that


def test_no_personal_data_reaches_the_page(page):
    for _ in range(2):  # Wednesday: the same person signs up a second time
        _status, state = page("/api/run", {"night": "next"})
    assert [m["member_id"] for m in state["quarantine"]["tables"]["members"]] == ["M-0304"]
    text = json.dumps(state)
    assert "@" not in text and "Priya" not in text


def test_nonsense_is_refused_politely(page):
    for path, body, words in [
        (
            "/api/fault",
            {"kind": "explode", "store": "S01", "day": "2026-08-30"},
            "There is no fault",
        ),
        (
            "/api/fault",
            {"kind": "no-file", "store": "S99", "day": "2026-08-30"},
            "There is no store",
        ),
        ("/api/fault", {"kind": "no-file", "store": "S01", "day": "2026-08-24"}, "has already run"),
        ("/api/fault", {"kind": "no-file", "store": "S01", "day": "2027-01-01"}, "only have days"),
        ("/api/fault", {"kind": "no-file", "store": "S01", "day": "soon"}, "is not a day"),
        ("/api/run", {"night": "2026-08-30"}, "has not come yet"),
        ("/api/fix", {"case": 99}, "from 1 to 13"),
        ("/api/fix", {}, "Say which planted case"),
    ]:
        status, answer = page(path, body)
        assert status == 400 and words in answer["error"], (path, body, answer)
    assert page("/api/nothing", {})[0] == 404


def test_a_blocked_gate_publishes_nothing_and_pages_someone(page, monkeypatch):
    """Break the stores table by hand, as milestone 4's test does, and run the night again."""
    page("/api/state")
    from lotus_pipeline.warehouse import connect

    with connect() as wh:
        wh.execute(
            "DELETE FROM raw.stores WHERE store_id = 'S06' "
            "AND _file_name = (SELECT max(_file_name) FROM raw.stores)"
        )
    _status, before = page("/api/state")
    _status, state = page("/api/run", {"night": before["clock"]["night"]})
    assert state["last_run"]["blocked"] and state["gate_summary"]["clean"]["blocked"] >= 1
    states = {t["step"]: t["state"] for t in state["tasks"]}
    assert states["gate-clean"] == "blocked" and states["publish"] == "not started"
    was = {s["store_id"]: s["net_sales_cents"] for s in before["report"]["stores"]}
    for store in state["report"]["stores"]:  # the last good numbers
        assert store["net_sales_cents"] == was[store["store_id"]]
    assert "page" in {a["level"] for a in state["alerts"]["open"]}
    assert page("/api/reset", {})[1]["report"]["can_be_paid"] == 6


def test_both_pages_are_served_and_ask_no_other_host_for_anything(page):
    for path in ("/", "/alerts"):
        status, html = page(path, raw=True)
        assert status == 200 and "<title>Lotus Retails" in html
        outside = re.findall(r"""(?:src|href)=["'](https?://[^"']+)""", html)
        assert outside == [], (path, outside)
        assert "googleapis" not in html and "cdn" not in html.lower()
    assert server.RUNBOOK.startswith("https://github.com/vijaysngrajput/")


@pytest.mark.parametrize("name", ["index.html", "alerts.html"])
def test_the_script_of_each_page_parses(name, tmp_path):
    if not shutil.which("node"):
        pytest.skip("node is not installed")
    script = "\n".join(re.findall(r"<script>(.*?)</script>", (HERE / name).read_text(), re.S))
    path = tmp_path / "page.js"
    path.write_text(script)
    done = subprocess.run(["node", "--check", str(path)], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
