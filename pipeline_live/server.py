"""The live page: run the finished pipeline night by night, and break it on purpose.

    uv run python pipeline_live/server.py     then your browser opens http://127.0.0.1:8040

Nothing else needs to be running. No Docker, no Postgres, no Airflow.

It runs the real code of milestone 5: the same load, the same three gates with their dbt
tests, the same quarantine, the same alerts. Two things are special here:

- The warehouse is DuckDB, a database in one file, under data/live/.
- The stores send their files into data/live/ too. Nothing here touches the milestones.

Two pages:
    /          the live page: run a night, make a store misbehave, fix it, run it again
    /alerts    the alerts page: who was told, how urgently, and what to do
"""

import json
import os
import shutil
import sys
import threading
import webbrowser
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
sys.path.insert(0, str(PROJECT / "src" / "m5_know_alert_recover"))  # the finished pipeline

from lotus_pipeline import alerts, config, health, pipeline, report  # noqa: E402
from lotus_pipeline.gates import GateBlocked  # noqa: E402
from lotus_pipeline.warehouse import connect, plain  # noqa: E402
from store_systems import files, planted, truth  # noqa: E402
from store_systems.catalog import (  # noqa: E402
    BONUS_WEEK_START,
    FIRST_DAY,
    LAST_DAY,
    STORES,
)
from store_systems.generate import build  # noqa: E402

PORT = int(os.environ.get("LOTUS_LIVE_PORT", "8040"))
DATA = Path(os.environ.get("LOTUS_LIVE_DATA", PROJECT / "data" / "live"))
START = BONUS_WEEK_START - timedelta(days=1)  # Sunday 2026-08-23: four clean weeks are loaded
RUNBOOK = (
    "https://github.com/vijaysngrajput/quality-gates-on-a-running-pipeline/blob/main/"
    "src/m5_know_alert_recover/RUNBOOK.md"
)
KINDS = {
    "file-twice": "Send the sales file twice",
    "rename-column": "Rename a column in the sales file",
    "late-file": "Send the sales file a day late",
    "bad-prices": "Put impossible prices in every tenth line",
    "no-file": "Send no sales file",
}
EXPECT = {
    "file-twice": "The extra copies should be set aside, and the store should still be paid",
    "rename-column": "The landing gate should refuse the file, and the store should wait",
    "late-file": "The store should wait tonight, and be right one night later",
    "bad-prices": "The whole file should be set aside, and the store should wait",
    "no-file": "The store should wait, and the other five should be published",
}


class Refused(Exception):
    """A request that makes no sense. The answer is a 400 with these words."""


def _day(value) -> str:
    if isinstance(value, str):
        value = date.fromisoformat(value[:10])
    return f"{value:%a %m-%d}"


class Live:
    """Everything the page works with: private stores, a private landing folder, a private warehouse."""

    def __init__(self, data: Path = DATA):
        self.data = data
        self.lock = threading.Lock()
        self.world = build()
        self.generation = 0
        self.reset()

    # ------------------------------------------------------------ where things are

    def _point_the_pipeline_here(self) -> None:
        config.WAREHOUSE = "duckdb"
        config.LANDING = self.folder / "landing"
        config.DUCKDB_PATH = self.folder / "lotus.duckdb"
        config.DBT_TARGET = self.folder / "dbt_target"
        config.ALERTS_LOG = self.folder / "alerts.log"

    @property
    def landing(self) -> Path:
        return self.folder / "landing"

    # ------------------------------------------------------------ the buttons

    def reset(self) -> None:
        """Back to the start: empty stores, an empty warehouse, and four clean weeks loaded."""
        self.generation += 1
        self.data.mkdir(parents=True, exist_ok=True)
        for old in self.data.glob("start_*"):
            shutil.rmtree(old, ignore_errors=True)
        # A new folder every time: the warehouse file of the last start may still be open.
        self.folder = self.data / f"start_{os.getpid()}_{self.generation}"
        self._point_the_pipeline_here()
        files.deliver_through(START, self.landing)
        self.clock = START
        self.last_run = None
        self._run(START)

    def _run(self, day: date) -> None:
        """Run one night with the real pipeline, and keep what each step said."""
        self._point_the_pipeline_here()
        said: list[str] = []
        started = datetime.now()
        blocked = None
        try:
            pipeline.run(day, say=said.append)
        except GateBlocked as stop:
            blocked = str(stop)
        self.last_run = {"day": day, "said": said, "blocked": blocked, "started": started}

    def run(self, night: str | None) -> None:
        if night in (None, "", "next"):
            day = self.clock + timedelta(days=1)
            if day > LAST_DAY:
                raise Refused(
                    f"The stores only have days up to {LAST_DAY}. Run a night again, or press Reset."
                )
            files.deliver(day, self.landing, self.world)
            self.clock = day
        else:
            day = _a_day(night)
            if day > self.clock:
                raise Refused(
                    f"The night of {day} has not come yet. The clock is on {self.clock}. "
                    "Press 'Run the next night'."
                )
            if day < START:
                raise Refused(f"The earliest night you can run again is {START}.")
            # A store that was fixed sends its file again, with the same name.
            files.deliver_through(self.clock, self.landing)
        self._run(day)

    def fault(self, kind: str, store: str, day: str) -> None:
        if kind not in KINDS:
            raise Refused(f"There is no fault '{kind}'. The faults are: {', '.join(KINDS)}.")
        if store not in STORES:
            raise Refused(f"There is no store '{store}'. The stores are: {', '.join(STORES)}.")
        when = _a_day(day)
        if when <= self.clock:
            raise Refused(
                f"The night of {when} has already run. A store can only misbehave on a night "
                f"that has not come yet: {self.clock + timedelta(days=1)} or later."
            )
        state = files.read_state(self.landing)
        entry = {"kind": kind, "store": store, "day": when.isoformat()}
        if entry not in state["faults"]:
            state["faults"].append(entry)
        files.write_state(self.landing, state)

    def fix(self, case: int | None, all_faults: bool) -> None:
        state = files.read_state(self.landing)
        if all_faults:
            state["faults"] = []
        elif case is not None:
            if case not in range(1, 14):
                raise Refused("A planted case is a number from 1 to 13.")
            if case not in state["fixed"]:
                state["fixed"].append(case)
        else:
            raise Refused(
                "Say which planted case to fix, from 1 to 13, or ask for all your faults."
            )
        files.write_state(self.landing, state)
        files.deliver_through(self.clock, self.landing)  # the corrected files arrive


def _a_day(text: str) -> date:
    try:
        day = date.fromisoformat(str(text))
    except ValueError as error:
        raise Refused(f"'{text}' is not a day. Write it like 2026-08-24.") from error
    if not FIRST_DAY <= day <= LAST_DAY:
        raise Refused(f"The stores only have days from {FIRST_DAY} to {LAST_DAY}.")
    return day


# ---------------------------------------------------------------- what the pages show


def _rows(wh, sql: str, params: tuple = ()) -> list[dict]:
    return [{k: plain(v) for k, v in r.items()} for r in wh.query(sql, params)]


def _exists(wh, table: str) -> bool:
    schema, name = table.split(".")
    return bool(
        wh.query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            (schema, name),
        )
    )


def _tasks(wh, live: Live) -> list[dict]:
    """Each step of the last run: ok, blocked, or not started."""
    run = live.last_run
    done = {
        r["step"]: r
        for r in _rows(
            wh,
            "SELECT step, status, seconds, detail FROM ops.run_history "
            "WHERE run_day = ? AND started_at >= ? ORDER BY started_at",
            (run["day"], run["started"]),
        )
    }
    texts = {}
    for text in run["said"]:
        first = text.strip().splitlines()[0] if text.strip() else ""
        for step in pipeline.STEPS:
            label = step.replace("-", " ") + ":"
            if first.startswith(label):
                texts[step] = text.strip()
        if first.startswith("Lotus Retails"):
            texts["report"] = text.strip()
    tasks, stopped = [], False
    for step in pipeline.STEPS:
        row = done.get(step)
        if row is None:
            sent_anyway = step == "alerts" and "alerts" in texts
            state = "ok" if sent_anyway else "not started" if stopped else "ok"
            tasks.append(
                {"step": step, "state": state, "seconds": None, "text": texts.get(step, "")}
            )
            continue
        if row["status"] != "ok":
            stopped = True
        text = run["blocked"] if row["status"] == "blocked" else texts.get(step, row["detail"])
        tasks.append(
            {"step": step, "state": row["status"], "seconds": row["seconds"], "text": text or ""}
        )
    return tasks


def _quarantine(wh) -> dict:
    found = {
        "sales_lines": [],
        "raw_sales_lines": [],
        "supplier_prices": [],
        "stock_counts": [],
        "members": [],
    }
    if not _exists(wh, "quarantine.sales_lines"):
        return {"tables": found, "total": 0}
    found["sales_lines"] = _rows(
        wh,
        "SELECT store_id, file_day, reason, is_a_copy, count(DISTINCT line_id) AS row_count, "
        "sum(amount_cents) AS cents FROM quarantine.sales_lines "
        "GROUP BY store_id, file_day, reason, is_a_copy ORDER BY file_day DESC, store_id, reason",
    )
    found["raw_sales_lines"] = _rows(
        wh,
        "SELECT store_id, file_day, reason, product_id, count(*) AS row_count, sum(amount_cents) AS cents "
        "FROM quarantine.raw_sales_lines GROUP BY store_id, file_day, reason, product_id "
        "ORDER BY file_day DESC, store_id",
    )
    found["supplier_prices"] = _rows(
        wh,
        "SELECT supplier_id, price_date, reason, count(*) AS row_count FROM quarantine.supplier_prices "
        "GROUP BY supplier_id, price_date, reason ORDER BY price_date DESC, supplier_id",
    )
    found["stock_counts"] = _rows(
        wh,
        "SELECT store_id, count_date, reason, count(*) AS row_count FROM quarantine.stock_counts "
        "GROUP BY store_id, count_date, reason ORDER BY count_date DESC, store_id",
    )
    # A member is named by number only. No name, no email, no phone: they are not in this table.
    found["members"] = _rows(
        wh, "SELECT member_id, reason FROM quarantine.members ORDER BY member_id"
    )
    total = wh.value("SELECT count(DISTINCT line_id) FROM quarantine.sales_lines") + sum(
        wh.value(f"SELECT count(*) FROM quarantine.{t}")
        for t in ("raw_sales_lines", "supplier_prices", "stock_counts", "members")
    )
    for row in found["sales_lines"] + found["raw_sales_lines"]:
        row["day"] = _day(row["file_day"])
        row["money"] = report.money(abs(int(row["cents"] or 0)))
    return {"tables": found, "total": int(total)}


def _report(wh, live: Live, day: date) -> dict:
    """The bonus report, held up against the answer key for the same days."""
    bonus = report.bonus_report(wh, day)
    monday = report.week_start(day)
    right = truth.week_net_sales(live.world, monday, day)
    stores = []
    for s in bonus["stores"]:
        key = right[s["store_id"]]
        stores.append(
            {
                **s,
                "net_sales": report.money(s["net_sales_cents"]),
                "bonus": report.money(s["bonus_cents"]) if s["pay"] else "",
                "answer_key": report.money(key),
                "equals_the_key": s["net_sales_cents"] == key,
                "apart": report.money(s["net_sales_cents"] - key),
            }
        )
    return {
        "week": f"{_day(monday)} to {_day(day)}",
        "stores": stores,
        "can_be_paid": sum(s["pay"] for s in stores),
        "tests": bonus["tests"],
        "tests_passed": bonus["tests_passed"],
        "warnings": bonus["warnings"],
    }


def _alerts(wh, live: Live, day: date) -> dict:
    if not _exists(wh, "ops.alerts"):
        return {"tonight": [], "open": [], "resolved": [], "shouted": [], "log": [], "people": []}

    def with_link(rows: list[dict]) -> list[dict]:
        for r in rows:
            r["runbook_url"] = f"{RUNBOOK}#{r['runbook']}" if r.get("runbook") else RUNBOOK
            for name in ("first_night", "last_night", "resolved_night"):
                if r.get(name):
                    r[name + "_text"] = _day(r[name])
        return rows

    order = "CASE level WHEN 'page' THEN 0 WHEN 'ticket' THEN 1 ELSE 2 END"
    still = with_link(
        _rows(
            wh,
            f"SELECT * FROM ops.alerts WHERE resolved_night IS NULL ORDER BY {order}, first_night, title",
        )
    )
    closed = with_link(
        _rows(
            wh,
            f"SELECT * FROM ops.alerts WHERE resolved_night IS NOT NULL ORDER BY resolved_night DESC, {order}, title",
        )
    )
    tonight = _rows(
        wh,
        f"SELECT event, level, to_whom, title FROM ops.alert_log WHERE night = ? ORDER BY event, {order}, title",
        (day,),
    )
    log = config.ALERTS_LOG.read_text().splitlines()[-40:] if config.ALERTS_LOG.exists() else []
    return {
        "tonight": tonight,
        "sent": sum(a["event"] == "new" and a["level"] != "message" for a in tonight),
        "digest": sum(a["event"] == "new" and a["level"] == "message" for a in tonight),
        "still_open": sum(a["event"] == "still open" for a in tonight),
        "resolved_tonight": sum(a["event"] == "resolved" for a in tonight),
        "open": still,
        "resolved": closed,
        "shouted": alerts.shout(wh, day),
        "log": log,
        "people": sorted({a["to_whom"] for a in still + closed}),
        "runbook": RUNBOOK,
    }


def state(live: Live) -> dict:
    """Everything both pages show, read fresh from the live warehouse."""
    live._point_the_pipeline_here()
    stores_state = files.read_state(live.landing)
    day = live.last_run["day"]
    with connect() as wh:
        gates = _rows(
            wh,
            "SELECT gate, test_name, number, title, action, lens, status, bad_rows FROM ops.gate_results "
            "WHERE run_day = ? ORDER BY CASE gate WHEN 'landing' THEN 0 WHEN 'clean' THEN 1 ELSE 2 END, "
            "CASE status WHEN 'pass' THEN 1 ELSE 0 END, number",
            (day,),
        )
        refused = (
            _rows(wh, "SELECT * FROM ops.refused_files ORDER BY file_name")
            if _exists(wh, "ops.refused_files")
            else []
        )
        found = {
            "tasks": _tasks(wh, live),
            "gates": gates,
            "quarantine": _quarantine(wh),
            "refused": refused,
            # A night that is run again republishes every day. So the report and the health
            # numbers go up to the newest night, the one the clock is on.
            "report": _report(wh, live, live.clock),
            "alerts": _alerts(wh, live, day),
            "health": health.health(wh, live.clock),
            "health_text": health.format_health(health.health(wh, live.clock)),
            "history": _rows(
                wh,
                "SELECT run_day, step, status, started_at, seconds FROM ops.run_history "
                "ORDER BY started_at DESC LIMIT 45",
            ),
        }
    summary = {}
    for gate in ("landing", "clean", "publish"):
        mine = [g for g in gates if g["gate"] == gate]
        failed = [g for g in mine if g["status"] != "pass"]
        summary[gate] = {
            "tests": len(mine),
            "passed": len(mine) - len(failed),
            "blocked": sum(g["action"] == "block" for g in failed),
            "set_aside": sum(g["action"] == "quarantine" for g in failed),
            "refused": sum(g["action"] == "refuse_file" for g in failed),
            "warned": sum(g["action"] == "warn" for g in failed),
        }
    next_day = live.clock + timedelta(days=1)
    return {
        "clock": {
            "night": live.clock.isoformat(),
            "night_text": _day(live.clock),
            "next": next_day.isoformat() if next_day <= LAST_DAY else None,
            "next_text": _day(next_day) if next_day <= LAST_DAY else None,
            "first": START.isoformat(),
            "last": LAST_DAY.isoformat(),
            "nights_left": [
                (live.clock + timedelta(days=n)).isoformat()
                for n in range(1, (LAST_DAY - live.clock).days + 1)
            ],
            "nights_run": [
                (START + timedelta(days=n)).isoformat()
                for n in range((live.clock - START).days + 1)
            ],
        },
        "stores": {
            "ids": {s: STORES[s][0] for s in STORES},
            "faults": stores_state["faults"],
            "fixed": sorted(stores_state["fixed"]),
            "kinds": KINDS,
            "expect": EXPECT,
            "cases": [
                {
                    "case": c["case"],
                    "name": c["name"],
                    "day": c["day"].isoformat(),
                    "day_text": _day(c["day"]),
                    "where": c.get("store") or c.get("supplier") or "members",
                    "fixed": c["case"] in stores_state["fixed"],
                }
                for c in planted.CASES
            ],
        },
        "last_run": {
            "day": day.isoformat(),
            "day_text": _day(day),
            "blocked": live.last_run["blocked"],
            "said": live.last_run["said"],
        },
        "gate_summary": summary,
        **found,
    }


# ---------------------------------------------------------------- the web server


def make_handler(live: Live):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args) -> None:  # keep the terminal quiet
            pass

        def _send(self, status: int, body: bytes, kind: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, value, status: int = 200) -> None:
            self._send(status, json.dumps(value, default=str).encode(), "application/json")

        def _page(self, name: str) -> None:
            self._send(200, (HERE / name).read_bytes(), "text/html; charset=utf-8")

        def do_GET(self) -> None:
            path = self.path.split("?")[0].rstrip("/") or "/"
            if path == "/":
                self._page("index.html")
            elif path == "/alerts":
                self._page("alerts.html")
            elif path == "/api/state":
                with live.lock:
                    self._json(state(live))
            else:
                self._json({"error": f"There is no page {path}."}, 404)

        def do_POST(self) -> None:
            path = self.path.split("?")[0].rstrip("/")
            length = int(self.headers.get("Content-Length") or 0)
            try:
                body = json.loads(self.rfile.read(length) or b"{}") if length else {}
                if not isinstance(body, dict):
                    raise Refused("Send the request as a JSON object.")
                with live.lock:
                    if path == "/api/run":
                        live.run(body.get("night"))
                    elif path == "/api/fault":
                        live.fault(
                            str(body.get("kind")), str(body.get("store")), str(body.get("day"))
                        )
                    elif path == "/api/fix":
                        case = body.get("case")
                        if case is not None and not isinstance(case, int):
                            raise Refused("A planted case is a number from 1 to 13.")
                        live.fix(case, bool(body.get("all_faults")))
                    elif path == "/api/reset":
                        live.reset()
                    else:
                        self._json({"error": f"There is no action {path}."}, 404)
                        return
                    self._json(state(live))
            except Refused as refused:
                self._json({"error": str(refused)}, 400)
            except json.JSONDecodeError:
                self._json({"error": "The request is not valid JSON."}, 400)

    return Handler


def create_server(port: int = PORT, data: Path = DATA) -> ThreadingHTTPServer:
    live = Live(data)
    server = ThreadingHTTPServer(("127.0.0.1", port), make_handler(live))
    server.live = live
    return server


if __name__ == "__main__":
    print("Loading four clean weeks into the live warehouse. A few seconds...")
    server = create_server()
    print(f"The live pipeline is at http://127.0.0.1:{PORT}          Stop it with Ctrl+C.")
    print(f"The alerts page is at  http://127.0.0.1:{PORT}/alerts")
    if not os.environ.get("LOTUS_LIVE_NO_BROWSER"):  # set it to keep the browser closed
        threading.Timer(1.0, lambda: webbrowser.open(f"http://127.0.0.1:{PORT}")).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped. Everything it made is in data/live/. Delete that folder any time.")
