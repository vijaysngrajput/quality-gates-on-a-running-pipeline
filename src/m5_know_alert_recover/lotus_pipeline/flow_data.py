"""Build the numbers shown on milestone_5_flow.html and milestone_5_recap.html.

The pages must never show a number typed by hand. This script runs the real pipeline,
gates, alerts and all, on private warehouses (DuckDB files in a temporary folder):

    the bonus week        seven nights, with the alerts each night sent
    the recovery          four stores send a corrected file, and four nights are run again
    four drills           a fault switched on in the stores on a clean Thursday, then fixed
    one fault that blocks planted in the lab, to see a page go out

    uv run python -m lotus_pipeline.flow_data          write the pages' data
    uv run python -m lotus_pipeline.flow_data --check  fail if a page is out of date

It never touches your own warehouse, Docker or Airflow.
"""

import csv
import json
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import Path

from lotus_pipeline import alerts, config, gates, health, history, load, pipeline, report
from lotus_pipeline.report import money
from lotus_pipeline.warehouse import connect, plain
from store_systems import files, planted, truth
from store_systems.catalog import BONUS_WEEK_START, LAST_DAY, PRODUCTS, UNLISTED_PRODUCT
from store_systems.generate import build as build_world

MILESTONE_DIR = config.MILESTONE_DIR
PROJECT_DIR = MILESTONE_DIR.parents[1]
PAGE = MILESTONE_DIR / "milestone_5_flow.html"
RECAP_PAGE = MILESTONE_DIR / "milestone_5_recap.html"
DAG_FILE = MILESTONE_DIR / "dags" / "lotus_nightly_dag.py"
DBT = MILESTONE_DIR / "dbt_project"
MARKER = re.compile(
    r'(<script id="page-data" type="application/json">)(.*?)(</script>)', flags=re.DOTALL
)
WEEK = [BONUS_WEEK_START + timedelta(days=n) for n in range(7)]
MON, TUE, WED, THU, FRI, SAT, SUN = WEEK
LAB_DAY = BONUS_WEEK_START - timedelta(days=1)  # the lab holds the four clean weeks
DRILL_DAY = date(2026, 8, 20)  # a clean Thursday, for the four drills
TASKS = ["stores_send_files", "load", "gate_landing", "clean", "gate_clean", "build",
         "gate_publish", "publish"]  # fmt: skip
NAMES = {**{p: v[0] for p, v in PRODUCTS.items()}, UNLISTED_PRODUCT[0]: UNLISTED_PRODUCT[1]}

QUESTION = (
    "Can I pay the bonus from last week's net sales, store by store? "
    "And if I cannot trust a number, which one, and why?"
)
NOTHING_WRONG = (
    "Nothing is still wrong. A wrong row is stopped, a person is told on the night, "
    "and a fixed day comes back right."
)
OTHER_PROJECTS = [
    ["Building a pipeline from nothing", "Daily Orders: API to Warehouse"],
    ["Checks on events as they arrive", "The clickstream project"],
    ["A lake, and table formats", "The lakehouse project"],
    ["A dashboard on top of the tables", "The warehouse project"],
    ["AI", "Retail Profit Stack"],
]
A_REAL_TEAM = [
    ["A paging service, and chat alerts", "Here the callback is real, and the alert goes to a "
     "table and a log file. A team would send the same alert to PagerDuty, Slack or email"],
    ["A quality tool", "Great Expectations or Soda, in place of hand-written tests. "
     "The reasons for each check stay the same"],
    ["A schema registry", "A service that stores every agreed schema and refuses a breaking "
     "change by itself. Here the contract is one page, written by hand"],
    ["Lineage tools", "They draw the map of what reads what. Here it was drawn by hand"],
    ["Machine-learned anomaly detection", "For patterns a four-week average cannot see: "
     "a slow drift, a holiday that moves"],
]  # fmt: skip

# The words this milestone teaches. The meanings are the same as in GLOSSARY.md.
WORDS = {
    "Run history": "What the pipeline remembers about its own runs: which night, which step, "
    "how it ended, how long it took.",
    "Freshness": "How new the data is. A freshness check asks: did the newest data arrive in time?",
    "Volume": "How much data arrived. A volume check asks: is the row count near the usual count?",
    "SLI, SLO and SLA": "An SLI is a number you measure, such as the share of files that arrived "
    "by 01:00. An SLO is the target for it. An SLA is a promise to other people, with a cost "
    "when it is broken.",
    "Observability": "Being able to tell from outside whether the data is right, not only "
    "whether the job ran. It watches five signals: freshness, volume, schema, distribution "
    "and lineage. Also called the five pillars.",
    "Alert": "A message to a person that something needs them: what failed, how bad it is, "
    "what to do.",
    "Callback": "A function Airflow calls by itself when a task changes state, such as when "
    "it fails.",
    "Routing": "Sending each alert to the person who can act on it.",
    "Alert fatigue": "People get so many alerts that they stop reading them.",
    "Runbook": "A short note that says what an alert means and the first things to check.",
    "Incident": "A time when the data was wrong or late for the people who use it.",
    "Root cause": "The first thing that went wrong, the one that made the others happen.",
    "Reprocessing": "Loading and building the same data again, after the source was corrected.",
    "Backfill": "Running the pipeline again for past days, with one command.",
    "Safe re-run": "A run that can be repeated and gives the same result: nothing is counted "
    "twice.",
    "Incident note": "A short written record of an incident: what happened, who was hurt, why, "
    "and what was changed. Also called a post-mortem.",
}

# Words from milestone 4. The first steps of a case walk through the gates again and recall them.
RECALLED = {
    "Gate": "A step in the pipeline that runs checks and decides whether the data may go on.",
    "Gate placement": "Where a gate stands: when the files land, after cleaning, or before "
    "publish. The same check protects different things in each place.",
    "Generic test": "A dbt test that is ready-made and switched on with one line, "
    "such as `not_null` or `unique`.",
    "Severity": "How serious a failed test is for dbt: `error` stops the gate, "
    "`warn` lets the run go on.",
    "Stored failures": "The rows a test returned, kept by dbt in a table named after the test.",
    "Quarantine": "Bad rows are set aside in their own table with the reason, "
    "and the good rows carry on.",
    "Write, audit, publish": "Build the output in a side table, check it there, "
    "and only then show it to its readers.",
    "Partial load": "A load where the good rows went on and the bad ones did not. "
    "The output must say what it is missing.",
    "Rejected rows": "The rows a check refused. Also called rejects.",
    "Dead-letter table": "A table for things that could not be processed at all, "
    "such as a whole file that was refused.",
    "Warn or block": "The choice for a failed check: let the run go on and record it, "
    "or stop the run.",
    "Upstream failure": "A task that failed earlier in the DAG. Airflow does not start the "
    "tasks after it, and marks them `upstream_failed`.",
}
WORD_BOOK = {**RECALLED, **WORDS}

STAGES = [
    {"id": "files", "role": "Source", "name": "Store files", "task": "stores_send_files"},
    {"id": "raw", "role": "As received", "name": "Raw tables", "task": "load"},
    {"id": "clean", "role": "Typed, joined", "name": "Clean tables", "task": "clean"},
    {"id": "checks", "role": "The rules, in dbt", "name": "Three gates", "task": None},
    {"id": "outputs", "role": "What people read", "name": "Outputs", "task": "build, publish"},
    {"id": "people", "role": "Consumers", "name": "The people", "task": None},
]

# The three places a gate stands. In milestones 1 to 3 these were dashed lines.
GATES = [
    {"id": "landing", "before": "raw", "task": "gate_landing", "when": "when the files land",
     "can": "It can refuse a whole file", "state": ""},
    {"id": "clean", "before": "checks", "task": "gate_clean", "when": "after cleaning",
     "can": "It sets bad rows aside, and blocks on a broken reference table", "state": ""},
    {"id": "publish", "before": "outputs", "task": "gate_publish",
     "when": "before the outputs are published",
     "can": "It blocks on a difference nobody can explain", "state": ""},
]  # fmt: skip

ACTION_WORDS = {
    "block": "block",
    "quarantine": "set aside",
    "refuse_file": "file refused",
    "warn": "warning",
}
EMPTY = {"count": "", "cards": []}


# ---------------------------------------------------------------- small helpers


def _day(day: date) -> str:
    return f"{day:%a %m-%d}"


def _count(n: int, thing: str) -> str:
    return f"{n} {thing}" if n == 1 else f"{n} {thing}s"


def _qty(value) -> str:
    number = float(value)
    return f"{number:g}"


def _mask(email: str) -> str:
    """An email with only its first letter and its domain: personal data stays off the page."""
    name, _, domain = email.strip().partition("@")
    return f"{name[:1]}•••@{domain}"


def _card(title: str, amount: str, sub: str, tone: str = "") -> dict:
    return {"id": title, "amount": amount, "sub": sub, "tone": tone}


def _rows(sql: str, params: tuple = ()) -> list[dict]:
    with connect() as wh:
        found = [{k: plain(v) for k, v in r.items()} for r in wh.query(sql, params)]
    return sorted(found, key=lambda r: json.dumps(r, sort_keys=True, default=str))


def _table(
    rows: list[dict], caption: str, limit: int = 4, columns: list[str] | None = None
) -> dict:
    """A few rows as a small table for the page. Cents become dollars."""
    if not rows:
        return {"caption": caption, "columns": [], "rows": [], "more": 0}
    columns = columns or list(rows[0])

    def show(column: str, value) -> str:
        if value is None:
            return ""
        if column.endswith("_cents"):
            return money(int(value))
        return str(value)

    return {
        "caption": caption,
        "columns": [c.removesuffix("_cents") for c in columns],
        "rows": [[show(c, r.get(c)) for c in columns] for r in rows[:limit]],
        "more": max(len(rows) - limit, 0),
    }


def _read_csv(landing: Path, table: str, name: str) -> tuple[list[str], list[dict]]:
    path = landing / table / name
    if not path.exists():
        return [], []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        return list(reader.fieldnames or []), list(reader)


def _line_card(row: dict, tone: str = "", sub: str | None = None) -> dict:
    text = sub or f"{row['line_type']} · qty {_qty(row['quantity'])}"
    return _card(NAMES.get(row["product_id"], row["product_id"]), money(int(row["amount_cents"])),
                 text, tone)  # fmt: skip


def _tasks(done: int, running: str | None = None, failed: str | None = None) -> dict:
    """The state Airflow gives each task: the first `done` succeeded."""
    states = {}
    for i, name in enumerate(TASKS):
        if failed and i > TASKS.index(failed):
            states[name] = "upstream_failed"
        elif name == failed:
            states[name] = "failed"
        elif i < done:
            states[name] = "success"
        elif name == running:
            states[name] = "running"
        else:
            states[name] = ""
    return states


def _test_file(gate: str, name: str) -> dict:
    """Where dbt holds one rule: the file, what its config tells dbt, and the query."""
    path = DBT / "tests" / gate / f"{name}.sql"
    text = path.read_text(encoding="utf-8")
    config_line = next(line for line in text.splitlines() if line.startswith("{{ config("))
    sql = text.split(config_line, 1)[1].strip()
    sql = re.sub(r"\{\{ source\('(\w+)', '(\w+)'\) \}\}", r"\1.\2", sql)
    sql = re.sub(r"\{\{ ref\('(\w+)'\) \}\}", r"prepared.\1", sql)
    severity = re.search(r"severity='(\w+)'", config_line).group(1)
    action = re.search(r"'action': '(\w+)'", config_line).group(1)
    return {
        "path": f"dbt_project/tests/{gate}/{name}.sql",
        "config": f"severity='{severity}', and in meta: action '{action}'",
        "sql": sql,
    }


EMPTY = {"count": "", "cards": []}
NO_ALERTS = {"counts": "", "cards": []}

# ---------------------------------------------------------------- running a night


def _gate(result: gates.GateResult) -> dict:
    """What one gate found: every test, and the stored rows of the tests that failed."""
    tests, stored = [], {}
    for t in result.tests:
        if t.kind != "data test":
            continue
        tests.append({"name": t.name, "title": t.title, "number": t.number, "action": t.action,
                      "lens": t.lens, "protects": t.protects, "failed": t.failed,
                      "bad_rows": t.bad_rows})  # fmt: skip
        if t.failed and t.name != "supplier_contract":
            stored[t.name] = _rows(f"SELECT * FROM {t.table}")
    return {
        "gate": result.gate,
        "tests": tests,
        "stored": stored,
        "blocked": [t.name for t in result.blocked_by],
        "refused": result.refused,
    }


def _alert(a) -> dict:
    return {"level": a.level, "to": a.to_whom, "title": a.title, "how_bad": a.how_bad,
            "do": a.what_to_do, "runbook": a.runbook}  # fmt: skip


def _send(day: date, failed_task: str | None = None) -> dict:
    """Send tonight's alerts for real, and keep what happened as plain values."""
    with connect() as wh:
        done = alerts.send(wh, day, failed_task)
    return {
        "sent": [_alert(a) for a in done["sent"]],
        "digest": [_alert(a) for a in done["digest"]],
        "still_open": [_alert(a) for a in done["still_open"]],
        "resolved": sorted(
            (
                {"level": r["level"], "to": r["to_whom"], "title": r["title"]}
                for r in done["resolved"]
            ),
            key=lambda r: r["title"],
        ),
        "shouted": done["shouted"],
    }


def _history(day: date) -> list[dict]:
    """The run history up to `day`, one entry per night of its week (or of the drill's days)."""
    first = min(report.week_start(day), day)
    with connect() as wh:
        nights = history.last_run_of_each_night(wh, first, day)
    return [
        {"day": _day(n["run_day"]), "published": bool(n["published"]), "runs": int(n["runs"]),
         "blocked_at": n["blocked_at"]}
        for n in nights
    ]  # fmt: skip


def _snapshot(day: date) -> dict:
    with connect() as wh:
        bonus = report.bonus_report(wh, day)
        healthy = health.health(wh, day)
    healthy.pop("night_runs")  # how many seconds a night took is different on every computer
    healthy.pop("slowest_night_seconds")
    return {
        "report": bonus,
        "health": healthy,
        "sales": {
            (r["sales_date"], r["store_id"]): r
            for r in _rows("SELECT * FROM output.daily_store_sales")
        },
        "status": {
            (r["sales_date"], r["store_id"]): r
            for r in _rows("SELECT * FROM output.store_day_status")
        },
        "aside_lines": _rows(
            "SELECT line_id, store_id, file_day, product_id, quantity, amount_cents, line_type, "
            "sold_at, check_key, reason FROM quarantine.sales_lines"
        ),
        "aside_raw": _rows("SELECT * FROM quarantine.raw_sales_lines"),
        "refused": _rows("SELECT * FROM ops.refused_files"),
        "open_tickets": _rows(
            "SELECT title, to_whom FROM ops.alerts WHERE resolved_night IS NULL AND level = 'ticket'"
        ),
    }


def _run_night(day: date, first: str = "load") -> dict:
    """Run one night step by step, as pipeline.run does, and write down what happened."""
    night = {"day": day.isoformat(), "gates": {}, "blocked_at": None, "load": None}
    for step in pipeline.STEPS[pipeline.STEPS.index(first) :]:
        if step == "report":  # the report is read later, from the warehouse
            continue
        started = datetime.now()
        if step.startswith("gate-"):
            gate = step.removeprefix("gate-")
            found = _gate(gates.run_gate(gate, day))
            night["gates"][gate] = found
            with connect() as wh:
                history.record(
                    wh, day, step, "blocked" if found["blocked"] else "ok", started, step
                )
            if found["blocked"]:
                night["blocked_at"] = gate
                night["alerts"] = _send(day, failed_task=step)  # someone is still told
                night["history"] = _history(day)
                return night
            continue
        if step == "load":
            with connect() as wh:
                night["load"] = load.load(wh, day)
        elif step == "alerts":
            night["alerts"] = _send(day)
        else:
            pipeline.RUN_STEP[step](day)
        with connect() as wh:
            history.record(wh, day, step, "ok", started, step)
        if step == "clean":  # what milestone 1 would have published: no gate has acted yet
            night["ungated"] = {
                (r["sales_date"], r["store_id"]): int(r["cents"])
                for r in _rows(
                    "SELECT sales_date, store_id, sum(amount_cents) AS cents "
                    "FROM clean.sales_lines GROUP BY sales_date, store_id"
                )
            }
    night.update(_snapshot(day))
    night["history"] = _history(day)
    return night


def _point_at(folder: Path, landing: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    config.WAREHOUSE = "duckdb"
    config.DUCKDB_PATH = folder / "lotus.duckdb"
    config.LANDING = landing
    config.DBT_TARGET = folder / "dbt_target"
    config.ALERTS_LOG = folder / "alerts.log"


def _copy(base: Path, folder: Path) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    for name in ("lotus.duckdb", "lotus.duckdb.wal"):
        if (base / name).exists():
            shutil.copy(base / name, folder / name)


# ---------------------------------------------------------------- what the page shows of a night


def alert_cards(found: dict, about: str = "") -> dict:
    """Tonight's alerts as cards for the alerts lane. The ones about this case come first."""

    def first(items: list[dict]) -> list[dict]:
        return (
            sorted(items, key=lambda a: (about not in a["title"], a["title"])) if about else items
        )

    cards = [
        {"level": a["level"], "to": a["to"], "title": a["title"], "do": a["do"], "state": "sent"}
        for a in first(found["sent"])
    ]
    cards += [
        {"level": r["level"], "to": r["to"], "title": r["title"], "do": "", "state": "resolved"}
        for r in first(found["resolved"])
    ]
    cards += [
        {"level": a["level"], "to": a["to"], "title": a["title"], "do": a["do"], "state": "digest"}
        for a in first(found["digest"])
    ]
    return {
        "counts": f"{len(found['sent'])} sent · {len(found['still_open'])} still open, not sent "
        f"again · {len(found['resolved'])} resolved · {len(found['digest'])} in the digest",
        "cards": cards[:4],
    }


def health_lines(h: dict) -> list[dict]:
    """The four health numbers, each with its target and whether it was met."""
    fresh = h["files_on_time_percent"] >= h["files_on_time_target"]
    lines = [
        {"mark": "ok" if h["nights_published"] == h["nights"] else "miss", "label": "Did it run?",
         "text": f"{h['nights_published']} of {h['nights']} nights published"},
        {"mark": "ok" if fresh else "miss", "label": "Is the data fresh?",
         "text": f"{h['files_on_time']} of {h['files_expected']} sales files by 01:00: "
         f"{h['files_on_time_percent']}%. Target {h['files_on_time_target']}%"},
        {"mark": "", "label": "Did every store report?",
         "text": f"{h['store_days_ok']} of {h['store_days']} store days can be paid from"},
    ]  # fmt: skip
    for f in h["late_files"]:
        lines.append({"mark": "miss", "label": "late", "text": f"{f['store_id']} {f['day'][5:]}, "
                      f"{f['hours_late']} hours after it was due"})  # fmt: skip
    for f in h["missing_files"]:
        lines.append(
            {"mark": "miss", "label": "missing", "text": f"{f['store_id']} {f['day'][5:]}"}
        )
    return lines


# ---------------------------------------------------------------- the steps of a case


class Case:
    """One fault, followed through the pipeline. Steps are added in order."""

    def __init__(self, cid: str, night: dict, **facts):
        self.night = night
        self.steps: list[dict] = []
        self.data = {"id": cid, "lab": False, "table_is_right": False, "note": "", **facts}
        self.gates_run: list[str] = []
        self.gate_state = {"landing": "", "clean": "", "publish": ""}
        self.aside: dict = dict(EMPTY)
        self.dead: dict = dict(EMPTY)
        self.seen: dict = {}
        self.day = date.fromisoformat(night["day"])
        self.alerts: dict = dict(NO_ALERTS)
        self.history: list = []
        self.health: list = []

    def clock(self, kind: str) -> str:
        run = self.day + timedelta(days=1)
        return {
            "evening": f"{_day(self.day)}, the stores close",
            "night": f"{_day(run)}, 02:00",
            "morning": f"{_day(run)}, 08:00",
        }.get(kind, kind)  # or any text, for the days after

    def gate_lines(self) -> list[dict]:
        """The three gates, as the middle stage shows them at this step."""
        lines = []
        for g in GATES:
            found = self.night["gates"].get(g["id"])
            if g["id"] not in self.gates_run or not found:
                lines.append({"gate": g["task"], "text": "waiting", "tone": ""})
                continue
            failed = [t for t in found["tests"] if t["failed"]]
            if not failed:
                text, tone = f"{len(found['tests'])} tests, all passed", "done"
            else:
                parts = {}
                for t in failed:
                    parts[ACTION_WORDS[t["action"]]] = parts.get(ACTION_WORDS[t["action"]], 0) + 1
                text = f"{len(found['tests'])} tests: " + ", ".join(
                    f"{n} {word}" for word, n in parts.items()
                )
                tone = "failed" if found["blocked"] else "acted"
            lines.append({"gate": g["task"], "text": text, "tone": tone})
        return lines

    def tests_now(self, now: str | None = None) -> dict:
        failed = {}
        for gate in self.gates_run:
            for t in self.night["gates"].get(gate, {"tests": []})["tests"]:
                if t["failed"]:
                    failed[t["name"]] = {"bad_rows": t["bad_rows"], "action": t["action"]}
        return {"run": list(self.gates_run), "failed": failed, "now": now}

    def add(self, sid: str, active: str, title: str, text: str, word: str, clock: str,
            tasks: dict, stages: dict, tone: str = "", now: str | None = None, **more) -> None:  # fmt: skip
        # A stage keeps showing its rows until a later step changes them. The outputs and
        # the people only show what this step says: an old report must not linger.
        self.seen.update({k: v for k, v in stages.items() if k in ("files", "raw", "clean")})
        full = {
            s["id"]: stages.get(s["id"], self.seen.get(s["id"], dict(EMPTY)))
            for s in STAGES
            if s["id"] != "checks"
        }
        full["checks"] = {"lines": self.gate_lines()}
        self.steps.append(
            {
                "id": sid, "active": active, "title": title, "text": text, "tone": tone,
                "word": {"term": word, "meaning": WORD_BOOK[word], "new": word in WORDS},
                "clock": self.clock(clock), "tasks": tasks, "stages": full,
                "gates": dict(self.gate_state), "tests": self.tests_now(now),
                "quarantine": dict(self.aside), "dead": dict(self.dead),
                "alerts": dict(self.alerts), "history": list(self.history),
                "health": list(self.health), "callback": more.pop("callback", ""),
                "moves": more.pop("moves", []), "plant": more.pop("plant", False), **more,
            }
        )  # fmt: skip

    def ran(self, gate: str) -> dict:
        """Mark a gate as run, and say how it ended."""
        self.gates_run.append(gate)
        found = self.night["gates"][gate]
        failed = [t for t in found["tests"] if t["failed"]]
        self.gate_state[gate] = "blocked" if found["blocked"] else "acted" if failed else "passed"
        return found

    def caught(self, gate: str, name: str, rows: list[dict] | None = None, caption: str = "",
               columns: list[str] | None = None) -> dict:  # fmt: skip
        """What the page shows when one dbt test fails: its file, its config, its rows."""
        found = self.night["gates"][gate]
        test = next(t for t in found["tests"] if t["name"] == name)
        where = _test_file(gate, name)
        stored = found["stored"][name] if rows is None else rows
        return {
            "facts": [
                ["dbt holds the rule", where["path"]],
                ["It tells dbt", where["config"]],
                ["Airflow runs it", f"task {next(g['task'] for g in GATES if g['id'] == gate)}, "
                 "with the command python -m lotus_pipeline gate-" + gate],
                ["It protects", test["protects"] or "Nobody was named. It is a standard check"],
            ],
            "sql": where["sql"],
            "rows": _table(stored, caption or f"The rows dbt stored in audit.{name}", columns=columns),
        }  # fmt: skip

    def finish(self) -> dict:
        self.data["steps"] = self.steps
        return self.data


def _status_text(row: dict | None) -> str:
    if row is None:
        return "no day is expected"
    if row["status"] == "ok":
        return "ok: the receipts add up to the tills"
    return (
        "do not pay yet: "
        + {
            "no_sales_file": "no sales file arrived",
            "file_refused": "the sales file was refused",
            "rows_set_aside": f"{_count(int(row['rows_set_aside']), 'row')} set aside "
            f"({money(abs(int(row['cents_set_aside'])))})",
            "no_till_total": "no till total to compare with",
            "unexplained": "the two counts differ, and nobody knows why",
        }[row["problem"]]
    )


def _people(night: dict, store: str) -> dict:
    """The bonus report of this night, as the people stage shows it."""
    stores = night["report"]["stores"]
    cards = []
    for s in stores:
        if s["store_id"] != store and s["pay"]:
            continue
        cards.append(
            _card(
                f"{s['store_id']} · {s['manager_name']}",
                money(s["bonus_cents"]) if s["pay"] else "–",
                "Pay? yes" if s["pay"] else "DO NOT PAY YET. " + " ".join(s["why_not"]),
                "good" if s["pay"] else "bad",
            )
        )
    ready = sum(s["pay"] for s in stores)
    return {
        "count": f"The bonus report: {ready} of {len(stores)} stores can be paid",
        "cards": cards[:4],
    }


def _outputs(night: dict, store: str, day: date, staged: bool = False) -> dict:
    sales = night["sales"].get((day.isoformat(), store))
    status = night["status"].get((day.isoformat(), store))
    cards = []
    if sales:
        cards.append(_card(f"{store} · {_day(day)}", money(int(sales["net_sales_cents"])),
                           f"{sales['receipts']} receipts · {sales['lines']} lines",
                           "good" if status and status["status"] == "ok" else "changed"))  # fmt: skip
    cards.append(_card("store_day_status", "", _status_text(status),
                       "good" if status and status["status"] == "ok" else "bad"))  # fmt: skip
    return {
        "count": "daily_store_sales",
        "staging": "staging: written, not yet published" if staged else "published",
        "cards": cards,
    }


# ---------------------------------------------------------------- the sales cases


def _sales_case(cid: str, night: dict, landing: Path, key: dict, *, store: str, day: date,
                name: str, hint: str, story: str, tests: list[str], word: str,
                copies: bool = False) -> Case:  # fmt: skip
    """A fault in one store's sales file that the clean gate sets aside. The first act."""
    iso = day.isoformat()
    right = key[(day, store)]["net_sales_cents"]
    old = night["ungated"].get((iso, store), 0)
    aside = [r for r in night["aside_lines"] if r["store_id"] == store and r["file_day"] == iso]
    raw_aside = [r for r in night["aside_raw"] if r["store_id"] == store and r["file_day"] == iso]
    bad_ids = sorted({r["line_id"] for r in aside} | {r["line_id"] for r in raw_aside})
    reasons = sorted({r["reason"] for r in aside + raw_aside})
    _columns, sent = _read_csv(landing, "sales_lines", f"sales_lines_{store}_{iso}.csv")
    _c, again = _read_csv(landing, "sales_lines", f"sales_lines_{store}_{iso}_2.csv")
    sent += again
    bad = [r for r in sent if r["line_id"] in bad_ids]
    good = [r for r in sent if r["line_id"] not in bad_ids][:3]
    manager = next(s for s in night["report"]["stores"] if s["store_id"] == store)
    status = night["status"][(iso, store)]
    published = int(night["sales"][(iso, store)]["net_sales_cents"])
    ok = status["status"] == "ok"

    case = Case(
        cid, night, label=f"Case {cid}", store=store, hint=hint, name=name,
        where=f"{_day(day)} · store {store}, {manager['store_name']}", table="sales_lines",
        output="daily_store_sales", says_label="Before the gates, the report said",
        key_label="The answer key says", pipeline_says=money(old),
        answer_key_says=money(right),
        caught_by=[f"gate_clean · {t.replace('_', ' ')} · set aside" for t in tests],
    )  # fmt: skip
    bad_cards = [_line_card(r, "bad") for r in bad[:3]]
    files_stage = {
        "count": f"{len(sent)} lines from {store}",
        "cards": bad_cards + [_line_card(r) for r in good],
    }
    case.add("plant", "files", "What the store sent", story, "Gate", "evening",
             _tasks(0, "stores_send_files"), {"files": files_stage}, plant=True)  # fmt: skip

    wrong_by = old - right
    case.add(
        "before", "people", f"First, the old run: five green checks, and {money(old)} reaches the report",
        f"This is milestone 1 again. The five standard checks pass. Today three of them are "
        f"generic tests in dbt, and they still pass. The report says {money(old)} for {store} "
        f"on {_day(day)}. The receipts say {money(right)}. "
        + (f"That is {money(abs(wrong_by))} too {'high' if wrong_by > 0 else 'low'}, and "
           f"{manager['manager_name']} is paid from it." if wrong_by else "The number is right."),
        "Generic test", "morning", _tasks(0),
        {"files": files_stage,
         "outputs": {"count": "daily_store_sales, with no gate", "staging": "",
                     "cards": [_card(f"{store} · {_day(day)}", money(old), "what milestone 1 published", "bad"),
                               _card("Answer key", money(right), "from the till journal", "good")]},
         "people": {"count": "The old report", "cards": [
             _card(f"{store} · {manager['manager_name']}", money(report.bonus_cents(old)),
                   f"bonus on {money(old)}. Checks: 5 of 5 passed", "bad")]}},
        tone="problem",
    )  # fmt: skip

    landing_gate = case.ran("landing")
    refused = night["gates"]["landing"]["refused"]
    case.dead = (
        {
            "count": _count(len(refused), "file") + " refused",
            "cards": [_card(f, "", "refused at the door", "bad") for f in refused],
        }
        if refused
        else dict(EMPTY)
    )
    case.add(
        "landing", "raw", "Now the same night, with gates. The first one stands where the files land",
        f"Airflow loads the files, then starts gate_landing. It runs {len(landing_gate['tests'])} "
        "tests on the raw tables and on the files' own columns. This file has every column, so "
        "it goes on. A gate this early cannot judge a value yet: every value is still text.",
        "Gate placement", "night", _tasks(3),
        {"files": files_stage, "raw": {"count": f"{len(sent)} lines from {store}",
                                       "cards": bad_cards + [_line_card(r) for r in good]}},
        moves=[{"from": "files", "to": "raw"}],
    )  # fmt: skip

    clean_stage = {"count": f"{len(sent) - len(raw_aside)} lines from {store}",
                   "cards": [_line_card(r, "bad") for r in bad if r["line_id"] not in {x["line_id"] for x in raw_aside}][:3]
                   + [_line_card(r) for r in good]}  # fmt: skip
    case.add(
        "clean", "clean", "Cleaning gives every value its type. The bad rows are still there",
        "The clean step builds the clean tables again from the raw rows, every night. So every "
        "bad row is back in the table, and is judged again."
        + (" The lines with no product did not get this far: the join dropped them. "
           "The gate will go back to the raw rows for them." if raw_aside else ""),
        "Gate", "night", _tasks(4, "gate_clean"),
        {"raw": {"count": f"{len(sent)} lines from {store}", "cards": bad_cards},
         "clean": clean_stage},
        moves=[{"from": "raw", "to": "clean"}],
    )  # fmt: skip

    clean_gate = case.ran("clean")
    first = tests[0]
    stored = [r for r in clean_gate["stored"][first] if r.get("store_id") == store]
    case.add(
        "caught", "tests", f"gate_clean: the test \"{first.replace('_', ' ')}\" fails",
        f"dbt runs {len(clean_gate['tests'])} tests on the clean tables. This one returns "
        f"{_count(len(stored), 'row')} for {store}. dbt keeps those rows in a table named after "
        "the test. Its severity is warn, so dbt itself goes on. The file also says what the "
        "pipeline must do: set the rows aside."
        + (f" {len(tests) - 1} more test{'s' if len(tests) > 2 else ''} caught the same fault: "
           + ", ".join(t.replace("_", " ") for t in tests[1:]) + "." if len(tests) > 1 else ""),
        word, "night", _tasks(4, "gate_clean"), {"clean": clean_stage}, tone="caught", now=first,
        **case.caught("clean", first, stored),
    )  # fmt: skip

    aside_cards = [_line_card(r, "bad", "; ".join(reasons)) for r in bad[:3]]
    case.aside = {
        "count": f"{_count(len(bad_ids), 'row')} set aside from {store}",
        "cards": aside_cards,
    }
    left = [_line_card(r, "good") for r in good]
    verb = "the extra copies leave the table" if copies else "the bad rows leave the table"
    case.add(
        "aside", "quarantine", f"The task acts: {_count(len(bad_ids), 'row')} set aside, and the good rows go on",
        f"Now {verb}. They go to the quarantine table with the reason: {'; '.join(reasons)}. "
        "Nothing is deleted and nothing is guessed. A person can read every row that was set "
        "aside. The run goes on with the rows that are left.",
        "Quarantine", "night", _tasks(5),
        {"clean": {"count": f"{len(sent) - len(bad_ids)} lines from {store} are left", "cards": left}},
        tone="caught", moves=[{"from": "clean", "to": "quarantine"}],
    )  # fmt: skip

    case.ran("publish")
    till = int(status["till_cents"])
    outputs = _outputs(night, store, day, staged=True)
    case.add(
        "audit", "outputs",
        "Write, audit, publish: " + ("the day adds up to the tills again" if ok else "the day no longer adds up, and the output says so"),
        f"The build task writes tonight's outputs into staging, where nobody reads them. "
        f"gate_publish checks them there. For {store} on {_day(day)} the lines that are left add "
        f"up to {money(published)}. The tills counted {money(till)}. "
        + ("They are equal, to the cent. Removing a copy repaired the day." if ok else
           f"They are {money(abs(published - till))} apart, and the rows set aside explain it. "
           "So the gate does not block. It marks the day: do not pay yet."),
        "Write, audit, publish", "night", _tasks(6, "gate_publish"),
        {"clean": {"count": f"{len(sent) - len(bad_ids)} lines from {store} are left", "cards": left},
         "outputs": outputs},
        tone="" if ok else "caught", moves=[{"from": "clean", "to": "outputs"}],
    )  # fmt: skip

    people = _people(night, store)
    outputs = _outputs(night, store, day)
    pay = manager["pay"]
    case.add(
        "people", "people",
        f"{manager['manager_name']}: " + ("can be paid" if pay else "do not pay yet, and here is the row that says why"),
        "The publish task copies the checked tables over the tables people read. The bonus report "
        "now answers both sentences of the Monday question. "
        + (f"{store} can be paid: its number equals the answer key, {money(right)}." if pay and ok else
           f"{store} is held back. " + " ".join(manager["why_not"]) + ". The other stores are not held up by it."),
        "Rejected rows" if ok else "Partial load", "morning", _tasks(8),
        {"outputs": outputs, "people": people}, tone="" if pay else "caught",
        moves=[{"from": "outputs", "to": "people"}],
    )  # fmt: skip
    case.data["outcome"] = f"{_count(len(bad_ids), 'row')} set aside. " + (
        f"The day is right again, {money(published)}, and {store} can be paid for it"
        if ok
        else f"{store}: do not pay yet"
    )
    case.facts = {"store": store, "day": day, "manager": manager, "right": right, "bad": bad,
                  "bad_ids": bad_ids, "people": people, "outputs": outputs, "sent": len(sent)}  # fmt: skip
    return case


# ---------------------------------------------------------------- the build of everything


def count_tests() -> int:
    """How many tests this milestone has, counted by pytest itself."""
    listing = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q"],
        cwd=MILESTONE_DIR, capture_output=True, text=True, check=False,
    ).stdout  # fmt: skip
    found = re.search(r"(\d+) tests? collected", listing)
    return int(found.group(1)) if found else 0


def dag_tasks() -> list[str]:
    """The task ids, read from the DAG file itself."""
    text = DAG_FILE.read_text(encoding="utf-8")
    arrows = re.search(r"\(\s*(stores_send_files >>.*?publish)\s*\)", text, re.S).group(1)
    return [t.strip() for t in arrows.split(">>")]


def glossary_words() -> int | None:
    """Every word of the five milestones, counted in GLOSSARY.md. None until milestone 5 is in it."""
    text = (PROJECT_DIR / "GLOSSARY.md").read_text(encoding="utf-8")
    if "## Milestone 5" not in text:
        return None
    return len([line for line in text.splitlines() if line.startswith("| **")])


MILESTONES = [
    ("m1_green_checks_wrong_number", "Green checks, wrong number"),
    ("m2_checks_with_a_reason", "Checks with a reason"),
    ("m3_every_table_is_different", "Every table is different"),
    ("m4_gates_and_quarantine", "Gates and quarantine"),
    ("m5_know_alert_recover", "Know, alert, recover"),
]


def journey() -> list[list[str]]:
    """The five milestones, each with its "still wrong" line, read from its README."""
    rows = []
    for number, (folder, title) in enumerate(MILESTONES, start=1):
        readme = MILESTONE_DIR.parent / folder / "README.md"
        line = "Nothing"  # milestone 5 has nothing left: it may not have a README yet
        if readme.exists():
            found = re.search(
                r"^## Still wrong\s+> (.+)$", readme.read_text(encoding="utf-8"), re.M
            )
            if found:
                line = found.group(1).strip()
            elif number < 5:
                raise ValueError(f"{readme} has no 'Still wrong' section")
        rows.append([str(number), title, line])
    return rows


@lru_cache(maxsize=1)
def _built() -> str:
    from lotus_pipeline import flow_cases

    before = (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING, config.DBT_TARGET,
              config.ALERTS_LOG)  # fmt: skip
    world = build_world()
    key = truth.daily_store_sales(world)
    try:
        with tempfile.TemporaryDirectory(prefix="lotus_m5_flow_") as temp:
            temp = Path(temp)
            landing = temp / "landing"
            files.deliver_through(LAST_DAY, landing)
            base = temp / "base"
            _point_at(base, landing)
            _run_night(LAB_DAY)

            _copy(base, temp / "week")
            _point_at(temp / "week", landing)
            nights = {day: _run_night(day) for day in WEEK}

            recovered = flow_cases.recover_the_week(temp, landing)
            drills = flow_cases.run_the_drills(temp)
            cases = flow_cases.all_cases(nights, recovered, drills, landing, temp, world, key)
            cases.append(flow_cases.block_case(temp, base, landing))
            cases.append(flow_cases.monday_answer(nights, recovered, world))
    finally:
        (config.WAREHOUSE, config.DUCKDB_PATH, config.LANDING, config.DBT_TARGET,
         config.ALERTS_LOG) = before  # fmt: skip

    last = nights[SUN]
    after = recovered[5]["night"]  # Sunday was the last night to be run again
    every_test = [
        {
            **{k: t[k] for k in ("name", "number", "action", "lens")},
            "gate": gate,
            "title": t["title"][0].upper() + t["title"][1:],
            "new": t["name"] in ("file_arrived_on_time", "impossible_price"),
        }
        for gate in ("landing", "clean", "publish")
        for t in last["gates"][gate]["tests"]
    ]
    week_rows = [
        {
            "day": _day(day),
            "can_be_paid": sum(s["pay"] for s in nights[day]["report"]["stores"]),
            "sent": len(nights[day]["alerts"]["sent"]),
            "still_open": len(nights[day]["alerts"]["still_open"]),
            "resolved": len(nights[day]["alerts"]["resolved"]),
            "digest": len(nights[day]["alerts"]["digest"]),
            "shouted": nights[day]["alerts"]["shouted"],
            "titles": [a["title"] for a in nights[day]["alerts"]["sent"]],
        }
        for day in WEEK
    ]
    right = truth.week_net_sales(world, BONUS_WEEK_START)

    def stores(night: dict) -> list[dict]:
        return [
            {"store": f"{s['store_id']} {s['store_name']}", "manager": s["manager_name"],
             "net_sales": money(s["net_sales_cents"]), "answer_key": money(right[s["store_id"]]),
             "equal": s["net_sales_cents"] == right[s["store_id"]],
             "bonus": money(s["bonus_cents"]) if s["pay"] else "–",
             "pay": s["pay"], "why_not": s["why_not"]}
            for s in night["report"]["stores"]
        ]  # fmt: skip

    h = last["health"]
    sent, shouted = sum(r["sent"] for r in week_rows), sum(r["shouted"] for r in week_rows)
    late, missing = h["late_files"][0], h["missing_files"][0]
    recap = {
        "question": QUESTION,
        "nothing_wrong": NOTHING_WRONG,
        "before": {"through": _day(SUN), "stores": stores(last),
                   "can_be_paid": sum(s["pay"] for s in last["report"]["stores"])},
        "after": {"through": _day(SUN), "stores": stores(after),
                  "can_be_paid": sum(s["pay"] for s in after["report"]["stores"]),
                  "open_tickets": sorted(t["title"] for t in after["open_tickets"]
                                         if t["to_whom"] == alerts.OPERATIONS)},
        "week": week_rows,
        "alerts_sent": sent,
        "alerts_shouted": shouted,
        "health": {"lines": health_lines(h), **{k: h[k] for k in (
            "nights", "nights_published", "files_expected", "files_on_time",
            "files_on_time_percent", "files_on_time_target", "store_days", "store_days_ok")}},
        "recovery": [
            {"case": f"Case {n}", "night": _day(r["day"]) if r["day"] == r["through"]
             else f"{_day(r['day'])} and {_day(r['through'])}",
             "fix": r["fix"], "loaded_again": r["night"]["load"]["loaded_again"],
             "resolved": [x["title"] for x in r["night"]["alerts"]["resolved"] if x["level"] == "ticket"]}
            for n, r in recovered.items()
        ],
        "drills": [
            {"drill": d["name"], "store": d["store"], "gate": d["gate"], "set_aside": d["set_aside"],
             "manager_saw": d["manager_saw"], "told": d["told"], "fixed": d["fixed"],
             "right_again": d["right_again"]}
            for d in drills.values()
        ],
        "dbt": {
            "landing": sum(t["name"] != "supplier_contract" for t in last["gates"]["landing"]["tests"]),
            "clean": len(last["gates"]["clean"]["tests"]),
            "publish": len(last["gates"]["publish"]["tests"]),
        },
        "other_projects": OTHER_PROJECTS,
        "a_real_team": A_REAL_TEAM,
        "quiz": [
            {"question": f"It is {_day(TUE + timedelta(days=1))}, 02:00. S04 sent no sales file for "
             "Tuesday. The pipeline publishes the other five stores and marks S04 \"do not pay "
             "yet\". Milestone 4 did that too. What is different tonight?",
             "answer": "Someone is told. When publish succeeds, Airflow calls a callback, and one "
             f"ticket goes to the head of retail operations: \"{week_rows[1]['titles'][-1]}\". It "
             "says how much is in doubt and what to do: ask the store's back office for the file. "
             "On Tuesday night the store can still send it. On Monday morning it is too late."},
            {"question": "The same ticket about S04 is still true on Wednesday, Thursday and "
             "Friday night. A teammate wants it sent again every night, so nobody forgets. "
             "Good idea?",
             "answer": f"No. That is how {shouted} alerts come from one week in place of {sent}: "
             "every failed test, every night. People then stop reading them, and miss the one "
             "that is new. That is alert fatigue. Send a problem once, when it is new. Count it "
             "as still open after that, and say so when it is resolved."},
            {"question": "The store sends the right file for Monday. A colleague offers to save "
             "time: change the one wrong row in the warehouse by hand. What do you say?",
             "answer": "Never repair in the warehouse. The clean tables are built again from the "
             "raw rows every night, so the hand-made fix is gone tomorrow, and nobody can see it "
             "was made. Let the store send the corrected file, and run that one night again. The "
             "load step sees that the file changed and loads it again, and a file that did not "
             "change is skipped. That is a safe re-run."},
        ],
        "words": [{"term": term, "meaning": meaning} for term, meaning in WORDS.items()],
        "journey": journey(),
        "late": f"{late['store_id']} {late['day'][5:]}, {late['hours_late']} hours late",
        "missing": f"{missing['store_id']} {missing['day'][5:]}",
    }  # fmt: skip

    flow = {
        "recap": recap,
        "milestone": 5,
        "of": 5,
        "title": "Know, alert, recover",
        "lede": "Customers of Lotus Retails pay at a till: the machine at the counter, also "
        "called a cash register. The gates of milestone 4 stop a wrong row at 02:00, and nobody "
        "hears about it. Now the pipeline remembers its runs, tells the person who can act, and "
        "takes a corrected file back. Pick a fault, and watch it stopped, told and made right.",
        "question": QUESTION,
        "dag": {"id": re.search(r'dag_id="(\w+)"', DAG_FILE.read_text()).group(1), "tasks": dag_tasks()},
        "callbacks": {"on_failure": "every task", "on_success": "publish",
                      "command": "python -m lotus_pipeline alerts --day 2026-08-24"},
        "stages": STAGES,
        "gates": GATES,
        "tests": every_test,
        "tests_note": {
            "dbt": recap["dbt"]["landing"] + recap["dbt"]["clean"] + recap["dbt"]["publish"],
            "command": "python -m lotus_pipeline gate-clean --day 2026-08-24",
        },
        "tables": ["sales_lines", "till_totals", "products", "stores", "stock_counts",
                   "supplier_prices", "members"],
        "outputs": ["daily_store_sales", "replenishment_list", "store_day_status"],
        "cases": cases,
        "words": recap["words"],
        "nothing_wrong": NOTHING_WRONG,
        "other_projects": OTHER_PROJECTS,
        "a_real_team": A_REAL_TEAM,
    }  # fmt: skip
    return json.dumps(flow, ensure_ascii=False, default=str)


def build() -> dict:
    """Everything the flow page shows, from real runs."""
    return json.loads(_built())


def build_recap(flow: dict | None = None) -> dict:
    """The numbers shown on milestone_5_recap.html."""
    flow = flow or build()
    recap = dict(flow["recap"])
    dbt = recap["dbt"]
    recap["numbers"] = {
        "notebooks": len(list((MILESTONE_DIR / "notebooks").glob("*.ipynb"))),
        "tests": count_tests(),
        "dbt_tests": dbt["landing"] + dbt["clean"] + dbt["publish"],
        "alerts_sent": recap["alerts_sent"],
        "alerts_shouted": recap["alerts_shouted"],
        "files_on_time": recap["health"]["files_on_time"],
        "files_expected": recap["health"]["files_expected"],
        "can_be_paid_before": recap["before"]["can_be_paid"],
        "can_be_paid_after": recap["after"]["can_be_paid"],
        "stores": len(recap["after"]["stores"]),
        "drills": len(recap["drills"]),
        "planted_cases": len(planted.CASES),
        "words": len(WORDS),
        "all_words": glossary_words(),
    }
    recap["dag"] = flow["dag"]
    return recap


def _read(page: Path) -> dict:
    return json.loads(MARKER.search(page.read_text(encoding="utf-8")).group(2))


def _write(page: Path, data: dict) -> None:
    payload = json.dumps(data, indent=1, ensure_ascii=False).replace("</", "<\\/")
    text = page.read_text(encoding="utf-8")
    page.write_text(
        MARKER.sub(lambda m: m.group(1) + "\n" + payload + "\n" + m.group(3), text),
        encoding="utf-8",
    )


def page_data() -> dict:
    """The data currently written inside the flow page."""
    return _read(PAGE)


def recap_page_data() -> dict:
    """The data currently written inside the recap page."""
    return _read(RECAP_PAGE)


def main() -> int:
    flow = build()
    recap = build_recap(flow)
    if "--check" in sys.argv:
        ok = page_data() == flow and recap_page_data() == recap
        print(
            "The pages match the code."
            if ok
            else "The pages are out of date. Run: uv run python -m lotus_pipeline.flow_data"
        )
        return 0 if ok else 1
    _write(PAGE, flow)
    _write(RECAP_PAGE, recap)
    print(
        f"Wrote {len(flow['cases'])} cases into {PAGE.name}, and the numbers into {RECAP_PAGE.name}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
