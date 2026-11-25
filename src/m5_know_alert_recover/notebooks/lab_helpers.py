"""Small helpers for the milestone 5 lab.

Nothing here is hidden magic. Open this file any time you want to see how
something works.

In this milestone the nights run under Airflow, as they do in production. So the
stores' files live where Airflow's stores live: inside the Airflow container.
"""

import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

LAB_DIR = Path(__file__).resolve().parent
MILESTONE_DIR = LAB_DIR.parent
PROJECT_DIR = MILESTONE_DIR.parents[1]
RESULTS_DIR = LAB_DIR / "results"
INCIDENTS_DIR = MILESTONE_DIR / "incidents"

# Lets a notebook write `from lotus_pipeline import ...` (the production code)
# and `import airflow_tools`.
if str(MILESTONE_DIR) not in sys.path:
    sys.path.insert(0, str(MILESTONE_DIR))

import airflow_tools  # noqa: E402
from lotus_pipeline import alerts, health, report  # noqa: E402
from lotus_pipeline.warehouse import WarehouseDown, connect, plain  # noqa: E402

DAG = airflow_tools.DAG
FIRST_NIGHT = date(2026, 8, 24)  # Monday of the bonus week
LAST_NIGHT = date(2026, 8, 30)  # Sunday of the bonus week
WEEK = [FIRST_NIGHT + timedelta(days=n) for n in range(7)]
MON, TUE, WED, THU, FRI, SAT, SUN = WEEK
DRILL_DAY = date(2026, 8, 20)  # a clean Thursday, used for the recovery drills
SCHEMAS = ("raw", "clean", "output", "ops", "staging", "quarantine", "audit", "prepared")

pd.set_option("display.max_rows", 80)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 120)
pd.set_option("display.max_columns", None)


# ---- The warehouse ----------------------------------------------------------


def sql(query: str, params: tuple = ()) -> pd.DataFrame:
    """Run one SQL query on the warehouse, and show the result as a table."""
    with connect() as wh:
        cursor = (
            wh.con.execute(query.replace("?", "%s"), params) if params else wh.con.execute(query)
        )
        names = [d[0] for d in cursor.description]
        rows = cursor.fetchall()
    return pd.DataFrame([[plain(v) for v in row] for row in rows], columns=names)


def _exists(table: str) -> bool:
    schema, name = table.split(".")
    with connect() as wh:
        return bool(
            wh.query(
                "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                (schema, name),
            )
        )


# ---- Starting again, and running nights -------------------------------------


def start_again() -> None:
    """Empty everything of this milestone: the warehouse, the stores' files, the alerts log,
    and Airflow's own memory of this DAG's runs.

    The stores go back to the planted week: every fault you switched on is off, and every
    case you fixed is broken again.
    """
    with connect() as wh:
        for schema in SCHEMAS:
            wh.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    airflow_tools.stores("reset")
    subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow", "sh", "-c", ": > /var/lotus/alerts.log"],
        cwd=PROJECT_DIR, capture_output=True, text=True, check=False,
    )  # fmt: skip
    # Airflow forgets the runs of this DAG. It reads the DAG file again within seconds.
    airflow_tools.airflow("dags", "delete", DAG, "-y")
    airflow_tools.wait_until_ready()
    print("Empty: the warehouse of this milestone, the stores' landing folder, the alerts log,")
    print(f"and Airflow's runs of {DAG}.")


def run_nights(nights: list[date]) -> pd.DataFrame:
    """Let Airflow run some nights, one after the other. One row per night."""
    rows = []
    for night in nights:
        started = time.time()
        states = airflow_tools.run_night(night.isoformat())
        rows.append(
            {
                "night": f"{night:%a %m-%d}",
                "tasks": len(states),
                "succeeded": sum(s["state"] == "success" for s in states),
                "seconds": round(time.time() - started),
                "stores that can be paid": can_be_paid(night),
            }
        )
    return pd.DataFrame(rows)


def backfill(first_day: str, last_day: str) -> list[dict]:
    """Airflow's backfill, through airflow_tools.backfill, for the nights first_day to last_day.

    airflow_tools.backfill returns as soon as the nights have run. Airflow marks the backfill
    itself as finished a few seconds later, and refuses a new backfill of the same DAG until
    then. So when it says "Another backfill is running", wait a little and ask again.
    """
    deadline = time.time() + 180
    while True:
        try:
            return airflow_tools.backfill(first_day, last_day)
        except RuntimeError as refused:
            if "AlreadyRunningBackfill" not in str(refused) or time.time() > deadline:
                raise
            time.sleep(5)


def _nights_done() -> list[date]:
    """The nights of the bonus week that have published, in order."""
    if not _exists("ops.run_history"):
        return []
    done = sql(
        "SELECT DISTINCT run_day FROM ops.run_history WHERE step = 'publish' AND status = 'ok' "
        "ORDER BY run_day"
    )
    return [date.fromisoformat(str(d)) for d in done["run_day"]]


def _as_planted() -> bool:
    """True when nothing was switched on or fixed at the stores."""
    status = airflow_tools.stores("status")
    return "Faults you set: none" in status and "Cases fixed:    none" in status


def prepare_nights(through: date = LAST_NIGHT) -> None:
    """Make sure Airflow has run the bonus week from Monday up to `through`, as planted.

    The notebooks run in order, and each one goes on from where the last one stopped.
    When the nights you need are there, this only says so. When some are missing, Airflow
    runs them. When a drill or a fix changed the week, everything is emptied first, and
    Airflow runs the nights again. Each night takes about half a minute.
    """
    try:
        done = _nights_done()
    except WarehouseDown as down:
        print(down)
        return
    airflow_tools.wait_until_ready()
    wanted = [d for d in WEEK if d <= through]
    if done != wanted:
        if done == wanted[: len(done)] and _as_planted():
            missing = wanted[len(done) :]
        else:
            start_again()
            missing = wanted
        print(f"Airflow runs {len(missing)} nights, about half a minute each ...")
        run_nights(missing)
    print(
        f"The bonus week ran under Airflow: {len(wanted)} nights, "
        f"{wanted[0]:%a %m-%d} to {wanted[-1]:%a %m-%d}. "
        f"{can_be_paid(through)} of 6 stores can be paid."
    )


# ---- Airflow's own run history ----------------------------------------------


def _clock(text: str | None) -> datetime | None:
    return datetime.fromisoformat(text) if text else None


def airflow_runs() -> pd.DataFrame:
    """Airflow's own list of the runs of this DAG: one row per night."""
    rows = []
    for r in airflow_tools._json("dags", "list-runs", DAG):
        start, end = _clock(r["start_date"]), _clock(r["end_date"])
        rows.append(
            {
                "run_id": r["run_id"],
                "for the night of": r["logical_date"][:10],
                "state": r["state"],
                "started (UTC)": f"{start:%Y-%m-%d %H:%M:%S}" if start else None,
                "seconds": round((end - start).total_seconds()) if start and end else None,
            }
        )
    return pd.DataFrame(rows).sort_values("for the night of").reset_index(drop=True)


def airflow_tasks(night: date) -> pd.DataFrame:
    """Airflow's record of each task of one night's run: its state, and how long it took."""
    rows = []
    for r in airflow_tools._json("tasks", "states-for-dag-run", DAG, f"night_{night}"):
        start, end = _clock(r["start_date"]), _clock(r["end_date"])
        rows.append(
            {
                "task": r["task_id"],
                "state": r["state"],
                "started (UTC)": f"{start:%H:%M:%S}" if start else None,
                "seconds": round((end - start).total_seconds(), 1) if start and end else None,
            }
        )
    order = ["stores_send_files", "load", "gate_landing", "clean", "gate_clean", "build",
             "gate_publish", "publish"]  # fmt: skip
    table = pd.DataFrame(rows)
    return table.sort_values("task", key=lambda t: t.map(order.index)).reset_index(drop=True)


def callback_said(night: date, task: str = "publish") -> None:
    """What the callback printed into the log of one task, the last time it ran.

    Airflow keeps one log file per try of a task. This reads the newest one.
    """
    folder = f"/opt/airflow/logs/dag_id={DAG}/run_id=night_{night}/task_id={task}"
    done = subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow", "sh", "-c",
         f'cat "$(ls -t "{folder}"/*.log | head -1)"'],
        cwd=PROJECT_DIR, capture_output=True, text=True, check=False,
    )  # fmt: skip
    said = []
    for line in done.stdout.splitlines():
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("logger") == "task.stdout":
            said.append(event.get("event", ""))
    start = max((n for n, text in enumerate(said) if text.startswith("alerts:")), default=None)
    if start is None:
        print("The callback has printed nothing in this log.")
        return
    print("\n".join(text for text in said[start:] if text))


# ---- The bonus report, health, alerts ---------------------------------------


def bonus_report(day: date = LAST_NIGHT) -> dict:
    with connect() as wh:
        return report.bonus_report(wh, day)


def show_report(day: date = LAST_NIGHT) -> None:
    print(report.format_report(bonus_report(day)))


def paid_stores(day: date = LAST_NIGHT) -> list[str]:
    return [store["store_id"] for store in bonus_report(day)["stores"] if store["pay"]]


def can_be_paid(day: date) -> int:
    return len(paid_stores(day))


def manager_line(store: str, day: date) -> None:
    """One store's lines of the bonus report: what its manager sees."""
    for s in bonus_report(day)["stores"]:
        if s["store_id"] == store:
            pay = f"yes, bonus {money(s['bonus_cents'])}" if s["pay"] else "DO NOT PAY YET"
            print(
                f"{store}  {s['manager_name']}  net sales {money(s['net_sales_cents'])}  Pay? {pay}"
            )
            for reason in s["why_not"]:
                print(f"     {reason}")


def health_numbers(day: date = LAST_NIGHT) -> dict:
    with connect() as wh:
        return health.health(wh, day)


def show_health(day: date = LAST_NIGHT) -> None:
    """The same as typing:  uv run python -m lotus_pipeline health --day 2026-08-30"""
    print(health.format_health(health_numbers(day)))


def shouted(day: date) -> list[str]:
    """The careless way to alert: one line for every failed test of one night."""
    with connect() as wh:
        return alerts.shout(wh, day)


def alerts_of(day: date, about: str | None = None) -> pd.DataFrame:
    """What happened to the alerts on one night: new, still open, or resolved.

    `about` keeps only the alerts whose title holds it, like "S03".
    """
    found = sql(
        "SELECT event, level, to_whom, title FROM ops.alert_log WHERE night = ? "
        "ORDER BY CASE event WHEN 'new' THEN 1 WHEN 'resolved' THEN 2 ELSE 3 END, "
        "CASE level WHEN 'page' THEN 1 WHEN 'ticket' THEN 2 ELSE 3 END, title",
        (day,),
    )
    if about:
        found = found[found["title"].str.contains(about, regex=False)]
    return found.reset_index(drop=True)


def alerts_log(last: int = 20) -> None:
    """The last lines of the alerts log the callbacks write, with the long lines folded."""
    text = airflow_tools.alerts_log(last)
    if not text:
        print("(the alerts log is empty)")
    for line in text.splitlines():
        print(
            line.replace(". Do: ", ".\n                  Do: ").replace(
                " (RUNBOOK", "\n                  (RUNBOOK"
            )
        )


def open_alerts(level: str | None = None) -> pd.DataFrame:
    found = sql(
        "SELECT level, to_whom, title, first_night FROM ops.alerts WHERE resolved_night IS NULL "
        "ORDER BY CASE level WHEN 'page' THEN 1 WHEN 'ticket' THEN 2 ELSE 3 END, first_night, title"
    )
    return found[found["level"] == level].reset_index(drop=True) if level else found


def night_by_night() -> pd.DataFrame:
    """For every night of the week: what shouting would send, and what was really sent."""
    log = sql("SELECT night, event, level FROM ops.alert_log")
    rows = []
    for night in WEEK:
        tonight = log[log["night"].astype(str) == night.isoformat()]
        new = tonight[tonight["event"] == "new"]
        rows.append(
            {
                "night": f"{night:%a %m-%d}",
                "shout: one per failed test": len(shouted(night)),
                "sent: new tickets": int((new["level"] != "message").sum()),
                "still open, not sent again": int((tonight["event"] == "still open").sum()),
                "new messages, kept for the digest": int((new["level"] == "message").sum()),
                "resolved": int((tonight["event"] == "resolved").sum()),
            }
        )
    table = pd.DataFrame(rows)
    total = {"night": "the week"} | {c: int(table[c].sum()) for c in table.columns[1:]}
    return pd.concat([table, pd.DataFrame([total])], ignore_index=True)


def routing_table() -> pd.DataFrame:
    """Who is told about what, and how urgently. Read from alerts.py, the one place it lives."""
    rows = [
        {"when": "a test whose action is block fails", "level": "page", "to whom": alerts.ON_CALL},
        {"when": "a task crashes", "level": "page", "to whom": alerts.ON_CALL},
        {"when": "a store's day cannot be paid from", "level": "ticket", "to whom": alerts.OPERATIONS},
        {"when": "a supplier's file is refused", "level": "ticket", "to whom": "Finance, and the buying team"},
    ]  # fmt: skip
    for test, (level, to, meaning, _do, _runbook) in alerts.WARNINGS.items():
        rows.append({"when": f"{test}: ... {meaning}", "level": level, "to whom": to})
    order = {"page": 0, "ticket": 1, "message": 2}
    table = pd.DataFrame(rows)
    return table.sort_values("level", key=lambda c: c.map(order), kind="stable").reset_index(
        drop=True
    )


def gate_results(day: date, only_failed: bool = True) -> pd.DataFrame:
    """What the three gates found on one night, from the table the pipeline keeps."""
    found = sql(
        "SELECT gate, test_name, action, status, bad_rows, title FROM ops.gate_results "
        "WHERE run_day = ? ORDER BY CASE gate WHEN 'landing' THEN 1 WHEN 'clean' THEN 2 ELSE 3 END, "
        "number, test_name",
        (day,),
    )
    return found[found["status"] != "pass"].reset_index(drop=True) if only_failed else found


def store_day(store: str, day: date) -> pd.DataFrame:
    """One store's day: what is published, what the tills counted, and its status."""
    return sql(
        "SELECT store_id, sales_date, lines_cents, till_cents, rows_set_aside, status, problem "
        "FROM output.store_day_status WHERE store_id = ? AND sales_date = ?",
        (store, day),
    )


def set_aside(store: str, day: date) -> pd.DataFrame:
    """The sales lines of one store's day in quarantine, counted by the reason they went."""
    return sql(
        "SELECT check_key AS why, count(DISTINCT line_id) AS lines, sum(amount_cents) AS cents "
        "FROM quarantine.sales_lines WHERE store_id = ? AND file_day = ? "
        "GROUP BY check_key ORDER BY lines",
        (store, day),
    )


# ---- The drills -------------------------------------------------------------


def start_drill(kind: str, store: str, day: date = DRILL_DAY) -> pd.DataFrame:
    """Empty everything, switch one fault on at one store, and let Airflow run that night."""
    start_again()
    print(airflow_tools.stores("fault", kind, "--store", store, "--day", day.isoformat()))
    return pd.DataFrame(airflow_tools.run_night(day.isoformat()))


def proof(store: str, day: date) -> pd.DataFrame:
    """The published number of one store's day, beside the answer key."""
    row = store_day(store, day)
    published = sql(
        "SELECT net_sales_cents FROM output.daily_store_sales WHERE store_id = ? AND sales_date = ?",
        (store, day),
    )
    cents = int(published.loc[0, "net_sales_cents"]) if len(published) else 0
    key = answer_key(store, day)
    return pd.DataFrame(
        [
            {
                "store": store,
                "day": f"{day:%a %m-%d}",
                "published": money(cents),
                "answer key": money(key),
                "equal": cents == key,
                "status": row.loc[0, "status"],
            }
        ]
    )


# ---- Files ------------------------------------------------------------------


def show_file(path: str, start: str | None = None, lines: int = 40) -> None:
    """Print part of a file of this milestone: from the first line that holds `start`."""
    text = (MILESTONE_DIR / path).read_text().splitlines()
    first = next((n for n, line in enumerate(text) if start and start in line), 0)
    print(f"# {path}" + (f", from line {first + 1}" if first else ""))
    print("\n".join(text[first : first + lines]))


def runbook(section: str) -> None:
    """Print one section of RUNBOOK.md: the one an alert points at."""
    text = (MILESTONE_DIR / "RUNBOOK.md").read_text()
    wanted = section.replace("-", " ").lower()
    parts = [part for part in text.split("\n## ") if part.lower().startswith(wanted)]
    print("## " + parts[0].strip() if parts else f"There is no section '{section}'.")


# ---- Saving what the lab found ----------------------------------------------


def save_result(name: str, value) -> None:
    """Keep a result, so a test can hold the production code against it."""
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{name}.json"
    path.write_text(json.dumps(value, indent=2) + "\n")
    print(f"Saved {path.relative_to(MILESTONE_DIR)}")


def load_result(name: str):
    path = RESULTS_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Run notebook 8 first: {path.name} is not there yet.")
    return json.loads(path.read_text())


# ---- Small things -----------------------------------------------------------


def money(cents) -> str:
    """Whole cents as dollars: 79618 becomes $796.18."""
    cents = int(cents)
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) // 100:,}.{abs(cents) % 100:02d}"


def _key() -> dict:
    return json.loads((PROJECT_DIR / "answer_key" / "answer_key.json").read_text())


def answer_key(store: str, day: date) -> int:
    """The true net sales of one store on one day, in cents. No pipeline code made it."""
    for row in _key()["daily_store_sales"]:
        if row["store_id"] == store and row["sales_date"] == day.isoformat():
            return row["net_sales_cents"]
    raise KeyError(f"The answer key has no row for {store} on {day}.")


def answer_key_week(store: str) -> dict:
    """The true net sales and bonus of one store for the whole bonus week, in cents."""
    return _key()["bonus_week"]["stores"][store]
