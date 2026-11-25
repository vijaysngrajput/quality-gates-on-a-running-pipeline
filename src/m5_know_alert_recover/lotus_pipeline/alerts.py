"""Alerts: tell the person who can act. What failed, how bad it is, what to do.

An alert that reaches the wrong person, or says only "a test failed", changes nothing.
So every alert here has:

    a level      page (wake someone), ticket (today), message (read it when you can)
    a person     the one who can act on it. This is ROUTING
    what         in the words of the business: which store, which day, how much money
    what to do   the first step, and a link to the RUNBOOK

Two ways to send them, to feel the difference:

    shout     one alert for every failed test, every night, to everyone
    grouped   one alert for one problem, sent ONCE when it is new, to the person who can
              act. A problem that is still there is counted, not sent again. Messages are
              not sent at all: they wait in a digest

Where an alert goes: the table ops.alerts, one line in the alerts log, and the task's log.
A real team would plug in email, Slack, or a paging service such as PagerDuty. The decision
about level and person stays the same.
"""

from dataclasses import dataclass
from datetime import date

from lotus_pipeline import config
from lotus_pipeline.load import run_moment
from lotus_pipeline.report import money
from lotus_pipeline.warehouse import Warehouse

LEVELS = ("page", "ticket", "message")
ON_CALL = "The data engineer on call"
OPERATIONS = "Head of retail operations"

CREATE = (
    "CREATE TABLE IF NOT EXISTS ops.alerts (fingerprint TEXT, level TEXT, to_whom TEXT, "
    "title TEXT, how_bad TEXT, what_to_do TEXT, runbook TEXT, first_night DATE, "
    "last_night DATE, resolved_night DATE)"
)
CREATE_LOG = (
    "CREATE TABLE IF NOT EXISTS ops.alert_log (night DATE, fingerprint TEXT, event TEXT, "
    "level TEXT, to_whom TEXT, title TEXT)"
)


@dataclass(frozen=True)
class Alert:
    fingerprint: str  # names one problem. The same problem tomorrow has the same fingerprint
    level: str
    to_whom: str
    title: str
    how_bad: str
    what_to_do: str
    runbook: str  # the section of RUNBOOK.md

    def line(self) -> str:
        return (
            f"{self.level.upper():<8}to {self.to_whom}: {self.title}. {self.how_bad} "
            f"Do: {self.what_to_do} (RUNBOOK.md#{self.runbook})"
        )


def _exists(wh: Warehouse, table: str) -> bool:
    schema, name = table.split(".")
    return bool(
        wh.query(
            "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
            (schema, name),
        )
    )


def _failed(wh: Warehouse, day: date) -> dict[str, dict]:
    if not _exists(wh, "ops.gate_results"):
        return {}
    rows = wh.query("SELECT * FROM ops.gate_results WHERE run_day = ? AND status <> 'pass'", (day,))
    return {r["test_name"]: r for r in rows}


def _day(value) -> str:
    return f"{value:%a %m-%d}"


# ---------------------------------------------------------------- the alerts of one night


def _blocked(failed: dict, failed_task: str | None) -> list[Alert]:
    alerts = [
        Alert(
            f"blocked|{name}", "page", ON_CALL,
            f"The {r['gate']} gate BLOCKED: {r['title']}",
            f"{r['bad_rows']} bad rows. Nothing new was published. Every reader sees yesterday's numbers.",
            "Read the rows the test kept. Fix the table or the file at its source. Run the night again.",
            "a-gate-blocked",
        )
        for name, r in failed.items()
        if r["action"] == "block"
    ]  # fmt: skip
    if failed_task and not alerts:
        alerts.append(
            Alert(
                f"task_failed|{failed_task}", "page", ON_CALL,
                f"The task {failed_task} failed with an error",
                "It is not a failed check. The program itself stopped. Nothing new was published.",
                "Read the log of the task in Airflow. Fix the cause. Run the night again.",
                "a-task-failed",
            )
        )  # fmt: skip
    return alerts


STORE_DAY = {
    "no_sales_file": ("No sales file arrived", "ask-a-store-for-its-file",
                      "Ask the store's back office to send the file. Do not pay this store's bonus yet."),
    "file_refused": ("The sales file was refused", "a-file-was-refused",
                     "Ask the store's back office for a file with the right columns. Do not pay yet."),
    "rows_set_aside": ("Rows were set aside, and the day does not add up", "rows-were-set-aside",
                       "Look at the rows in quarantine.sales_lines. Ask the store for a corrected file. Do not pay yet."),
    "no_till_total": ("No till total to compare with", "ask-a-store-for-its-file",
                      "Ask the store to close its tills and send the totals. Do not pay yet."),
}  # fmt: skip


def _store_days(wh: Warehouse) -> list[Alert]:
    if not _exists(wh, "output.store_day_status"):
        return []
    alerts = []
    rows = wh.query(
        "SELECT s.*, t.manager_name FROM output.store_day_status s "
        "JOIN output.stores t ON t.store_id = s.store_id WHERE s.status <> 'ok' "
        "ORDER BY s.sales_date, s.store_id"
    )
    for r in rows:
        if r["problem"] not in STORE_DAY:
            continue
        title, runbook, do = STORE_DAY[r["problem"]]
        apart = money(abs(int(r["lines_cents"]) - int(r["till_cents"] or 0)))
        how_bad = (
            f"The receipts and the tills are {apart} apart. {r['manager_name']}'s bonus waits."
        )
        if r["problem"] == "rows_set_aside":
            how_bad = (
                f"{r['rows_set_aside']} row{'s' if r['rows_set_aside'] != 1 else ''}, "
                f"{money(abs(int(r['cents_set_aside'])))}: {r['reasons']}. " + how_bad
            )
        alerts.append(
            Alert(
                f"store_day|{r['store_id']}|{r['sales_date']}", "ticket", OPERATIONS,
                f"{r['store_id']} {_day(r['sales_date'])}: {title}", how_bad, do, runbook,
            )
        )  # fmt: skip
    return alerts


def _refused_files(wh: Warehouse) -> list[Alert]:
    if not _exists(wh, "ops.refused_files"):
        return []
    alerts = []
    for r in wh.query(
        "SELECT * FROM ops.refused_files WHERE table_name <> 'sales_lines' ORDER BY file_name"
    ):
        to = "Finance, and the buying team" if r["table_name"] == "supplier_prices" else ON_CALL
        alerts.append(
            Alert(
                f"refused|{r['file_name']}", "ticket", to,
                f"The file {r['file_name']} was refused", f"{r['reason']}. The last good file is in use.",
                "Ask the sender for a file that keeps the contract. Until then nothing new is used.",
                "a-file-was-refused",
            )
        )  # fmt: skip
    return alerts


# test name -> level, person, what it means, what to do, runbook section, how to name a row
WARNINGS = {
    "price_of_the_day": (
        "ticket", "Finance", "charged a price that is not in the price list",
        "Ask the store to update the prices in its tills. The sales number is right.", "a-warning"),
    "order_without_a_count": (
        "ticket", "The replenishment team", "has a product with no stock count",
        "Count the shelf, or order that product by hand. The list does not guess.", "a-warning"),
    "one_person_once": (
        "message", "Marketing", "is the same person as an older member",
        "Merge the two accounts. Only the older one gets email until then.", "a-warning"),
    "normal_volume": (
        "message", OPERATIONS, "had a day far from its normal day",
        "It is a reason to look, not a proof of a fault. Check the other alerts for this store.", "a-warning"),
    "file_arrived_on_time": (
        "message", "Store operations", "sent its file late",
        "Nothing to repair: the file is loaded. Ask why it was late if it happens again.", "a-warning"),
    "file_has_a_new_column": (
        "message", ON_CALL, "has a column nobody agreed",
        "Nothing breaks. Ask the sender what the new column means.", "a-warning"),
    "true_key_unique": (
        "message", ON_CALL, "sent sales lines twice. The extra copies were removed",
        "Nothing to repair if the day adds up to the tills. Ask why the export ran twice.", "a-warning"),
    # The other tables. Each warning or set-aside has one person who can act on it.
    "stores_did_not_change": (
        "ticket", OPERATIONS, "changed in the stores table",
        "Confirm the change with head office. This table should stand still.", "a-warning"),
    "price_change_is_believable": (
        "ticket", "The buying team", "has a new price far from its old price",
        "Check the price that was typed. Every till charges it.", "a-warning"),
    "retired_means_ended": (
        "message", "The buying team", "is retired and has no last day",
        "Type the last day of the product.", "a-warning"),
    "one_count_per_day": (
        "message", "The replenishment team", "has a product counted twice",
        "Ask the store which count is true.", "a-warning"),
    "count_values_allowed": (
        "ticket", "The replenishment team", "has a stock count that cannot be true. It was set aside",
        "Count the shelf again, or order that product by hand.", "a-warning"),
    "cost_is_filled": (
        "ticket", "Finance, and the buying team", "has no cost. The row was set aside",
        "Ask the supplier for the cost. The last good cost is in use.", "a-warning"),
    "cost_near_the_last_file": (
        "ticket", "Finance, and the buying team", "has a cost far from the last file. The row was set aside",
        "Ask the supplier if the cost or its unit changed. The last good cost is in use.", "a-warning"),
    "cost_below_the_shelf_price": (
        "ticket", "Finance, and the buying team", "costs more than it sells for. The row was set aside",
        "Ask the supplier if the cost or its unit changed. The last good cost is in use.", "a-warning"),
    "supplier_row_fits_the_product": (
        "ticket", "Finance, and the buying team", "has a supplier row that does not fit the product",
        "Ask the supplier which product and unit the row is for.", "a-warning"),
    "email_has_the_right_shape": (
        "message", "Marketing", "has an email that cannot be delivered. The member was set aside",
        "Ask the member for the right address at the till.", "a-warning"),
    "member_cells_filled": (
        "message", "Marketing", "has an empty cell marketing needs. The member was set aside",
        "Do not email this member until the row is complete.", "a-warning"),
    "member_fits_the_real_world": (
        "message", "Marketing", "has an unknown home store, or a join date in the future",
        "Correct the member's home store.", "a-warning"),
}  # fmt: skip

# These tests need no alert of their own. What they find reaches a person another way.
COVERED = {
    # rows of sales_lines that were set aside: the store's day gets one ticket, with the reasons
    "required_cells", "right_shape", "line_adds_up", "refund_gives_money_back",
    "refund_not_larger_than_sale", "inside_opening_hours", "believable_quantity",
    "impossible_price", "every_sale_has_a_product", "store_is_not_ready_to_pay",
    "every_till_reported",  # the store's day says "no till total"
    "a_count_for_every_day",  # the order list says "no count", and that has its own ticket
    "order_without_a_cost",  # a refused or set-aside supplier row already has a ticket
    "file_is_missing_a_column", "supplier_contract",  # a refused file has its own ticket
}  # fmt: skip


def _subjects(wh: Warehouse, test: str) -> list[str]:
    """What the rows a test kept are about: a store and day, a file, or a thing. Each once."""
    table = f"audit.{test}"
    if not _exists(wh, table):
        return []
    columns = {
        r["column_name"]
        for r in wh.query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'audit' AND table_name = ?", (test,),
        )
    }  # fmt: skip
    if {"store_id", "sales_date"} <= columns:
        rows = wh.query(f"SELECT DISTINCT store_id, sales_date FROM {table} ORDER BY 2, 1")
        return [f"{r['store_id']} {_day(r['sales_date'])}" for r in rows]
    column = "file_name" if "file_name" in columns else "thing"
    return [str(r[column]) for r in wh.query(f"SELECT DISTINCT {column} FROM {table} ORDER BY 1")]


def _warnings(wh: Warehouse, failed: dict) -> list[Alert]:
    alerts = []
    for test, (level, to, meaning, do, runbook) in WARNINGS.items():
        if test not in failed:
            continue
        for subject in _subjects(wh, test):
            alerts.append(
                Alert(f"{test}|{subject}", level, to, f"{subject} {meaning}",
                      f"{failed[test]['title']}.", do, runbook)
            )  # fmt: skip
    return alerts


def tonight(wh: Warehouse, day: date, failed_task: str | None = None) -> list[Alert]:
    """Every problem that exists after the run for `day`, the most urgent first."""
    failed = _failed(wh, day)
    found = (
        _blocked(failed, failed_task) + _store_days(wh) + _refused_files(wh) + _warnings(wh, failed)
    )
    return sorted(found, key=lambda a: (LEVELS.index(a.level), a.fingerprint))


def shout(wh: Warehouse, day: date) -> list[str]:
    """The careless way: one alert for every failed test, to everyone, every night."""
    return [
        f"ALERT to everyone: the test {name} failed with {r['bad_rows']} bad rows"
        for name, r in sorted(_failed(wh, day).items())
    ]


# ---------------------------------------------------------------- send only what is new


def send(wh: Warehouse, day: date, failed_task: str | None = None) -> dict:
    """Compare tonight's problems with the open ones. Send what is new. Close what is gone.

    Running a night again sends nothing twice: a problem that is still there is still open.
    And when the night is run again after a fix, the problem is gone, and its alert is closed.
    """
    wh.execute(CREATE)
    wh.execute(CREATE_LOG)

    now = tonight(wh, day, failed_task)
    open_before = {
        r["fingerprint"]: r
        for r in wh.query("SELECT * FROM ops.alerts WHERE resolved_night IS NULL")
    }
    new, still_open = [], []
    for alert in now:
        if alert.fingerprint in open_before:
            still_open.append(alert)
            wh.execute(
                "UPDATE ops.alerts SET last_night = ? WHERE fingerprint = ? AND resolved_night IS NULL",
                (day, alert.fingerprint),
            )
        else:
            new.append(alert)
            wh.execute(
                "INSERT INTO ops.alerts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                (alert.fingerprint, alert.level, alert.to_whom, alert.title, alert.how_bad,
                 alert.what_to_do, alert.runbook, day, day),
            )  # fmt: skip
    current = {a.fingerprint for a in now}
    resolved = [r for name, r in open_before.items() if name not in current]
    for r in resolved:
        wh.execute(
            "UPDATE ops.alerts SET resolved_night = ? WHERE fingerprint = ? AND resolved_night IS NULL",
            (day, r["fingerprint"]),
        )

    sent = [a for a in new if a.level != "message"]  # a message waits in the digest
    lines = [f"{run_moment(day):%Y-%m-%d %H:%M}  {a.line()}" for a in sent]
    lines += [
        f"{run_moment(day):%Y-%m-%d %H:%M}  RESOLVED to {r['to_whom']}: {r['title']}"
        for r in resolved
        if r["level"] != "message"
    ]

    def remember(fingerprint: str, event: str, level: str, to_whom: str, title: str) -> None:
        seen = wh.value(
            "SELECT count(*) FROM ops.alert_log WHERE night = ? AND fingerprint = ? AND event = ?",
            (day, fingerprint, event),
        )
        if not seen:
            wh.execute(
                "INSERT INTO ops.alert_log VALUES (?, ?, ?, ?, ?, ?)",
                (day, fingerprint, event, level, to_whom, title),
            )

    for event, alerts in (("new", new), ("still open", still_open)):
        for a in alerts:
            remember(a.fingerprint, event, a.level, a.to_whom, a.title)
    for r in resolved:
        remember(r["fingerprint"], "resolved", r["level"], r["to_whom"], r["title"])
    if lines:
        config.ALERTS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with config.ALERTS_LOG.open("a") as log:
            log.write("\n".join(lines) + "\n")
    return {
        "sent": sent,
        "digest": [a for a in new if a.level == "message"],
        "still_open": still_open,
        "resolved": resolved,
        "lines": lines,
        "shouted": len(shout(wh, day)),
    }


def format_sent(done: dict) -> str:
    counts = {level: sum(a.level == level for a in done["sent"]) for level in LEVELS}
    lines = [
        f"alerts:       {len(done['sent'])} sent ({counts['page']} page, {counts['ticket']} ticket). "
        f"{len(done['still_open'])} still open, not sent again. {len(done['resolved'])} resolved. "
        f"{len(done['digest'])} in the digest."
    ]
    lines += [f"    {line[18:]}" for line in done["lines"]]
    return "\n".join(lines)
