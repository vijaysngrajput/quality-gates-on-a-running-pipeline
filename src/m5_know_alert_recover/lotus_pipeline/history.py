"""Run history: what the pipeline remembers about its own runs.

Every step of every run leaves one row: which night it was for, how it ended, when it
started on the real clock, and how long it took. Nothing is ever deleted, so a night that
was run three times has three sets of rows.

Airflow keeps its own run history too: every run and every task, with its state. This table
is the pipeline's own, so the history is there even when a person ran a step by hand.
"""

from datetime import date, datetime

from lotus_pipeline.warehouse import Warehouse

CREATE = (
    "CREATE TABLE IF NOT EXISTS ops.run_history (run_day DATE, step TEXT, status TEXT, "
    "started_at TIMESTAMP, seconds NUMERIC(10, 2), detail TEXT)"
)


def record(
    wh: Warehouse, day: date, step: str, status: str, started_at: datetime, detail: str
) -> None:
    """Write one row: one step of one run. status is ok, blocked or error."""
    wh.execute(CREATE)
    seconds = round((datetime.now() - started_at).total_seconds(), 2)
    first_line = detail.strip().splitlines()[0][:300] if detail.strip() else ""
    wh.execute(
        "INSERT INTO ops.run_history VALUES (?, ?, ?, ?, ?, ?)",
        (day, step, status, started_at, seconds, first_line),
    )


def last_run_of_each_night(wh: Warehouse, first: date, last: date) -> list[dict]:
    """For every night in a range: did its newest run publish, and how long did it take?"""
    wh.execute(CREATE)
    rows = wh.query(
        """
        SELECT h.run_day, h.step, h.status, h.started_at, h.seconds
        FROM ops.run_history h
        JOIN (
            SELECT run_day, step, max(started_at) AS started_at
            FROM ops.run_history GROUP BY run_day, step
        ) newest
          ON newest.run_day = h.run_day AND newest.step = h.step AND newest.started_at = h.started_at
        WHERE h.run_day BETWEEN ? AND ?
        ORDER BY h.run_day, h.started_at
        """,
        (first, last),
    )
    nights: dict[date, dict] = {}
    for r in rows:
        night = nights.setdefault(
            r["run_day"], {"run_day": r["run_day"], "steps": 0, "seconds": 0.0, "published": False,
                           "blocked_at": None, "runs": 0}
        )  # fmt: skip
        night["steps"] += 1
        night["seconds"] += float(r["seconds"])
        if r["step"] == "publish" and r["status"] == "ok":
            night["published"] = True
        if r["status"] != "ok":
            night["blocked_at"] = r["step"]
            night["published"] = False
    for night in nights.values():
        night["runs"] = int(
            wh.value(
                "SELECT count(*) FROM ops.run_history WHERE run_day = ? AND step = 'load'",
                (night["run_day"],),
            )
        )
    return list(nights.values())
