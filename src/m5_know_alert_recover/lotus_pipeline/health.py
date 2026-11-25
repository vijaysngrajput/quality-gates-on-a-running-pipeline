"""Is the pipeline healthy? Four plain questions, each answered with a number.

    Did it run?                every night has a run that published
    Did it finish on time?     how long a night took, against a target
    Is the data fresh?         the share of sales files that arrived by 01:00
    Did every store report?    the share of store days that can be paid from

A number you measure is an SLI. The target for it is an SLO. The checks of milestones 2
to 4 ask whether tonight's DATA is right. These ask whether the PIPELINE is doing its job.
"""

from datetime import date, timedelta

from lotus_pipeline import config, history
from lotus_pipeline.report import week_start
from lotus_pipeline.warehouse import Warehouse

# One row per sales file a store should have sent: did it come, and when.
FILES = """
SELECT e.store_id, e.day,
       f.received_at,
       CAST(CAST(e.day + 1 AS TIMESTAMP) + INTERVAL '1 hour' AS TIMESTAMP) AS due_at
FROM prepared.expected_store_days e
LEFT JOIN (
    SELECT substr(file_name, 13, 3) AS store_id, CAST(substr(file_name, 17, 10) AS DATE) AS file_day,
           min(received_at) AS received_at
    FROM raw.loaded_files
    WHERE table_name = 'sales_lines'
    GROUP BY substr(file_name, 13, 3), CAST(substr(file_name, 17, 10) AS DATE)
) f
  ON f.store_id = e.store_id AND f.file_day = e.day
WHERE e.day BETWEEN ? AND ?
ORDER BY e.day, e.store_id
"""


def percent(part: int, whole: int) -> float:
    return round(100 * part / whole, 1) if whole else 0.0


def health(wh: Warehouse, day: date) -> dict:
    """The four answers for the week of `day`, from its Monday up to `day`."""
    monday = week_start(day)
    days = (day - monday).days + 1

    nights = history.last_run_of_each_night(wh, monday, day)
    published = [n for n in nights if n["published"]]
    slowest = max((n["seconds"] for n in nights), default=0.0)

    files = wh.query(FILES, (monday, day))
    on_time = [f for f in files if f["received_at"] and f["received_at"] <= f["due_at"]]
    late = [f for f in files if f["received_at"] and f["received_at"] > f["due_at"]]
    missing = [f for f in files if not f["received_at"]]

    status = wh.query(
        "SELECT status, count(*) AS days FROM output.store_day_status "
        "WHERE sales_date BETWEEN ? AND ? GROUP BY status",
        (monday, day),
    )
    ok = sum(int(r["days"]) for r in status if r["status"] == "ok")
    all_days = sum(int(r["days"]) for r in status)
    return {
        "week_start": monday.isoformat(),
        "through": day.isoformat(),
        "nights": days,
        "nights_published": len(published),
        "night_runs": [
            {
                "run_day": n["run_day"].isoformat(),
                "published": n["published"],
                "seconds": round(n["seconds"], 1),
                "runs": n["runs"],
                "blocked_at": n["blocked_at"],
            }
            for n in nights
        ],  # fmt: skip
        "slowest_night_seconds": round(slowest, 1),
        "night_seconds_target": config.NIGHT_SECONDS_TARGET,
        "files_expected": len(files),
        "files_on_time": len(on_time),
        "files_on_time_percent": percent(len(on_time), len(files)),
        "files_on_time_target": config.FILES_ON_TIME_TARGET,
        "late_files": [
            {
                "store_id": f["store_id"],
                "day": f["day"].isoformat(),
                "hours_late": round((f["received_at"] - f["due_at"]).total_seconds() / 3600, 1),
            }
            for f in late
        ],  # fmt: skip
        "missing_files": [
            {"store_id": f["store_id"], "day": f["day"].isoformat()} for f in missing
        ],
        "store_days": all_days,
        "store_days_ok": ok,
        "store_days_ok_percent": percent(ok, all_days),
    }


def format_health(h: dict) -> str:
    monday, through = date.fromisoformat(h["week_start"]), date.fromisoformat(h["through"])

    def mark(good: bool) -> str:
        return "ok  " if good else "MISS"

    ran = h["nights_published"] == h["nights"]
    fast = h["slowest_night_seconds"] <= h["night_seconds_target"]
    fresh = h["files_on_time_percent"] >= h["files_on_time_target"]
    lines = [
        f"Is the pipeline healthy? Week of {monday:%a %Y-%m-%d}, up to {through:%a %Y-%m-%d}",
        "",
        f"  {mark(ran)} Did it run?              {h['nights_published']} of {h['nights']} nights published",
        f"  {mark(fast)} Did it finish on time?   the slowest night took {h['slowest_night_seconds']:.0f} "
        f"seconds. Target: under {h['night_seconds_target']}",
        f"  {mark(fresh)} Is the data fresh?       {h['files_on_time']} of {h['files_expected']} sales files "
        f"arrived by 01:00: {h['files_on_time_percent']}%. Target: {h['files_on_time_target']}%",
    ]
    for f in h["late_files"]:
        lines.append(
            f"         late:    {f['store_id']} {f['day']}, {f['hours_late']} hours after it was due"
        )
    for f in h["missing_files"]:
        lines.append(f"         missing: {f['store_id']} {f['day']}")
    lines.append(
        f"  {'    '} Did every store report?  {h['store_days_ok']} of {h['store_days']} store days "
        f"can be paid from: {h['store_days_ok_percent']}%"
    )
    return "\n".join(lines)


def next_day(day: date) -> date:
    return day + timedelta(days=1)
