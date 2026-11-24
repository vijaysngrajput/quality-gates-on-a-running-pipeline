"""The bonus report: what the head of retail operations reads on Monday.

It reads one table, output.daily_store_sales, and nothing else.
"""

from datetime import date, timedelta

from lotus_pipeline import config
from lotus_pipeline.warehouse import Warehouse

WEEK_SALES = """
SELECT s.store_id,
       s.store_name,
       s.manager_name,
       coalesce(sum(d.net_sales_cents), 0) AS net_sales_cents,
       count(d.sales_date)                 AS days_reported
FROM clean.stores s
LEFT JOIN output.daily_store_sales d
  ON d.store_id = s.store_id
 AND d.sales_date BETWEEN ? AND ?
GROUP BY s.store_id, s.store_name, s.manager_name
ORDER BY s.store_id
"""


def week_start(day: date) -> date:
    """The Monday of the week a day is in."""
    return day - timedelta(days=day.weekday())


def bonus_cents(net_sales_cents: int) -> int:
    """1% of net sales, rounded to a cent."""
    return (net_sales_cents * config.BONUS_RATE_PERCENT + 50) // 100


def bonus_report(wh: Warehouse, day: date) -> dict:
    """Each store's net sales and bonus, from the Monday of `day`'s week up to `day`."""
    monday = week_start(day)
    stores = []
    for row in wh.query(WEEK_SALES, (monday, day)):
        net = int(row["net_sales_cents"])
        stores.append(
            {
                "store_id": row["store_id"],
                "store_name": row["store_name"],
                "manager_name": row["manager_name"],
                "net_sales_cents": net,
                "bonus_cents": bonus_cents(net),
                "days_reported": int(row["days_reported"]),
            }
        )
    checks = wh.query(
        "SELECT count(*) AS total, sum(CASE WHEN passed THEN 1 ELSE 0 END) AS passed "
        "FROM ops.check_results WHERE run_day = ?",
        (day,),
    )[0]
    return {
        "week_start": monday.isoformat(),
        "through": day.isoformat(),
        "stores": stores,
        "checks_total": int(checks["total"] or 0),
        "checks_passed": int(checks["passed"] or 0),
    }


def money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) // 100:,}.{abs(cents) % 100:02d}"


def format_report(report: dict) -> str:
    monday = date.fromisoformat(report["week_start"])
    through = date.fromisoformat(report["through"])
    lines = [
        f"Lotus Retails. Store managers' bonus, week of {monday:%a %Y-%m-%d}",
        f"Net sales from {monday:%a %Y-%m-%d} to {through:%a %Y-%m-%d}",
        "",
        f"  {'Store':<32}{'Manager':<18}{'Net sales':>12}{'Bonus (1%)':>12}",
    ]
    for s in report["stores"]:
        lines.append(
            f"  {s['store_id'] + '  ' + s['store_name']:<32}{s['manager_name']:<18}"
            f"{money(s['net_sales_cents']):>12}{money(s['bonus_cents']):>12}"
        )
    total = sum(s["net_sales_cents"] for s in report["stores"])
    bonus = sum(s["bonus_cents"] for s in report["stores"])
    lines += [
        f"  {'All six stores':<50}{money(total):>12}{money(bonus):>12}",
        "",
        f"  Checks: {report['checks_passed']} of {report['checks_total']} passed.",
    ]
    return "\n".join(lines)
