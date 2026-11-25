"""The bonus report: what the head of retail operations reads on Monday.

Since milestone 4 it answers both sentences of her question:
    Can I pay the bonus from last week's net sales, store by store?
    And if I cannot trust a number, which one, and why?
"""

from datetime import date, timedelta

from lotus_pipeline import config
from lotus_pipeline.warehouse import Warehouse

WEEK_SALES = """
SELECT s.store_id,
       s.store_name,
       s.manager_name,
       coalesce(sum(d.net_sales_cents), 0) AS net_sales_cents
FROM output.stores s
LEFT JOIN output.daily_store_sales d
  ON d.store_id = s.store_id
 AND d.sales_date BETWEEN ? AND ?
GROUP BY s.store_id, s.store_name, s.manager_name
ORDER BY s.store_id
"""

NOT_READY = """
SELECT store_id, sales_date, problem, lines_cents, till_cents, rows_set_aside, cents_set_aside,
       reasons
FROM output.store_day_status
WHERE status <> 'ok' AND sales_date BETWEEN ? AND ?
ORDER BY store_id, sales_date
"""


def week_start(day: date) -> date:
    """The Monday of the week a day is in."""
    return day - timedelta(days=day.weekday())


def bonus_cents(net_sales_cents: int) -> int:
    """1% of net sales, rounded to a cent."""
    return (net_sales_cents * config.BONUS_RATE_PERCENT + 50) // 100


def money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) // 100:,}.{abs(cents) % 100:02d}"


def why(row: dict) -> str:
    """One store's day that cannot be paid from, said in one line."""
    day = f"{row['sales_date']:%a %m-%d}"
    reasons = row["reasons"] or ""
    apart = money(abs(int(row["lines_cents"]) - int(row["till_cents"] or 0)))
    if row["problem"] == "no_sales_file":
        return f"{day}: no sales file arrived. The tills counted {money(int(row['till_cents']))}"
    if row["problem"] == "file_refused":
        return f"{day}: the sales file was refused. {reasons}"
    if row["problem"] == "no_till_total":
        return f"{day}: no till total to compare with"
    if row["problem"] == "rows_set_aside":
        rows = int(row["rows_set_aside"])
        return (
            f"{day}: {rows} row{'s' if rows != 1 else ''} set aside "
            f"({money(abs(int(row['cents_set_aside'])))}): {reasons}"
        )
    return f"{day}: the receipts and the tills are {apart} apart, and nobody knows why"


def bonus_report(wh: Warehouse, day: date) -> dict:
    """Each store's net sales and bonus for the week up to `day`, and whether it can be paid."""
    monday = week_start(day)
    not_ready: dict[str, list[str]] = {}
    for row in wh.query(NOT_READY, (monday, day)):
        not_ready.setdefault(row["store_id"], []).append(why(row))
    stores = []
    for row in wh.query(WEEK_SALES, (monday, day)):
        net = int(row["net_sales_cents"])
        reasons = not_ready.get(row["store_id"], [])
        stores.append(
            {
                "store_id": row["store_id"],
                "store_name": row["store_name"],
                "manager_name": row["manager_name"],
                "net_sales_cents": net,
                "bonus_cents": bonus_cents(net),
                "pay": not reasons,
                "why_not": reasons,
            }
        )
    gates = wh.query(
        "SELECT count(*) AS tests, sum(CASE WHEN status = 'pass' THEN 1 ELSE 0 END) AS passed, "
        "sum(CASE WHEN status <> 'pass' AND action = 'warn' THEN 1 ELSE 0 END) AS warnings "
        "FROM ops.gate_results WHERE run_day = ?",
        (day,),
    )[0]
    return {
        "week_start": monday.isoformat(),
        "through": day.isoformat(),
        "stores": stores,
        "tests": int(gates["tests"] or 0),
        "tests_passed": int(gates["passed"] or 0),
        "warnings": int(gates["warnings"] or 0),
    }


def format_report(report: dict) -> str:
    monday = date.fromisoformat(report["week_start"])
    through = date.fromisoformat(report["through"])
    lines = [
        f"Lotus Retails. Store managers' bonus, week of {monday:%a %Y-%m-%d}",
        f"Net sales from {monday:%a %Y-%m-%d} to {through:%a %Y-%m-%d}",
        "",
        f"  {'Store':<32}{'Manager':<18}{'Net sales':>12}{'Bonus (1%)':>12}   Pay?",
    ]
    for s in report["stores"]:
        bonus = money(s["bonus_cents"]) if s["pay"] else "-"
        lines.append(
            f"  {s['store_id'] + '  ' + s['store_name']:<32}{s['manager_name']:<18}"
            f"{money(s['net_sales_cents']):>12}{bonus:>12}   {'yes' if s['pay'] else 'DO NOT PAY YET'}"
        )
        lines.extend(f"         {reason}" for reason in s["why_not"])
    ready = sum(s["pay"] for s in report["stores"])
    lines += [
        "",
        f"  {ready} of {len(report['stores'])} stores can be paid.",
        f"  Tonight's gates: {report['tests_passed']} of {report['tests']} tests passed, "
        f"{report['warnings']} warning{'' if report['warnings'] == 1 else 's'}.",
    ]
    return "\n".join(lines)
