"""Run the checks with a reason, and say what each one found.

In this milestone the checks only LOOK. They are not part of the nightly run yet,
so a failure changes nothing. That is what is still wrong at the end of milestone 3.
"""

from dataclasses import dataclass

from lotus_pipeline.checks import Check
from lotus_pipeline.other_checks import (
    MEMBER_CHECKS,
    PRODUCT_CHECKS,
    STOCK_CHECKS,
    STORE_CHECKS,
    SUPPLIER_CHECKS,
)
from lotus_pipeline.sales_checks import SALES_CHECKS
from lotus_pipeline.till_checks import TILL_CHECKS
from lotus_pipeline.warehouse import Warehouse, plain

LENSES = ("data", "business", "operational")
# The seven tables, in the order of their check cards.
CHECKS_BY_TABLE = {
    "sales_lines": SALES_CHECKS,
    "till_totals": TILL_CHECKS,
    "products": PRODUCT_CHECKS,
    "stores": STORE_CHECKS,
    "stock_counts": STOCK_CHECKS,
    "supplier_prices": SUPPLIER_CHECKS,
    "members": MEMBER_CHECKS,
}
ALL_CHECKS = [check for checks in CHECKS_BY_TABLE.values() for check in checks]
BY_KEY = {check.key: check for check in ALL_CHECKS}


@dataclass
class Finding:
    check: Check
    bad_rows: list[dict]

    @property
    def passed(self) -> bool:
        return not self.bad_rows

    def store_days(self) -> list[tuple[str, str]]:
        """Where the bad rows are: a store and a day, or a thing and a day. Each once."""
        found = set()
        for r in self.bad_rows:
            if "store_id" in r and "sales_date" in r:
                found.add((r["store_id"], plain(r["sales_date"])))
            else:
                found.add((str(r["thing"]), plain(r.get("day")) or ""))
        return sorted(found, key=lambda where: (where[1], where[0]))


def run_one(wh: Warehouse, key: str) -> Finding:
    if key not in BY_KEY:
        raise KeyError(f"There is no check '{key}'. The checks are: {', '.join(BY_KEY)}")
    return Finding(BY_KEY[key], wh.query(BY_KEY[key].sql))


def run_all(wh: Warehouse, checks: list[Check] = ALL_CHECKS) -> list[Finding]:
    return [Finding(check, wh.query(check.sql)) for check in checks]


def table_of(check: Check) -> str:
    return next(table for table, checks in CHECKS_BY_TABLE.items() if check in checks)


def format_results(findings: list[Finding]) -> str:
    lines = [f"The checks with a reason: {len(findings)} checks on {len(CHECKS_BY_TABLE)} tables"]
    for table, checks in CHECKS_BY_TABLE.items():
        mine = [f for f in findings if f.check in checks]
        if not mine:
            continue
        lines += ["", f"  {table}"]
        for f in mine:
            if f.passed:
                found = "pass"
            else:
                where = [f"{thing} {day[5:]}".strip() for thing, day in f.store_days()]
                shown = ", ".join(where[:4]) + (
                    f", and {len(where) - 4} more" if len(where) > 4 else ""
                )
                found = f"FAIL  {len(f.bad_rows):>3} bad rows   {shown}"
            lines.append(f"    {f.check.number:>2}. {f.check.name:<52} {f.check.lens:<12} {found}")
    failed = sum(not f.passed for f in findings)
    lines += [
        "",
        f"  {failed} of {len(findings)} checks found bad rows. Nothing was stopped:",
        "  these checks are not part of the nightly run yet.",
    ]
    return "\n".join(lines)


def format_rows(finding: Finding, limit: int = 12) -> str:
    check = finding.check
    lines = [
        f"{check.number}. {check.name}",
        f"   lens:      {check.lens}",
        f"   answers:   {check.question}",
        f"   protects:  {check.protects}",
        f"   prepare:   {check.prepare}",
        f"   limit:     {check.limit}",
        "",
        f"   {len(finding.bad_rows)} bad rows"
        + (":" if finding.bad_rows else ". The check passes."),
    ]
    for row in finding.bad_rows[:limit]:
        lines.append("   " + "  ".join(f"{k}={plain(v)}" for k, v in row.items()))
    if len(finding.bad_rows) > limit:
        lines.append(f"   ... and {len(finding.bad_rows) - limit} more")
    return "\n".join(lines)


def side_by_side() -> list[dict]:
    """Put the seven cards side by side: which family of check is on which table.

    A family on every card is a habit worth having. A family on one card is there
    because of how that one table is made.
    """
    families: dict[str, dict[str, int]] = {}
    for table, checks in CHECKS_BY_TABLE.items():
        for check in checks:
            families.setdefault(check.family, {}).setdefault(table, 0)
            families[check.family][table] += 1
    rows = [
        {"family": family, "tables": len(tables), **{t: tables.get(t, 0) for t in CHECKS_BY_TABLE}}
        for family, tables in families.items()
    ]
    return sorted(rows, key=lambda r: (-r["tables"], r["family"]))


def format_side_by_side() -> str:
    tables = list(CHECKS_BY_TABLE)
    short = {"sales_lines": "sales", "till_totals": "tills", "products": "prod", "stores": "stores",
             "stock_counts": "stock", "supplier_prices": "suppl", "members": "memb"}  # fmt: skip
    lines = [
        "The seven check cards, side by side: how many checks of each family a table has",
        "",
        "  " + f"{'family of check':<36}" + "".join(f"{short[t]:>7}" for t in tables) + "   on",
    ]
    for row in side_by_side():
        cells = "".join(f"{row[t] or '.':>7}" for t in tables)
        lines.append(f"  {row['family']:<36}{cells}   {row['tables']} of {len(tables)}")
    return "\n".join(lines)
