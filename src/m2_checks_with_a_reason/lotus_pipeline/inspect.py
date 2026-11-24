"""Run the checks with a reason, and say what each one found.

In this milestone the checks only LOOK. They are not part of the nightly run yet,
so a failure changes nothing. That is what is still wrong at the end of milestone 3.
"""

from dataclasses import dataclass

from lotus_pipeline.checks import Check
from lotus_pipeline.sales_checks import SALES_CHECKS
from lotus_pipeline.till_checks import TILL_CHECKS
from lotus_pipeline.warehouse import Warehouse, plain

LENSES = ("data", "business", "operational")
ALL_CHECKS = SALES_CHECKS + TILL_CHECKS
BY_KEY = {check.key: check for check in ALL_CHECKS}


@dataclass
class Finding:
    check: Check
    bad_rows: list[dict]

    @property
    def passed(self) -> bool:
        return not self.bad_rows

    def store_days(self) -> list[tuple[str, str]]:
        """The stores and days the bad rows belong to, each once."""
        found = {(r["store_id"], plain(r["sales_date"])) for r in self.bad_rows}
        return sorted(found, key=lambda where: (where[1], where[0]))


def run_one(wh: Warehouse, key: str) -> Finding:
    if key not in BY_KEY:
        raise KeyError(f"There is no check '{key}'. The checks are: {', '.join(BY_KEY)}")
    return Finding(BY_KEY[key], wh.query(BY_KEY[key].sql))


def run_all(wh: Warehouse, checks: list[Check] = ALL_CHECKS) -> list[Finding]:
    return [Finding(check, wh.query(check.sql)) for check in checks]


def format_results(findings: list[Finding]) -> str:
    sales = sum(f.check in SALES_CHECKS for f in findings)
    lines = [
        f"The checks with a reason: {sales} on sales_lines, {len(findings) - sales} on till_totals",
        "",
    ]
    for lens in LENSES:
        lines.append(f"  Through the {lens} lens")
        for f in (f for f in findings if f.check.lens == lens):
            if f.passed:
                found = "pass"
            else:
                where = ", ".join(f"{store} {day[5:]}" for store, day in f.store_days())
                found = f"FAIL  {len(f.bad_rows):>3} bad rows   {where}"
            lines.append(f"    {f.check.number:>2}. {f.check.name:<46} {found}")
        lines.append("")
    failed = sum(not f.passed for f in findings)
    lines.append(f"  {failed} of {len(findings)} checks found bad rows. Nothing was stopped:")
    lines.append("  these checks are not part of the nightly run yet.")
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
