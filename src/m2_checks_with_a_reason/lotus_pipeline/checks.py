"""Step 3, check: the five standard checks.

They are the checks most teams write first, on every table, from a template.
Each one is a query that returns the bad rows. No rows means the check passes.

They run on the clean tables, before anything is published.
"""

from dataclasses import dataclass
from datetime import date

from lotus_pipeline.warehouse import Warehouse


@dataclass(frozen=True)
class Check:
    number: int
    name: str
    table: str
    sql: str  # returns the bad rows
    for_the_day: bool = False  # True when the query needs the run's business day
    # From milestone 2 on, a check also says why it exists. The five standard checks cannot.
    key: str = ""  # a short name for commands and tests
    lens: str = "data"  # business, data or operational
    question: str = ""  # which of the six questions it answers
    protects: str = ""  # who is hurt when it fails: the answer to question 2 of the check card
    prepare: str = "Nothing"  # what must be done to the data before the rule can be tested
    limit: str = ""  # the exact line between good and bad
    catches: tuple[int, ...] = ()  # the planted cases it catches


STANDARD_CHECKS = [
    Check(
        1,
        "The table is not empty",
        "clean.sales_lines",
        # One row comes back when the day has no lines at all.
        """
        SELECT 'no sales lines for this day' AS problem
        WHERE NOT EXISTS (SELECT 1 FROM clean.sales_lines WHERE sales_date = ?)
        """,
        for_the_day=True,
    ),
    Check(
        2,
        "The key is not null",
        "clean.sales_lines",
        "SELECT * FROM clean.sales_lines WHERE line_id IS NULL",
    ),
    Check(
        3,
        "The key is unique",
        "clean.sales_lines",
        """
        SELECT line_id, count(*) AS times
        FROM clean.sales_lines
        GROUP BY line_id
        HAVING count(*) > 1
        """,
    ),
    Check(
        4,
        "The status is an allowed value",
        "clean.sales_lines",
        """
        SELECT * FROM clean.sales_lines
        WHERE line_type IS NULL OR line_type NOT IN ('sale', 'refund')
        """,
    ),
    Check(
        5,
        "The total is positive",
        "clean.sales_lines",
        # A refund line is negative by design, so the total is checked per store and day.
        """
        SELECT sales_date, store_id, sum(amount_cents) AS net_sales_cents
        FROM clean.sales_lines
        GROUP BY sales_date, store_id
        HAVING sum(amount_cents) <= 0
        """,
    ),
]


@dataclass
class Result:
    check: Check
    bad_rows: list[dict]

    @property
    def passed(self) -> bool:
        return not self.bad_rows


def run_check(wh: Warehouse, check: Check, day: date) -> Result:
    params = (day,) if check.for_the_day else ()
    return Result(check, wh.query(check.sql, params))


def run_checks(wh: Warehouse, day: date) -> list[Result]:
    """Run the five checks, and remember the result of each for this day."""
    results = [run_check(wh, check, day) for check in STANDARD_CHECKS]
    wh.execute(
        "CREATE TABLE IF NOT EXISTS ops.check_results "
        "(run_day DATE, check_number INTEGER, check_name TEXT, table_name TEXT, "
        "passed BOOLEAN, bad_rows INTEGER)"
    )
    wh.execute("DELETE FROM ops.check_results WHERE run_day = ?", (day,))
    for r in results:
        wh.execute(
            "INSERT INTO ops.check_results VALUES (?, ?, ?, ?, ?, ?)",
            (day, r.check.number, r.check.name, r.check.table, r.passed, len(r.bad_rows)),
        )
    return results


def format_results(results: list[Result]) -> str:
    lines = []
    for r in results:
        mark = "PASS" if r.passed else f"FAIL ({len(r.bad_rows)} bad rows)"
        lines.append(f"  {r.check.number}. {r.check.name:<32} {r.check.table:<20} {mark}")
    passed = sum(r.passed for r in results)
    lines.append(f"  {passed} of {len(results)} checks passed.")
    return "\n".join(lines)
