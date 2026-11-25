"""The three gates: where a check can stop a pipeline.

    landing   when the files have landed    on the raw tables
    clean     after cleaning                on the clean tables
    publish   before the outputs go out     on the staging tables

A gate runs the dbt tests with its tag. dbt holds each rule, and says what to do when
the rule is broken. This file ACTS on the answer:

    block         the gate fails. The steps after it do not run, and the outputs keep
                  their last good numbers
    quarantine    the rows the test returned are set aside with the reason, and the good
                  rows carry on
    refuse_file   a whole file is set aside. None of its rows go on
    warn          the run continues, and the warning is recorded
"""

from dataclasses import dataclass
from datetime import date

from lotus_pipeline import config, contract, dbt_runner
from lotus_pipeline.clean import REFUSED_FILES
from lotus_pipeline.dbt_runner import TestResult
from lotus_pipeline.warehouse import Warehouse, connect

GATES = ("landing", "clean", "publish")
TOO_MANY = "too_many_bad_rows"
TOO_MANY_REASON = "More than 5% of this file was set aside, so the whole file is refused"


class GateBlocked(Exception):
    """A test whose action is `block` failed."""


@dataclass
class GateResult:
    gate: str
    day: date
    tests: list[TestResult]
    set_aside: dict[str, int]  # table -> rows set aside by this gate
    refused: list[str]  # files refused by this gate

    @property
    def blocked_by(self) -> list[TestResult]:
        return [t for t in self.tests if t.failed and t.action == "block"]

    @property
    def warnings(self) -> list[TestResult]:
        return [t for t in self.tests if t.failed and t.action == "warn"]


# ---------------------------------------------------------------- the tables a gate writes

QUARANTINE_TABLES = {
    "sales_lines": "CREATE TABLE quarantine.sales_lines AS SELECT l.*, CAST(NULL AS TEXT) AS check_key, "
    "CAST(NULL AS TEXT) AS reason, CAST(NULL AS BOOLEAN) AS is_a_copy, CAST(NULL AS DATE) AS set_aside_on "
    "FROM clean.sales_lines l WHERE 1 = 0",
    "raw_sales_lines": "CREATE TABLE quarantine.raw_sales_lines (line_id TEXT, store_id TEXT, "
    "file_day DATE, product_id TEXT, amount_cents BIGINT, check_key TEXT, reason TEXT, set_aside_on DATE)",
    "supplier_prices": "CREATE TABLE quarantine.supplier_prices AS SELECT s.*, CAST(NULL AS TEXT) AS check_key, "
    "CAST(NULL AS TEXT) AS reason, CAST(NULL AS DATE) AS set_aside_on FROM clean.supplier_prices s WHERE 1 = 0",
    "stock_counts": "CREATE TABLE quarantine.stock_counts AS SELECT c.*, CAST(NULL AS TEXT) AS check_key, "
    "CAST(NULL AS TEXT) AS reason, CAST(NULL AS DATE) AS set_aside_on FROM clean.stock_counts c WHERE 1 = 0",
    # Personal data does not travel: a member is set aside by number only.
    "members": "CREATE TABLE quarantine.members (member_id TEXT, check_key TEXT, reason TEXT, set_aside_on DATE)",
}

GATE_RESULTS = (
    "CREATE TABLE IF NOT EXISTS ops.gate_results (run_day DATE, gate TEXT, test_name TEXT, "
    "number INTEGER, title TEXT, action TEXT, lens TEXT, status TEXT, bad_rows INTEGER)"
)


def empty_quarantine(wh: Warehouse) -> None:
    """Start the night with empty quarantine tables. The clean tables were just rebuilt
    from the raw rows, so every bad row is back, and is judged again."""
    wh.execute("CREATE SCHEMA IF NOT EXISTS quarantine")
    for table, create in QUARANTINE_TABLES.items():
        wh.execute(f"DROP TABLE IF EXISTS quarantine.{table}")
        wh.execute(create)


# ---------------------------------------------------------------- landing: refuse a file


def refuse_files(wh: Warehouse, tests: list[TestResult], day: date) -> list[str]:
    """Set aside whole files: a missing column, or a supplier file that breaks the contract."""
    wh.execute(REFUSED_FILES)
    wh.execute("DELETE FROM ops.refused_files")
    for test in tests:
        if test.action == "refuse_file" and test.failed:
            wh.execute(
                f"INSERT INTO ops.refused_files SELECT file_name, table_name, ?, "
                f"'An expected column is missing: ' || min(missing_column), CAST(? AS DATE) "
                f"FROM {test.table} GROUP BY file_name, table_name",
                (test.name, day),
            )
    refused = {r["file_name"] for r in wh.query("SELECT file_name FROM ops.refused_files")}
    loaded = wh.query("SELECT file_name FROM raw.loaded_files WHERE table_name = 'supplier_prices'")
    broke_the_contract = 0
    for row in loaded:
        path = config.LANDING / "supplier_prices" / row["file_name"]
        if not path.exists():
            continue
        problems = contract.check_file(path)
        if contract.verdict(problems) != "refuse":
            continue
        broke_the_contract += 1
        if row["file_name"] in refused:
            continue
        worst = next(p for p in problems if p["kind"] in ("breaking", "value"))
        wh.execute(
            "INSERT INTO ops.refused_files VALUES (?, 'supplier_prices', 'supplier_contract', ?, ?)",
            (
                row["file_name"],
                f"The contract is broken: {worst['column']}: {worst['problem']}",
                day,
            ),
        )
    tests.append(
        TestResult(
            name="supplier_contract",
            title="A supplier's file keeps the contract",
            number=42,
            action="refuse_file",
            lens="business",
            protects="Finance: they pay suppliers from this file",
            status="fail" if broke_the_contract else "pass",
            bad_rows=broke_the_contract,
        )
    )
    return sorted(r["file_name"] for r in wh.query("SELECT file_name FROM ops.refused_files"))


# ---------------------------------------------------------------- clean: set rows aside


def _set_sales_lines_aside(wh: Warehouse, tests: list[TestResult], day: date) -> None:
    for test in tests:
        wh.execute(
            f"INSERT INTO quarantine.sales_lines SELECT l.*, ?, ?, ?, CAST(? AS DATE) "
            f"FROM clean.sales_lines l WHERE l.line_id IN (SELECT line_id FROM {test.table})",
            (test.name, test.title, test.is_a_copy, day),
        )
    # Too much of one file is bad: do not trust the rest of it either.
    wh.execute(
        f"""
        INSERT INTO quarantine.sales_lines
        SELECT l.*, ?, ?, FALSE, CAST(? AS DATE)
        FROM clean.sales_lines l
        JOIN (
            SELECT bad.store_id, bad.file_day
            FROM (
                SELECT store_id, file_day, count(DISTINCT line_id) AS bad_lines
                FROM quarantine.sales_lines WHERE NOT is_a_copy
                GROUP BY store_id, file_day
            ) bad
            JOIN (
                SELECT store_id, file_day, count(*) AS all_lines
                FROM clean.sales_lines GROUP BY store_id, file_day
            ) whole
              ON whole.store_id = bad.store_id AND whole.file_day = bad.file_day
            WHERE bad.bad_lines > {config.QUARANTINE_LIMIT} * whole.all_lines
        ) refused
          ON refused.store_id = l.store_id AND refused.file_day = l.file_day
        WHERE l.line_id NOT IN (SELECT line_id FROM quarantine.sales_lines)
        """,
        (TOO_MANY, TOO_MANY_REASON, day),
    )
    wh.execute(
        "DELETE FROM clean.sales_lines WHERE line_id IN (SELECT line_id FROM quarantine.sales_lines)"
    )


def set_rows_aside(wh: Warehouse, tests: list[TestResult], day: date) -> dict[str, int]:
    """Move the rows of every failed quarantine test out of the clean tables."""
    failed = [t for t in tests if t.action == "quarantine" and t.failed]

    def of(table: str) -> list[TestResult]:
        return [t for t in failed if t.sets_aside == table]

    _set_sales_lines_aside(wh, of("sales_lines"), day)

    # A sales line with no product never reached the clean table. Record it from the raw rows.
    for test in of("raw_sales_lines"):
        wh.execute(
            f"INSERT INTO quarantine.raw_sales_lines "
            f"SELECT l.line_id, l.store_id, CAST(substr(l._file_name, 17, 10) AS DATE), l.product_id, "
            f"CAST(l.amount_cents AS BIGINT), ?, ?, CAST(? AS DATE) "
            f"FROM raw.sales_lines l WHERE l.line_id IN (SELECT line_id FROM {test.table})",
            (test.name, test.title, day),
        )

    for test in of("supplier_prices"):
        wh.execute(
            f"INSERT INTO quarantine.supplier_prices SELECT s.*, ?, ?, CAST(? AS DATE) "
            f"FROM clean.supplier_prices s JOIN (SELECT DISTINCT supplier_id, product_id, day FROM {test.table}) f "
            f"ON f.supplier_id = s.supplier_id AND f.product_id = s.product_id AND f.day = s.price_date",
            (test.name, test.title, day),
        )
    wh.execute(
        "DELETE FROM clean.supplier_prices WHERE EXISTS (SELECT 1 FROM quarantine.supplier_prices q "
        "WHERE q.supplier_id = clean.supplier_prices.supplier_id "
        "AND q.product_id = clean.supplier_prices.product_id "
        "AND q.price_date = clean.supplier_prices.price_date)"
    )

    for test in of("stock_counts"):
        wh.execute(
            f"INSERT INTO quarantine.stock_counts SELECT c.*, ?, ?, CAST(? AS DATE) "
            f"FROM clean.stock_counts c JOIN (SELECT DISTINCT store_id, product_id, sales_date FROM {test.table}) f "
            f"ON f.store_id = c.store_id AND f.product_id = c.product_id AND f.sales_date = c.count_date",
            (test.name, test.title, day),
        )
    wh.execute(
        "DELETE FROM clean.stock_counts WHERE EXISTS (SELECT 1 FROM quarantine.stock_counts q "
        "WHERE q.store_id = clean.stock_counts.store_id AND q.product_id = clean.stock_counts.product_id "
        "AND q.count_date = clean.stock_counts.count_date)"
    )

    for test in of("members"):
        wh.execute(
            f"INSERT INTO quarantine.members SELECT DISTINCT thing, ?, ?, CAST(? AS DATE) FROM {test.table}",
            (test.name, test.title, day),
        )
    wh.execute(
        "DELETE FROM clean.members WHERE member_id IN (SELECT member_id FROM quarantine.members)"
    )

    return {
        "sales_lines": wh.value("SELECT count(DISTINCT line_id) FROM quarantine.sales_lines"),
        "raw_sales_lines": wh.value("SELECT count(*) FROM quarantine.raw_sales_lines"),
        "supplier_prices": wh.value(
            "SELECT count(*) FROM (SELECT DISTINCT supplier_id, product_id, price_date "
            "FROM quarantine.supplier_prices) q"
        ),
        "stock_counts": wh.value(
            "SELECT count(*) FROM (SELECT DISTINCT store_id, product_id, count_date "
            "FROM quarantine.stock_counts) q"
        ),
        "members": wh.value("SELECT count(DISTINCT member_id) FROM quarantine.members"),
    }


# ---------------------------------------------------------------- one gate


def record(wh: Warehouse, result: GateResult) -> None:
    """Remember what this gate found tonight."""
    wh.execute(GATE_RESULTS)
    wh.execute(
        "DELETE FROM ops.gate_results WHERE run_day = ? AND gate = ?", (result.day, result.gate)
    )
    for t in result.tests:
        wh.execute(
            "INSERT INTO ops.gate_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                result.day,
                result.gate,
                t.name,
                t.number,
                t.title,
                t.action,
                t.lens,
                t.status,
                t.bad_rows,
            ),
        )


def run_gate(gate: str, day: date) -> GateResult:
    """Run one gate for one night, act on what it finds, and remember it."""
    if gate not in GATES:
        raise ValueError(f"There is no gate '{gate}'. The gates are: {', '.join(GATES)}")
    with connect() as wh:  # the tables the tests read must exist before dbt starts
        wh.execute(REFUSED_FILES)
        if gate == "clean":
            empty_quarantine(wh)
    tests = dbt_runner.build(gate)

    set_aside, refused = {}, []
    with connect() as wh:
        if gate == "landing":
            refused = refuse_files(wh, tests, day)
        if gate == "clean":
            set_aside = set_rows_aside(wh, tests, day)
        result = GateResult(gate, day, tests, set_aside, refused)
        record(wh, result)
    return result


def format_gate(result: GateResult) -> str:
    data_tests = [t for t in result.tests if t.kind == "data test"]
    urgent_first = ["block", "refuse_file", "quarantine", "warn"]
    failed = sorted(
        (t for t in data_tests if t.failed), key=lambda t: (urgent_first.index(t.action), t.number)
    )
    lines = [
        f"gate {result.gate}: {len(data_tests)} tests, {len(data_tests) - len(failed)} passed."
    ]
    for t in failed:
        what = {
            "block": "BLOCK",
            "quarantine": "set aside",
            "refuse_file": "file refused",
            "warn": "warning",
        }[t.action]
        lines.append(f"    {what:<13}{t.bad_rows:>4}  {t.title}")
    if any(result.set_aside.values()):
        counts = ", ".join(f"{n} in {table}" for table, n in result.set_aside.items() if n)
        lines.append(f"    rows set aside: {counts}")
    if result.refused:
        lines.append(f"    files refused: {', '.join(result.refused)}")
    return "\n".join(lines)
