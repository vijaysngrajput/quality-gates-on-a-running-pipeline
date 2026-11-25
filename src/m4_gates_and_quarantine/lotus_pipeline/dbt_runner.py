"""Start dbt from Python, and read what it found.

dbt holds the rules. This file only starts it and turns its answer into plain values.
"""

import os
from dataclasses import dataclass
from pathlib import Path

from lotus_pipeline import config

PROJECT = config.MILESTONE_DIR / "dbt_project"


@dataclass
class TestResult:
    name: str  # the name of the test: the name of its file
    title: str  # the rule, in words
    number: int
    action: str  # block, quarantine, refuse_file or warn
    lens: str
    protects: str
    status: str  # pass or fail
    bad_rows: int
    sets_aside: str | None = None  # the table whose rows this test sets aside
    is_a_copy: bool = False
    kind: str = "data test"  # or unit test

    @property
    def failed(self) -> bool:
        return self.status != "pass"

    @property
    def table(self) -> str:
        """Where dbt stored the rows this test returned."""
        return f"audit.{self.name}"


class DbtError(RuntimeError):
    pass


def _environment() -> dict:
    """Tell dbt's profiles.yml which warehouse the pipeline is using right now."""
    return {
        "LOTUS_WAREHOUSE": config.WAREHOUSE,
        "LOTUS_PG_HOST": config.PG_HOST,
        "LOTUS_PG_PORT": str(config.PG_PORT),
        "LOTUS_DATABASE": config.DATABASE,
        "LOTUS_DUCKDB_PATH": str(config.DUCKDB_PATH),
    }


def invoke(args: list[str], quiet: bool = True):
    """Run one dbt command inside this Python process."""
    from dbt.cli.main import dbtRunner

    target = os.environ.get("DBT_TARGET_PATH", str(config.DBT_TARGET))
    logs = os.environ.get("DBT_LOG_PATH", str(Path(target) / "logs"))
    command = [
        *args, "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT),
        "--target-path", target, "--log-path", logs,
    ]  # fmt: skip
    if quiet:  # the pipeline prints its own summary. dbt's own lines stay in its log file
        command = ["--quiet", "--no-use-colors", "--log-level", "none", *command]
    before = dict(os.environ)
    os.environ.update(_environment())
    try:
        return dbtRunner().invoke(command)
    finally:
        os.environ.clear()
        os.environ.update(before)


def _title(node) -> str:
    first = (getattr(node, "raw_code", "") or "").strip().splitlines()[:1]
    if first and first[0].startswith("--"):
        return first[0].lstrip("- ").strip()
    return node.name.replace("standard_", "standard check: ").replace("_", " ")


def build(tag: str) -> list[TestResult]:
    """Build the models the tests of one gate need, run those tests, return one result each.

    Two dbt commands:
        dbt run  --select +tag:<gate>    build the prepared models the tests read
        dbt test --select tag:<gate>     run the data tests of this gate
    Unit tests are not run here. They test the SQL, not tonight's data, so they run
    when the code changes: `python -m lotus_pipeline dbt test --select test_type:unit`.
    """
    built = invoke(["run", "--select", f"+tag:{tag}"])
    if built.exception is not None or not built.success:
        failed = [
            f"{r.node.name}: {r.message}"
            for r in (built.result or [])
            if str(r.status) != "success"
        ]
        raise DbtError(f"dbt could not build the prepared models: {built.exception or failed}")
    done = invoke(["test", "--select", f"tag:{tag}"])
    if done.exception is not None or done.result is None:
        raise DbtError(f"dbt could not run: {done.exception}")
    results = []
    for r in done.result:
        node = r.node
        if node.resource_type not in ("test", "unit_test"):
            if str(r.status) not in ("success", "pass"):
                raise DbtError(f"dbt could not build {node.name}: {r.message}")
            continue
        if node.resource_type == "unit_test":
            results.append(
                TestResult(node.name, node.name.replace("_", " "), 0, "block", "data", "",
                           "pass" if str(r.status) == "pass" else "fail", r.failures or 0,
                           kind="unit test")
            )  # fmt: skip
            continue
        if str(r.status) == "error":
            raise DbtError(f"The test {node.name} could not run: {r.message}")
        meta = node.config.meta
        results.append(
            TestResult(
                name=node.name,
                title=_title(node),
                number=int(meta.get("number", 0)),
                action=meta.get("action", "block"),
                lens=meta.get("lens", "data"),
                protects=meta.get("protects", ""),
                status="pass" if str(r.status) == "pass" else "fail",
                bad_rows=int(r.failures or 0),
                sets_aside=meta.get("sets_aside"),
                is_a_copy=bool(meta.get("is_a_copy", False)),
            )
        )
    return sorted(results, key=lambda t: (t.kind, t.number, t.name))
