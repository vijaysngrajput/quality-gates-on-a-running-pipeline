"""Shared set-up for the tests.

The tests use their own landing folder and their own database, never yours.
Every pipeline test runs on DuckDB. It runs on Postgres too when the warehouse is up:
    docker compose up -d warehouse
"""

import shutil
from datetime import date, timedelta
from pathlib import Path

import pytest

from lotus_pipeline import config, pipeline, report
from lotus_pipeline.warehouse import WarehouseDown, connect, plain
from store_systems import files, truth
from store_systems.catalog import BONUS_WEEK_START, LAST_DAY
from store_systems.generate import build

WEEK = [BONUS_WEEK_START + timedelta(days=n) for n in range(7)]
SCHEMAS = ("raw", "clean", "output", "ops", "staging", "quarantine", "audit", "prepared")


@pytest.fixture(scope="session")
def world():
    return build()


@pytest.fixture(scope="session")
def key(world):
    """The answer key: (day, store) -> the true numbers. No pipeline code made it."""
    return truth.daily_store_sales(world)


@pytest.fixture(scope="session")
def landing(tmp_path_factory):
    """Every file the stores send in five weeks. A run only sees what had arrived by its night."""
    folder = tmp_path_factory.mktemp("landing")
    files.deliver_through(LAST_DAY, folder)
    return folder


def _use(kind: str, landing, folder) -> None:
    config.WAREHOUSE = kind
    config.LANDING = landing
    config.DUCKDB_PATH = folder / "lotus_test.duckdb"
    config.DATABASE = "lotus_m4_test"
    config.DBT_TARGET = folder / "dbt_target"


def _empty_postgres() -> None:
    with connect() as wh:
        for schema in SCHEMAS:
            wh.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def rows(sql: str, params: tuple = ()) -> list[dict]:
    with connect() as wh:
        return [{k: plain(v) for k, v in r.items()} for r in wh.query(sql, params)]


def snapshot(day: date) -> dict:
    """What the warehouse holds right after the run for `day`."""
    with connect() as wh:
        bonus = report.bonus_report(wh, day)
    return {
        "sales": {
            (r["sales_date"], r["store_id"]): r
            for r in rows("SELECT * FROM output.daily_store_sales")
        },
        "status": {
            (r["sales_date"], r["store_id"]): r
            for r in rows("SELECT * FROM output.store_day_status")
        },
        "gates": {
            r["test_name"]: r
            for r in rows("SELECT * FROM ops.gate_results WHERE run_day = ?", (day,))
        },
        "report": bonus,
        "set_aside": {
            "sales_lines": rows(
                "SELECT DISTINCT line_id, store_id, file_day, check_key FROM quarantine.sales_lines"
            ),
            "raw_sales_lines": rows("SELECT * FROM quarantine.raw_sales_lines"),
            "supplier_prices": rows("SELECT * FROM quarantine.supplier_prices"),
            "stock_counts": rows("SELECT * FROM quarantine.stock_counts"),
            "members": rows("SELECT * FROM quarantine.members"),
        },
        "refused": rows("SELECT * FROM ops.refused_files ORDER BY file_name"),
        "orders": {
            (r["store_id"], r["product_id"]): r
            for r in rows(
                "SELECT * FROM output.replenishment_list WHERE for_date = CAST(? AS DATE) + 1",
                (day,),
            )
        },
        "members": rows("SELECT member_id FROM clean.members ORDER BY member_id"),
    }


@pytest.fixture(scope="session", params=["duckdb", "postgres"])
def week(request, landing, tmp_path_factory):
    """Run the bonus week night by night, through the gates. A snapshot of every night."""
    kind = request.param
    folder = tmp_path_factory.mktemp(kind)
    _use(kind, landing, folder)
    if kind == "postgres":
        try:
            _empty_postgres()
        except WarehouseDown:
            pytest.skip("Postgres is not running: docker compose up -d warehouse")
    nights = {}
    for day in WEEK:
        pipeline.run(day, say=lambda text: None)
        nights[day] = snapshot(day)
    return {"kind": kind, "nights": nights, "last": nights[WEEK[-1]],
            "point_at": lambda: _use(kind, landing, folder)}  # fmt: skip


@pytest.fixture(autouse=True)
def _a_test_of_the_week_reads_the_week(request):
    """Other fixtures point the pipeline at their own warehouse. Point it back first."""
    if "week" in request.fixturenames:
        request.getfixturevalue("week")["point_at"]()


@pytest.fixture(scope="session")
def clean_weeks(landing, tmp_path_factory):
    """A DuckDB file that holds the four clean weeks only: the run for Sunday 23 August."""
    folder = tmp_path_factory.mktemp("clean_weeks")
    _use("duckdb", landing, folder)
    pipeline.run(WEEK[0] - timedelta(days=1), say=lambda text: None)
    return {"file": folder / "lotus_test.duckdb", "night": snapshot(WEEK[0] - timedelta(days=1))}


@pytest.fixture
def lab(clean_weeks, landing, tmp_path):
    """A private copy of the four clean weeks. A test plants one fault in it, then runs the gates."""
    shutil.copy(clean_weeks["file"], tmp_path / "lotus_test.duckdb")
    wal = Path(str(clean_weeks["file"]) + ".wal")  # changes DuckDB has not folded into the file yet
    if wal.exists():
        shutil.copy(wal, tmp_path / "lotus_test.duckdb.wal")
    _use("duckdb", landing, tmp_path)
    return date(2026, 8, 23)


@pytest.fixture
def own_warehouse(tmp_path, landing):
    """Point the pipeline at an empty DuckDB file, and open nothing."""
    _use("duckdb", landing, tmp_path)


@pytest.fixture
def empty_warehouse(tmp_path, landing):
    """An empty DuckDB file, for tests that build their own small tables."""
    _use("duckdb", landing, tmp_path)
    with connect() as wh:
        yield wh


def plant(sql: str, params: tuple = ()) -> None:
    with connect() as wh:
        wh.execute(sql, params)
