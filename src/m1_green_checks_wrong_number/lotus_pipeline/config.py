"""Settings. Everything a person might change is here."""

import os
from pathlib import Path

MILESTONE_DIR = Path(__file__).resolve().parent.parent
PROJECT_DIR = MILESTONE_DIR.parents[1]

# Where the stores' files arrive. The store systems write here:
#   uv run python -m store_systems deliver --day 2026-08-24
LANDING = Path(os.environ.get("LOTUS_LANDING", PROJECT_DIR / "data" / "landing"))

# Which warehouse to use.
#   postgres  the real one, in Docker. Start it with:  docker compose up -d warehouse
#   duckdb    a database in one file. Nothing to start. The two-minute run uses it
WAREHOUSE = os.environ.get("LOTUS_WAREHOUSE", "postgres")

# Postgres. Inside Docker, Airflow reaches it as host "warehouse", port 5432.
PG_HOST = os.environ.get("LOTUS_PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("LOTUS_PG_PORT", "5450"))
PG_USER = "lotus"
PG_PASSWORD = "lotus"
DATABASE = os.environ.get("LOTUS_DATABASE", "lotus_m1")  # each milestone has its own database

# DuckDB: one file.
DUCKDB_PATH = Path(
    os.environ.get("LOTUS_DUCKDB_PATH", MILESTONE_DIR / "data" / "warehouse" / "lotus.duckdb")
)

RUN_HOUR = 2  # the nightly run is at 02:00, for the business day that just ended
BONUS_RATE_PERCENT = 1  # a manager's bonus is 1% of the store's net sales for the week
