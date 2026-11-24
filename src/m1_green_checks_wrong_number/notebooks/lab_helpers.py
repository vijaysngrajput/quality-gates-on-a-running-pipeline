"""Small helpers for the milestone 1 lab.

Nothing here is hidden magic. Open this file any time you want to see how
something works.
"""

import json
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd
from IPython.display import Markdown, display

LAB_DIR = Path(__file__).resolve().parent
MILESTONE_DIR = LAB_DIR.parent
PROJECT_DIR = MILESTONE_DIR.parents[1]
RESULTS_DIR = LAB_DIR / "results"

# Lets a notebook write `from lotus_pipeline import ...` (the production code)
# and `import airflow_tools`.
if str(MILESTONE_DIR) not in sys.path:
    sys.path.insert(0, str(MILESTONE_DIR))

from lotus_pipeline import config, pipeline  # noqa: E402
from lotus_pipeline.warehouse import WarehouseDown, connect, plain  # noqa: E402
from store_systems import files  # noqa: E402

CHAPTERS = "https://www.d8loop.com/modules/knowledge-corner/data-pipeline/"
LANDING = config.LANDING

pd.set_option("display.max_rows", 60)
pd.set_option("display.width", 160)


# ---- Is everything running? -------------------------------------------------


def check_setup(airflow: bool = False) -> None:
    """Say whether the warehouse is running, and Airflow when the notebook needs it."""
    try:
        with connect() as wh:
            wh.value("SELECT 1")
        print(f"The warehouse is running: Postgres on port {config.PG_PORT}.")
    except WarehouseDown as down:
        print(down)
        return
    if airflow:
        import airflow_tools

        if airflow_tools.is_running():
            print("Airflow is running: http://localhost:8080")
        else:
            print(airflow_tools.HOW_TO_START)


# ---- The stores -------------------------------------------------------------


def stores_send(through: str) -> None:
    """The stores send every file up to the night after `through`."""
    count = files.deliver_through(date.fromisoformat(through), LANDING)
    print(f"The stores sent {count} files, up to the night of {through}.")
    print(f"They are in {LANDING.relative_to(PROJECT_DIR)}")


def show_file(table: str, name: str, rows: int = 5) -> pd.DataFrame:
    """The first rows of one file in the landing folder, exactly as it arrived."""
    return pd.read_csv(LANDING / table / name, dtype=str, keep_default_na=False).head(rows)


# ---- The warehouse ----------------------------------------------------------


def sql(query: str, params: tuple = ()) -> pd.DataFrame:
    """Run one SQL query on the warehouse, and show the result as a table."""
    with connect() as wh:
        rows = wh.query(query, params)
    return pd.DataFrame([{k: plain(v) for k, v in row.items()} for row in rows])


def run_pipeline(day: str, steps: tuple[str, ...] = pipeline.STEPS) -> None:
    """Run the pipeline's steps for one business day, as a plain program."""
    pipeline.run(date.fromisoformat(day), steps)


# ---- Small things -----------------------------------------------------------


def money(cents: int) -> str:
    """Whole cents as dollars: 79618 becomes $796.18."""
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) // 100:,}.{abs(cents) % 100:02d}"


def show_doc(name: str, heading: str) -> None:
    """Show one section of a design document or a check card."""
    folder = "checks" if name.startswith("sales_lines") else "docs"
    text = (MILESTONE_DIR / folder / name).read_text()
    match = re.search(rf"^## {re.escape(heading)}\n(.*?)(?=^## |\Z)", text, re.M | re.S)
    if not match:
        raise ValueError(f"There is no section '{heading}' in {name}")
    display(Markdown(f"**From `{folder}/{name}`, section {heading}**\n\n{match.group(1).strip()}"))


def save_result(name: str, value) -> None:
    """Keep a result, so a later notebook and a test can read it."""
    RESULTS_DIR.mkdir(exist_ok=True)
    path = RESULTS_DIR / f"{name}.json"
    path.write_text(json.dumps(value, indent=2) + "\n")
    print(f"Saved {path.relative_to(MILESTONE_DIR)}")


def load_result(name: str):
    path = RESULTS_DIR / f"{name}.json"
    if not path.exists():
        raise FileNotFoundError(f"Run the earlier notebooks first: {path.name} is not there yet.")
    return json.loads(path.read_text())
