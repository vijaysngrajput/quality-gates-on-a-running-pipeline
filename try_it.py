"""See it work in two minutes, before you install Docker.

    uv run python try_it.py              the given pipeline, for the night of Monday 24 August 2026
    uv run python try_it.py --finished   the finished pipeline, on the same night

Nothing needs to be running. The stores send their files into data/try_it/, and the
pipeline uses DuckDB: a database in one file. Each run starts from nothing.
"""

import os
import shutil
import sys
from datetime import date
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent
FOLDER = PROJECT_DIR / "data" / "try_it"
NIGHT = date(2026, 8, 24)

GIVEN = PROJECT_DIR / "src" / "m1_green_checks_wrong_number"
FINISHED = PROJECT_DIR / "src" / "m5_know_alert_recover"


def main() -> int:
    finished = "--finished" in sys.argv[1:]
    shutil.rmtree(FOLDER, ignore_errors=True)
    os.environ["LOTUS_WAREHOUSE"] = "duckdb"
    os.environ["LOTUS_LANDING"] = str(FOLDER / "landing")
    os.environ["LOTUS_DUCKDB_PATH"] = str(FOLDER / "lotus.duckdb")
    os.environ["LOTUS_ALERTS_LOG"] = str(FOLDER / "alerts.log")
    os.environ["DBT_TARGET_PATH"] = str(FOLDER / "dbt_target")
    sys.path.insert(0, str(FINISHED if finished else GIVEN))

    from lotus_pipeline import pipeline
    from store_systems import files, truth
    from store_systems.generate import build

    count = files.deliver_through(NIGHT, FOLDER / "landing")
    print(f"It is the night after {NIGHT:%A %d %B %Y}. Six stores have closed their tills.")
    print(f"stores:  {count} files sent: four earlier weeks, and this Monday.\n")
    pipeline.run(NIGHT)

    if finished:
        print()
        print("The same data, the same night. Now a gate stopped the bad row, the report says")
        print("which store to hold back and why, and the right person was told.")
        print("The five milestones build this, one step at a time:  README.md")
    if not finished:
        right = truth.daily_store_sales(build())[(NIGHT, "S03")]["net_sales_cents"]
        print()
        print("Five checks, all green. Now add up store S03's receipts yourself:")
        print(
            f"  the receipts say ${right // 100}.{right % 100:02d}. The report above says something else."
        )
        print("Milestone 1 finds out why:  src/m1_green_checks_wrong_number/README.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
