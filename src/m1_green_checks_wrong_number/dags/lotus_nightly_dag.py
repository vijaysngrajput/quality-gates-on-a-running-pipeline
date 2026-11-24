"""The Lotus Retails nightly pipeline, on a schedule.

One DAG, five tasks, four arrows:

    stores_send_files  >>  load  >>  clean  >>  check  >>  publish

Airflow decides WHEN each task runs, in WHAT ORDER, and what happens when one FAILS:
a task that fails stops every task after it. So if `check` fails, `publish` does not run.

Airflow holds no business logic. Each task starts the same command you can type yourself:
    python -m lotus_pipeline load --day 2026-08-24
"""

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, CronDataIntervalTimetable

MILESTONE_DIR = Path(__file__).resolve().parent.parent  # .../src/m1_green_checks_wrong_number
PROJECT_DIR = MILESTONE_DIR.parents[1]
PYTHON = "/opt/lotus-venv/bin/python"  # the pipeline's own virtual environment, inside the image
PIPELINE = f"cd {MILESTONE_DIR} && {PYTHON} -m lotus_pipeline"
STORES = f"cd {PROJECT_DIR} && {PYTHON} -m store_systems"

# Every run is FOR one business day: the day the stores were open. In templates it is {{ ds }}.
# The run HAPPENS at 02:00 the night after, when the stores have sent their files.
BUSINESS_DAY = "{{ ds }}"

with DAG(
    dag_id="lotus_nightly_m1",
    description="Load the stores' files, clean them, run five standard checks, publish two tables.",
    # Every night at 02:00. Each run covers the 24 hours before it.
    schedule=CronDataIntervalTimetable("0 2 * * *", timezone="America/Chicago"),
    start_date=datetime(2026, 7, 27, 7, 0),  # the first business day: Monday 27 July 2026
    end_date=datetime(2026, 8, 31, 7, 0),  # the last one: Sunday 30 August 2026
    catchup=False,  # do not run every past night by itself. A person asks for the nights they want
    max_active_runs=1,  # one night at a time. Two runs must never write the same tables together
    default_args={"retries": 0},
    tags=["lotus", "milestone 1"],
) as dag:
    # The stores in this project are a small program, not real stores. This task makes them
    # send every file up to this night. Real stores need no such task: they send by themselves.
    stores_send_files = BashOperator(
        task_id="stores_send_files",
        bash_command=f"{STORES} deliver --through {BUSINESS_DAY}",
    )

    # Copy the new files into the raw tables, as received.
    load = BashOperator(task_id="load", bash_command=f"{PIPELINE} load --day {BUSINESS_DAY}")

    # Give every column its type, and join the tables that belong together.
    clean = BashOperator(task_id="clean", bash_command=f"{PIPELINE} clean --day {BUSINESS_DAY}")

    # The five standard checks. If one fails, this task fails, and publish does not run.
    check = BashOperator(task_id="check", bash_command=f"{PIPELINE} check --day {BUSINESS_DAY}")

    # Build the two tables people read, and print the bonus report into the task's log.
    publish = BashOperator(
        task_id="publish",
        bash_command=(
            f"{PIPELINE} publish --day {BUSINESS_DAY} && {PIPELINE} report --day {BUSINESS_DAY}"
        ),
    )

    stores_send_files >> load >> clean >> check >> publish
