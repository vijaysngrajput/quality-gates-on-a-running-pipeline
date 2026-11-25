"""The Lotus Retails nightly pipeline, with three gates, and alerts.

    stores_send_files >> load >> gate_landing >> clean >> gate_clean >> build >> gate_publish >> publish

New in milestone 5: CALLBACKS. A callback is a function Airflow calls by itself when a task
changes state. Two are used here:

    on_failure_callback   on every task. A gate that blocks, or a step that crashes: page someone
    on_success_callback   on publish. The night is done: send the tickets for what was set aside

Both call the same plain command, which decides who is told and how urgently:
    python -m lotus_pipeline alerts --day 2026-08-24

Airflow still holds no business logic. It knows WHEN to tell. The pipeline knows WHOM and WHAT.
"""

import subprocess
from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, CronDataIntervalTimetable

MILESTONE_DIR = Path(__file__).resolve().parent.parent  # .../src/m5_know_alert_recover
PROJECT_DIR = MILESTONE_DIR.parents[1]
PYTHON = "/opt/lotus-venv/bin/python"  # the pipeline's own virtual environment, inside the image
PIPELINE = f"cd {MILESTONE_DIR} && {PYTHON} -m lotus_pipeline"
STORES = f"cd {PROJECT_DIR} && {PYTHON} -m store_systems"

# Every run is FOR one business day: the day the stores were open. In templates it is {{ ds }}.
BUSINESS_DAY = "{{ ds }}"


def tell_someone(context) -> None:
    """Called by Airflow when a task fails, and when publish succeeds."""
    task = context["ti"].task_id
    day = context["ds"]
    failed = context["ti"].state == "failed"
    command = f"{PIPELINE} alerts --day {day}" + (f" --failed-task {task}" if failed else "")
    done = subprocess.run(command, shell=True, capture_output=True, text=True)
    print(done.stdout or done.stderr)  # the alerts also show in this task's log


def step(name: str) -> BashOperator:
    """One step of the pipeline as one task. `gate-clean` becomes the task `gate_clean`."""
    return BashOperator(
        task_id=name.replace("-", "_"), bash_command=f"{PIPELINE} {name} --day {BUSINESS_DAY}"
    )


with DAG(
    dag_id="lotus_nightly_m5",
    description="Load, gate, clean, gate, build, gate, publish. And tell the person who can act.",
    schedule=CronDataIntervalTimetable("0 2 * * *", timezone="America/Chicago"),
    start_date=datetime(2026, 7, 27, 7, 0),
    end_date=datetime(2026, 8, 31, 7, 0),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": tell_someone},
    tags=["lotus", "milestone 5"],
) as dag:
    stores_send_files = BashOperator(
        task_id="stores_send_files",
        bash_command=f"{STORES} deliver --through {BUSINESS_DAY}",
    )
    load = step("load")
    gate_landing = step("gate-landing")
    clean = step("clean")
    gate_clean = step("gate-clean")
    build = step("build")
    gate_publish = step("gate-publish")

    publish = BashOperator(
        task_id="publish",
        bash_command=(
            f"{PIPELINE} publish --day {BUSINESS_DAY} && {PIPELINE} report --day {BUSINESS_DAY}"
        ),
        on_success_callback=tell_someone,
    )

    (
        stores_send_files >> load >> gate_landing >> clean >> gate_clean
        >> build >> gate_publish >> publish
    )  # fmt: skip
