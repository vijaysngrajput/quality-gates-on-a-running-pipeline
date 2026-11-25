"""The Lotus Retails nightly pipeline, with three gates.

One DAG, eight tasks:

    stores_send_files >> load >> gate_landing >> clean >> gate_clean >> build >> gate_publish >> publish

A GATE is a task that runs the dbt tests of one place in the pipeline and acts on the result.
dbt holds each rule. Airflow decides what waits for it.

    gate_landing   when the files have landed. It can refuse a whole file
    gate_clean     after cleaning. It sets bad rows aside, and blocks on a broken reference table
    gate_publish   before the outputs go out. It blocks on a difference nobody can explain

When a gate BLOCKS, its task fails. Airflow then marks every task after it `upstream_failed`
and does not start them. So `publish` does not run, and the tables people read keep their
last good numbers.

Airflow holds no business logic. Each task starts a command you can type yourself:
    python -m lotus_pipeline gate-clean --day 2026-08-24
"""

from datetime import datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, CronDataIntervalTimetable

MILESTONE_DIR = Path(__file__).resolve().parent.parent  # .../src/m4_gates_and_quarantine
PROJECT_DIR = MILESTONE_DIR.parents[1]
PYTHON = "/opt/lotus-venv/bin/python"  # the pipeline's own virtual environment, inside the image
PIPELINE = f"cd {MILESTONE_DIR} && {PYTHON} -m lotus_pipeline"
STORES = f"cd {PROJECT_DIR} && {PYTHON} -m store_systems"

# Every run is FOR one business day: the day the stores were open. In templates it is {{ ds }}.
BUSINESS_DAY = "{{ ds }}"


def step(name: str) -> BashOperator:
    """One step of the pipeline as one task. `gate-clean` becomes the task `gate_clean`."""
    return BashOperator(
        task_id=name.replace("-", "_"), bash_command=f"{PIPELINE} {name} --day {BUSINESS_DAY}"
    )


with DAG(
    dag_id="lotus_nightly_m4",
    description="Load, clean and publish, with a gate when files land, after cleaning, and before publish.",
    schedule=CronDataIntervalTimetable("0 2 * * *", timezone="America/Chicago"),
    start_date=datetime(2026, 7, 27, 7, 0),
    end_date=datetime(2026, 8, 31, 7, 0),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0
    },  # a test that failed on the data will fail again. A person must look
    tags=["lotus", "milestone 4"],
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

    # Copy the audited tables over the tables people read, and print the bonus report.
    publish = BashOperator(
        task_id="publish",
        bash_command=(
            f"{PIPELINE} publish --day {BUSINESS_DAY} && {PIPELINE} report --day {BUSINESS_DAY}"
        ),
    )

    (
        stores_send_files >> load >> gate_landing >> clean >> gate_clean
        >> build >> gate_publish >> publish
    )  # fmt: skip
