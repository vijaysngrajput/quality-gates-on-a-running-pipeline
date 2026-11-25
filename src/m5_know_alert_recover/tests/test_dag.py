"""The DAG files. Airflow is not installed in the project's virtual environment,
so the files are read as text here. When the Airflow container is running, it is asked too."""

import ast
import re
import sys
from pathlib import Path

import pytest

MILESTONE = Path(__file__).resolve().parent.parent
SOURCE = (MILESTONE / "dags" / "lotus_nightly_dag.py").read_text()
TASKS = ["stores_send_files", "load", "gate_landing", "clean", "gate_clean", "build",
         "gate_publish", "publish"]  # fmt: skip


def test_the_file_is_valid_python():
    ast.parse(SOURCE)


def test_eight_tasks_in_one_line_with_a_gate_in_three_places():
    arrows = re.search(r"\(\s*(stores_send_files >>.*?publish)\s*\)", SOURCE, re.S).group(1)
    assert [t.strip() for t in arrows.split(">>")] == TASKS
    assert TASKS.index("load") < TASKS.index("gate_landing") < TASKS.index("clean")
    assert TASKS.index("clean") < TASKS.index("gate_clean") < TASKS.index("build")
    assert TASKS.index("build") < TASKS.index("gate_publish") < TASKS.index("publish")


def test_every_task_starts_a_plain_command():
    """Airflow holds no business logic, and no rule: the rules are in dbt."""
    for step in ("load", "gate-landing", "clean", "gate-clean", "build", "gate-publish"):
        assert f'step("{step}")' in SOURCE
    assert "import lotus_pipeline" not in SOURCE and "from lotus_pipeline" not in SOURCE
    assert "SELECT" not in SOURCE


def test_someone_is_told_when_a_task_fails_and_when_the_night_is_published():
    """The callbacks: a real Airflow feature. Only where the alert goes is local."""
    assert '"on_failure_callback": tell_someone' in SOURCE  # on every task
    assert "on_success_callback=tell_someone" in SOURCE  # on publish
    assert "alerts --day" in SOURCE and "--failed-task" in SOURCE


def test_a_gate_is_not_tried_again():
    assert '"retries": 0' in SOURCE and "catchup=False" in SOURCE and "max_active_runs=1" in SOURCE


def test_the_real_airflow_reads_the_dag_without_errors():
    sys.path.insert(0, str(MILESTONE))
    import airflow_tools

    if not airflow_tools.is_running():
        pytest.skip("Airflow is not running: docker compose up -d --build")
    airflow_tools.wait_until_ready()
    assert airflow_tools.tasks() == sorted(TASKS)
    errors = airflow_tools.airflow("dags", "list-import-errors")
    assert "m5_know_alert_recover" not in errors
