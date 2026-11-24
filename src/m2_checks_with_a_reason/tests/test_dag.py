"""The DAG file. Airflow is not installed in the project's virtual environment,
so the file is read as text here. When the Airflow container is running, it is asked too."""

import ast
import re
import sys
from pathlib import Path

import pytest

MILESTONE = Path(__file__).resolve().parent.parent
DAG_FILE = MILESTONE / "dags" / "lotus_nightly_dag.py"
SOURCE = DAG_FILE.read_text()
TASKS = ["stores_send_files", "load", "clean", "check", "publish"]


def test_the_file_is_valid_python():
    ast.parse(SOURCE)


def test_five_tasks_in_one_line():
    assert re.findall(r'task_id="(\w+)"', SOURCE) == TASKS
    assert " >> ".join(TASKS) in SOURCE


def test_every_task_starts_a_plain_command():
    """Airflow holds no business logic: each task is the command a person can type."""
    for step in ("load", "clean", "check", "publish", "report"):
        assert f"{{PIPELINE}} {step} --day {{BUSINESS_DAY}}" in SOURCE
    assert "import lotus_pipeline" not in SOURCE and "from lotus_pipeline" not in SOURCE


def test_it_never_runs_by_itself_on_old_nights():
    assert "catchup=False" in SOURCE and "max_active_runs=1" in SOURCE


def test_the_real_airflow_reads_it_without_errors():
    sys.path.insert(0, str(MILESTONE))
    import airflow_tools

    if not airflow_tools.is_running():
        pytest.skip("Airflow is not running: docker compose up -d --build")
    airflow_tools.wait_until_ready()
    assert airflow_tools.tasks() == sorted(TASKS)
    assert "lotus_nightly_dag" not in airflow_tools.airflow("dags", "list-import-errors")
