"""Small tools to drive Airflow from Python: the lab notebooks and the tests use them.

Every function here runs a normal `airflow` command inside the Airflow container.
You could type each one in a terminal, from the project folder:

    docker compose exec airflow airflow <command>
"""

import json
import subprocess
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]  # where docker-compose.yml is
DAG = "lotus_nightly_m2"

HOW_TO_START = (
    "Airflow is not running. Start it from the project folder:\n"
    "  docker compose up -d --build\n"
    "The first start takes a few minutes. Then open http://localhost:8080"
)


def airflow(*args: str, check: bool = True) -> str:
    """Run one airflow command in the container, and return what it printed."""
    try:
        done = subprocess.run(
            ["docker", "compose", "exec", "-T", "airflow", "airflow", *args],
            cwd=PROJECT_DIR, capture_output=True, text=True, check=False,
        )  # fmt: skip
    except FileNotFoundError as error:
        raise ConnectionError("Docker is not installed. See this milestone's README.") from error
    problem = done.stderr.lower()
    if done.returncode != 0 and (
        "is not running" in problem or "no such service" in problem or "daemon" in problem
    ):
        raise ConnectionError(HOW_TO_START)
    if check and done.returncode != 0:
        raise RuntimeError(
            f"airflow {' '.join(args)} failed:\n{(done.stderr or done.stdout)[-800:]}"
        )
    return done.stdout


def _json(*args: str) -> list[dict]:
    text = airflow(*args, "--output", "json")
    start = text.find("[")
    return json.loads(text[start:]) if start != -1 else []


def is_running() -> bool:
    try:
        airflow("version")
        return True
    except (ConnectionError, RuntimeError):
        return False


def wait_until_ready(dag_id: str = DAG, seconds: int = 180) -> None:
    """Wait until Airflow has read the DAG file, then switch the DAG on."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        try:
            if any(d["dag_id"] == dag_id for d in _json("dags", "list")):
                airflow("dags", "unpause", dag_id)
                return
        except RuntimeError:
            pass  # Airflow is still starting
        time.sleep(3)
    raise TimeoutError(f"Airflow did not show the DAG {dag_id} within {seconds} seconds.")


def tasks(dag_id: str = DAG) -> list[str]:
    """The tasks of the DAG, in the order Airflow lists them."""
    return [line.strip() for line in airflow("tasks", "list", dag_id).splitlines() if line.strip()]


def run_night(day: str, dag_id: str = DAG, seconds: int = 300) -> list[dict]:
    """Ask Airflow to run one business day, wait for it, and return the state of every task.

    Running the same day again is safe: the old run is removed first.
    """
    run_id = f"night_{day}"
    airflow("dags", "delete-run", dag_id, run_id, "--yes", check=False)  # not in every version
    done = airflow(
        "dags", "trigger", dag_id, "--logical-date", f"{day}T07:00:00+00:00", "--run-id", run_id,
        check=False,
    )  # fmt: skip
    if "already exists" in done:
        run_id = f"night_{day}_{int(time.time())}"
        airflow("dags", "trigger", dag_id, "--logical-date", f"{day}T07:00:00+00:00",
                "--run-id", run_id)  # fmt: skip
    deadline = time.time() + seconds
    while time.time() < deadline:
        states = task_states(run_id, dag_id)
        runs = [r for r in _json("dags", "list-runs", dag_id) if r["run_id"] == run_id]
        if runs and runs[0]["state"] in ("success", "failed"):
            return states
        time.sleep(2)
    raise TimeoutError(f"The run for {day} did not finish within {seconds} seconds.")


def task_states(run_id: str, dag_id: str = DAG) -> list[dict]:
    """One row per task of one run: its name, its state, and how long it took."""
    rows = _json("tasks", "states-for-dag-run", dag_id, run_id)
    order = {name: n for n, name in enumerate(
        ["stores_send_files", "load", "clean", "check", "publish"])}  # fmt: skip
    rows.sort(key=lambda r: order.get(r["task_id"], 99))
    return [{"task": r["task_id"], "state": r["state"]} for r in rows]
