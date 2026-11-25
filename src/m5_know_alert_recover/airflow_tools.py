"""Small tools to drive Airflow from Python: the lab notebooks and the tests use them.

Every function here runs a normal `airflow` command inside the Airflow container.
You could type each one in a terminal, from the project folder:

    docker compose exec airflow airflow <command>
"""

import json
import subprocess
import time
from datetime import date, timedelta
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]  # where docker-compose.yml is
DAG = "lotus_nightly_m5"

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

    Airflow keeps one run per DAG and day. So the first time this TRIGGERS a new run,
    and after that it CLEARS the run that is there, which makes Airflow run it again.
    """
    after = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    earlier = [r for r in _json("dags", "list-runs", dag_id) if r["logical_date"].startswith(day)]
    if earlier:
        run_id, last_start = earlier[0]["run_id"], earlier[0]["start_date"]
        airflow("tasks", "clear", dag_id, "-s", day, "-e", after, "-y")
    else:
        run_id, last_start = f"night_{day}", None
        airflow("dags", "trigger", dag_id, "--logical-date", f"{day}T07:00:00+00:00",
                "--run-id", run_id)  # fmt: skip
    deadline = time.time() + seconds
    while time.time() < deadline:
        runs = [r for r in _json("dags", "list-runs", dag_id) if r["run_id"] == run_id]
        started_again = runs and runs[0]["start_date"] and runs[0]["start_date"] != last_start
        if started_again and runs[0]["state"] in ("success", "failed"):
            return task_states(run_id, dag_id)
        time.sleep(2)
    raise TimeoutError(f"The run for {day} did not finish within {seconds} seconds.")


def task_states(run_id: str, dag_id: str = DAG) -> list[dict]:
    """One row per task of one run: its name, its state, and how long it took."""
    rows = _json("tasks", "states-for-dag-run", dag_id, run_id)
    order = {name: n for n, name in enumerate(
        ["stores_send_files", "load", "gate_landing", "clean", "gate_clean", "build",
         "gate_publish", "publish"])}  # fmt: skip
    rows.sort(key=lambda r: order.get(r["task_id"], 99))
    return [{"task": r["task_id"], "state": r["state"]} for r in rows]


def backfill(first_day: str, last_day: str, dag_id: str = DAG, seconds: int = 900) -> list[dict]:
    """Run a range of past nights again with ONE command, oldest first, one at a time.

    This is Airflow's backfill. `--reprocess-behavior completed` says: run these nights
    again even though they already ran. Without it Airflow skips a night that has a run.
    """
    before = {
        r["run_id"]: r["start_date"]
        for r in _json("dags", "list-runs", dag_id)
        if first_day <= r["logical_date"][:10] <= last_day
    }
    # A night's run is stamped 02:00 in Lotus's time zone, so the range ends the morning after.
    until = (date.fromisoformat(last_day) + timedelta(days=1)).isoformat()
    # Airflow runs one backfill of a DAG at a time, and counts the last one as running for a
    # few seconds after its nights have finished. So ask again until it says yes.
    asked = time.time()
    while True:
        try:
            airflow(
                "backfill", "create", "--dag-id", dag_id, "--from-date", first_day, "--to-date", until,
                "--reprocess-behavior", "completed", "--max-active-runs", "1",
            )  # fmt: skip
            break
        except RuntimeError as refused:
            if "AlreadyRunningBackfill" not in str(refused) or time.time() - asked > 180:
                raise
            time.sleep(5)
    deadline = time.time() + seconds
    while time.time() < deadline:
        runs = [
            r for r in _json("dags", "list-runs", dag_id)
            if first_day <= r["logical_date"][:10] <= last_day
        ]  # fmt: skip
        done = [
            r for r in runs
            if r["state"] in ("success", "failed") and r["start_date"] != before.get(r["run_id"])
        ]  # fmt: skip
        if runs and len(done) == len(runs) and len(runs) >= len(before):
            return sorted(
                ({"day": r["logical_date"][:10], "state": r["state"]} for r in runs),
                key=lambda r: r["day"],
            )
        time.sleep(3)
    raise TimeoutError(
        f"The backfill {first_day} to {last_day} did not finish in {seconds} seconds."
    )


def stores(*args: str) -> str:
    """Run a store_systems command where Airflow's stores live: inside the container.

    Under Airflow the stores' files are in a Docker volume, so a fault or a fix must be
    switched on there:  stores("fix", "--case", "1")
    """
    done = subprocess.run(
        ["docker", "compose", "exec", "-T", "-w", "/opt/lotus", "airflow",
         "/opt/lotus-venv/bin/python", "-m", "store_systems", *args],
        cwd=PROJECT_DIR, capture_output=True, text=True, check=False,
    )  # fmt: skip
    if done.returncode != 0:
        raise RuntimeError(f"store_systems {' '.join(args)} failed:\n{done.stderr[-600:]}")
    return done.stdout.strip()


def alerts_log(last: int = 20) -> str:
    """The last lines of the alerts log that Airflow's callbacks write."""
    done = subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow", "tail", "-n", str(last), "/var/lotus/alerts.log"],
        cwd=PROJECT_DIR, capture_output=True, text=True, check=False,
    )  # fmt: skip
    return done.stdout.strip()
