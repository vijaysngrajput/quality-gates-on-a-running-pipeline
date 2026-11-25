"""What the lab notebooks saved must equal what the production pipeline does."""

import json
from pathlib import Path

import nbformat
import pytest

from conftest import WEEK
from lotus_pipeline import health
from lotus_pipeline.warehouse import connect
from store_systems import truth
from store_systems.catalog import BONUS_WEEK_START

NOTEBOOKS = Path(__file__).resolve().parent.parent / "notebooks"
RESULT = NOTEBOOKS / "results" / "answer_key_monday.json"
MON, TUE, WED, THU, FRI, SAT, SUN = WEEK


def test_there_are_eight_notebooks_and_each_was_run_without_an_error():
    notebooks = sorted(NOTEBOOKS.glob("*.ipynb"))
    assert [p.name[:2] for p in notebooks] == [f"{n:02d}" for n in range(1, 9)]
    for path in notebooks:
        cells = nbformat.read(path, as_version=4).cells
        code = [c for c in cells if c.cell_type == "code"]
        assert all(c.execution_count for c in code), f"{path.name} has a cell that was not run"
        assert not [o for c in code for o in c.outputs if o.output_type == "error"], path.name
        text = "".join(c.source for c in cells)
        assert "What you now know" in text, path.name
        assert "Say it in an interview" in text, path.name
        assert "Who does what." in text, f"{path.name} must say which tool does what"


def test_no_notebook_output_holds_a_path_of_the_computer_it_ran_on():
    for path in sorted(NOTEBOOKS.glob("*.ipynb")):
        assert "/Users/" not in path.read_text(), path.name


def test_the_monday_answer_of_the_lab_equals_the_production_recovery(drill, world):
    """Notebook 8 runs the week and the recovery through Airflow, and saves the answer.
    Here the same recovery runs as a plain program. Both must end in the same place."""
    if not RESULT.exists():
        pytest.skip("Run notebook 8 first: it saves the Monday answer.")
    saved = json.loads(RESULT.read_text())

    drill.stores("deliver", "--through", "2026-08-30")
    sent_per_night = []
    for day in WEEK:
        night = drill.night(day.isoformat())
        sent_per_night.append(
            sum(a["event"] == "new" and a["level"] != "message" for a in night["alert_log"])
        )
    for case in (1, 3, 5, 6):  # four stores send it right. S04's Tuesday file never comes
        drill.stores("fix", "--case", str(case))
    drill.stores("deliver", "--through", "2026-08-30")
    for day in (MON, THU, SAT, SUN):  # only the four nights that were wrong
        night = drill.night(day.isoformat())

    assert saved["night"] == SUN.isoformat()
    assert saved["alerts_sent_per_night"] == sent_per_night == [1, 2, 0, 2, 3, 1, 1]

    stores = {s["store_id"]: s for s in night["report"]["stores"]}
    assert saved["can_be_paid"] == [s for s in stores if stores[s]["pay"]]
    assert saved["can_be_paid"] == ["S01", "S02", "S03", "S05", "S06"]
    right = truth.week_net_sales(world, BONUS_WEEK_START)
    for store, s in stores.items():
        mine = saved["stores"][store]
        assert mine == {
            "pay": s["pay"], "net_sales_cents": s["net_sales_cents"],
            "bonus_cents": s["bonus_cents"], "why_not": s["why_not"],
        }, store  # fmt: skip
        if s["pay"]:  # every store that can be paid is paid exactly what its receipts say
            assert mine["net_sales_cents"] == right[store], store
            assert mine["bonus_cents"] == truth.bonus_cents(right[store]), store
    assert saved["stores"]["S04"]["why_not"] == [
        "Tue 08-25: no sales file arrived. The tills counted $587.42"
    ]

    with connect() as wh:
        h = health.health(wh, SUN)
    assert saved["health"] == {key: h[key] for key in saved["health"]}
