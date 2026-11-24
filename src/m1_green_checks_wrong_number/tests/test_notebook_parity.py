"""What the lab notebooks saved must equal what the production code and the answer key say."""

import json
from datetime import date
from pathlib import Path

import nbformat
import pytest

from lotus_pipeline import by_hand, pipeline
from lotus_pipeline.warehouse import connect

NOTEBOOKS = Path(__file__).resolve().parent.parent / "notebooks"
RESULT = NOTEBOOKS / "results" / "answer_key_by_hand.json"


def test_there_are_eight_notebooks_and_each_was_run_without_an_error():
    notebooks = sorted(NOTEBOOKS.glob("*.ipynb"))
    assert [p.name[:2] for p in notebooks] == [f"{n:02d}" for n in range(1, 9)]
    for path in notebooks:
        cells = nbformat.read(path, as_version=4).cells
        code = [c for c in cells if c.cell_type == "code"]
        assert not [o for c in code for o in c.outputs if o.output_type == "error"], path.name
        assert "What you now know" in "".join(c.source for c in cells) or path.name.startswith("08")


def test_the_notebooks_answer_equals_the_production_answer(own_warehouse, landing, key):
    if not RESULT.exists():
        pytest.skip("Run notebook 3 first: it saves the answer worked out by hand.")
    saved = json.loads(RESULT.read_text())
    day = date.fromisoformat(saved["day"])
    pipeline.run(day, say=lambda text: None)
    with connect() as wh:
        found = by_hand.compare(wh, saved["store_id"], day, landing)
    assert saved["by_hand_cents"] == found["by_hand_cents"] == 77720
    assert saved["by_hand_cents"] == key[(day, saved["store_id"])]["net_sales_cents"]
    assert saved["pipeline_cents"] == found["pipeline_cents"] == 79618
    assert saved["bad_line_id"] == found["suspects"][0]["line_id"]
