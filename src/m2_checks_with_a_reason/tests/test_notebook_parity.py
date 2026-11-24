"""What the lab notebooks saved must equal what the production checks find."""

import json
from pathlib import Path

import nbformat
import pytest

from lotus_pipeline.sales_checks import SALES_CHECKS

NOTEBOOKS = Path(__file__).resolve().parent.parent / "notebooks"
RESULT = NOTEBOOKS / "results" / "answer_key_checks.json"


def test_there_are_eight_notebooks_and_each_was_run_without_an_error():
    notebooks = sorted(NOTEBOOKS.glob("*.ipynb"))
    assert [p.name[:2] for p in notebooks] == [f"{n:02d}" for n in range(1, 9)]
    for path in notebooks:
        cells = nbformat.read(path, as_version=4).cells
        code = [c for c in cells if c.cell_type == "code"]
        assert all(c.execution_count for c in code), f"{path.name} has a cell that was not run"
        assert not [o for c in code for o in c.outputs if o.output_type == "error"], path.name
        text = "".join(c.source for c in cells)
        assert "What you now know" in text or path.name.startswith("08"), path.name
        assert "Say it in an interview" in text or path.name.startswith("08"), path.name


def test_every_check_written_by_hand_equals_the_production_check(week):
    if not RESULT.exists():
        pytest.skip("Run the notebooks first: they save what each hand-written check found.")
    saved = json.loads(RESULT.read_text())
    assert sorted(saved) == sorted(check.key for check in SALES_CHECKS)
    for key, yours in saved.items():
        finding = week["findings"][key]
        assert yours["bad_rows"] == len(finding.bad_rows), key
        assert [tuple(where) for where in yours["store_days"]] == finding.store_days(), key
