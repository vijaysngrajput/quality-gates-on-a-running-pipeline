"""What the lab notebooks saved must equal what the production checks find."""

import json
from pathlib import Path

import nbformat
import pytest

from lotus_pipeline.other_checks import OTHER_CHECKS

NOTEBOOKS = Path(__file__).resolve().parent.parent / "notebooks"
RESULT = NOTEBOOKS / "results" / "answer_key_checks.json"

# The checks the learner writes by hand in the lab, at least one for each of the five tables.
WRITTEN_BY_HAND = {
    "one_price_at_a_time", "no_gap_between_prices", "price_change_is_believable",
    "six_stores_once", "every_store_is_known", "stores_did_not_change",
    "a_count_for_every_day",
    "files_have_their_columns", "cost_is_filled", "cost_near_the_last_file",
    "cost_below_the_shelf_price",
    "one_person_once",
}  # fmt: skip


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


def test_the_hand_written_checks_are_checks_of_this_milestone():
    assert WRITTEN_BY_HAND <= {check.key for check in OTHER_CHECKS}


def test_every_check_written_by_hand_equals_the_production_check(week):
    if not RESULT.exists():
        pytest.skip("Run the notebooks first: they save what each hand-written check found.")
    saved = json.loads(RESULT.read_text())
    assert set(saved) == WRITTEN_BY_HAND
    for key, yours in saved.items():
        finding = week["findings"][key]
        assert yours["bad_rows"] == len(finding.bad_rows), key
        assert [tuple(place) for place in yours["where"]] == finding.store_days(), key
