"""What the lab notebooks saved must equal what the production pipeline does."""

import json
from pathlib import Path

import nbformat
import pytest

NOTEBOOKS = Path(__file__).resolve().parent.parent / "notebooks"
RESULT = NOTEBOOKS / "results" / "answer_key_gates.json"


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
        assert "Who does what." in text, f"{path.name} must say which tool does what"


def test_no_notebook_output_holds_a_path_of_the_computer_it_ran_on():
    for path in sorted(NOTEBOOKS.glob("*.ipynb")):
        assert "/Users/" not in path.read_text(), path.name


def test_the_sunday_night_of_the_lab_equals_the_production_run(week):
    """Notebook 8 runs the week through Airflow and saves what it sees. The week of the
    tests runs the same code as a plain program. Both must end in the same place."""
    if not RESULT.exists():
        pytest.skip("Run notebook 8 first: it saves what the gates left on Sunday night.")
    saved = json.loads(RESULT.read_text())
    last = week["last"]
    stores = last["report"]["stores"]
    assert saved["night"] == "2026-08-30"
    assert saved["can_be_paid"] == [s["store_id"] for s in stores if s["pay"]] == ["S01"]
    assert saved["why_not"] == {s["store_id"]: s["why_not"] for s in stores if not s["pay"]}
    aside = last["set_aside"]
    assert saved["rows_set_aside"] == {
        "sales_lines": len({r["line_id"] for r in aside["sales_lines"]}),
        "raw_sales_lines": len(aside["raw_sales_lines"]),
        "supplier_prices": len(aside["supplier_prices"]),
        "stock_counts": len(aside["stock_counts"]),
        "members": len({r["member_id"] for r in aside["members"]}),
    }
    assert saved["files_refused"] == [r["file_name"] for r in last["refused"]]
