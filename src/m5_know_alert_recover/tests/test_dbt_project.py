"""The dbt project: every rule is a test, and every test says what a failure does."""

import re
from pathlib import Path

from lotus_pipeline import dbt_runner

PROJECT = Path(__file__).resolve().parent.parent / "dbt_project"
TESTS = sorted(PROJECT.glob("tests/*/*.sql"))
ACTIONS = {"block", "quarantine", "refuse_file", "warn"}


def config_of(path: Path) -> dict:
    text = path.read_text()
    return {
        "tags": re.search(r"tags=\['(\w+)'\]", text).group(1),
        "severity": re.search(r"severity='(\w+)'", text).group(1),
        "action": re.search(r"'action': '(\w+)'", text).group(1),
        "lens": re.search(r"'lens': '(\w+)'", text).group(1),
        "text": text,
    }


def test_there_is_a_test_at_each_of_the_three_gates():
    per_gate = {
        gate: len(list(PROJECT.glob(f"tests/{gate}/*.sql")))
        for gate in ("landing", "clean", "publish")
    }
    assert per_gate == {"landing": 3, "clean": 36, "publish": 5}


def test_every_test_says_its_rule_and_what_a_failure_does():
    for path in TESTS:
        found = config_of(path)
        assert found["text"].startswith("-- "), path.name  # the rule, in words, on the first line
        assert "When it fails:" in found["text"], path.name
        assert found["tags"] == path.parent.name, path.name  # the folder is the gate
        assert found["action"] in ACTIONS and found["lens"] in ("data", "business", "operational")


def test_only_a_block_is_an_error_for_dbt():
    """dbt's severity `error` stops the gate. Everything else is `warn`, and the pipeline acts."""
    for path in TESTS:
        found = config_of(path)
        assert (found["severity"] == "error") == (found["action"] == "block"), path.name


def test_a_test_that_sets_rows_aside_names_the_table():
    for path in TESTS:
        found = config_of(path)
        assert ("'sets_aside'" in found["text"]) == (found["action"] == "quarantine"), path.name


def test_no_test_on_members_selects_personal_data():
    for path in TESTS:
        text = path.read_text()
        if "'members')" not in text:
            continue
        chosen = " ".join(re.findall(r"^\s*SELECT (.*?)$", text, re.M))
        for column in ("full_name", "phone"):
            assert column not in chosen, path.name
        assert not re.search(r"\bemail\b(?! has)", chosen.replace("lower(trim(email))", "")), (
            path.name
        )


def test_the_unit_tests_pass_on_their_own_rows(lab):
    """A unit test brings its own few rows, and the answer. It does not read tonight's data.
    The tables must exist, because dbt looks up the type of each column."""
    done = dbt_runner.invoke(["test", "--select", "test_type:unit"])
    assert done.success
    assert sorted(r.node.name for r in done.result) == [
        "a_till_with_no_lines_counts_as_zero", "the_five_answers_of_store_day_status",
    ]  # fmt: skip
