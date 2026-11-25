"""The same word must mean the same thing on the pages, in the README and in the glossary."""

from pathlib import Path

from lotus_pipeline import flow_data

MILESTONE = Path(__file__).resolve().parent.parent
GLOSSARY = (MILESTONE.parents[1] / "GLOSSARY.md").read_text()
README = (MILESTONE / "README.md").read_text()


def test_every_word_of_the_pages_is_in_the_glossary_with_the_same_meaning():
    for word, meaning in flow_data.WORDS.items():
        assert f"| **{word}** | {meaning} |" in GLOSSARY, word


def test_every_word_is_in_the_readme_with_the_same_meaning():
    for word, meaning in flow_data.WORDS.items():
        assert f"| **{word}** | {meaning.rstrip('.')} |" in README, word


def test_every_glossary_word_names_its_notebook_and_has_an_interview_line():
    section = GLOSSARY.split("## Milestone 3:")[1].split("\n## ")[0]
    rows = [line for line in section.splitlines() if line.startswith("| **")]
    assert len(rows) == len(flow_data.WORDS) == 9
    for row in rows:
        cells = [c.strip() for c in row.strip("|").split("|")]
        assert cells[2].startswith("Milestone 3, notebook ")
        assert cells[3].startswith('"') and cells[3].endswith('"')
