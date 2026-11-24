"""Turn the check cards into a data file and a page.

    uv run python checks/build_page.py          write check_cards.json and check_cards.html
    uv run python checks/build_page.py --check  fail if either is out of date

A check card is one Markdown file per table: checks/<table>.md. It has eight fixed
headings, `## 1.` to `## 8.`, the eight questions of the method.

    checks/check_cards.json   every card as data. The D8Loop website reads it
    checks/check_cards.html   every card side by side
"""

import json
import re
import sys
from pathlib import Path

FOLDER = Path(__file__).resolve().parent
JSON_FILE = FOLDER / "check_cards.json"
PAGE = FOLDER / "check_cards.html"
MARKER = re.compile(
    r'(<script id="page-data" type="application/json">)(.*?)(</script>)', flags=re.DOTALL
)

# The eight questions of a check card, in order.
QUESTIONS = [
    "What does one row mean?",
    "Who uses this table, and what do they decide with it?",
    "How is the data made? So what can go wrong?",
    "Which of those would hurt most?",
    "What must be prepared before it can be checked?",
    "The check",
    "What happens when it fails?",
    "The proof",
]

# The seven tables, in the order the project meets them.
# `card_in` is the milestone that writes the table's card.
TABLES = [
    {
        "table": "sales_lines",
        "one_row": "One item line on one receipt",
        "made": "Each till writes it. Each store sends a file every night",
        "why": "Money. Refunds. Goods sold by weight. A store's file can be late, missing or sent twice",
        "card_in": 1,
    },
    {
        "table": "till_totals",
        "one_row": "One till's own total for one day",
        "made": "The till adds it up when it is closed",
        "why": "A second, independent count of the same money. It exists to be compared with sales_lines",
        "card_in": 3,
    },
    {
        "table": "products",
        "one_row": "One product, with its price and the dates that price is valid",
        "made": "Typed by the buying team",
        "why": "It changes. A price has a start and an end. A product can be retired",
        "card_in": 3,
    },
    {
        "table": "stores",
        "one_row": "One store, with its opening hours",
        "made": "Typed once, rarely changed",
        "why": "Tiny. Wrong here is wrong everywhere",
        "card_in": 3,
    },
    {
        "table": "stock_counts",
        "one_row": "One product in one store on one day",
        "made": "Counted by hand or by scanner",
        "why": "A count of one moment. A missing day is a gap, not a zero",
        "card_in": 3,
    },
    {
        "table": "supplier_prices",
        "one_row": "One product's cost from one supplier",
        "made": "A CSV file the supplier sends",
        "why": "Lotus does not control it. Columns move, units change, files repeat",
        "card_in": 3,
    },
    {
        "table": "members",
        "one_row": "One loyalty member",
        "made": "Typed by the customer in an app",
        "why": "People. The same person twice, half-filled rows, personal data",
        "card_in": 3,
    },
]

HEADING = re.compile(r"^## (\d)\. (.+)$")


def _cells(line: str) -> list[str]:
    return [cell.strip() for cell in line.strip().strip("|").split("|")]


def _blocks(lines: list[str]) -> list[dict]:
    """Markdown lines as blocks: a paragraph, or a table with a head and rows."""
    blocks: list[dict] = []
    paragraph: list[str] = []
    table: list[list[str]] = []

    def close() -> None:
        if paragraph:
            blocks.append({"type": "p", "text": " ".join(paragraph)})
            paragraph.clear()
        if table:
            blocks.append({"type": "table", "head": table[0], "rows": table[1:]})
            table.clear()

    for line in lines:
        text = line.strip()
        if text.startswith("|"):
            if paragraph:
                close()
            if not set(text) <= set("|-: "):  # skip the |---|---| line
                table.append(_cells(text))
        elif text:
            if table:
                close()
            paragraph.append(text)
        else:
            close()
    close()
    return blocks


def read_card(path: Path) -> dict:
    """One check card file as data."""
    intro: list[str] = []
    sections: dict[int, dict] = {}
    current = None
    for line in path.read_text(encoding="utf-8").splitlines():
        found = HEADING.match(line)
        if found:
            current = int(found.group(1))
            sections[current] = {"title": found.group(2).strip(), "lines": []}
        elif line.startswith("# "):
            continue
        elif current is None:
            intro.append(line)
        else:
            sections[current]["lines"].append(line)
    if sorted(sections) != list(range(1, 9)):
        raise ValueError(f"{path.name} must have the eight headings '## 1.' to '## 8.'")

    questions = []
    for number in range(1, 9):
        blocks = _blocks(sections[number]["lines"])
        first = blocks[0]["text"] if blocks and blocks[0]["type"] == "p" else ""
        questions.append(
            {
                "number": number,
                "question": QUESTIONS[number - 1],
                "title": sections[number]["title"],
                "answered": not first.startswith("Not yet"),
                "blocks": blocks,
            }
        )
    state = next((b["text"] for b in _blocks(intro) if b["text"].startswith("**State")), "")
    return {"state": state, "questions": questions}


def build_data() -> dict:
    cards = []
    for table in TABLES:
        path = FOLDER / f"{table['table']}.md"
        card = read_card(path) if path.exists() else None
        cards.append(
            {
                **table,
                "has_card": card is not None,
                "file": f"checks/{path.name}" if card else None,
                "state": card["state"] if card else "",
                "answered": sum(q["answered"] for q in card["questions"]) if card else 0,
                "questions": card["questions"] if card else [],
            }
        )
    unknown = {p.stem for p in FOLDER.glob("*.md")} - {t["table"] for t in TABLES}
    if unknown:
        raise ValueError(f"A check card for a table that does not exist: {sorted(unknown)}")
    return {"questions": QUESTIONS, "cards": cards}


def as_json(data: dict) -> str:
    return json.dumps(data, indent=1, ensure_ascii=False) + "\n"


def page_data() -> dict:
    return json.loads(MARKER.search(PAGE.read_text(encoding="utf-8")).group(2))


def main() -> int:
    data = build_data()
    if "--check" in sys.argv:
        ok = page_data() == data and JSON_FILE.read_text(encoding="utf-8") == as_json(data)
        print("The check card page is up to date." if ok else "The check card page is out of date.")
        return 0 if ok else 1
    JSON_FILE.write_text(as_json(data), encoding="utf-8")
    payload = json.dumps(data, indent=1, ensure_ascii=False).replace("</", "<\\/")
    text = PAGE.read_text(encoding="utf-8")
    PAGE.write_text(
        MARKER.sub(lambda m: m.group(1) + "\n" + payload + "\n" + m.group(3), text),
        encoding="utf-8",
    )
    written = sum(card["has_card"] for card in data["cards"])
    print(
        f"Wrote {written} of {len(data['cards'])} check cards into {JSON_FILE.name} and {PAGE.name}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
