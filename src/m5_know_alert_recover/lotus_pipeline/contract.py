"""Hold a supplier's file against the data contract, before it is loaded.

A check on a table can only say that something is wrong after the load. A contract says
what a file must look like at the door: its columns, the type and rule of each value,
and what kind of change is allowed.

    uv run python -m lotus_pipeline contract                  every supplier file that arrived
    uv run python -m lotus_pipeline contract --file <path>    one file

Three kinds of problem:
    breaking   an agreed column is missing. Readers of the old shape fail
    additive   a column nobody agreed. Old readers still work. Reported, not refused
    value      a value breaks the rule of its column
"""

import csv
import re
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

import yaml

from lotus_pipeline import config

CONTRACTS = Path(__file__).resolve().parent.parent / "contracts"


def load_contract(name: str = "supplier_prices") -> dict:
    return yaml.safe_load((CONTRACTS / f"{name}.yml").read_text())


def _matches_shape(value: str, shape: str) -> bool:
    """A shape like P-000: a 0 stands for any digit, every other character for itself."""
    if len(value) != len(shape):
        return False
    return all(v.isdigit() if s == "0" else v == s for v, s in zip(value, shape))


def _value_problem(column: dict, value: str, file_date: str | None) -> str | None:
    """Why one value breaks the rule of its column, or None when it is fine."""
    if value is None or value == "":
        return "the cell is empty"
    if "allowed" in column and value not in column["allowed"]:
        return f"not one of {', '.join(column['allowed'])}"
    if "shape" in column and not _matches_shape(value, column["shape"]):
        return f"not shaped like {column['shape']}"
    if column["type"] == "decimal":
        try:
            number = Decimal(value)
        except InvalidOperation:
            return "not a number"
        decimals = -number.as_tuple().exponent
        if "decimals" in column and decimals != column["decimals"]:
            return f"{decimals} decimals, not {column['decimals']}: dollars are written like 2.10"
        if "min" in column and number < Decimal(str(column["min"])):
            return f"below the smallest allowed value, {column['min']}"
        if "max" in column and number > Decimal(str(column["max"])):
            return f"above the largest allowed value, {column['max']}"
    if column["type"] == "date":
        try:
            date.fromisoformat(value)
        except ValueError:
            return "not a date like 2026-08-25"
        if file_date and value != file_date:
            return f"not the date in the file name, {file_date}"
    return None


def check_file(path: Path, contract: dict | None = None) -> list[dict]:
    """Every way one file breaks the contract. An empty list means the file keeps it."""
    contract = contract or load_contract()
    agreed = {column["name"]: column for column in contract["columns"]}
    named = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
    file_date = named.group(1) if named else None
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        found = reader.fieldnames or []
        rows = list(reader)

    problems = []
    for name in agreed:
        if name not in found:
            problems.append({"kind": "breaking", "column": name, "row": None, "value": None,
                             "problem": "an agreed column is missing"})  # fmt: skip
    for name in found:
        if name not in agreed:
            problems.append({"kind": "additive", "column": name, "row": None, "value": None,
                             "problem": "a column that is not in the contract"})  # fmt: skip
    for number, row in enumerate(rows, start=2):  # line 1 of the file is the header
        for name, column in agreed.items():
            if name not in found:
                continue
            why = _value_problem(column, row[name], file_date)
            if why:
                problems.append({"kind": "value", "column": name, "row": number,
                                 "value": row[name], "problem": why})  # fmt: skip
    return problems


def verdict(problems: list[dict]) -> str:
    """What to do with the file: refuse it, or accept it, with or without a note."""
    kinds = {p["kind"] for p in problems}
    if kinds & {"breaking", "value"}:
        return "refuse"
    return "accept, and report" if kinds else "accept"


def check_landing(landing: Path | None = None) -> list[dict]:
    """Every supplier file in the landing folder, held against the contract."""
    folder = (landing or config.LANDING) / "supplier_prices"
    results = []
    for path in sorted(folder.glob("*.csv")) if folder.exists() else []:
        problems = check_file(path)
        results.append({"file": path.name, "verdict": verdict(problems), "problems": problems})
    return results


def format_results(results: list[dict]) -> str:
    contract = load_contract()
    lines = [f"The contract {contract['name']}, version {contract['version']}", ""]
    for result in results:
        lines.append(f"  {result['file']:<42} {result['verdict']}")
        shown = {}
        for p in result["problems"]:
            shown.setdefault((p["kind"], p["column"], p["problem"]), []).append(p)
        for (kind, column, problem), same in shown.items():
            example = (
                f", like {same[0]['value']} on line {same[0]['row']}" if same[0]["row"] else ""
            )
            count = f" in {len(same)} rows" if same[0]["row"] else ""
            lines.append(f"      {kind:<9} {column}: {problem}{count}{example}")
    refused = sum(r["verdict"] == "refuse" for r in results)
    lines += ["", f"  {len(results)} files. {refused} would be refused at the door."]
    return "\n".join(lines)
