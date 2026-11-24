"""The number by hand: one store's net sales, added up straight from the file it sent.

This is how a person reads a pile of receipts: add every sale, take away every refund.
It uses no table and no SQL, so it can be held up against the pipeline's number.
"""

import csv
from datetime import date
from pathlib import Path

from lotus_pipeline import config
from lotus_pipeline.warehouse import Warehouse


def read_lines(store_id: str, day: date, landing: Path | None = None) -> list[dict]:
    """Every line of the file one store sent for one day."""
    path = (landing or config.LANDING) / "sales_lines" / f"sales_lines_{store_id}_{day}.csv"
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def net_sales_by_hand(lines: list[dict]) -> dict:
    """Add the sales, take away the refunds. A refund is money given back, whatever its sign."""
    sales = sum(int(x["amount_cents"]) for x in lines if x["line_type"] == "sale")
    refunds = sum(abs(int(x["amount_cents"])) for x in lines if x["line_type"] == "refund")
    return {
        "receipts": len({x["receipt_id"] for x in lines}),
        "lines": len(lines),
        "sales_cents": sales,
        "refunds_cents": refunds,
        "net_sales_cents": sales - refunds,
    }


def till_total(store_id: str, day: date, landing: Path | None = None) -> int:
    """What the store's tills counted themselves: a second count of the same money."""
    path = (landing or config.LANDING) / "till_totals" / f"till_totals_{store_id}_{day}.csv"
    with path.open(newline="") as f:
        return sum(int(row["net_total_cents"]) for row in csv.DictReader(f))


def compare(wh: Warehouse, store_id: str, day: date, landing: Path | None = None) -> dict:
    """Three counts of one store's day: the pipeline's, the one by hand, and the tills' own."""
    lines = read_lines(store_id, day, landing)
    by_hand = net_sales_by_hand(lines)["net_sales_cents"]
    pipeline = wh.value(
        "SELECT net_sales_cents FROM output.daily_store_sales WHERE store_id = ? AND sales_date = ?",
        (store_id, day),
    )
    pipeline = int(pipeline) if pipeline is not None else None
    return {
        "store_id": store_id,
        "day": day.isoformat(),
        "pipeline_cents": pipeline,
        "by_hand_cents": by_hand,
        "till_total_cents": till_total(store_id, day, landing),
        "difference_cents": None if pipeline is None else pipeline - by_hand,
        # The lines where the two ways of adding up disagree: a refund that is not negative.
        "suspects": [x for x in lines if x["line_type"] == "refund" and int(x["amount_cents"]) > 0],
    }
