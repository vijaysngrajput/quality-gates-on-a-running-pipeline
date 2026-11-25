"""Profiling: read a table before you check it.

Three plain questions, asked of any table:

    how many rows, and how many per store per day?
    what are the smallest and largest values in each column?
    how many cells are empty?

Each surprise in the answers is a candidate check.
"""

from datetime import date

from lotus_pipeline.warehouse import Warehouse, plain


def column_names(wh: Warehouse, table: str) -> list[str]:
    schema, name = table.split(".")
    rows = wh.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
        (schema, name),
    )
    if not rows:
        raise ValueError(f"There is no table {table}. Run the pipeline first.")
    return [r["column_name"] for r in rows]


def profile_columns(wh: Warehouse, table: str) -> list[dict]:
    """One row per column: empty cells, different values, smallest and largest value."""
    columns = column_names(wh, table)
    total = wh.value(f"SELECT count(*) FROM {table}")
    rows = []
    for column in columns:
        found = wh.query(
            f"SELECT count(*) - count({column}) AS empty_cells, "
            f"count(DISTINCT {column}) AS different_values, "
            f"min({column}) AS smallest, max({column}) AS largest FROM {table}"
        )[0]
        rows.append(
            {
                "column": column,
                "rows": int(total),
                "empty_cells": int(found["empty_cells"]),
                "empty_percent": round(100 * found["empty_cells"] / total, 1) if total else 0.0,
                "different_values": int(found["different_values"]),
                "smallest": plain(found["smallest"]),
                "largest": plain(found["largest"]),
            }
        )
    return rows


def rows_per_store_per_day(wh: Warehouse, first: date, last: date) -> list[dict]:
    """How many sales lines each store has on each day. A hole or a jump shows here first."""
    return wh.query(
        "SELECT sales_date, store_id, count(*) AS lines, count(DISTINCT receipt_id) AS receipts, "
        "sum(amount_cents) AS net_sales_cents "
        "FROM clean.sales_lines WHERE sales_date BETWEEN ? AND ? "
        "GROUP BY sales_date, store_id ORDER BY sales_date, store_id",
        (first, last),
    )


def amounts_by_line_type(wh: Warehouse) -> list[dict]:
    """The smallest and largest amount of a sale, and of a refund. Read them apart."""
    return wh.query(
        "SELECT line_type, count(*) AS lines, min(amount_cents) AS smallest_cents, "
        "max(amount_cents) AS largest_cents, min(quantity) AS smallest_quantity, "
        "max(quantity) AS largest_quantity "
        "FROM clean.sales_lines GROUP BY line_type ORDER BY line_type"
    )


def raw_against_clean(wh: Warehouse) -> dict:
    """Did the cleaning step lose rows? The joins can drop a line without an error."""
    raw = wh.value("SELECT count(*) FROM raw.sales_lines")
    clean = wh.value("SELECT count(*) FROM clean.sales_lines")
    return {"raw_rows": int(raw), "clean_rows": int(clean), "lost_rows": int(raw) - int(clean)}


def format_profile(table: str, rows: list[dict]) -> str:
    lines = [
        f"Profile of {table}: {rows[0]['rows']:,} rows",
        "",
        f"  {'column':<22}{'empty':>8}{'empty %':>9}{'different':>11}  smallest .. largest",
    ]
    for r in rows:
        lines.append(
            f"  {r['column']:<22}{r['empty_cells']:>8,}{r['empty_percent']:>8.1f}%"
            f"{r['different_values']:>11,}  {r['smallest']} .. {r['largest']}"
        )
    return "\n".join(lines)
