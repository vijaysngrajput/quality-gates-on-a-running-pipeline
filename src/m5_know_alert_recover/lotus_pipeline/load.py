"""Step 1, load: copy the files the stores sent into the raw tables, as received.

Nothing is changed and nothing is judged. Every value stays text.
Three columns are added to every row, so we can always say where it came from:

    _file_name     the file the row was in
    _received_at   when that file arrived
    _loaded_at     when this pipeline copied it
"""

import csv
import hashlib
from datetime import date, datetime, time, timedelta
from pathlib import Path

from lotus_pipeline import config
from lotus_pipeline.warehouse import Warehouse

# table -> the columns the pipeline expects in its files
TABLES = {
    "sales_lines": [
        "line_id", "receipt_id", "line_no", "store_id", "till_id", "sold_at", "product_id",
        "quantity", "unit_price_cents", "amount_cents", "line_type", "refund_of_receipt_id",
        "member_id",
    ],
    "till_totals": [
        "business_date", "store_id", "till_id", "receipts", "gross_sales_cents",
        "refunds_cents", "net_total_cents", "closed_at",
    ],
    "products": [
        "product_id", "product_name", "category", "unit", "unit_price_cents", "valid_from",
        "valid_to", "status",
    ],
    "stores": [
        "store_id", "store_name", "town", "opens_at", "closes_at", "manager_name", "tills",
    ],
    "stock_counts": [
        "count_date", "store_id", "product_id", "counted_qty", "counted_by", "counted_at",
    ],
    "supplier_prices": ["supplier_id", "product_id", "unit_cost", "unit", "price_date"],
    "members": [
        "member_id", "full_name", "email", "phone", "home_store_id", "joined_on",
        "marketing_opt_in",
    ],
}  # fmt: skip

META = ["_file_name", "_received_at", "_loaded_at"]


def run_moment(day: date) -> datetime:
    """When the run for business day `day` happens: 02:00 the next morning."""
    return datetime.combine(day + timedelta(days=1), time(config.RUN_HOUR, 0))


def create_raw_tables(wh: Warehouse) -> None:
    for table, columns in TABLES.items():
        fields = ", ".join(f"{c} TEXT" for c in columns)
        wh.execute(
            f"CREATE TABLE IF NOT EXISTS raw.{table} "
            f"({fields}, _file_name TEXT, _received_at TIMESTAMP, _loaded_at TIMESTAMP)"
        )
    wh.execute(
        "CREATE TABLE IF NOT EXISTS raw.loaded_files "
        "(file_name TEXT, table_name TEXT, row_count INTEGER, "
        "received_at TIMESTAMP, loaded_at TIMESTAMP, checksum TEXT)"
    )
    # New in milestone 3: the columns each file really had, one row per column.
    # The raw tables cannot say this. A column the loader did not expect is not in them.
    wh.execute(
        "CREATE TABLE IF NOT EXISTS raw.file_columns "
        "(file_name TEXT, table_name TEXT, column_name TEXT, position_in_file INTEGER)"
    )


def arrivals(landing: Path) -> list[dict]:
    """Every file in the landing folder, with the moment it arrived."""
    path = landing / "_arrivals.csv"
    if not path.exists():
        return []
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["received_at"] = datetime.strptime(row["received_at"], "%Y-%m-%d %H:%M:%S")
    return rows


def read_header(path: Path) -> list[str]:
    """The column names in the first line of a CSV file, as the sender wrote them."""
    with path.open(newline="") as f:
        return next(csv.reader(f), [])


def read_file(path: Path, columns: list[str]) -> list[tuple]:
    """Read one CSV file by column NAME. An empty cell becomes NULL.

    A column the file does not have also becomes NULL. Nothing fails.
    """
    with path.open(newline="") as f:
        return [tuple(row.get(c) or None for c in columns) for row in csv.DictReader(f)]


def checksum(path: Path) -> str:
    """A short fingerprint of a file's bytes. The same bytes always give the same fingerprint."""
    return hashlib.sha1(path.read_bytes()).hexdigest()


def forget_file(wh: Warehouse, table: str, name: str) -> None:
    """Remove every trace of one loaded file, so it can be loaded again."""
    wh.execute(f"DELETE FROM raw.{table} WHERE _file_name = ?", (name,))
    wh.execute("DELETE FROM raw.loaded_files WHERE file_name = ?", (name,))
    wh.execute("DELETE FROM raw.file_columns WHERE file_name = ?", (name,))


def load(wh: Warehouse, day: date, landing: Path | None = None) -> dict:
    """Load every file that had arrived by this run and is new, or has changed.

    New in milestone 5: REPROCESSING. A store that sent a wrong file sends a corrected one
    with the same name. The load step keeps a fingerprint of every file it loaded. When the
    fingerprint of a file is different now, its old rows are removed and the file is loaded
    again. A file that did not change is skipped, so running a night twice is still safe.
    """
    landing = landing or config.LANDING
    create_raw_tables(wh)
    loaded_at = run_moment(day)
    already = {
        r["file_name"]: r["checksum"]
        for r in wh.query("SELECT file_name, checksum FROM raw.loaded_files")
    }

    files, rows_loaded, again = 0, 0, []
    for arrival in arrivals(landing):
        name, table = arrival["file_name"], arrival["table_name"]
        if arrival["received_at"] > loaded_at or table not in TABLES:
            continue
        path = landing / table / name
        fingerprint = checksum(path)
        if already.get(name) == fingerprint:
            continue  # the same file as last time: nothing to do
        if name in already:
            forget_file(wh, table, name)  # a corrected file: its old rows go first
            again.append(name)
        columns = TABLES[table]
        rows = read_file(path, columns)
        stamped = [row + (name, arrival["received_at"], loaded_at) for row in rows]
        wh.insert_rows(f"raw.{table}", columns + META, stamped)
        wh.execute(
            "INSERT INTO raw.loaded_files VALUES (?, ?, ?, ?, ?, ?)",
            (name, table, len(rows), arrival["received_at"], loaded_at, fingerprint),
        )
        header = read_header(path)
        wh.insert_rows(
            "raw.file_columns",
            ["file_name", "table_name", "column_name", "position_in_file"],
            [(name, table, column, n) for n, column in enumerate(header, start=1)],
        )
        files += 1
        rows_loaded += len(rows)
    return {"files": files, "rows": rows_loaded, "loaded_again": again}
