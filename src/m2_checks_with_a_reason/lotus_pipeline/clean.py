"""Step 2, clean: turn the raw text into typed tables, and join what belongs together.

Every clean table is built again from the raw tables on every run.
"""

import csv
from pathlib import Path

from lotus_pipeline.warehouse import Warehouse

CALENDAR_FILE = Path(__file__).parent / "reference" / "calendar.csv"

# The three tables that arrive whole every night: keep the newest file.
STORES = """
CREATE TABLE clean.stores AS
SELECT store_id,
       store_name,
       town,
       CAST(opens_at AS TIME)   AS opens_at,
       CAST(closes_at AS TIME)  AS closes_at,
       manager_name,
       CAST(tills AS INTEGER)   AS tills
FROM raw.stores
WHERE _file_name = (SELECT max(_file_name) FROM raw.stores)
"""

PRODUCTS = """
CREATE TABLE clean.products AS
SELECT product_id,
       product_name,
       category,
       unit,
       CAST(unit_price_cents AS INTEGER) AS unit_price_cents,
       CAST(valid_from AS DATE)          AS valid_from,
       CAST(valid_to AS DATE)            AS valid_to,
       status
FROM raw.products
WHERE _file_name = (SELECT max(_file_name) FROM raw.products)
"""

MEMBERS = """
CREATE TABLE clean.members AS
SELECT member_id,
       full_name,
       email,
       phone,
       home_store_id,
       CAST(joined_on AS DATE)           AS joined_on,
       CAST(marketing_opt_in AS BOOLEAN) AS marketing_opt_in
FROM raw.members
WHERE _file_name = (SELECT max(_file_name) FROM raw.members)
"""

TILL_TOTALS = """
CREATE TABLE clean.till_totals AS
SELECT CAST(business_date AS DATE)         AS business_date,
       store_id,
       till_id,
       CAST(receipts AS INTEGER)           AS receipts,
       CAST(gross_sales_cents AS BIGINT)   AS gross_sales_cents,
       CAST(refunds_cents AS BIGINT)       AS refunds_cents,
       CAST(net_total_cents AS BIGINT)     AS net_total_cents,
       CAST(closed_at AS TIMESTAMP)        AS closed_at
FROM raw.till_totals
"""

STOCK_COUNTS = """
CREATE TABLE clean.stock_counts AS
SELECT CAST(count_date AS DATE)            AS count_date,
       store_id,
       product_id,
       CAST(counted_qty AS NUMERIC(12, 3)) AS counted_qty,
       counted_by,
       CAST(counted_at AS TIMESTAMP)       AS counted_at
FROM raw.stock_counts
"""

SUPPLIER_PRICES = """
CREATE TABLE clean.supplier_prices AS
SELECT supplier_id,
       product_id,
       CAST(round(CAST(unit_cost AS NUMERIC(12, 2)) * 100) AS INTEGER) AS unit_cost_cents,
       unit,
       CAST(price_date AS DATE) AS price_date
FROM raw.supplier_prices
"""

# One row per item line. The day of a sale is the date on the till's clock.
# The joins add the product's name and the store's name.
SALES_LINES = """
CREATE TABLE clean.sales_lines AS
SELECT l.line_id,
       l.receipt_id,
       CAST(l.line_no AS INTEGER)            AS line_no,
       l.store_id,
       s.store_name,
       l.till_id,
       CAST(l.sold_at AS TIMESTAMP)          AS sold_at,
       CAST(CAST(l.sold_at AS TIMESTAMP) AS DATE) AS sales_date,
       l.product_id,
       p.product_name,
       p.category,
       p.unit,
       CAST(l.quantity AS NUMERIC(12, 3))    AS quantity,
       CAST(l.unit_price_cents AS INTEGER)   AS unit_price_cents,
       CAST(l.amount_cents AS BIGINT)        AS amount_cents,
       l.line_type,
       l.refund_of_receipt_id,
       l.member_id
FROM raw.sales_lines l
JOIN clean.stores s
  ON s.store_id = l.store_id
JOIN clean.products p
  ON p.product_id = l.product_id
 AND CAST(CAST(l.sold_at AS TIMESTAMP) AS DATE)
     BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
"""

# The order matters: sales_lines joins to stores and products.
STEPS = {
    "stores": STORES,
    "products": PRODUCTS,
    "members": MEMBERS,
    "till_totals": TILL_TOTALS,
    "stock_counts": STOCK_COUNTS,
    "supplier_prices": SUPPLIER_PRICES,
    "sales_lines": SALES_LINES,
}


def build_calendar(wh: Warehouse) -> int:
    """New in milestone 2: the days that are not normal, typed by head office.

    reference/calendar.csv is REFERENCE DATA: a small list that people keep by hand,
    and that other data is judged against. A row for ALL stores becomes one row per store.
    """
    wh.execute("DROP TABLE IF EXISTS clean.calendar")
    wh.execute(
        "CREATE TABLE clean.calendar (calendar_date DATE, store_id TEXT, kind TEXT, "
        "opens_at TIME, closes_at TIME, note TEXT)"
    )
    stores = [r["store_id"] for r in wh.query("SELECT store_id FROM clean.stores ORDER BY 1")]
    rows = []
    with CALENDAR_FILE.open(newline="") as f:
        for row in csv.DictReader(f):
            for store_id in stores if row["store_id"] == "ALL" else [row["store_id"]]:
                rows.append(
                    (row["calendar_date"], store_id, row["kind"], row["opens_at"] or None,
                     row["closes_at"] or None, row["note"])
                )  # fmt: skip
    for row in rows:
        wh.execute(
            "INSERT INTO clean.calendar VALUES (CAST(? AS DATE), ?, ?, CAST(? AS TIME), "
            "CAST(? AS TIME), ?)",
            row,
        )
    return len(rows)


def clean(wh: Warehouse) -> dict[str, int]:
    """Rebuild every clean table. Returns the row count of each."""
    counts = {}
    for table, sql in STEPS.items():
        wh.execute(f"DROP TABLE IF EXISTS clean.{table}")
        wh.execute(sql)
        counts[table] = wh.value(f"SELECT count(*) FROM clean.{table}")
    build_calendar(wh)
    return counts
