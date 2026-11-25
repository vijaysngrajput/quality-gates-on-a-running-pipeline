"""Write, audit, publish: the outputs are built in a side room, checked, and only then shown.

    build     WRITE the outputs of tonight into the schema `staging`
    (gate)    AUDIT them: the publish gate runs its tests on the staging tables
    publish   PUBLISH: copy the staging tables over the tables people read, in one step

If the gate blocks, publish does not run. The tables people read keep their last good numbers.

    output.daily_store_sales    one row per store per day. The bonus is paid from it
    output.replenishment_list   one row per store per product: how much to order
    output.store_day_status     new: can this store's day be paid from, and if not, why
"""

from datetime import date

from lotus_pipeline.warehouse import Warehouse

DAILY_STORE_SALES = """
CREATE TABLE staging.daily_store_sales AS
SELECT sales_date,
       store_id,
       count(DISTINCT receipt_id) AS receipts,
       count(*)                   AS lines,
       sum(amount_cents)          AS net_sales_cents
FROM clean.sales_lines
GROUP BY sales_date, store_id
"""

# Lotus's rule: keep two days of sales on the shelf.
#   order = 2 x what sold yesterday - what is on the shelf, and never below zero
#
# Changed in milestone 4: a product with no count is NOT read as an empty shelf.
# Its stock and its order stay empty, and the note says why.
REPLENISHMENT_LIST = """
CREATE TABLE staging.replenishment_list AS
SELECT CAST(? AS DATE) + 1 AS for_date,
       s.store_id,
       p.product_id,
       p.product_name,
       coalesce(sold.quantity, 0)       AS sold_yesterday,
       stock.counted_qty                AS stock_counted,
       CASE WHEN stock.counted_qty IS NULL THEN NULL
            ELSE greatest(2 * coalesce(sold.quantity, 0) - stock.counted_qty, 0) END AS order_qty,
       cost.unit_cost_cents,
       CAST(round(
           CASE WHEN stock.counted_qty IS NULL THEN NULL
                ELSE greatest(2 * coalesce(sold.quantity, 0) - stock.counted_qty, 0) END
           * cost.unit_cost_cents) AS BIGINT) AS order_cost_cents,
       CASE WHEN stock.counted_qty IS NULL THEN 'no count: order by hand' END AS note
FROM clean.stores s
CROSS JOIN clean.products p
LEFT JOIN (
    SELECT store_id, product_id, sum(quantity) AS quantity
    FROM clean.sales_lines
    WHERE sales_date = CAST(? AS DATE)
    GROUP BY store_id, product_id
) sold
  ON sold.store_id = s.store_id AND sold.product_id = p.product_id
LEFT JOIN clean.stock_counts stock
  ON stock.store_id = s.store_id AND stock.product_id = p.product_id
 AND stock.count_date = CAST(? AS DATE)
LEFT JOIN (
    SELECT product_id, unit_cost_cents,
           row_number() OVER (PARTITION BY product_id ORDER BY price_date DESC) AS newest
    FROM clean.supplier_prices
    WHERE price_date <= CAST(? AS DATE)
) cost
  ON cost.product_id = p.product_id AND cost.newest = 1
WHERE p.status = 'active'
  AND CAST(? AS DATE) BETWEEN p.valid_from AND coalesce(p.valid_to, DATE '9999-12-31')
  AND NOT EXISTS (
      SELECT 1 FROM clean.calendar c
      WHERE c.store_id = s.store_id AND c.calendar_date = CAST(? AS DATE) AND c.kind = 'closed'
  )
"""


def build(wh: Warehouse, day: date) -> dict[str, int]:
    """WRITE: build tonight's outputs in the staging schema. Nobody reads them there."""
    wh.execute("CREATE SCHEMA IF NOT EXISTS staging")
    wh.execute("DROP TABLE IF EXISTS staging.daily_store_sales")
    wh.execute(DAILY_STORE_SALES)
    wh.execute("DROP TABLE IF EXISTS staging.replenishment_list")
    wh.execute(REPLENISHMENT_LIST, (day, day, day, day, day, day))
    return {
        "daily_store_sales": wh.value("SELECT count(*) FROM staging.daily_store_sales"),
        "replenishment_list": wh.value("SELECT count(*) FROM staging.replenishment_list"),
    }


def publish(wh: Warehouse, day: date) -> dict[str, int]:
    """PUBLISH: copy the audited staging tables over the tables people read.

    It all happens in one transaction: a reader sees the old tables or the new ones,
    never half of each.
    """
    wh.execute("DROP TABLE IF EXISTS output.daily_store_sales")
    wh.execute("CREATE TABLE output.daily_store_sales AS SELECT * FROM staging.daily_store_sales")

    wh.execute(
        "CREATE TABLE IF NOT EXISTS output.replenishment_list AS "
        "SELECT * FROM staging.replenishment_list WHERE 1 = 0"
    )
    wh.execute("DELETE FROM output.replenishment_list WHERE for_date = CAST(? AS DATE) + 1", (day,))
    wh.execute("INSERT INTO output.replenishment_list SELECT * FROM staging.replenishment_list")

    wh.execute("DROP TABLE IF EXISTS output.store_day_status")
    wh.execute("CREATE TABLE output.store_day_status AS SELECT * FROM prepared.store_day_status")
    return {
        "daily_store_sales": wh.value("SELECT count(*) FROM output.daily_store_sales"),
        "replenishment_list": wh.value(
            "SELECT count(*) FROM output.replenishment_list WHERE for_date = CAST(? AS DATE) + 1",
            (day,),
        ),
        "not_ready": wh.value("SELECT count(*) FROM output.store_day_status WHERE status <> 'ok'"),
    }
