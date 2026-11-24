"""Step 4, publish: build the two tables people read.

output.daily_store_sales    one row per store per day. The bonus is paid from it
output.replenishment_list   one row per store per product: how much to order
"""

from datetime import date

from lotus_pipeline.warehouse import Warehouse

DAILY_STORE_SALES = """
CREATE TABLE output.daily_store_sales AS
SELECT sales_date,
       store_id,
       count(DISTINCT receipt_id) AS receipts,
       count(*)                   AS lines,
       sum(amount_cents)          AS net_sales_cents
FROM clean.sales_lines
GROUP BY sales_date, store_id
"""

CREATE_REPLENISHMENT_LIST = """
CREATE TABLE IF NOT EXISTS output.replenishment_list (
    for_date         DATE,
    store_id         TEXT,
    product_id       TEXT,
    product_name     TEXT,
    sold_yesterday   NUMERIC(12, 3),
    stock_counted    NUMERIC(12, 3),
    order_qty        NUMERIC(12, 3),
    unit_cost_cents  INTEGER,
    order_cost_cents BIGINT
)
"""

# Lotus's rule: keep two days of sales on the shelf.
#   order = 2 x what sold yesterday - what is on the shelf, and never below zero
# The day is given four times: for_date, the product's price dates, sales, stock and cost.
REPLENISHMENT_LIST = """
INSERT INTO output.replenishment_list
SELECT CAST(? AS DATE) + 1 AS for_date,
       s.store_id,
       p.product_id,
       p.product_name,
       coalesce(sold.quantity, 0)       AS sold_yesterday,
       coalesce(stock.counted_qty, 0)   AS stock_counted,
       greatest(2 * coalesce(sold.quantity, 0) - coalesce(stock.counted_qty, 0), 0) AS order_qty,
       cost.unit_cost_cents,
       CAST(round(
           greatest(2 * coalesce(sold.quantity, 0) - coalesce(stock.counted_qty, 0), 0)
           * cost.unit_cost_cents) AS BIGINT) AS order_cost_cents
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
"""


def publish(wh: Warehouse, day: date) -> dict[str, int]:
    """Rebuild daily_store_sales, and write the order list for the day after `day`."""
    wh.execute("DROP TABLE IF EXISTS output.daily_store_sales")
    wh.execute(DAILY_STORE_SALES)

    wh.execute(CREATE_REPLENISHMENT_LIST)
    wh.execute("DELETE FROM output.replenishment_list WHERE for_date = CAST(? AS DATE) + 1", (day,))
    wh.execute(REPLENISHMENT_LIST, (day, day, day, day, day))
    return {
        "daily_store_sales": wh.value("SELECT count(*) FROM output.daily_store_sales"),
        "replenishment_list": wh.value(
            "SELECT count(*) FROM output.replenishment_list WHERE for_date = CAST(? AS DATE) + 1",
            (day,),
        ),
    }
