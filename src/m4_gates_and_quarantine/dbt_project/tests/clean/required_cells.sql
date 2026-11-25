-- A cell that must be filled is filled
--
-- Prepare first: Decide, column by column, in which rows an empty cell is wrong
-- The limit:     No empty amount, quantity, price, time, product or receipt. A refund names its receipt, and a sale does not
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 1, 'action': 'quarantine', 'lens': 'data', 'question': 'Complete?', 'protects': 'Store managers and the head of retail operations: net sales, so pay', 'sets_aside': 'sales_lines'}) }}

SELECT line_id, store_id, sales_date, 'a cell that must be filled is empty' AS problem
FROM {{ source('clean', 'sales_lines') }}
WHERE receipt_id IS NULL OR line_no IS NULL OR sold_at IS NULL OR product_id IS NULL
   OR quantity IS NULL OR unit_price_cents IS NULL OR amount_cents IS NULL
UNION ALL
SELECT line_id, store_id, sales_date, 'a refund that names no receipt' AS problem
FROM {{ source('clean', 'sales_lines') }}
WHERE line_type = 'refund' AND refund_of_receipt_id IS NULL
UNION ALL
SELECT line_id, store_id, sales_date, 'a sale that names a refunded receipt' AS problem
FROM {{ source('clean', 'sales_lines') }}
WHERE line_type = 'sale' AND refund_of_receipt_id IS NOT NULL
