-- A product is counted once a day
--
-- Prepare first: Group by the key: day, store and product
-- The limit:     Each count_date + store_id + product_id is there exactly once
-- When it fails: WARN: the run continues, and the warning is recorded.
{{ config(tags=['clean'], severity='warn', meta={'number': 27, 'action': 'warn', 'lens': 'data', 'question': 'Unique?', 'protects': 'The replenishment team: they order from this number. Two counts for one shelf: which one is true?'}) }}

SELECT store_id, count_date AS sales_date, product_id, count(*) AS times
FROM {{ source('clean', 'stock_counts') }}
GROUP BY store_id, count_date, product_id
HAVING count(*) > 1
