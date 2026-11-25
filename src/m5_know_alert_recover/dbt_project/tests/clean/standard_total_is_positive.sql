-- Standard check 5 of milestone 1: the total is positive
--
-- When it fails: BLOCK.
{{ config(tags=['clean'], severity='error', meta={'number': 105, 'action': 'block', 'lens': 'data', 'question': 'Valid?', 'protects': ''}) }}

SELECT sales_date, store_id, sum(amount_cents) AS net_sales_cents
FROM {{ source('clean', 'sales_lines') }}
GROUP BY sales_date, store_id
HAVING sum(amount_cents) <= 0
