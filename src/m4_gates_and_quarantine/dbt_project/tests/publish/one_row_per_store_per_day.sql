-- The table the bonus is paid from has one row per store per day
--
-- When it fails: BLOCK.
{{ config(tags=['publish'], severity='error', meta={'number': 39, 'action': 'block', 'lens': 'data', 'question': 'Unique?', 'protects': 'Store managers: a store there twice is paid twice'}) }}

SELECT store_id, sales_date, count(*) AS times
FROM {{ source('staging', 'daily_store_sales') }}
GROUP BY store_id, sales_date
HAVING count(*) > 1 OR sum(net_sales_cents) <= 0
