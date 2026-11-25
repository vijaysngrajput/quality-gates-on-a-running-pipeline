-- Every order on the list is worked out from a real count
--
-- When it fails: WARN. The list says "no count: order by hand" for that product,
--                where milestone 1 read the gap as an empty shelf.
{{ config(tags=['publish'], severity='warn', meta={'number': 41, 'action': 'warn', 'lens': 'operational', 'question': 'Complete?', 'protects': 'The replenishment team: a missing count is not an empty shelf'}) }}

SELECT store_id, for_date AS sales_date, product_id, note
FROM {{ source('staging', 'replenishment_list') }}
WHERE stock_counted IS NULL
