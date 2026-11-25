-- A product on sale has a price on every day
--
-- Prepare first: For every price that ends, look for the price that starts the next day
-- The limit:     An active price that ends is followed, the next day, by another price
-- When it fails: BLOCK: the tasks after this gate do not run. The outputs keep their last good numbers.
{{ config(tags=['clean'], severity='error', meta={'number': 18, 'action': 'block', 'lens': 'data', 'question': 'Complete?', 'protects': 'Everyone who reads the two outputs: a sales line is joined to this table. A sale on a day with no price is dropped by the join'}) }}

SELECT a.product_id AS thing, a.valid_to + 1 AS day, a.product_name,
       a.valid_to AS price_ends
FROM {{ source('clean', 'products') }} a
LEFT JOIN {{ source('clean', 'products') }} b
  ON b.product_id = a.product_id AND b.valid_from = a.valid_to + 1
WHERE a.valid_to IS NOT NULL AND a.status = 'active' AND b.product_id IS NULL
