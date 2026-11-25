-- A product has one price on any day
--
-- Prepare first: Join the table to itself: every price row of a product against its later rows
-- The limit:     No two price rows of one product cover the same day
-- When it fails: BLOCK: the tasks after this gate do not run. The outputs keep their last good numbers.
{{ config(tags=['clean'], severity='error', meta={'number': 17, 'action': 'block', 'lens': 'data', 'question': 'Unique?', 'protects': 'Everyone who reads the two outputs: a sales line is joined to this table. Two prices on one day join one sales line twice'}) }}

SELECT a.product_id AS thing, b.valid_from AS day, a.product_name,
       a.valid_from AS first_from, a.valid_to AS first_to, b.valid_from AS second_from
FROM {{ source('clean', 'products') }} a
JOIN {{ source('clean', 'products') }} b
  ON b.product_id = a.product_id AND b.valid_from > a.valid_from
 AND b.valid_from <= coalesce(a.valid_to, DATE '9999-12-31')
