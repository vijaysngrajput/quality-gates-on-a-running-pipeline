-- A weight is a believable weight
--
-- Prepare first: Join each line to its product for the unit: pieces or kilograms
-- The limit:     Goods sold by weight: not more than 20 kg on one line. Goods sold by the piece: a whole number
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 10, 'action': 'quarantine', 'lens': 'business', 'question': 'Valid?', 'protects': 'Store managers and the head of retail operations: net sales, so pay', 'sets_aside': 'sales_lines'}) }}

SELECT line_id, store_id, sales_date, product_name, unit, quantity, amount_cents
FROM {{ source('clean', 'sales_lines') }}
WHERE (unit = 'kg' AND abs(quantity) > 20)
   OR (unit = 'each' AND quantity <> round(quantity))
