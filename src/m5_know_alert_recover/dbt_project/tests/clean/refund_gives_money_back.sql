-- A refund gives money back
--
-- Prepare first: Read sales and refunds apart
-- The limit:     A refund has a quantity and an amount below zero. A sale has both above zero
-- When it fails: QUARANTINE: the rows this test returns are set aside with this reason. The good rows carry on.
{{ config(tags=['clean'], severity='warn', meta={'number': 6, 'action': 'quarantine', 'lens': 'business', 'question': 'Valid?', 'protects': 'Store managers and the head of retail operations: net sales, so pay', 'sets_aside': 'sales_lines'}) }}

SELECT line_id, store_id, sales_date, line_type, quantity, amount_cents
FROM {{ source('clean', 'sales_lines') }}
WHERE (line_type = 'refund' AND (amount_cents >= 0 OR quantity >= 0))
   OR (line_type = 'sale' AND (amount_cents <= 0 OR quantity <= 0))
